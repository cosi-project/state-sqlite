// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//nolint:dupword
package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/cosi-project/state-sqlite/pkg/sqlitexx"
)

// Create a resource.
//
// If a resource already exists, Create returns an error.
func (st *State) Create(ctx context.Context, res resource.Resource, opts ...state.CreateOption) error {
	var options state.CreateOptions

	for _, opt := range opts {
		opt(&options)
	}

	resCopy := res.DeepCopy()

	if err := resCopy.Metadata().SetOwner(options.Owner); err != nil {
		return fmt.Errorf("failed to set owner on create %q: %w", resCopy.Metadata(), err)
	}

	resCopy.Metadata().SetCreated(time.Now())
	resCopy.Metadata().SetVersion(resCopy.Metadata().Version().Next())

	var labels []byte

	if !resCopy.Metadata().Labels().Empty() {
		var err error

		labels, err = json.Marshal(resCopy.Metadata().Labels().Raw())
		if err != nil {
			return fmt.Errorf("failed to marshal labels: %w", err)
		}
	}

	var finalizers []byte

	if !resCopy.Metadata().Finalizers().Empty() {
		var err error

		finalizers, err = json.Marshal(resCopy.Metadata().Finalizers())
		if err != nil {
			return fmt.Errorf("failed to marshal finalizers: %w", err)
		}
	}

	m, err := st.marshaler.MarshalResource(resCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal resource: %w", err)
	}

	conn, err := st.db.Take(ctx)
	if err != nil {
		return fmt.Errorf("error taking connection for create: %w", err)
	}

	defer st.db.Put(conn)

	q, err := sqlitexx.NewQuery(
		conn,
		`INSERT INTO `+st.options.TablePrefix+`resources 
		(
			namespace, 
			type, 
			id, 
			version, 
			created_at, 
			updated_at, 
			labels, 
			finalizers,
			phase, 
			owner, 
			spec
		) 
		VALUES 
		($namespace, $type, $id, $version, $created_at, $updated_at, jsonb($labels), jsonb($finalizers), $phase, $owner, $spec)`,
	)
	if err != nil {
		return fmt.Errorf("preparing insert statement: %w", err)
	}

	err = q.
		BindString("$namespace", resCopy.Metadata().Namespace()).
		BindString("$type", resCopy.Metadata().Type()).
		BindString("$id", resCopy.Metadata().ID()).
		BindUint64("$version", resCopy.Metadata().Version().Value()).
		BindInt64("$created_at", resCopy.Metadata().Created().Unix()).
		BindInt64("$updated_at", resCopy.Metadata().Updated().Unix()).
		BindBytes("$labels", labels).
		BindBytes("$finalizers", finalizers).
		BindInt("$phase", int(resCopy.Metadata().Phase())).
		BindString("$owner", resCopy.Metadata().Owner()).
		BindBytes("$spec", m).
		Exec()
	if err != nil {
		if isUniqueViolationError(err) {
			return ErrAlreadyExists(res.Metadata())
		}

		return fmt.Errorf("inserting resource into database: %w", err)
	}

	st.sub.Notify(resCopy.Metadata())

	// This should be safe, because we don't allow to share metadata between goroutines even for read-only
	// purposes.
	*res.Metadata() = *resCopy.Metadata()

	return nil
}

// Update a resource.
//
// If a resource doesn't exist, error is returned.
// On update current version of resource `new` in the state should match
// the version on the backend, otherwise conflict error is returned.
//
//nolint:gocognit
func (st *State) Update(ctx context.Context, newResource resource.Resource, opts ...state.UpdateOption) error {
	options := state.DefaultUpdateOptions()

	for _, opt := range opts {
		opt(&options)
	}

	resCopy := newResource.DeepCopy()

	conn, err := st.db.Take(ctx)
	if err != nil {
		return fmt.Errorf("error taking connection for update: %w", err)
	}

	defer st.db.Put(conn)

	err = func() (err error) {
		doneFn, transErr := sqlitex.ImmediateTransaction(conn)
		if transErr != nil {
			return fmt.Errorf("starting transaction for update: %w", transErr)
		}
		defer doneFn(&err)

		var (
			currentOwner string
			currentVer   uint64
			createdAt    int64
			currentPhase int
		)

		q, err := sqlitexx.NewQuery(
			conn,
			`SELECT owner, version, created_at, phase 
	 		FROM `+st.options.TablePrefix+`resources
			WHERE namespace = $namespace AND type = $type AND id = $id`,
		)
		if err != nil {
			return fmt.Errorf("preparing query for current resource state: %w", err)
		}

		if err = q.
			BindString("$namespace", newResource.Metadata().Namespace()).
			BindString("$type", newResource.Metadata().Type()).
			BindString("$id", newResource.Metadata().ID()).
			QueryRow(func(stmt *sqlite.Stmt) error {
				currentOwner = stmt.GetText("owner")
				currentVer = uint64(stmt.GetInt64("version"))
				createdAt = stmt.GetInt64("created_at")
				currentPhase = int(stmt.GetInt64("phase"))

				return nil
			}); err != nil {
			if errors.Is(err, sqlitexx.ErrNoRows) {
				return fmt.Errorf("failed to update: %w", ErrNotFound(newResource.Metadata()))
			}

			return fmt.Errorf("error querying current resource state: %w", err)
		}

		if currentVer != newResource.Metadata().Version().Value() {
			return fmt.Errorf("failed to update: %w", ErrVersionConflict(newResource.Metadata(), newResource.Metadata().Version().Value(), currentVer))
		}

		if currentOwner != options.Owner {
			return fmt.Errorf("failed to update: %w", ErrOwnerConflict(newResource.Metadata(), currentOwner))
		}

		if options.ExpectedPhase != nil && currentPhase != int(*options.ExpectedPhase) {
			return fmt.Errorf("failed to update: %w", ErrPhaseConflict(newResource.Metadata(), *options.ExpectedPhase))
		}

		updated := time.Now()

		resCopy.Metadata().SetUpdated(updated)
		resCopy.Metadata().SetCreated(time.Unix(createdAt, 0))
		resCopy.Metadata().SetVersion(resCopy.Metadata().Version().Next())

		m, err := st.marshaler.MarshalResource(resCopy)
		if err != nil {
			return fmt.Errorf("failed to marshal resource: %w", err)
		}

		var labels []byte

		if !resCopy.Metadata().Labels().Empty() {
			labels, err = json.Marshal(resCopy.Metadata().Labels().Raw())
			if err != nil {
				return fmt.Errorf("failed to marshal labels: %w", err)
			}
		} else {
			labels = []byte("null")
		}

		var finalizers []byte

		if !resCopy.Metadata().Finalizers().Empty() {
			finalizers, err = json.Marshal(resCopy.Metadata().Finalizers())
			if err != nil {
				return fmt.Errorf("failed to marshal finalizers: %w", err)
			}
		}

		q, err = sqlitexx.NewQuery(
			conn,
			`UPDATE `+st.options.TablePrefix+`resources
				SET 
					version = $version, 
					updated_at = $updated_at,
					labels = jsonb($labels),
					finalizers = jsonb($finalizers),
					phase = $phase, 
					owner = $owner, 
					spec = $spec
				WHERE
					namespace = $namespace AND type = $type AND id = $id AND version = $version_old`,
		)
		if err != nil {
			return fmt.Errorf("preparing update statement: %w", err)
		}

		if err = q.
			BindUint64("$version", resCopy.Metadata().Version().Value()).
			BindInt64("$updated_at", resCopy.Metadata().Updated().Unix()).
			BindBytes("$labels", labels).
			BindBytes("$finalizers", finalizers).
			BindInt("$phase", int(resCopy.Metadata().Phase())).
			BindString("$owner", resCopy.Metadata().Owner()).
			BindBytes("$spec", m).
			BindString("$namespace", resCopy.Metadata().Namespace()).
			BindString("$type", resCopy.Metadata().Type()).
			BindString("$id", resCopy.Metadata().ID()).
			BindUint64("$version_old", currentVer).
			Exec(); err != nil {
			return fmt.Errorf("error updating resource in database: %w", err)
		}

		if conn.Changes() != 1 {
			return fmt.Errorf("failed to update: %w", ErrVersionConflict(newResource.Metadata(), newResource.Metadata().Version().Value(), currentVer))
		}

		return nil
	}()
	if err != nil {
		return err
	}

	st.sub.Notify(resCopy.Metadata())

	// This should be safe, because we don't allow to share metadata between goroutines even for read-only
	// purposes.
	*newResource.Metadata() = *resCopy.Metadata()

	return nil
}

// Destroy a resource.
//
// If a resource doesn't exist, error is returned.
// If a resource has pending finalizers, error is returned.
func (st *State) Destroy(ctx context.Context, ptr resource.Pointer, opts ...state.DestroyOption) error {
	var options state.DestroyOptions

	for _, opt := range opts {
		opt(&options)
	}

	err := func() (err error) {
		var conn *sqlite.Conn

		conn, err = st.db.Take(ctx)
		if err != nil {
			return fmt.Errorf("error taking connection for destroy: %w", err)
		}

		defer st.db.Put(conn)

		doneFn, transErr := sqlitex.ImmediateTransaction(conn)
		if transErr != nil {
			return fmt.Errorf("starting transaction for destroy: %w", transErr)
		}
		defer doneFn(&err)

		var (
			currentOwner      string
			currentVer        uint64
			currentFinalizers []byte
		)

		q, err := sqlitexx.NewQuery(
			conn,
			`SELECT owner, json(finalizers) AS finalizers, version
	 		FROM `+st.options.TablePrefix+`resources
			WHERE namespace = $namespace AND type = $type AND id = $id`,
		)
		if err != nil {
			return fmt.Errorf("preparing query for current resource state: %w", err)
		}

		err = q.
			BindString("$namespace", ptr.Namespace()).
			BindString("$type", ptr.Type()).
			BindString("$id", ptr.ID()).
			QueryRow(
				func(stmt *sqlite.Stmt) error {
					currentOwner = stmt.GetText("owner")

					currentFinalizers = make([]byte, stmt.GetLen("finalizers"))
					stmt.GetBytes("finalizers", currentFinalizers)
					currentVer = uint64(stmt.GetInt64("version"))

					return nil
				},
			)
		if err != nil {
			if errors.Is(err, sqlitexx.ErrNoRows) {
				return fmt.Errorf("failed to delete: %w", ErrNotFound(ptr))
			}

			return fmt.Errorf("error querying current resource state: %w", err)
		}

		if currentOwner != options.Owner {
			return fmt.Errorf("failed to destroy: %w", ErrOwnerConflict(ptr, currentOwner))
		}

		if len(currentFinalizers) != 0 {
			var fins resource.Finalizers

			// attempt to unmarshal finalizers, but ignore errors, as it's only for message
			json.Unmarshal(currentFinalizers, &fins) //nolint:errcheck

			return fmt.Errorf("failed to destroy: %w", ErrPendingFinalizers(ptr, fins))
		}

		q, err = sqlitexx.NewQuery(
			conn,
			`DELETE FROM `+st.options.TablePrefix+`resources
				  WHERE
		 			namespace = $namespace AND type = $type AND id = $id AND version = $version`,
		)
		if err != nil {
			return fmt.Errorf("preparing delete statement: %w", err)
		}

		err = q.
			BindString("$namespace", ptr.Namespace()).
			BindString("$type", ptr.Type()).
			BindString("$id", ptr.ID()).
			BindUint64("$version", currentVer).
			Exec()
		if err != nil {
			return fmt.Errorf("error deleting resource from database: %w", err)
		}

		if conn.Changes() != 1 {
			return fmt.Errorf("failed to delete: %w", ErrVersionConflict(ptr, currentVer, currentVer))
		}

		return nil
	}()
	if err != nil {
		return err
	}

	st.sub.Notify(ptr)

	return nil
}
