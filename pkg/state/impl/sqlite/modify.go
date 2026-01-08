// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//nolint:dupword
package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
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
	} else {
		labels = []byte("null")
	}

	var finalizers []byte

	if !resCopy.Metadata().Finalizers().Empty() {
		var err error

		finalizers, err = json.Marshal(resCopy.Metadata().Finalizers())
		if err != nil {
			return fmt.Errorf("failed to marshal finalizers: %w", err)
		}
	} else {
		finalizers = []byte("null")
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

	err = sqlitex.Execute(conn, `INSERT INTO `+st.options.TablePrefix+`resources 
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
		&sqlitex.ExecOptions{
			Named: map[string]any{
				"$namespace":  resCopy.Metadata().Namespace(),
				"$type":       resCopy.Metadata().Type(),
				"$id":         resCopy.Metadata().ID(),
				"$version":    resCopy.Metadata().Version().Value(),
				"$created_at": resCopy.Metadata().Created().Unix(),
				"$updated_at": resCopy.Metadata().Updated().Unix(),
				"$labels":     labels,
				"$finalizers": string(finalizers),
				"$phase":      int(resCopy.Metadata().Phase()),
				"$owner":      resCopy.Metadata().Owner(),
				"$spec":       m,
			},
		},
	)
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
			found        bool
		)

		if err = sqlitex.Execute(conn, `SELECT owner, version, created_at, phase 
	 		FROM `+st.options.TablePrefix+`resources
			WHERE namespace = $namespace AND type = $type AND id = $id`,
			&sqlitex.ExecOptions{
				Named: map[string]any{
					"$namespace": newResource.Metadata().Namespace(),
					"$type":      newResource.Metadata().Type(),
					"$id":        newResource.Metadata().ID(),
				},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					currentOwner = stmt.GetText("owner")
					currentVer = uint64(stmt.GetInt64("version"))
					createdAt = stmt.GetInt64("created_at")
					currentPhase = int(stmt.GetInt64("phase"))
					found = true

					return nil
				},
			}); err != nil {
			return fmt.Errorf("error querying current resource state: %w", err)
		}

		if !found {
			return fmt.Errorf("failed to update: %w", ErrNotFound(resCopy.Metadata()))
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
		} else {
			finalizers = []byte("null")
		}

		if err = sqlitex.Execute(conn,
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
			&sqlitex.ExecOptions{
				Named: map[string]any{
					"$version":     resCopy.Metadata().Version().Value(),
					"$updated_at":  resCopy.Metadata().Updated().Unix(),
					"$labels":      labels,
					"$finalizers":  finalizers,
					"$phase":       int(resCopy.Metadata().Phase()),
					"$owner":       resCopy.Metadata().Owner(),
					"$spec":        m,
					"$namespace":   resCopy.Metadata().Namespace(),
					"$type":        resCopy.Metadata().Type(),
					"$id":          resCopy.Metadata().ID(),
					"$version_old": currentVer,
				},
			},
		); err != nil {
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
			found             bool
		)

		err = sqlitex.Execute(conn, `SELECT owner, json(finalizers) AS finalizers, version
	 		FROM `+st.options.TablePrefix+`resources
			WHERE namespace = $namespace AND type = $type AND id = $id`,
			&sqlitex.ExecOptions{
				Named: map[string]any{
					"$namespace": ptr.Namespace(),
					"$type":      ptr.Type(),
					"$id":        ptr.ID(),
				},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					currentOwner = stmt.GetText("owner")

					currentFinalizers = make([]byte, stmt.GetLen("finalizers"))
					stmt.GetBytes("finalizers", currentFinalizers)
					currentVer = uint64(stmt.GetInt64("version"))
					found = true

					return nil
				},
			},
		)
		if err != nil {
			return fmt.Errorf("error querying current resource state: %w", err)
		}

		if !found {
			return fmt.Errorf("failed to delete: %w", ErrNotFound(ptr))
		}

		if currentOwner != options.Owner {
			return fmt.Errorf("failed to destroy: %w", ErrOwnerConflict(ptr, currentOwner))
		}

		if len(currentFinalizers) != 0 && !bytes.Equal(currentFinalizers, []byte("null")) {
			var fins resource.Finalizers

			// attempt to unmarshal finalizers, but ignore errors, as it's only for message
			json.Unmarshal(currentFinalizers, &fins) //nolint:errcheck

			return fmt.Errorf("failed to destroy: %w", ErrPendingFinalizers(ptr, fins))
		}

		err = sqlitex.Execute(conn,
			`DELETE FROM `+st.options.TablePrefix+`resources
				  WHERE
		 			namespace = $namespace AND type = $type AND id = $id AND version = $version`,
			&sqlitex.ExecOptions{
				Named: map[string]any{
					"$namespace": ptr.Namespace(),
					"$type":      ptr.Type(),
					"$id":        ptr.ID(),
					"$version":   currentVer,
				},
			},
		)
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
