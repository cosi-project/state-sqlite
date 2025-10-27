// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//nolint:dupword
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
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

	tx, err := st.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("error starting create transaction: %w", err)
	}

	defer tx.Rollback() //nolint:errcheck

	_, err = tx.ExecContext(ctx,
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
		(?, ?, ?, ?, ?, ?, jsonb(?), jsonb(?), ?, ?, ?)`,
		resCopy.Metadata().Namespace(),
		resCopy.Metadata().Type(),
		resCopy.Metadata().ID(),
		resCopy.Metadata().Version().Value(),
		resCopy.Metadata().Created().Unix(),
		resCopy.Metadata().Updated().Unix(),
		labels,
		finalizers,
		int(resCopy.Metadata().Phase()),
		resCopy.Metadata().Owner(),
		m,
	)
	if err != nil {
		if isUniqueViolationError(err) {
			return ErrAlreadyExists(res.Metadata())
		}

		return fmt.Errorf("inserting resource into database: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing create transaction: %w", err)
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
func (st *State) Update(ctx context.Context, newResource resource.Resource, opts ...state.UpdateOption) error {
	options := state.DefaultUpdateOptions()

	for _, opt := range opts {
		opt(&options)
	}

	tx, err := st.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("error starting update transaction: %w", err)
	}

	defer tx.Rollback() //nolint:errcheck

	resCopy := newResource.DeepCopy()

	var (
		currentOwner string
		currentVer   uint64
		createdAt    int64
		currentPhase int
	)

	err = tx.QueryRowContext(ctx, `SELECT owner, version, created_at, phase 
	 		FROM `+st.options.TablePrefix+`resources
			WHERE namespace = ? AND type = ? AND id = ?`,
		newResource.Metadata().Namespace(),
		newResource.Metadata().Type(),
		newResource.Metadata().ID(),
	).Scan(
		&currentOwner,
		&currentVer,
		&createdAt,
		&currentPhase,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to update: %w", ErrNotFound(resCopy.Metadata()))
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
	}

	var finalizers []byte

	if !resCopy.Metadata().Finalizers().Empty() {
		finalizers, err = json.Marshal(resCopy.Metadata().Finalizers())
		if err != nil {
			return fmt.Errorf("failed to marshal finalizers: %w", err)
		}
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE `+st.options.TablePrefix+`resources
		SET 
			version = ?, 
			updated_at = ?,
			labels = jsonb(?),
			finalizers = jsonb(?),
			phase = ?, 
			owner = ?, 
			spec = ?
		WHERE
		 	namespace = ? AND type = ? AND id = ? AND version = ?`,
		resCopy.Metadata().Version().Value(),
		resCopy.Metadata().Updated().Unix(),
		labels,
		finalizers,
		int(resCopy.Metadata().Phase()),
		resCopy.Metadata().Owner(),
		m,
		resCopy.Metadata().Namespace(),
		resCopy.Metadata().Type(),
		resCopy.Metadata().ID(),
		currentVer,
	)
	if err != nil {
		return fmt.Errorf("error updating resource in database: %w", err)
	}

	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		if affectedErr != nil {
			return fmt.Errorf("error checking affected rows: %w", affectedErr)
		}

		return fmt.Errorf("error updating resource: %w", ErrVersionConflict(newResource.Metadata(), newResource.Metadata().Version().Value(), currentVer))
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing update transaction: %w", err)
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

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting update transaction: %w", err)
	}

	defer tx.Rollback() //nolint:errcheck

	var (
		currentOwner      string
		currentVer        uint64
		currentFinalizers []byte
	)

	err = tx.QueryRowContext(ctx, `SELECT owner, json(finalizers), version
	 		FROM `+st.options.TablePrefix+`resources
			WHERE namespace = ? AND type = ? AND id = ?`,
		ptr.Namespace(),
		ptr.Type(),
		ptr.ID(),
	).Scan(
		&currentOwner,
		&currentFinalizers,
		&currentVer,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to delete: %w", ErrNotFound(ptr))
		}

		return fmt.Errorf("error querying current resource state: %w", err)
	}

	if currentOwner != options.Owner {
		return fmt.Errorf("failed to destroy: %w", ErrOwnerConflict(ptr, currentOwner))
	}

	if currentFinalizers != nil {
		var fins resource.Finalizers

		// attempt to unmarshal finalizers, but ignore errors, as it's only for message
		json.Unmarshal(currentFinalizers, &fins) //nolint:errcheck

		return fmt.Errorf("failed to destroy: %w", ErrPendingFinalizers(ptr, fins))
	}

	result, err := tx.ExecContext(ctx,
		`DELETE FROM `+st.options.TablePrefix+`resources
		WHERE
		 	namespace = ? AND type = ? AND id = ? AND version = ?`,
		ptr.Namespace(),
		ptr.Type(),
		ptr.ID(),
		currentVer,
	)
	if err != nil {
		return fmt.Errorf("error deleting resource from database: %w", err)
	}

	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return fmt.Errorf("error deleting resource: %w", ErrVersionConflict(ptr, currentVer, currentVer))
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing delete transaction: %w", err)
	}

	st.sub.Notify(ptr)

	return nil
}
