// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package sqlite provides an implementation of state.State in sqlite.
package sqlite

import (
	"context"
	"database/sql"

	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/impl/store"
)

// State implements state storage in sqlite database.
type State struct {
	db        *sql.DB
	marshaler store.Marshaler
}

// Check interface implementation.
var _ state.CoreState = &State{}

// NewState creates new State with default options.
func NewState(ctx context.Context, db *sql.DB, marshaler store.Marshaler) (*State, error) {
	st := &State{
		db:        db,
		marshaler: marshaler,
	}

	if err := st.migrate(ctx); err != nil {
		return nil, err
	}

	return st, nil
}
