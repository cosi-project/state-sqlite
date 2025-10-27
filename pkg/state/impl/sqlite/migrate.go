// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	_ "embed"
	"fmt"
)

//go:embed schema/schema.sql
var schemaSQL string

// migrate applies necessary database migrations.
func (st *State) migrate(ctx context.Context) error {
	_, err := st.db.ExecContext(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("applying schema migration: %w", err)
	}

	return nil
}
