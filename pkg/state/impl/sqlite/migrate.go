// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	_ "embed"
	"fmt"

	"zombiezen.com/go/sqlite/sqlitex"
)

//go:embed schema/schema.sql
var schemaSQL string

// migrate applies necessary database migrations.
func (st *State) migrate(ctx context.Context) error {
	schemaReplaced := fmt.Sprintf(schemaSQL, st.options.TablePrefix)

	conn, err := st.db.Take(ctx)
	if err != nil {
		return fmt.Errorf("taking connection for migration: %w", err)
	}

	defer st.db.Put(conn)

	if err = sqlitex.ExecScript(conn, schemaReplaced); err != nil {
		return fmt.Errorf("applying schema migration: %w", err)
	}

	return nil
}
