// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"fmt"

	"zombiezen.com/go/sqlite"

	"github.com/cosi-project/state-sqlite/pkg/sqlitexx"
)

// DBSize returns the size in bytes of tables used by this package.
//
// It uses SQLite's dbstat virtual table to calculate the size of the
// resources and events tables within the main database file (logical
// table page usage), which does not include any separate WAL/SHM files.
func (st *State) DBSize(ctx context.Context) (int64, error) {
	conn, err := st.db.Take(ctx)
	if err != nil {
		return 0, fmt.Errorf("error taking connection for db size: %w", err)
	}

	defer st.db.Put(conn)

	var size int64

	q, err := sqlitexx.NewQuery(
		conn,
		`SELECT coalesce(SUM(pgsize), 0) AS total_size FROM dbstat WHERE name = $table1 OR name = $table2`,
	)
	if err != nil {
		return 0, fmt.Errorf("preparing query for db size: %w", err)
	}

	if err = q.
		BindString("$table1", st.options.TablePrefix+"resources").
		BindString("$table2", st.options.TablePrefix+"events").
		QueryRow(
			func(stmt *sqlite.Stmt) error {
				size = stmt.GetInt64("total_size")

				return nil
			},
		); err != nil {
		return 0, fmt.Errorf("failed to get db size: %w", err)
	}

	return size, nil
}
