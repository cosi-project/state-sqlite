// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package sqlitexx provides another set of extensions for the sqlitex package.
package sqlitexx

import (
	"errors"
	"iter"

	"zombiezen.com/go/sqlite"
)

// ErrNoRows is returned when a query returns no rows.
var ErrNoRows = errors.New("sqlitexx: no rows in result set")

// Query represents a prepared SQL query.
type Query struct {
	conn *sqlite.Conn
	stmt *sqlite.Stmt
}

// ResultFunc is a function that processes a row from a query result.
type ResultFunc = func(stmt *sqlite.Stmt) error

// NewQuery creates a query that can be executed.
//
// NewQuery supports only named parameters.
// The query should be executed to free up prepared resources.
func NewQuery(conn *sqlite.Conn, query string) (*Query, error) {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &Query{
		conn: conn,
		stmt: stmt,
	}, nil
}

// BindString binds a string parameter.
func (q *Query) BindString(name, value string) *Query {
	q.stmt.SetText(name, value)

	return q
}

// BindStringIfSet binds a string parameter if the value is not empty.
func (q *Query) BindStringIfSet(name, value string) *Query {
	if value != "" {
		q.stmt.SetText(name, value)
	}

	return q
}

// BindBytes binds a []byte parameter.
//
// If value is nil, NULL is bound.
func (q *Query) BindBytes(name string, value []byte) *Query {
	if value != nil {
		q.stmt.SetBytes(name, value)
	}

	return q
}

// BindInt64 binds an int64 parameter.
func (q *Query) BindInt64(name string, value int64) *Query {
	q.stmt.SetInt64(name, value)

	return q
}

// BindInt binds an int parameter.
func (q *Query) BindInt(name string, value int) *Query {
	return q.BindInt64(name, int64(value))
}

// BindUint64 binds a uint64 parameter.
func (q *Query) BindUint64(name string, value uint64) *Query {
	return q.BindInt64(name, int64(value))
}

// Exec executes the query without returning any rows.
func (q *Query) Exec() (err error) {
	defer func() {
		resetErr := q.stmt.Reset()
		if err == nil {
			err = resetErr
		}
	}()

	hasRows, err := q.stmt.Step()
	if err == nil && hasRows {
		err = errors.New("sqlitexx: Exec: query returned rows")
	}

	return err
}

// QueryRow executes the query and asserts a single row.
//
// If no rows are returned, ErrNoRows is returned.
func (q *Query) QueryRow(resultFn ResultFunc) (err error) {
	defer func() {
		resetErr := q.stmt.Reset()
		if err == nil {
			err = resetErr
		}
	}()

	hasRow, err := q.stmt.Step()
	if err != nil {
		return err
	}

	if !hasRow {
		return ErrNoRows
	}

	err = resultFn(q.stmt)
	if err == nil {
		hasRow, err = q.stmt.Step()
		if err == nil && hasRow {
			err = errors.New("sqlitexx: QueryRow: multiple rows returned")
		}
	}

	return err
}

// QueryAll executes the query and processes all returned rows.
func (q *Query) QueryAll(resultFn ResultFunc) (err error) {
	defer func() {
		resetErr := q.stmt.Reset()
		if err == nil {
			err = resetErr
		}
	}()

	for {
		hasRow, err := q.stmt.Step()
		if err != nil {
			return err
		}

		if !hasRow {
			break
		}

		if err := resultFn(q.stmt); err != nil {
			return err
		}
	}

	return nil
}

// QueryIter returns an iterator for the query results.
//
// The statement is reset when the iteration completes or breaks early.
func (q *Query) QueryIter() iter.Seq2[*sqlite.Stmt, error] {
	return func(yield func(*sqlite.Stmt, error) bool) {
		defer q.stmt.Reset() //nolint:errcheck

		for {
			hasRow, err := q.stmt.Step()
			if err != nil {
				yield(nil, err)

				return
			}

			if !hasRow {
				return
			}

			if !yield(q.stmt, nil) {
				return
			}
		}
	}
}
