// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"fmt"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite/internal/filter"
)

// Get a resource by type and ID.
//
// If a resource is not found, error is returned.
func (st *State) Get(ctx context.Context, ptr resource.Pointer, opts ...state.GetOption) (resource.Resource, error) {
	var options state.GetOptions

	for _, opt := range opts {
		opt(&options)
	}

	conn, err := st.db.Take(ctx)
	if err != nil {
		return nil, fmt.Errorf("taking connection for get: %w", err)
	}

	defer st.db.Put(conn)

	var (
		spec  []byte
		found bool
	)

	err = sqlitex.Execute(conn, `SELECT spec
		FROM `+st.options.TablePrefix+`resources
		WHERE namespace = $namespace AND type = $type AND id = $id`,
		&sqlitex.ExecOptions{
			Named: map[string]any{
				"$namespace": ptr.Namespace(),
				"$type":      ptr.Type(),
				"$id":        ptr.ID(),
			},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				spec = make([]byte, stmt.GetLen("spec"))
				stmt.GetBytes("spec", spec)

				found = true

				return nil
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error querying resource %q: %w", ptr, err)
	}

	if !found {
		return nil, fmt.Errorf("failed to get: %w", ErrNotFound(ptr))
	}

	res, err := st.marshaler.UnmarshalResource(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal resource %q: %w", ptr, err)
	}

	return res, nil
}

// List resources by type.
func (st *State) List(ctx context.Context, resourceKind resource.Kind, opts ...state.ListOption) (resource.List, error) {
	var options state.ListOptions

	for _, opt := range opts {
		opt(&options)
	}

	matches := func(res resource.Resource) bool {
		return options.LabelQueries.Matches(*res.Metadata().Labels()) && options.IDQuery.Matches(*res.Metadata())
	}

	conn, err := st.db.Take(ctx)
	if err != nil {
		return resource.List{}, fmt.Errorf("taking connection for get: %w", err)
	}

	defer st.db.Put(conn)

	var result resource.List

	err = sqlitex.Execute(conn, `SELECT spec
		FROM `+st.options.TablePrefix+`resources
		WHERE namespace = $namespace AND type = $type AND `+filter.CompileLabelQueries(options.LabelQueries),
		&sqlitex.ExecOptions{
			Named: map[string]any{
				"$namespace": resourceKind.Namespace(),
				"$type":      resourceKind.Type(),
			},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				spec := make([]byte, stmt.GetLen("spec"))
				stmt.GetBytes("spec", spec)

				var res resource.Resource

				res, err = st.marshaler.UnmarshalResource(spec)
				if err != nil {
					return fmt.Errorf("failed to unmarshal resource of kind %q: %w", resourceKind, err)
				}

				if !matches(res) {
					return nil
				}

				result.Items = append(result.Items, res)

				return nil
			},
		},
	)
	if err != nil {
		return resource.List{}, fmt.Errorf("error querying resources of kind %q: %w", resourceKind, err)
	}

	return result, nil
}
