// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"

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

	var spec []byte

	err := st.db.QueryRowContext(ctx, `SELECT spec
		FROM `+st.options.TablePrefix+`resources
		WHERE namespace = ? AND type = ? AND id = ?`,
		ptr.Namespace(),
		ptr.Type(),
		ptr.ID(),
	).Scan(
		&spec,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("failed to get: %w", ErrNotFound(ptr))
		}

		return nil, fmt.Errorf("error querying resource %q: %w", ptr, err)
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

	rows, err := st.db.QueryContext(ctx, `SELECT spec
		FROM `+st.options.TablePrefix+`resources
		WHERE namespace = ? AND type = ? AND `+filter.CompileLabelQueries(options.LabelQueries),
		resourceKind.Namespace(),
		resourceKind.Type(),
	)
	if err != nil {
		return resource.List{}, fmt.Errorf("error querying resources of kind %q: %w", resourceKind, err)
	}

	defer rows.Close() //nolint:errcheck

	var result resource.List

	for rows.Next() {
		var spec []byte

		if err := rows.Scan(&spec); err != nil {
			return resource.List{}, fmt.Errorf("error scanning resource of kind %q: %w", resourceKind, err)
		}

		res, err := st.marshaler.UnmarshalResource(spec)
		if err != nil {
			return resource.List{}, fmt.Errorf("failed to unmarshal resource of kind %q: %w", resourceKind, err)
		}

		if !matches(res) {
			continue
		}

		result.Items = append(result.Items, res)
	}

	if err := rows.Err(); err != nil {
		return resource.List{}, fmt.Errorf("error iterating over resources of kind %q: %w", resourceKind, err)
	}

	return result, nil
}
