// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package filter_test

import (
	"testing"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/stretchr/testify/assert"

	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite/internal/filter"
)

func TestCompile(t *testing.T) {
	t.Parallel()

	for _, test := range []struct { //nolint:govet
		name string

		queries  resource.LabelQueries
		expected string
	}{
		{
			name:     "no queries",
			expected: "true",
		},
		{
			name: "single equal query",
			queries: resource.LabelQueries{
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key:   "foo",
							Op:    resource.LabelOpEqual,
							Value: []string{"bar"},
						},
					},
				},
			},
			expected: `((labels ->> '$."foo"' = 'bar'))`,
		},
		{
			name: "multiple queries",
			queries: resource.LabelQueries{
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key:   "foo",
							Op:    resource.LabelOpEqual,
							Value: []string{"bar"},
						},
					},
				},
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key:   "env",
							Op:    resource.LabelOpIn,
							Value: []string{"prod", "staging"},
						},
					},
				},
			},
			expected: `((labels ->> '$."foo"' = 'bar')) OR ((labels ->> '$."env"' IN ('prod', 'staging')))`,
		},
		{
			name: "exists and not exists",
			queries: resource.LabelQueries{
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key: "foo",
							Op:  resource.LabelOpExists,
						},
						{
							Key:    "bar",
							Op:     resource.LabelOpExists,
							Invert: true,
						},
					},
				},
			},
			expected: `((labels ->> '$."foo"' IS NOT NULL) AND (labels ->> '$."bar"' IS NULL))`,
		},
		{
			name: "unsupported term",
			queries: resource.LabelQueries{
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key:   "foo",
							Op:    resource.LabelOpLT,
							Value: []string{"bar"},
						},
					},
				},
			},
			expected: "true",
		},
		{
			name: "mixed supported and unsupported terms",
			queries: resource.LabelQueries{
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key:   "foo",
							Op:    resource.LabelOpEqual,
							Value: []string{"bar"},
						},
						{
							Key:   "baz",
							Op:    resource.LabelOpLT,
							Value: []string{"qux"},
						},
					},
				},
			},
			expected: `((labels ->> '$."foo"' = 'bar'))`,
		},
		{
			name: "inverted equal",
			queries: resource.LabelQueries{
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key:    "foo",
							Op:     resource.LabelOpEqual,
							Value:  []string{"bar"},
							Invert: true,
						},
					},
				},
			},
			expected: `((labels ->> '$."foo"' != 'bar'))`,
		},
		{
			name: "empty in values",
			queries: resource.LabelQueries{
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key:   "foo",
							Op:    resource.LabelOpIn,
							Value: []string{},
						},
					},
				},
			},
			expected: "((false))",
		},
		{
			name: "inverted empty in values",
			queries: resource.LabelQueries{
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key:    "foo",
							Op:     resource.LabelOpIn,
							Value:  []string{},
							Invert: true,
						},
					},
				},
			},
			expected: "((true))",
		},
		{
			name: "escaping special characters",
			queries: resource.LabelQueries{
				resource.LabelQuery{
					Terms: []resource.LabelTerm{
						{
							Key:   `talos.dev/email`,
							Op:    resource.LabelOpEqual,
							Value: []string{"foo'bar\"baz"},
						},
					},
				},
			},
			expected: `((labels ->> '$."talos.dev/email"' = 'foo''bar"baz'))`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := filter.CompileLabelQueries(test.queries)
			assert.Equal(t, test.expected, compiled)
		})
	}
}
