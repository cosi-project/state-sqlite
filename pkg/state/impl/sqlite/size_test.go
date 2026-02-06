// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"strconv"
	"testing"

	"github.com/cosi-project/runtime/pkg/state/conformance"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite"
)

func TestDBSizeEmpty(t *testing.T) {
	t.Parallel()

	withSqliteCore(t, func(st *sqlite.State) {
		size, err := st.DBSize(t.Context())
		require.NoError(t, err)
		assert.Greater(t, size, int64(0), "even empty tables should have some overhead")
	})
}

func TestDBSizeGrowsWithData(t *testing.T) {
	t.Parallel()

	withSqliteCore(t, func(st *sqlite.State) {
		sizeBefore, err := st.DBSize(t.Context())
		require.NoError(t, err)

		for i := range 100 {
			require.NoError(t, st.Create(t.Context(), conformance.NewPathResource("ns1", strconv.Itoa(i))))
		}

		sizeAfter, err := st.DBSize(t.Context())
		require.NoError(t, err)

		assert.Greater(t, sizeAfter, sizeBefore, "size should grow after inserting resources")
	})
}
