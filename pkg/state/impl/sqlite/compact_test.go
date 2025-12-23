// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/cosi-project/runtime/pkg/state/conformance"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite"
)

func TestCompactEmpty(t *testing.T) {
	t.Parallel()

	withSqliteCore(t, func(st *sqlite.State) {
		result, err := st.Compact(t.Context())
		require.NoError(t, err)
		assert.Empty(t, *result)
	})
}

func TestCompactNotEnoughEvents(t *testing.T) {
	t.Parallel()

	withSqliteCore(t, func(st *sqlite.State) {
		for i := range 10 {
			require.NoError(t, st.Create(t.Context(), conformance.NewPathResource("ns1", strconv.Itoa(i))))
		}

		result, err := st.Compact(t.Context())
		require.NoError(t, err)
		assert.EqualValues(t, 0, result.EventsCompacted)
		assert.EqualValues(t, 10, result.RemainingEvents)

		for i := range 10 {
			require.NoError(t, st.Create(t.Context(), conformance.NewPathResource("ns2", strconv.Itoa(i))))
		}

		result, err = st.Compact(t.Context())
		require.NoError(t, err)
		// should not compact yet, as max age is not reached
		assert.EqualValues(t, 0, result.EventsCompacted)
		assert.EqualValues(t, 20, result.RemainingEvents)
	}, sqlite.WithCompactKeepEvents(10), sqlite.WithCompactionInterval(0))
}

func TestCompactEvents(t *testing.T) {
	t.Parallel()

	withSqliteCore(t, func(st *sqlite.State) {
		for i := range 20 {
			require.NoError(t, st.Create(t.Context(), conformance.NewPathResource("ns1", strconv.Itoa(i))))
		}

		result, err := st.Compact(t.Context())
		require.NoError(t, err)
		assert.EqualValues(t, 10, result.EventsCompacted)
		assert.EqualValues(t, 10, result.RemainingEvents)

		for i := range 20 {
			require.NoError(t, st.Create(t.Context(), conformance.NewPathResource("ns2", strconv.Itoa(i))))
		}

		result, err = st.Compact(t.Context())
		require.NoError(t, err)
		assert.EqualValues(t, 20, result.EventsCompacted)
		assert.EqualValues(t, 10, result.RemainingEvents)
	}, sqlite.WithCompactKeepEvents(10), sqlite.WithCompactMinAge(-time.Minute), sqlite.WithCompactionInterval(0))
}
