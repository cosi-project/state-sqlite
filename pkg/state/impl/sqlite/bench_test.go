// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"strconv"
	"testing"

	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/conformance"
	"github.com/stretchr/testify/require"
)

func BenchmarkCreate(b *testing.B) {
	b.ReportAllocs()

	withSqlite(b, func(st state.State) {
		for i := range b.N {
			path := conformance.NewPathResource("bench-ns", strconv.Itoa(i))

			require.NoError(b, st.Create(b.Context(), path))
		}
	})
}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()

	withSqlite(b, func(st state.State) {
		path := conformance.NewPathResource("bench-ns", "1")
		require.NoError(b, st.Create(b.Context(), path))

		b.ResetTimer()

		for b.Loop() {
			_, err := st.Get(b.Context(), path.Metadata())
			require.NoError(b, err)
		}
	})
}
