// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/cosi-project/runtime/pkg/resource/protobuf"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/conformance"
	"github.com/cosi-project/runtime/pkg/state/impl/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"
	_ "modernc.org/sqlite"

	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func init() {
	must(protobuf.RegisterResource(conformance.PathResourceType, &conformance.PathResource{}))
}

func withSqlite(t testing.TB, fn func(state.State), opts ...sqlite.StateOption) { //nolint:unparam
	t.Helper()

	withSqliteCore(t, func(s *sqlite.State) {
		fn(state.WrapCore(s))
	}, opts...)
}

func withSqliteCore(t testing.TB, fn func(*sqlite.State), opts ...sqlite.StateOption) {
	t.Helper()

	dir := t.TempDir()

	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "state.db")+"?_txlock=immediate&_pragma=busy_timeout(50000)&_pragma=journal_mode(WAL)")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	coreState, err := sqlite.NewState(t.Context(), db, store.ProtobufMarshaler{},
		append(
			[]sqlite.StateOption{
				sqlite.WithTablePrefix("test_"),
				sqlite.WithLogger(zaptest.NewLogger(t)),
			},
			opts...,
		)...,
	)
	require.NoError(t, err)

	t.Cleanup(coreState.Close)

	t.Cleanup(func() {
		// we assert eventually here, because we need to wait for Watch* goroutines to exit
		assert.Eventually(t,
			coreState.EmptySubscriptions,
			time.Second, time.Millisecond,
			"expected no active subscriptions",
		)
	})

	fn(coreState)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
