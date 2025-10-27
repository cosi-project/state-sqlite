// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/cosi-project/runtime/pkg/resource/protobuf"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/conformance"
	"github.com/cosi-project/runtime/pkg/state/impl/store"
	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite"
	"github.com/stretchr/testify/require"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func init() {
	must(protobuf.RegisterResource(conformance.PathResourceType, &conformance.PathResource{}))
}

func withSqlite(t *testing.T, fn func(state.State)) {
	t.Helper()

	dir := t.TempDir()

	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "state.db")+"?_txlock=immediate&_pragma=busy_timeout(5000)")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	coreState, err := sqlite.NewState(t.Context(), db, store.ProtobufMarshaler{})
	require.NoError(t, err)

	fn(state.WrapCore(coreState))
}
