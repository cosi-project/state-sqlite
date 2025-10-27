// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"testing"

	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/conformance"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestSimpleOps(t *testing.T) {
	t.Cleanup(
		func() { goleak.VerifyNone(t, goleak.IgnoreCurrent()) },
	)

	withSqlite(t, func(st state.State) {
		ctx := t.Context()

		path1 := conformance.NewPathResource("ns1", "var/run")

		require.NoError(t, st.Create(ctx, path1))

		err := st.Create(ctx, path1)
		require.Error(t, err)
		require.True(t, state.IsConflictError(err))

		path1.Metadata().Labels().Set("env", "prod")
		require.NoError(t, st.Update(ctx, path1))

		require.NoError(t, path1.Metadata().SetOwner("controller-1"))
		require.NoError(t, st.Update(ctx, path1))

		require.NoError(t, st.Destroy(ctx, path1.Metadata(), state.WithDestroyOwner("controller-1")))
	})
}
