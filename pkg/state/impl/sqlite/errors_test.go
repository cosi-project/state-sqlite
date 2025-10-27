// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"errors"
	"testing"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/stretchr/testify/require"

	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite"
)

func TestErrors(t *testing.T) {
	res := resource.NewMetadata("ns", "a", "b", resource.VersionUndefined)

	require.Implements(t, (*state.ErrConflict)(nil), sqlite.ErrAlreadyExists(res))
	require.Implements(t, (*state.ErrConflict)(nil), sqlite.ErrVersionConflict(res, 1, 2))
	require.Implements(t, (*state.ErrConflict)(nil), sqlite.ErrOwnerConflict(res, "owner"))
	require.Implements(t, (*state.ErrConflict)(nil), sqlite.ErrPendingFinalizers(res))
	require.Implements(t, (*state.ErrConflict)(nil), sqlite.ErrPhaseConflict(res, resource.PhaseRunning))
	require.Implements(t, (*state.ErrNotFound)(nil), sqlite.ErrNotFound(res))

	require.True(t, state.IsConflictError(sqlite.ErrAlreadyExists(res), state.WithResourceType("a")))
	require.False(t, state.IsConflictError(sqlite.ErrAlreadyExists(res), state.WithResourceType("b")))
	require.True(t, state.IsConflictError(sqlite.ErrAlreadyExists(res), state.WithResourceNamespace("ns")))

	require.True(t, state.IsInvalidWatchBookmarkError(sqlite.ErrInvalidWatchBookmark(errors.New("invalid"))))
}
