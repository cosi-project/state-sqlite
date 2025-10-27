// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/conformance"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSimpleOps(t *testing.T) {
	t.Parallel()

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

		path2 := conformance.NewPathResource("ns1", "var/run")

		require.NoError(t, st.Create(ctx, path2, state.WithCreateOwner("owner2")))
		require.Equal(t, "owner2", path2.Metadata().Owner())

		path2Back, err := st.Get(ctx, path2.Metadata())
		require.NoError(t, err)

		require.Equal(t, path2.Metadata().Owner(), path2Back.Metadata().Owner())
	})
}

func TestDestroy(t *testing.T) {
	t.Parallel()

	res := conformance.NewPathResource("default", "/")

	withSqlite(t, func(s state.State) {
		ctx := t.Context()

		var eg errgroup.Group

		err := s.Create(ctx, res)
		assert.NoError(t, err)

		for range 10 {
			eg.Go(func() error {
				err := s.Destroy(ctx, res.Metadata())
				if err != nil && !state.IsNotFoundError(err) {
					return err
				}

				return nil
			})
		}

		require.NoError(t, eg.Wait())
	})
}

func TestWatchWithBookmarks(t *testing.T) {
	t.Parallel()

	withSqlite(t, func(s state.State) {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		path1 := conformance.NewPathResource("ns1", "res/watch-with-bookmarks")

		ch := make(chan state.Event)

		require.NoError(t, s.Watch(ctx, path1.Metadata(), ch))

		require.NoError(t, s.Create(ctx, path1))

		ready, e := s.Teardown(ctx, path1.Metadata())
		require.NoError(t, e)
		assert.True(t, ready)

		assert.NoError(t, s.AddFinalizer(ctx, path1.Metadata(), "A"))
		assert.NoError(t, s.RemoveFinalizer(ctx, path1.Metadata(), "A"))
		assert.NoError(t, s.Destroy(ctx, path1.Metadata()))

		// should receive 6 events, including initial "destroyed" event
		const numEvents = 6

		for i := range numEvents {
			select {
			case ev := <-ch:
				if i != 0 {
					// initial event might not have a bookmark
					assert.NotNil(t, ev.Bookmark)
				}

				t.Logf("event %d: %+v", i, ev)
			case <-time.After(time.Second):
				assert.FailNow(t, "timed out waiting for event")
			}
		}
	})
}

func TestLabelMatchEscaping(t *testing.T) {
	t.Parallel()

	withSqlite(t, func(st state.State) {
		ctx := t.Context()

		res1 := conformance.NewPathResource("ns1", "res/label-escaping-1")
		res1.Metadata().Labels().Set("key.with.dots", "value'with'quotes")

		require.NoError(t, st.Create(ctx, res1))

		items, err := st.List(ctx, res1.Metadata(), state.WithLabelQuery(resource.LabelEqual("key.with.dots", "value'with'quotes")))
		require.NoError(t, err)
		require.Len(t, items.Items, 1)

		require.Equal(t, res1.Metadata().ID(), items.Items[0].Metadata().ID())
	})
}
