// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlitexx_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	zombiesqlite "zombiezen.com/go/sqlite"

	"github.com/cosi-project/state-sqlite/pkg/sqlitexx"
)

func newTestPool(t *testing.T, opts sqlitexx.PoolOptions) *sqlitexx.Pool {
	t.Helper()

	if opts.Flags == 0 {
		opts.Flags = zombiesqlite.OpenReadWrite | zombiesqlite.OpenCreate | zombiesqlite.OpenWAL | zombiesqlite.OpenURI
	}

	uri := "file:" + filepath.Join(t.TempDir(), "test.db")

	pool, err := sqlitexx.NewPool(uri, opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, pool.Close())
	})

	return pool
}

func TestPoolBasicTakePut(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, sqlitexx.PoolOptions{
		LowWatermark:  2,
		HighWatermark: 4,
	})

	conn, err := pool.Take(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)

	pool.Put(conn)
}

func TestPoolPutNil(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, sqlitexx.PoolOptions{})
	pool.Put(nil) // must be a no-op
}

func TestPoolMemoryURIRejected(t *testing.T) {
	t.Parallel()

	_, err := sqlitexx.NewPool(":memory:", sqlitexx.PoolOptions{})
	require.Error(t, err)
}

func TestPoolLowWatermarkCaches(t *testing.T) {
	t.Parallel()

	const lowWM = 3

	pool := newTestPool(t, sqlitexx.PoolOptions{
		LowWatermark:  lowWM,
		HighWatermark: 8,
	})

	// Take and return lowWM connections so they are cached.
	conns := make([]*zombiesqlite.Conn, lowWM)

	for i := range conns {
		c, err := pool.Take(t.Context())
		require.NoError(t, err)

		conns[i] = c
	}

	for _, c := range conns {
		pool.Put(c)
	}

	// Take them again; they should be the same objects (reused from the cache).
	reused := make(map[*zombiesqlite.Conn]bool, lowWM)

	for _, c := range conns {
		reused[c] = true
	}

	for i := range conns {
		c, err := pool.Take(t.Context())
		require.NoError(t, err)

		assert.True(t, reused[c], "expected a cached connection on iteration %d", i)

		pool.Put(c)
	}
}

func TestPoolAboveLowWatermarkClosesOnReturn(t *testing.T) {
	t.Parallel()

	const (
		lowWM  = 2
		highWM = 5
	)

	pool := newTestPool(t, sqlitexx.PoolOptions{
		LowWatermark:  lowWM,
		HighWatermark: highWM,
	})

	// Take highWM connections simultaneously.
	conns := make([]*zombiesqlite.Conn, highWM)

	for i := range conns {
		c, err := pool.Take(t.Context())
		require.NoError(t, err)

		conns[i] = c
	}

	// Return all; only lowWM should survive (the rest are closed).
	for _, c := range conns {
		pool.Put(c)
	}

	// Now take lowWM+1 connections. The first lowWM come from the cache,
	// the extra one is a freshly-opened connection.
	cached := make(map[*zombiesqlite.Conn]bool, highWM)

	for _, c := range conns {
		cached[c] = true
	}

	fresh := make([]*zombiesqlite.Conn, lowWM+1)

	for i := range fresh {
		c, err := pool.Take(t.Context())
		require.NoError(t, err)

		fresh[i] = c
	}

	cachedCount := 0

	for _, c := range fresh {
		if cached[c] {
			cachedCount++
		}
	}

	assert.Equal(t, lowWM, cachedCount, "expected exactly lowWM cached connections to be reused")

	for _, c := range fresh {
		pool.Put(c)
	}
}

func TestPoolHighWatermarkBlocks(t *testing.T) {
	t.Parallel()

	const highWM = 2

	pool := newTestPool(t, sqlitexx.PoolOptions{
		LowWatermark:  1,
		HighWatermark: highWM,
	})

	// Hold highWM connections.
	held := make([]*zombiesqlite.Conn, highWM)

	for i := range held {
		c, err := pool.Take(t.Context())
		require.NoError(t, err)

		held[i] = c
	}

	// A Take with a short timeout should fail.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err := pool.Take(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	for _, c := range held {
		pool.Put(c)
	}
}

func TestPoolHighWatermarkUnblocksOnReturn(t *testing.T) {
	t.Parallel()

	const highWM = 2

	pool := newTestPool(t, sqlitexx.PoolOptions{
		LowWatermark:  1,
		HighWatermark: highWM,
	})

	// Hold highWM connections.
	held := make([]*zombiesqlite.Conn, highWM)

	for i := range held {
		c, err := pool.Take(t.Context())
		require.NoError(t, err)

		held[i] = c
	}

	// Start a goroutine that will unblock after a short delay.
	go func() {
		time.Sleep(30 * time.Millisecond)
		pool.Put(held[0])
	}()

	conn, err := pool.Take(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)

	pool.Put(conn)
	pool.Put(held[1])
}

func TestPoolContextCancellation(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, sqlitexx.PoolOptions{
		LowWatermark:  1,
		HighWatermark: 1,
	})

	// Exhaust the pool.
	conn, err := pool.Take(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // already canceled

	_, err = pool.Take(ctx)
	require.ErrorIs(t, err, context.Canceled)

	pool.Put(conn)
}

func TestPoolCloseUnblocksWaiters(t *testing.T) {
	t.Parallel()

	pool, err := sqlitexx.NewPool(
		"file:"+filepath.Join(t.TempDir(), "test.db"),
		sqlitexx.PoolOptions{
			Flags:         zombiesqlite.OpenReadWrite | zombiesqlite.OpenCreate | zombiesqlite.OpenWAL | zombiesqlite.OpenURI,
			LowWatermark:  1,
			HighWatermark: 1,
		},
	)
	require.NoError(t, err)

	// Exhaust the pool.
	conn, err := pool.Take(t.Context())
	require.NoError(t, err)

	errCh := make(chan error, 1)

	go func() {
		_, err := pool.Take(t.Context())
		errCh <- err
	}()

	// Give the goroutine time to block in Take.
	time.Sleep(20 * time.Millisecond)

	// Return the connection, then close the pool.
	pool.Put(conn)
	require.NoError(t, pool.Close())

	// The waiting goroutine should unblock with an error.
	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Take to unblock after Close")
	}
}

func TestPoolCloseInterruptsInUse(t *testing.T) {
	t.Parallel()

	pool, err := sqlitexx.NewPool(
		"file:"+filepath.Join(t.TempDir(), "test.db"),
		sqlitexx.PoolOptions{
			Flags:         zombiesqlite.OpenReadWrite | zombiesqlite.OpenCreate | zombiesqlite.OpenWAL | zombiesqlite.OpenURI,
			LowWatermark:  1,
			HighWatermark: 2,
		},
	)
	require.NoError(t, err)

	conn, err := pool.Take(t.Context())
	require.NoError(t, err)

	// Close pool in a goroutine; it should interrupt conn and wait for Put.
	closeDone := make(chan error, 1)

	go func() {
		closeDone <- pool.Close()
	}()

	// Give Close time to interrupt the connection.
	time.Sleep(20 * time.Millisecond)

	// Returning the connection lets Close finish.
	pool.Put(conn)

	require.NoError(t, <-closeDone)
}

func TestPoolConcurrentAccess(t *testing.T) {
	t.Parallel()

	const (
		lowWM      = 3
		highWM     = 8
		goroutines = 20
		iterations = 50
	)

	pool := newTestPool(t, sqlitexx.PoolOptions{
		LowWatermark:  lowWM,
		HighWatermark: highWM,
	})

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			for range iterations {
				conn, err := pool.Take(t.Context())
				if !assert.NoError(t, err) {
					return
				}

				// Simulate brief work.
				time.Sleep(time.Millisecond)

				pool.Put(conn)
			}
		}()
	}

	wg.Wait()
}

func TestPoolDefaultWatermarks(t *testing.T) {
	t.Parallel()

	// Zero values should apply defaults without panicking.
	pool := newTestPool(t, sqlitexx.PoolOptions{})

	const concurrency = 5

	var wg sync.WaitGroup

	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()

			conn, err := pool.Take(t.Context())
			require.NoError(t, err)
			pool.Put(conn)
		}()
	}

	wg.Wait()
}

func TestPoolPutPanicOnUnknownConn(t *testing.T) {
	t.Parallel()

	pool1 := newTestPool(t, sqlitexx.PoolOptions{LowWatermark: 1, HighWatermark: 2})
	pool2 := newTestPool(t, sqlitexx.PoolOptions{LowWatermark: 1, HighWatermark: 2})

	conn, err := pool1.Take(t.Context())
	require.NoError(t, err)

	assert.Panics(t, func() {
		pool2.Put(conn)
	})

	pool1.Put(conn)
}

func TestPoolCloseTwice(t *testing.T) {
	t.Parallel()

	pool, err := sqlitexx.NewPool(
		"file:"+filepath.Join(t.TempDir(), "test.db"),
		sqlitexx.PoolOptions{
			Flags: zombiesqlite.OpenReadWrite | zombiesqlite.OpenCreate | zombiesqlite.OpenWAL | zombiesqlite.OpenURI,
		},
	)
	require.NoError(t, err)
	require.NoError(t, pool.Close())
	require.Error(t, pool.Close())
}

func TestPoolTakeAfterClose(t *testing.T) {
	t.Parallel()

	pool, err := sqlitexx.NewPool(
		"file:"+filepath.Join(t.TempDir(), "test.db"),
		sqlitexx.PoolOptions{
			Flags: zombiesqlite.OpenReadWrite | zombiesqlite.OpenCreate | zombiesqlite.OpenWAL | zombiesqlite.OpenURI,
		},
	)
	require.NoError(t, err)
	require.NoError(t, pool.Close())

	_, err = pool.Take(t.Context())
	require.Error(t, err)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
