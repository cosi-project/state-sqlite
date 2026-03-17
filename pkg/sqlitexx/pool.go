// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlitexx

import (
	"context"
	"expvar"
	"fmt"
	"sync"

	"zombiezen.com/go/sqlite"
)

var poolConnections expvar.Int

func init() {
	expvar.Publish("sqlitexx_pool_connections", &poolConnections)
}

// PoolOptions configures [NewPool].
type PoolOptions struct {
	// Flags is interpreted the same way as the argument to [sqlite.Open].
	// A Flags value of 0 defaults to:
	//
	//  - SQLITE_OPEN_READWRITE
	//  - SQLITE_OPEN_CREATE
	//  - SQLITE_OPEN_WAL
	//  - SQLITE_OPEN_URI
	Flags sqlite.OpenFlags

	// LowWatermark is the minimum number of idle connections kept in the pool.
	// When a connection is returned and the idle count is below this threshold,
	// the connection is cached; otherwise it is closed.
	//
	// Default is 2.
	LowWatermark int

	// HighWatermark is the maximum total number of connections (idle + in-use).
	// Take will block until a connection becomes available if this limit is reached.
	//
	// Default is 10. Must be >= LowWatermark.
	HighWatermark int
}

// Pool is a dynamically-sized pool of SQLite connections.
//
// Connections are created on demand up to HighWatermark. Up to LowWatermark
// idle connections are retained; any extras are closed when returned.
// It is safe for concurrent use by multiple goroutines.
type Pool struct { //nolint:govet
	uri           string
	flags         sqlite.OpenFlags
	lowWatermark  int
	highWatermark int

	mu         sync.Mutex
	free       []*sqlite.Conn
	totalConns int
	// inUse is bounded by highWatermark; its internal table never grows beyond that.
	inUse      map[*sqlite.Conn]context.CancelFunc
	closed     bool
	closedChan chan struct{}
	// avail is closed (and replaced) whenever a connection may become available.
	// Waiters snapshot it under mu, then block on the snapshot.
	avail chan struct{}

	// wg counts all live connections (idle + in-use).
	wg sync.WaitGroup
}

// NewPool opens a dynamically-sized pool of SQLite connections.
func NewPool(uri string, opts PoolOptions) (*Pool, error) {
	if uri == ":memory:" {
		return nil, fmt.Errorf(`sqlite: ":memory:" does not work with multiple connections, use "file::memory:?mode=memory&cache=shared"`)
	}

	lowWM := opts.LowWatermark
	if lowWM < 1 {
		lowWM = 2
	}

	highWM := opts.HighWatermark
	if highWM < 1 {
		highWM = 10
	}

	if highWM < lowWM {
		highWM = lowWM
	}

	flags := opts.Flags
	if flags == 0 {
		flags = sqlite.OpenReadWrite |
			sqlite.OpenCreate |
			sqlite.OpenWAL |
			sqlite.OpenURI
	}

	p := &Pool{
		uri:           uri,
		flags:         flags,
		lowWatermark:  lowWM,
		highWatermark: highWM,
		inUse:         make(map[*sqlite.Conn]context.CancelFunc),
		closedChan:    make(chan struct{}),
		avail:         make(chan struct{}),
	}

	return p, nil
}

// Take returns an SQLite connection from the Pool.
//
// If no connection is available and the high watermark has been reached,
// Take blocks until a connection is returned, the context is canceled,
// or the pool is closed. The context also controls the connection's interrupt
// lifetime via [sqlite.Conn.SetInterrupt].
//
// All non-nil connections returned by Take must be returned via [Pool.Put].
func (p *Pool) Take(ctx context.Context) (*sqlite.Conn, error) {
	for {
		p.mu.Lock()

		if p.closed {
			p.mu.Unlock()

			return nil, fmt.Errorf("get sqlite connection: pool closed")
		}

		// Fast path: reuse an idle connection.
		if len(p.free) > 0 {
			conn := p.free[len(p.free)-1]
			p.free = p.free[:len(p.free)-1]

			// Release the lock before calling SetInterrupt: cancelInterrupt()
			// inside SetInterrupt does a synchronous channel handshake with the
			// previous monitor goroutine, and holding p.mu during that would
			// deadlock with notify() or put() that also need the lock.
			p.mu.Unlock()

			ctx2, cancel := context.WithCancel(ctx)
			conn.SetInterrupt(ctx2.Done())

			// Re-acquire to register in inUse and check for concurrent Close.
			p.mu.Lock()

			if p.closed {
				// Pool was closed while we were preparing the connection.
				p.mu.Unlock()
				conn.SetInterrupt(nil)
				cancel()
				p.mu.Lock()
				p.totalConns--
				p.mu.Unlock()
				conn.Close() //nolint:errcheck
				p.wg.Done()
				poolConnections.Add(-1)

				return nil, fmt.Errorf("get sqlite connection: pool closed")
			}

			p.inUse[conn] = cancel
			p.mu.Unlock()

			return conn, nil
		}

		// Create a new connection if below the high watermark.
		if p.totalConns < p.highWatermark {
			p.totalConns++
			p.mu.Unlock()

			conn, err := sqlite.OpenConn(p.uri, p.flags)
			if err != nil {
				p.mu.Lock()
				p.totalConns--
				p.mu.Unlock()
				p.notify()

				return nil, fmt.Errorf("get sqlite connection: %w", err)
			}

			p.wg.Add(1)
			poolConnections.Add(1)

			ctx2, cancel := context.WithCancel(ctx)
			conn.SetInterrupt(ctx2.Done())

			p.mu.Lock()
			p.inUse[conn] = cancel
			p.mu.Unlock()

			return conn, nil
		}

		// High watermark reached: wait for availability.
		avail := p.avail
		p.mu.Unlock()

		select {
		case <-avail:
			// Something changed; retry.
		case <-ctx.Done():
			return nil, fmt.Errorf("get sqlite connection: %w", ctx.Err())
		case <-p.closedChan:
			return nil, fmt.Errorf("get sqlite connection: pool closed")
		}
	}
}

// Put returns a connection to the pool.
//
// Put panics if conn was not obtained from this pool.
// Put(nil) is a no-op.
//
// If the number of idle connections is below LowWatermark, the connection is
// cached; otherwise it is closed immediately.
func (p *Pool) Put(conn *sqlite.Conn) {
	if conn == nil {
		return
	}

	if query := conn.CheckReset(); query != "" {
		panic(fmt.Sprintf("connection returned to pool has active statement: %q", query))
	}

	p.put(conn)
}

func (p *Pool) put(conn *sqlite.Conn) {
	p.mu.Lock()

	cancel, found := p.inUse[conn]
	if !found {
		p.mu.Unlock()
		panic("sqlite.Pool.Put: connection not created by this pool")
	}

	delete(p.inUse, conn)
	p.mu.Unlock()

	// IMPORTANT: clear the interrupt and cancel the context BEFORE making the
	// connection visible in the free pool. If we added the connection to free
	// first, a concurrent Take() could grab it and call SetInterrupt on it
	// while the old monitor goroutine (with the same cancelCh) is still alive.
	// Both would then try to use the same cancelCh simultaneously, causing a
	// permanent deadlock in cancelInterrupt().
	conn.SetInterrupt(nil)
	cancel()

	// Now decide whether to return the connection to the free pool or discard it.
	p.mu.Lock()

	if p.closed || len(p.free) >= p.lowWatermark {
		// Pool is closing or above the low watermark: discard.
		p.totalConns--
		p.mu.Unlock()

		conn.Close() //nolint:errcheck
		p.wg.Done()
		poolConnections.Add(-1)
		p.notify()

		return
	}

	p.free = append(p.free, conn)
	p.mu.Unlock()

	p.notify()
}

// notify wakes all goroutines waiting in Take for a connection to become available.
func (p *Pool) notify() {
	p.mu.Lock()
	ch := p.avail
	p.avail = make(chan struct{})
	p.mu.Unlock()

	close(ch)
}

// Close interrupts and closes all connections, waiting for in-use connections
// to be returned via [Pool.Put].
func (p *Pool) Close() error {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()

		return fmt.Errorf("sqlite: pool already closed")
	}

	p.closed = true

	// Collect all in-use cancel funcs.
	cancelList := make([]context.CancelFunc, 0, len(p.inUse))
	for _, cancel := range p.inUse {
		cancelList = append(cancelList, cancel)
	}

	// Grab idle connections to close.
	freeCopy := make([]*sqlite.Conn, len(p.free))
	copy(freeCopy, p.free)
	p.free = p.free[:0]

	p.mu.Unlock()

	// Unblock waiting Takes and interrupt in-use connections.
	close(p.closedChan)

	for _, cancel := range cancelList {
		cancel()
	}

	// Close idle connections.
	var closeErr error

	for _, conn := range freeCopy {
		p.mu.Lock()
		p.totalConns--
		p.mu.Unlock()

		if err := conn.Close(); err != nil && closeErr == nil {
			closeErr = err
		}

		p.wg.Done()
		poolConnections.Add(-1)
	}

	// Wait for all in-use connections to be returned (they close themselves in put).
	p.wg.Wait()

	return closeErr
}
