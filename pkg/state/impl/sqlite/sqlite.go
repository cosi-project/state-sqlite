// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package sqlite provides an implementation of state.State in sqlite.
package sqlite

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/impl/store"
	"go.uber.org/zap"

	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite/internal/sub"
)

// State implements state storage in sqlite database.
type State struct {
	db                  *sql.DB
	marshaler           store.Marshaler
	sub                 *sub.Manager
	shutdown            chan struct{}
	compactionCtx       context.Context //nolint:containedctx
	compactionCtxCancel context.CancelFunc
	options             StateOptions
	wg                  sync.WaitGroup
	compactMu           sync.Mutex
}

// StateOptions configures sqlite state.
type StateOptions struct {
	// Logger is the logger to use for logging.
	Logger *zap.Logger

	// TablePrefix is the prefix to use for all tables used by the sqlite state.
	//
	// Default is empty string.
	// Setting a table prefix allows multiple independent states to share the same database.
	TablePrefix string

	// CompactionInterval is the interval between automatic database compactions.
	//
	// Default is 30 minutes.
	CompactionInterval time.Duration

	// CompactMaxEvents is the maximum number of events to keep during compaction.
	//
	// Default is 1000.
	CompactMaxEvents int

	// CompactMinAge is the minimum age of events to keep during compaction.
	//
	// It might be important to keep recent events to allow restarting a watch
	// from a bookmark.
	//
	// Default is 1 hour.
	CompactMinAge time.Duration
}

// StateOption configures sqlite state.
type StateOption func(*StateOptions)

// DefaultStateOptions returns default sqlite state options.
func DefaultStateOptions() StateOptions {
	return StateOptions{
		Logger:             zap.NewNop(),
		TablePrefix:        "",
		CompactionInterval: 30 * time.Minute,
		CompactMaxEvents:   1000,
		CompactMinAge:      time.Hour,
	}
}

// WithTablePrefix sets the table prefix for all tables used by the sqlite state.
func WithTablePrefix(prefix string) StateOption {
	return func(opts *StateOptions) {
		opts.TablePrefix = prefix
	}
}

// WithCompactionInterval sets the interval between automatic database compactions.
func WithCompactionInterval(interval time.Duration) StateOption {
	return func(opts *StateOptions) {
		opts.CompactionInterval = interval
	}
}

// WithCompactMaxEvents sets the maximum number of events to keep during compaction.
func WithCompactMaxEvents(maxEvents int) StateOption {
	return func(opts *StateOptions) {
		opts.CompactMaxEvents = maxEvents
	}
}

// WithCompactMinAge sets the minimum age of events to keep during compaction.
func WithCompactMinAge(minAge time.Duration) StateOption {
	return func(opts *StateOptions) {
		opts.CompactMinAge = minAge
	}
}

// WithLogger sets the logger for the sqlite state.
func WithLogger(logger *zap.Logger) StateOption {
	return func(opts *StateOptions) {
		opts.Logger = logger
	}
}

// Check interface implementation.
var _ state.CoreState = &State{}

// NewState creates new State with default options.
//
// The following options should be enabled on the sqlite database:
//   - busy_timeout pragma should be set to a reasonable value (e.g. 5000 ms)
//   - journal_mode pragma should be set to WAL
//   - txlock=immediate should be set in the DSN to avoid busy errors on concurrent writes.
func NewState(ctx context.Context, db *sql.DB, marshaler store.Marshaler, opts ...StateOption) (*State, error) {
	compactionCtx, compactionCtxCancel := context.WithCancel(context.Background())

	st := &State{
		db:                  db,
		marshaler:           marshaler,
		sub:                 sub.NewManager(),
		options:             DefaultStateOptions(),
		shutdown:            make(chan struct{}),
		compactionCtx:       compactionCtx,
		compactionCtxCancel: compactionCtxCancel,
	}

	for _, opt := range opts {
		opt(&st.options)
	}

	if err := st.migrate(ctx); err != nil {
		return nil, err
	}

	if st.options.CompactionInterval > 0 {
		st.wg.Add(1)

		go st.runCompaction() //nolint:contextcheck
	}

	return st, nil
}

// Close shuts down the state and releases all resources.
func (s *State) Close() {
	s.compactionCtxCancel()
	close(s.shutdown)
	s.wg.Wait()
}
