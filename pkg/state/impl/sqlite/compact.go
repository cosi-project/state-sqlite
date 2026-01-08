// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/siderolabs/gen/panicsafe"
	"go.uber.org/zap"
	"zombiezen.com/go/sqlite"

	"github.com/cosi-project/state-sqlite/pkg/sqlitexx"
)

// CompactionInfo holds information about a compaction operation.
type CompactionInfo struct {
	EventsCompacted int64
	RemainingEvents int64
}

// Compact performs database compaction.
func (st *State) Compact(ctx context.Context) (*CompactionInfo, error) {
	st.compactMu.Lock()
	defer st.compactMu.Unlock()

	conn, err := st.db.Take(ctx)
	if err != nil {
		return nil, fmt.Errorf("error taking connection for compaction: %w", err)
	}

	defer st.db.Put(conn)

	var (
		minEventID, maxEventID int64
		info                   CompactionInfo
	)

	q, err := sqlitexx.NewQuery(
		conn,
		`SELECT coalesce(min(event_id), 0) AS min_event_id, coalesce(max(event_id), 0) AS max_event_id FROM `+st.options.TablePrefix+`events`,
	)
	if err != nil {
		return nil, fmt.Errorf("preparing query for compaction: %w", err)
	}

	if err = q.QueryRow(
		func(stmt *sqlite.Stmt) error {
			minEventID = stmt.GetInt64("min_event_id")
			maxEventID = stmt.GetInt64("max_event_id")

			return nil
		},
	); err != nil {
		return nil, fmt.Errorf("failed to get event ID range for compaction: %w", err)
	}

	if minEventID == 0 && maxEventID == 0 {
		// no events
		return &info, nil
	}

	// we estimate number of events by subtracting min from max
	// this works well enough even with gaps in event IDs
	info.RemainingEvents = maxEventID - minEventID + 1

	if info.RemainingEvents <= int64(st.options.CompactKeepEvents) {
		// no need to compact
		return &info, nil
	}

	// pick cutoff event ID based on min events to keep (don't drop more than CompactKeepEvents)
	cutoffEventID := maxEventID - int64(st.options.CompactKeepEvents) + 1

	// perform binary search on events table in the range [minEventID, cutoffEventID)
	// to find the first event that is newer than min age
	cutoffTime := time.Now().Add(-st.options.CompactMinAge).Unix()

	var (
		left, right    = minEventID, cutoffEventID
		eventTimestamp int64
	)

	for left < right {
		mid := (left + right) / 2

		if mid == minEventID {
			// there are no older events
			break
		}

		eventTimestamp = 0

		q, err = sqlitexx.NewQuery(
			conn,
			// event_id might have gaps, so we use max(event_id) < mid to find the closest one
			`SELECT max(event_id), event_timestamp FROM `+st.options.TablePrefix+`events WHERE event_id < $mid`,
		)
		if err != nil {
			return nil, fmt.Errorf("preparing query for event timestamp during compaction: %w", err)
		}

		if err = q.
			BindInt64("$mid", mid).
			QueryRow(
				func(stmt *sqlite.Stmt) error {
					eventTimestamp = stmt.GetInt64("event_timestamp")

					return nil
				},
			); err != nil {
			return nil, fmt.Errorf("failed to get event timestamp for compaction: %w", err)
		}

		if eventTimestamp == 0 {
			return nil, fmt.Errorf("failed to find event timestamp for event ID less than %d", mid)
		}

		if eventTimestamp < cutoffTime {
			left = mid + 1
		} else {
			right = mid
		}
	}

	if eventTimestamp > cutoffTime {
		// all events are newer than min age
		return &info, nil
	}

	cutoffEventID = left

	// delete events older than cutoffEventID
	// we will delete in batches of 1000 to avoid long transactions

	for {
		q, err := sqlitexx.NewQuery(
			conn,
			`DELETE FROM `+st.options.TablePrefix+`events WHERE event_id IN (SELECT event_id FROM `+st.options.TablePrefix+`events WHERE event_id < $cutoff LIMIT 1000)`,
		)
		if err != nil {
			return nil, fmt.Errorf("preparing delete statement for compaction: %w", err)
		}

		if err = q.
			BindInt64("$cutoff", cutoffEventID).
			Exec(); err != nil {
			return nil, fmt.Errorf("failed to delete old events during compaction: %w", err)
		}

		rowsAffected := conn.Changes()

		info.EventsCompacted += int64(rowsAffected)
		info.RemainingEvents -= int64(rowsAffected)

		if rowsAffected == 0 {
			// done
			break
		}
	}

	return &info, nil
}

func (st *State) runCompaction() {
	defer st.wg.Done()

	ticker := time.NewTicker(st.options.CompactionInterval)
	defer ticker.Stop()

	for {
		var (
			info *CompactionInfo
			err  error
		)

		err = panicsafe.RunErrF(func() error {
			info, err = st.Compact(st.compactionCtx)

			return err
		})()
		if err != nil {
			st.options.Logger.Error("failed to compact database", zap.Error(err))
		} else {
			st.options.Logger.Info("database compaction completed",
				zap.Int64("events_compacted", info.EventsCompacted),
				zap.Int64("remaining_events", info.RemainingEvents),
			)
		}

		select {
		case <-st.shutdown:
			return
		case <-ticker.C:
		}
	}
}
