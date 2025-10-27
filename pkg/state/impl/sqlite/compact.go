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
)

// CompactionInfo holds information about a compaction operation.
type CompactionInfo struct {
	EventsCompacted int64
	RemainingEvents int64
}

// Compact performs database compaction.
func (s *State) Compact(ctx context.Context) (*CompactionInfo, error) {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	var (
		minEventID, maxEventID int64
		info                   CompactionInfo
	)

	if err := s.db.QueryRowContext(ctx,
		`SELECT coalesce(min(event_id), 0), coalesce(max(event_id), 0) FROM `+s.options.TablePrefix+`events`,
	).Scan(&minEventID, &maxEventID); err != nil {
		return nil, fmt.Errorf("failed to get event ID range for compaction: %w", err)
	}

	if minEventID == 0 && maxEventID == 0 {
		// no events
		return &info, nil
	}

	// we estimate number of events by subtracting min from max
	// this works well enough even with gaps in event IDs
	info.RemainingEvents = maxEventID - minEventID + 1

	if info.RemainingEvents <= int64(s.options.CompactMaxEvents) {
		// no need to compact
		return &info, nil
	}

	// pick cutoff event ID based on max events to keep
	cutoffEventID := maxEventID - int64(s.options.CompactMaxEvents) + 1

	// perform binary search on events table in the range [minEventID, cutoffEventID)
	// to find the first event that is newer than min age
	cutoffTime := time.Now().Add(-s.options.CompactMinAge).Unix()

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

		if err := s.db.QueryRowContext(ctx,
			// event_id might have gaps, so we use max(event_id) < mid to find the closest one
			`SELECT max(event_id), event_timestamp FROM `+s.options.TablePrefix+`events WHERE event_id < ?`,
			mid,
		).Scan(new(int64), &eventTimestamp); err != nil {
			return nil, fmt.Errorf("failed to get event timestamp for compaction: %w", err)
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
		res, err := s.db.ExecContext(ctx,
			`DELETE FROM `+s.options.TablePrefix+`events WHERE event_id IN (SELECT event_id FROM `+s.options.TablePrefix+`events WHERE event_id < ? LIMIT 1000)`,
			cutoffEventID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to delete old events during compaction: %w", err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("failed to get rows affected during compaction: %w", err)
		}

		info.EventsCompacted += rowsAffected
		info.RemainingEvents -= rowsAffected

		if rowsAffected == 0 {
			// done
			break
		}
	}

	return &info, nil
}

func (s *State) runCompaction() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.options.CompactionInterval)
	defer ticker.Stop()

	for {
		var (
			info *CompactionInfo
			err  error
		)

		err = panicsafe.RunErrF(func() error {
			info, err = s.Compact(s.compactionCtx)

			return err
		})()
		if err != nil {
			s.options.Logger.Error("failed to compact database", zap.Error(err))
		} else {
			s.options.Logger.Info("database compaction completed",
				zap.Int64("events_compacted", info.EventsCompacted),
				zap.Int64("remaining_events", info.RemainingEvents),
			)
		}

		select {
		case <-s.shutdown:
			return
		case <-ticker.C:
		}
	}
}
