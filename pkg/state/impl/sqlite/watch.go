// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/siderolabs/gen/channel"
	"github.com/siderolabs/gen/xslices"
)

func encodeBookmark(revision int64) state.Bookmark {
	return binary.BigEndian.AppendUint64(nil, uint64(revision))
}

func decodeBookmark(bookmark state.Bookmark) (int64, error) {
	if len(bookmark) != 8 {
		return 0, ErrInvalidWatchBookmark(fmt.Errorf("invalid bookmark length: %d", len(bookmark)))
	}

	return int64(binary.BigEndian.Uint64(bookmark)), nil
}

func (st *State) convertEvent(ctx context.Context, resourcePointer resource.Kind, eventID int64, specBefore, specAfter []byte, eventType int) state.Event {
	var event state.Event

	switch eventType {
	case 1: // Created
		res, err := st.marshaler.UnmarshalResource(specAfter)
		if err != nil {
			return state.Event{
				Type:  state.Errored,
				Error: fmt.Errorf("unmarshal created event for watch %q: %w", resourcePointer, err),
			}
		}

		event.Type = state.Created
		event.Resource = res
	case 2: // Updated
		res, err := st.marshaler.UnmarshalResource(specAfter)
		if err != nil {
			return state.Event{
				Type:  state.Errored,
				Error: fmt.Errorf("unmarshal updated event for watch %q: %w", resourcePointer, err),
			}
		}

		oldRes, err := st.marshaler.UnmarshalResource(specBefore)
		if err != nil {
			return state.Event{
				Type:  state.Errored,
				Error: fmt.Errorf("unmarshal old resource for updated event for watch %q: %w", resourcePointer, err),
			}
		}

		event.Type = state.Updated
		event.Resource = res
		event.Old = oldRes
	case 3: // Deleted
		res, err := st.marshaler.UnmarshalResource(specBefore)
		if err != nil {
			return state.Event{
				Type:  state.Errored,
				Error: fmt.Errorf("unmarshal deleted event for watch %q: %w", resourcePointer, err),
			}
		}

		event.Type = state.Destroyed
		event.Resource = res
	default:
		return state.Event{
			Type:  state.Errored,
			Error: fmt.Errorf("unknown event type %d for watch %q", eventType, resourcePointer),
		}
	}

	event.Bookmark = encodeBookmark(eventID)

	return event
}

// Watch state of a resource by type.
//
// It's fine to watch for a resource which doesn't exist yet.
// Watch is canceled when context gets canceled.
// Watch sends initial resource state as the very first event on the channel,
// and then sends any updates to the resource as events.
func (st *State) Watch(ctx context.Context, ptr resource.Pointer, ch chan<- state.Event, opts ...state.WatchOption) error {
	var options state.WatchOptions

	for _, opt := range opts {
		opt(&options)
	}

	var (
		initialEvent state.Event
		eventID      int64
	)

	switch {
	case options.TailEvents != 0:
		return fmt.Errorf("failed to watch: %w", ErrUnsupported("tailEvents"))
	case options.StartFromBookmark != nil:
		var err error

		eventID, err = decodeBookmark(options.StartFromBookmark)
		if err != nil {
			return fmt.Errorf("failed to watch %q: %w", ptr, err)
		}
	default:
		// figure out initial state of the watch process
		tx, err := st.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return fmt.Errorf("starting watch transaction: %w", err)
		}

		defer tx.Rollback() //nolint:errcheck

		var spec []byte

		err = tx.QueryRowContext(ctx, `SELECT spec
		FROM resources
		WHERE namespace = ? AND type = ? AND id = ?`,
			ptr.Namespace(),
			ptr.Type(),
			ptr.ID(),
		).Scan(
			&spec,
		)

		exists := true

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// resource doesn't exist yet
				exists = false
			} else {
				return fmt.Errorf("querying initial resource state for watch %q: %w", ptr, err)
			}
		}

		if exists {
			var res resource.Resource

			res, err = st.marshaler.UnmarshalResource(spec)
			if err != nil {
				return fmt.Errorf("unmarshal initial resource state for watch %q: %w", ptr, err)
			}

			initialEvent.Type = state.Created
			initialEvent.Resource = res
		} else {
			initialEvent.Type = state.Destroyed
			initialEvent.Resource = resource.NewTombstone(
				resource.NewMetadata(
					ptr.Namespace(),
					ptr.Type(),
					ptr.ID(),
					resource.VersionUndefined,
				),
			)
		}

		err = tx.QueryRowContext(ctx, `SELECT max(event_id) FROM events`).Scan(
			&eventID,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("querying initial event ID for watch %q: %w", ptr, err)
		}

		initialEvent.Bookmark = encodeBookmark(eventID)
	}

	go func() {
		if initialEvent.Resource != nil {
			if !channel.SendWithContext(ctx, ch, initialEvent) {
				// If the channel is closed, we should stop the watch
				return
			}
		}

		pollTicker := time.NewTicker(time.Millisecond * 5)
		defer pollTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-pollTicker.C:
			}

			var (
				specBefore, specAfter []byte
				newEventID            int64
				eventType             int
			)

			err := st.db.QueryRowContext(ctx, `
					SELECT event_id, spec_before, spec_after, event_type
					FROM events
					WHERE event_id > ? AND namespace = ? AND type = ? AND id = ?
					ORDER BY event_id ASC LIMIT 1`,
				eventID,
				ptr.Namespace(),
				ptr.Type(),
				ptr.ID(),
			).Scan(
				&newEventID,
				&specBefore,
				&specAfter,
				&eventType,
			)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				if errors.Is(err, sql.ErrNoRows) {
					// no new events
					continue
				}

				channel.SendWithContext(ctx, ch, state.Event{
					Type:  state.Errored,
					Error: fmt.Errorf("querying events for watch %q: %w", ptr, err),
				})

				return
			}

			eventID = newEventID

			event := st.convertEvent(ctx, ptr, eventID, specBefore, specAfter, eventType)

			if !channel.SendWithContext(ctx, ch, event) {
				// If the channel is closed, we should stop the watch
				return
			}
		}
	}()

	return nil
}

// WatchKind watches resources of specific kind (namespace and type).
func (st *State) WatchKind(ctx context.Context, resourceKind resource.Kind, ch chan<- state.Event, opts ...state.WatchKindOption) error {
	return st.watchKind(ctx, resourceKind, ch, nil, "watchKind", opts...)
}

// WatchKindAggregated watches resources of specific kind (namespace and type), updates are sent aggregated.
func (st *State) WatchKindAggregated(ctx context.Context, resourceKind resource.Kind, ch chan<- []state.Event, opts ...state.WatchKindOption) error {
	return st.watchKind(ctx, resourceKind, nil, ch, "watchKindAggregated", opts...)
}

func (st *State) watchKind(ctx context.Context, resourceKind resource.Kind, singleCh chan<- state.Event, aggCh chan<- []state.Event, opName string, opts ...state.WatchKindOption) error {
	var options state.WatchKindOptions

	for _, opt := range opts {
		opt(&options)
	}

	matches := func(res resource.Resource) bool {
		return options.LabelQueries.Matches(*res.Metadata().Labels()) && options.IDQuery.Matches(*res.Metadata())
	}

	var (
		bootstrapList []resource.Resource
		eventID       int64
	)

	switch {
	case options.TailEvents > 0:
		return fmt.Errorf("failed to %s: %w", opName, ErrUnsupported("tailEvents"))
	case options.StartFromBookmark != nil && options.BootstrapContents:
		return fmt.Errorf("failed to %s: %w", opName, ErrUnsupported("startFromBookmark and bootstrapContents"))
	case options.StartFromBookmark != nil:
		var err error

		eventID, err = decodeBookmark(options.StartFromBookmark)
		if err != nil {
			return fmt.Errorf("failed to %s %q: %w", opName, resourceKind, err)
		}
	case options.BootstrapContents:
		// figure out initial state of the watch process
		tx, err := st.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return fmt.Errorf("starting watch transaction: %w", err)
		}

		defer tx.Rollback() //nolint:errcheck

		// [TODO]: trivial implementation for now, without pushing down any queries to SQL
		rows, err := st.db.QueryContext(ctx, `SELECT spec
		FROM resources
		WHERE namespace = ? AND type = ?`,
			resourceKind.Namespace(),
			resourceKind.Type(),
		)
		if err != nil {
			return fmt.Errorf("error querying resources of kind %q: %w", resourceKind, err)
		}

		defer rows.Close() //nolint:errcheck

		for rows.Next() {
			var spec []byte

			if err := rows.Scan(&spec); err != nil {
				return fmt.Errorf("error scanning resource of kind %q: %w", resourceKind, err)
			}

			res, err := st.marshaler.UnmarshalResource(spec)
			if err != nil {
				return fmt.Errorf("failed to unmarshal resource of kind %q: %w", resourceKind, err)
			}

			if !matches(res) {
				continue
			}

			bootstrapList = append(bootstrapList, res)
		}

		err = tx.QueryRowContext(ctx, `SELECT max(event_id) FROM events`).Scan(
			&eventID,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("querying initial event ID for watch %q: %w", resourceKind, err)
		}
	default:
		err := st.db.QueryRowContext(ctx, `SELECT max(event_id) FROM events`).Scan(
			&eventID,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("querying initial event ID for watch %s: %w", resourceKind, err)
		}
	}

	go func() {
		if options.BootstrapContents {
			switch {
			case singleCh != nil:
				for _, res := range bootstrapList {
					if !channel.SendWithContext(ctx, singleCh,
						state.Event{
							Type:     state.Created,
							Resource: res,
						},
					) {
						return
					}
				}

				if !channel.SendWithContext(
					ctx, singleCh,
					state.Event{
						Type:     state.Bootstrapped,
						Resource: resource.NewTombstone(resource.NewMetadata(resourceKind.Namespace(), resourceKind.Type(), "", resource.VersionUndefined)),
						Bookmark: encodeBookmark(eventID),
					},
				) {
					return
				}
			case aggCh != nil:
				events := xslices.Map(bootstrapList, func(r resource.Resource) state.Event {
					return state.Event{
						Type:     state.Created,
						Resource: r,
					}
				})

				events = append(events, state.Event{
					Type:     state.Bootstrapped,
					Resource: resource.NewTombstone(resource.NewMetadata(resourceKind.Namespace(), resourceKind.Type(), "", resource.VersionUndefined)),
					Bookmark: encodeBookmark(eventID),
				})

				if !channel.SendWithContext(ctx, aggCh, events) {
					return
				}
			}

			// make the list nil so that it gets GC'ed, we don't need it anymore after this point
			bootstrapList = nil
		}

		if options.BootstrapBookmark {
			event := state.Event{
				Type:     state.Noop,
				Resource: resource.NewTombstone(resource.NewMetadata(resourceKind.Namespace(), resourceKind.Type(), "", resource.VersionUndefined)),
				Bookmark: encodeBookmark(eventID),
			}

			switch {
			case singleCh != nil:
				if !channel.SendWithContext(ctx, singleCh, event) {
					return
				}
			case aggCh != nil:
				if !channel.SendWithContext(ctx, aggCh, []state.Event{event}) {
					return
				}
			}
		}

		pollTicker := time.NewTicker(time.Millisecond * 5)
		defer pollTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-pollTicker.C:
			}

			var events []state.Event

			if queryErr := func() error {
				rows, err := st.db.QueryContext(ctx, `
					SELECT event_id, spec_before, spec_after, event_type
					FROM events
					WHERE event_id > ? AND namespace = ? AND type = ?
					ORDER BY event_id ASC LIMIT 1`,
					eventID,
					resourceKind.Namespace(),
					resourceKind.Type(),
				)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return err
					}

					return fmt.Errorf("querying events for watch %s: %w", resourceKind, err)
				}

				defer rows.Close() //nolint:errcheck

				for rows.Next() {
					var (
						specBefore, specAfter []byte
						newEventID            int64
						eventType             int
					)

					err = rows.Scan(
						&newEventID,
						&specBefore,
						&specAfter,
						&eventType,
					)
					if err != nil {
						return fmt.Errorf("scanning event for watch %s: %w", resourceKind, err)
					}

					eventID = newEventID

					event := st.convertEvent(ctx, resourceKind, eventID, specBefore, specAfter, eventType)
					if event.Type == state.Errored {
						return event.Error
					}

					switch event.Type {
					case state.Created, state.Destroyed:
						if !matches(event.Resource) {
							// skip the event
							continue
						}
					case state.Updated:
						oldMatches := matches(event.Old)
						newMatches := matches(event.Resource)

						switch {
						// transform the event if matching fact changes with the update
						case oldMatches && !newMatches:
							event.Type = state.Destroyed
							event.Old = nil
						case !oldMatches && newMatches:
							event.Type = state.Created
							event.Old = nil
						case newMatches && oldMatches:
							// passthrough the event
						default:
							// skip the event
							continue
						}
					case state.Errored, state.Bootstrapped, state.Noop:
						panic("should never be reached")
					}

					events = append(events, event)
				}

				if err := rows.Err(); err != nil {
					return fmt.Errorf("iterating events for watch %s: %w", resourceKind, err)
				}

				return nil
			}(); queryErr != nil {
				watchErrorEvent := state.Event{
					Type:  state.Errored,
					Error: queryErr,
				}

				switch {
				case singleCh != nil:
					channel.SendWithContext(ctx, singleCh, watchErrorEvent)
				case aggCh != nil:
					channel.SendWithContext(ctx, aggCh, []state.Event{watchErrorEvent})
				}

				return
			}

			if len(events) == 0 {
				continue
			}

			switch {
			case aggCh != nil:
				if !channel.SendWithContext(ctx, aggCh, events) {
					return
				}
			case singleCh != nil:
				for _, event := range events {
					if !channel.SendWithContext(ctx, singleCh, event) {
						return
					}
				}
			}

		}
	}()

	return nil
}
