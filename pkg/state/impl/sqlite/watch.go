// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/siderolabs/gen/channel"
	"github.com/siderolabs/gen/xslices"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/cosi-project/state-sqlite/pkg/sqlitexx"
	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite/internal/filter"
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

func (st *State) convertEvent(resourcePointer resource.Kind, eventID int64, specBefore, specAfter []byte, eventType int) state.Event {
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
//
//nolint:gocyclo,gocognit,cyclop,maintidx
func (st *State) Watch(ctx context.Context, ptr resource.Pointer, ch chan<- state.Event, opts ...state.WatchOption) error {
	var options state.WatchOptions

	for _, opt := range opts {
		opt(&options)
	}

	var (
		initialEvent state.Event
		eventID      int64
	)

	sub := st.sub.Subscribe(ptr)
	watchSetupFailed := true

	defer func() {
		if watchSetupFailed {
			sub.Unsubscribe()
		}
	}()

	conn, err := st.db.Take(ctx)
	if err != nil {
		return fmt.Errorf("taking connection for watch setup: %w", err)
	}

	defer st.db.Put(conn)

	switch {
	case options.TailEvents != 0:
		return fmt.Errorf("failed to watch: %w", ErrUnsupported("tailEvents"))
	case options.StartFromBookmark != nil:
		var err error

		eventID, err = decodeBookmark(options.StartFromBookmark)
		if err != nil {
			return fmt.Errorf("failed to watch %q: %w", ptr, err)
		}

		// verify that we still have the event in the log
		q, err := sqlitexx.NewQuery(
			conn,
			`SELECT 1 FROM `+st.options.TablePrefix+`events
				  WHERE event_id = $event_id`,
		)
		if err != nil {
			return fmt.Errorf("verifying bookmark for watch %q: %w", ptr, err)
		}

		if err = q.
			BindInt64("$event_id", eventID).
			QueryRow(func(*sqlite.Stmt) error {
				return nil
			}); err != nil {
			if errors.Is(err, sqlitexx.ErrNoRows) {
				return fmt.Errorf("failed to watch %q: %w", ptr, ErrInvalidWatchBookmark(errors.New("bookmark refers to compacted event")))
			}

			return fmt.Errorf("verifying bookmark for watch %q: %w", ptr, err)
		}

		// as we start from a bookmark, mark the subscription as notified to make first event fetch
		sub.TriggerNotify()
	default:
		// figure out initial state of the watch process
		err := func() (err error) {
			defer sqlitex.Transaction(conn)(&err)

			var spec []byte

			q, err := sqlitexx.NewQuery(
				conn,
				`SELECT spec
					FROM `+st.options.TablePrefix+`resources
					WHERE namespace = $namespace AND type = $type AND id = $id`,
			)
			if err != nil {
				return fmt.Errorf("preparing query for initial resource state for watch %q: %w", ptr, err)
			}

			err = q.
				BindString("$namespace", ptr.Namespace()).
				BindString("$type", ptr.Type()).
				BindString("$id", ptr.ID()).
				QueryRow(
					func(stmt *sqlite.Stmt) error {
						spec = make([]byte, stmt.GetLen("spec"))
						stmt.GetBytes("spec", spec)

						return nil
					},
				)

			if err != nil && !errors.Is(err, sqlitexx.ErrNoRows) {
				return fmt.Errorf("querying initial resource state for watch %q: %w", ptr, err)
			}

			exists := spec != nil

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

			q, err = sqlitexx.NewQuery(
				conn,
				`SELECT coalesce(max(event_id), 0) AS max_event_id FROM `+st.options.TablePrefix+`events`,
			)
			if err != nil {
				return fmt.Errorf("preparing query for initial event ID for watch %q: %w", ptr, err)
			}

			err = q.
				QueryRow(
					func(stmt *sqlite.Stmt) error {
						eventID = stmt.GetInt64("max_event_id")

						return nil
					},
				)
			if err != nil {
				return fmt.Errorf("querying initial event ID for watch %q: %w", ptr, err)
			}

			initialEvent.Bookmark = encodeBookmark(eventID)

			return nil
		}()
		if err != nil {
			return err
		}
	}

	resourceNamespace, resourceType, resourceID := ptr.Namespace(), ptr.Type(), ptr.ID()
	watchSetupFailed = false

	go func() {
		defer sub.Unsubscribe()

		if initialEvent.Resource != nil {
			if !channel.SendWithContext(ctx, ch, initialEvent) {
				// If the channel is closed, we should stop the watch
				return
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.NotifyCh():
			}

			var events []state.Event

			if err := func() error {
				conn, err := st.db.Take(ctx)
				if err != nil {
					return fmt.Errorf("taking connection for watch event: %w", err)
				}

				defer st.db.Put(conn)

				q, err := sqlitexx.NewQuery(
					conn,
					`SELECT event_id, spec_before, spec_after, event_type
					FROM `+st.options.TablePrefix+`events
					WHERE event_id > $event_id AND namespace = $namespace AND type = $type AND id = $id
					ORDER BY event_id ASC`,
				)
				if err != nil {
					return fmt.Errorf("preparing query for watch %q events: %w", ptr, err)
				}

				err = q.
					BindInt64("$event_id", eventID).
					BindString("$namespace", resourceNamespace).
					BindString("$type", resourceType).
					BindString("$id", resourceID).
					QueryAll(
						func(stmt *sqlite.Stmt) error {
							specBefore := make([]byte, stmt.GetLen("spec_before"))
							stmt.GetBytes("spec_before", specBefore)

							specAfter := make([]byte, stmt.GetLen("spec_after"))
							stmt.GetBytes("spec_after", specAfter)

							newEventID := stmt.GetInt64("event_id")
							eventType := int(stmt.GetInt64("event_type"))

							eventID = newEventID

							event := st.convertEvent(ptr, eventID, specBefore, specAfter, eventType)
							if event.Type == state.Errored {
								return event.Error
							}

							events = append(events, event)

							return nil
						},
					)
				if err != nil {
					return fmt.Errorf("querying events for watch %q: %w", ptr, err)
				}

				return nil
			}(); err != nil {
				channel.SendWithContext(ctx, ch, state.Event{
					Type:  state.Errored,
					Error: fmt.Errorf("watching %q: %w", ptr, err),
				})
			}

			for _, event := range events {
				if !channel.SendWithContext(ctx, ch, event) {
					// If the context is canceled, we should stop the watch
					return
				}
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

//nolint:gocyclo,gocognit,cyclop,maintidx
func (st *State) watchKind(ctx context.Context, resourceKind resource.Kind, singleCh chan<- state.Event, aggCh chan<- []state.Event, opName string, opts ...state.WatchKindOption) error {
	var options state.WatchKindOptions

	for _, opt := range opts {
		opt(&options)
	}

	matches := func(res resource.Resource) bool {
		return options.LabelQueries.Matches(*res.Metadata().Labels()) && options.IDQuery.Matches(*res.Metadata())
	}

	labelQuerySQL := filter.CompileLabelQueries(options.LabelQueries)

	sub := st.sub.Subscribe(resourceKind)
	watchSetupFailed := true

	defer func() {
		if watchSetupFailed {
			sub.Unsubscribe()
		}
	}()

	conn, err := st.db.Take(ctx)
	if err != nil {
		return fmt.Errorf("taking connection for watch kind setup: %w", err)
	}

	defer st.db.Put(conn)

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

		// verify that we still have the event in the log
		q, err := sqlitexx.NewQuery(
			conn,
			`SELECT 1 FROM `+st.options.TablePrefix+`events
		WHERE event_id = $event_id`,
		)
		if err != nil {
			return fmt.Errorf("verifying bookmark for watch %q: %w", resourceKind, err)
		}

		if err = q.
			BindInt64("$event_id", eventID).
			QueryRow(func(*sqlite.Stmt) error { return nil }); err != nil {
			if errors.Is(err, sqlitexx.ErrNoRows) {
				return fmt.Errorf("failed to watch %q: %w", resourceKind, ErrInvalidWatchBookmark(errors.New("bookmark refers to compacted event")))
			}

			return fmt.Errorf("verifying bookmark for watch %q: %w", resourceKind, err)
		}

		// trigger notification to start fetching events from the bookmark
		sub.TriggerNotify()
	case options.BootstrapContents:
		// figure out initial state of the watch process
		err := func() (err error) {
			defer sqlitex.Transaction(conn)(&err)

			q, err := sqlitexx.NewQuery(
				conn,
				`SELECT spec
					FROM `+st.options.TablePrefix+`resources
					WHERE namespace = $namespace AND type = $type AND `+labelQuerySQL,
			)
			if err != nil {
				return fmt.Errorf("preparing query for initial resource state for watch %q: %w", resourceKind, err)
			}

			err = q.
				BindString("$namespace", resourceKind.Namespace()).
				BindString("$type", resourceKind.Type()).
				QueryAll(
					func(stmt *sqlite.Stmt) error {
						spec := make([]byte, stmt.GetLen("spec"))
						stmt.GetBytes("spec", spec)

						var res resource.Resource

						res, err = st.marshaler.UnmarshalResource(spec)
						if err != nil {
							return fmt.Errorf("failed to unmarshal resource of kind %q: %w", resourceKind, err)
						}

						if !matches(res) {
							return nil
						}

						bootstrapList = append(bootstrapList, res)

						return nil
					},
				)
			if err != nil {
				return fmt.Errorf("error querying resources of kind %q: %w", resourceKind, err)
			}

			q, err = sqlitexx.NewQuery(
				conn,
				`SELECT coalesce(max(event_id), 0) AS max_event_id FROM `+st.options.TablePrefix+`events`,
			)
			if err != nil {
				return fmt.Errorf("preparing query for initial event ID for watch %q: %w", resourceKind, err)
			}

			err = q.
				QueryRow(
					func(stmt *sqlite.Stmt) error {
						eventID = stmt.GetInt64("max_event_id")

						return nil
					},
				)
			if err != nil {
				return fmt.Errorf("querying initial event ID for watch %s: %w", resourceKind, err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	default:
		q, err := sqlitexx.NewQuery(
			conn,
			`SELECT coalesce(max(event_id), 0) AS max_event_id FROM `+st.options.TablePrefix+`events`,
		)
		if err != nil {
			return fmt.Errorf("preparing query for initial event ID for watch %s: %w", resourceKind, err)
		}

		err = q.
			QueryRow(
				func(stmt *sqlite.Stmt) error {
					eventID = stmt.GetInt64("max_event_id")

					return nil
				},
			)
		if err != nil {
			return fmt.Errorf("querying initial event ID for watch %s: %w", resourceKind, err)
		}
	}

	resourceNamespace, resourceType := resourceKind.Namespace(), resourceKind.Type()
	watchSetupFailed = false

	go func() {
		defer sub.Unsubscribe()

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

		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.NotifyCh():
			}

			var events []state.Event

			if queryErr := func() error {
				conn, err := st.db.Take(ctx)
				if err != nil {
					return fmt.Errorf("taking connection for watch kind event: %w", err)
				}

				defer st.db.Put(conn)

				q, err := sqlitexx.NewQuery(
					conn,
					`SELECT event_id, spec_before, spec_after, event_type
					FROM `+st.options.TablePrefix+`events
					WHERE event_id > $event_id AND namespace = $namespace AND type = $type
					ORDER BY event_id ASC`,
				)
				if err != nil {
					return fmt.Errorf("preparing query for watch %s events: %w", resourceKind, err)
				}

				err = q.
					BindInt64("$event_id", eventID).
					BindString("$namespace", resourceNamespace).
					BindString("$type", resourceType).
					QueryAll(
						func(stmt *sqlite.Stmt) error {
							specBefore := make([]byte, stmt.GetLen("spec_before"))
							stmt.GetBytes("spec_before", specBefore)

							specAfter := make([]byte, stmt.GetLen("spec_after"))
							stmt.GetBytes("spec_after", specAfter)

							newEventID := stmt.GetInt64("event_id")
							eventType := int(stmt.GetInt64("event_type"))

							eventID = newEventID

							event := st.convertEvent(resourceKind, eventID, specBefore, specAfter, eventType)
							if event.Type == state.Errored {
								return event.Error
							}

							switch event.Type {
							case state.Created, state.Destroyed:
								if !matches(event.Resource) {
									// skip the event
									return nil
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
									return nil
								}
							case state.Errored, state.Bootstrapped, state.Noop:
								panic("should never be reached")
							}

							events = append(events, event)

							return nil
						},
					)
				if err != nil {
					return fmt.Errorf("querying events for watch %s: %w", resourceKind, err)
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
