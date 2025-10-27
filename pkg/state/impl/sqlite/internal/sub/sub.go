// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package sub contains simple subscription management.
package sub

import (
	"slices"
	"sync"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/siderolabs/gen/xslices"
)

// Manager defines a subscription manager.
type Manager struct {
	subscriptions map[key][]chan struct{}
	mu            sync.Mutex
}

type key struct {
	ns  resource.Namespace
	typ resource.Type
}

type subscription struct {
	ch  chan struct{}
	m   *Manager
	key key
}

// Subscription is an active subscription interface.
type Subscription interface {
	NotifyCh() <-chan struct{}
	TriggerNotify()
	Unsubscribe()
}

// NewManager creates a new subscription manager.
func NewManager() *Manager {
	return &Manager{
		subscriptions: make(map[key][]chan struct{}),
	}
}

// Subscribe creates a new subscription for the given resource kind.
func (m *Manager) Subscribe(resourceKind resource.Kind) Subscription {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := key{
		ns:  resourceKind.Namespace(),
		typ: resourceKind.Type(),
	}

	ch := make(chan struct{}, 1)

	m.subscriptions[k] = append(m.subscriptions[k], ch)

	return &subscription{
		ch:  ch,
		key: k,
		m:   m,
	}
}

// Notify notifies all subscribers about an event for the given resource kind.
func (m *Manager) Notify(resourceKind resource.Kind) {
	k := key{
		ns:  resourceKind.Namespace(),
		typ: resourceKind.Type(),
	}

	m.mu.Lock()
	subs := slices.Clone(m.subscriptions[k])
	m.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Empty checks whether there are any subscriptions.
func (m *Manager) Empty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.subscriptions) == 0
}

// NotifyCh implements Subscription interface.
func (s *subscription) NotifyCh() <-chan struct{} {
	return s.ch
}

// TriggerNotify implements Subscription interface.
func (s *subscription) TriggerNotify() {
	select {
	case s.ch <- struct{}{}:
	default:
	}
}

// Unsubscribe implements Subscription interface.
func (s *subscription) Unsubscribe() {
	s.m.mu.Lock()
	defer s.m.mu.Unlock()

	s.m.subscriptions[s.key] = xslices.FilterInPlace(s.m.subscriptions[s.key],
		func(ch chan struct{}) bool {
			return ch != s.ch
		},
	)

	if len(s.m.subscriptions[s.key]) == 0 {
		delete(s.m.subscriptions, s.key)
	}
}
