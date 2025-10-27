// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sub_test

import (
	"testing"

	"github.com/cosi-project/runtime/pkg/resource"

	"github.com/cosi-project/state-sqlite/pkg/state/impl/sqlite/internal/sub"
)

func TestManager(t *testing.T) {
	t.Parallel()

	m := sub.NewManager()

	s1 := m.Subscribe(resource.NewMetadata("ns1", "t1", "", resource.VersionUndefined))
	s2 := m.Subscribe(resource.NewMetadata("ns1", "t1", "", resource.VersionUndefined))

	select {
	case <-s1.NotifyCh():
		t.Fatal("unexpected notification")
	case <-s2.NotifyCh():
		t.Fatal("unexpected notification")
	default:
	}

	m.Notify(resource.NewMetadata("ns1", "t2", "", resource.VersionUndefined))

	select {
	case <-s1.NotifyCh():
		t.Fatal("unexpected notification")
	case <-s2.NotifyCh():
		t.Fatal("unexpected notification")
	default:
	}

	m.Notify(resource.NewMetadata("ns1", "t1", "", resource.VersionUndefined))
	m.Notify(resource.NewMetadata("ns1", "t1", "", resource.VersionUndefined))

	select {
	case <-s1.NotifyCh():
	default:
		t.Fatal("expected notification")
	}

	select {
	case <-s2.NotifyCh():
	default:
		t.Fatal("expected notification")
	}

	s1.Unsubscribe()

	m.Notify(resource.NewMetadata("ns1", "t1", "", resource.VersionUndefined))

	select {
	case <-s2.NotifyCh():
	default:
		t.Fatal("expected notification")
	}

	select {
	case <-s1.NotifyCh():
		t.Fatal("unexpected notification")
	default:
	}
}
