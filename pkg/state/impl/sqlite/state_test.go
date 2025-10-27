// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/conformance"
)

func TestSqliteConformance(t *testing.T) {
	t.Cleanup(
		func() { goleak.VerifyNone(t, goleak.IgnoreCurrent()) },
	)

	withSqlite(t, func(s state.State) {
		suite.Run(t, &conformance.StateSuite{
			State:      s,
			Namespaces: []resource.Namespace{"default", "controller", "system", "runtime"},
		})
	})
}
