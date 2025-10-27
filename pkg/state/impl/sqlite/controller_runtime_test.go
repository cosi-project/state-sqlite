// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite_test

import (
	"testing"

	"github.com/cosi-project/runtime/pkg/controller/conformance"
	"github.com/cosi-project/runtime/pkg/controller/runtime"
	"github.com/cosi-project/runtime/pkg/logging"
	"github.com/cosi-project/runtime/pkg/resource/protobuf"
	"github.com/cosi-project/runtime/pkg/state"
	tmust "github.com/siderolabs/gen/xtesting/must"
	suiterunner "github.com/stretchr/testify/suite"
)

func init() {
	must(protobuf.RegisterResource(conformance.IntResourceType, &conformance.IntResource{}))
	must(protobuf.RegisterResource(conformance.StrResourceType, &conformance.StrResource{}))
	must(protobuf.RegisterResource(conformance.SentenceResourceType, &conformance.SentenceResource{}))
}

func TestRuntimeConformance(t *testing.T) {
	t.Parallel()

	suite := &conformance.RuntimeSuite{
		SetupRuntime: func(rs *conformance.RuntimeSuite) {
			withSqlite(t, func(st state.State) {
				rs.State = st
			})

			rs.Runtime = tmust.Value(runtime.NewRuntime(rs.State, logging.DefaultLogger()))(rs.T())
		},
	}

	suiterunner.Run(t, suite)
}
