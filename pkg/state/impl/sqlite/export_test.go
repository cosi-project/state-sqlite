// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

// EmptySubscriptions checks whether there are any active subscriptions in the manager.
//
// Used in tests assertions.
func (st *State) EmptySubscriptions() bool {
	return st.sub.Empty()
}
