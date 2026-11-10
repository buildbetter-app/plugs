package service

import (
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/port"
)

// An explicitly null waiter list clears to [], as the native typed list patch
// does. An omitted member has no patch. This hook only canonicalizes the
// caller's selected member; bounds and entry types are validated afterward.
func normalizeAwaitWaiters(change port.Change) (port.Patch, bool) {
	var fields map[string]any
	if change.Op.Create != nil {
		fields = change.Op.Create.Fields
	} else if change.Op.Patch != nil {
		fields = change.Op.Patch.Fields
	}
	value, selected := fields["waiters"]
	if !selected || value != nil {
		return port.Patch{}, false
	}
	return port.Patch{Resource: change.Resource, Paths: map[string]json.RawMessage{"fields.waiters": json.RawMessage(`[]`)}}, true
}
