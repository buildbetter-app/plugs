package authz

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// ValidateBeadWrite is a host adapter guard, not a write implementation. Before
// MUST be the stored source at the authorization snapshot; After is the fully
// proposed source after mutators. Run it before dispatch and after mutation.
// Claim eligibility/leases, ontology, setters and transaction guards remain
// host responsibilities. This component grants no deputy or transition right.
func ValidateBeadWrite(method string, before, after model.Record, setter model.PrincipalRef, caller Caller, required []Action) error {
	if method != "bead.claim" && method != "bead.claimNext" && method != "bead.update" {
		return errors.New("unsupported authorization write adapter")
	}
	if caller.Principal.Kind != "agent" || !reflect.DeepEqual(setter, caller.Principal) {
		return errors.New("deputy or non-Agent write is unsupported")
	}
	if CanonicalRef(before.Ref) != nil || CanonicalRef(after.Ref) != nil || strings.Contains(before.Ref.ID, "/") || before.Ref.ID != after.Ref.ID || before.Kind != "bead" || after.Kind != before.Kind || before.Body == nil || after.Body == nil {
		return errors.New("existing canonical bead images required")
	}
	for _, key := range []string{"project", "parent", "owner", "assignee", "type", "labels", "restricted"} {
		b, bok := before.Body[key]
		a, aok := after.Body[key]
		if bok != aok || !reflect.DeepEqual(b, a) {
			return errors.New("permission-bearing reassignment is unsupported: " + key)
		}
	}
	p, ok := before.Body["project"].(string)
	if !ok || !ValidID(p) {
		return errors.New("explicit stored project assignment required")
	}
	if p, present := before.Body["parent"]; present && p != nil {
		encoded, err := json.Marshal(p)
		var r model.Ref
		if err != nil || json.Unmarshal(encoded, &r) != nil || CanonicalRef(r) != nil || strings.Contains(r.ID, "/") {
			return errors.New("canonical stored parent required")
		}
	}
	for _, a := range required {
		if a != Modify {
			return errors.New("unmapped permission denies: " + string(a))
		}
	}
	return nil
}
