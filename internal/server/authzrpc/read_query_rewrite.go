package authzrpc

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// fastUserReadPage recognizes only the fixed policy's unconditional User bead
// read permit. It records the same shared dependencies once and evaluates
// refs/actions caveats exactly; every other expression falls back to the
// reference FilterTemplate interpreter.
func fastUserReadPage(ctx context.Context, caller authz.Caller, facts *slice, template authz.FilterTemplate, items []service.Bead) ([]service.Bead, bool, error) {
	if template.NoPlugins || template.Request.Action != authz.Read || template.Request.ResourceKind != authz.Bead || !userPermit(template.Permit) || !falseExpr(template.Forbid) || len(template.Restrictions) != 0 {
		return nil, false, nil
	}
	if caller.Principal.Kind != "user" || caller.Principal.Sponsor != nil || !authz.ValidID(caller.Principal.ID) || !authz.ValidID(caller.VerificationID) || caller.CapturedEpoch.Validate() != nil {
		return nil, false, nil
	}
	for _, dep := range facts.dependencies() {
		if err := facts.Record(ctx, dep); err != nil {
			return nil, true, err
		}
	}
	bound, err := facts.CheckContext(ctx, caller)
	if err != nil {
		return nil, true, err
	}
	if !bound {
		return []service.Bead{}, true, nil
	}

	var refs map[string]struct{}
	for current := &caller.Principal; current != nil; current = current.Sponsor {
		for key, value := range current.Caveats {
			raw, err := json.Marshal(value)
			if err != nil {
				return nil, false, nil
			}
			switch key {
			case "refs":
				if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
					return nil, false, nil
				}
				var values []model.Ref
				if err := json.Unmarshal(raw, &values); err != nil {
					return nil, false, nil
				}
				currentRefs := make(map[string]struct{}, len(values))
				for _, ref := range values {
					if err := authz.CanonicalRef(ref); err != nil {
						return nil, false, nil
					}
					currentRefs[ref.ID] = struct{}{}
				}
				if refs == nil {
					refs = currentRefs
				} else {
					for id := range refs {
						if _, ok := currentRefs[id]; !ok {
							delete(refs, id)
						}
					}
				}
			case "actions":
				if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
					return nil, false, nil
				}
				var actions []string
				if err := json.Unmarshal(raw, &actions); err != nil {
					return nil, false, nil
				}
				found := false
				for _, action := range actions {
					if action == string(authz.Read) {
						found = true
					}
				}
				if !found {
					return []service.Bead{}, true, nil
				}
			default:
				// projects and provider-specific caveats require target-project
				// resolution; retain the ordinary interpreter for those forms.
				return nil, false, nil
			}
		}
	}
	filtered := make([]service.Bead, 0, len(items))
	for _, item := range items {
		if refs != nil {
			if _, ok := refs[item.ID]; !ok {
				continue
			}
		}
		filtered = append(filtered, item)
	}
	return filtered, true, nil
}

func userPermit(expr authz.FilterExpr) bool {
	switch expr.Op {
	case authz.PrincipalKind:
		return expr.Kind == "user"
	case authz.Any:
		found := false
		for _, arg := range expr.Args {
			if falseExpr(arg) {
				continue
			}
			if arg.Op != authz.PrincipalKind || arg.Kind != "user" || found {
				return false
			}
			found = true
		}
		return found
	default:
		return false
	}
}

func falseExpr(expr authz.FilterExpr) bool {
	switch expr.Op {
	case authz.False:
		return true
	case authz.Any:
		for _, arg := range expr.Args {
			if !falseExpr(arg) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
