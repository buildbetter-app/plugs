package authzrpc

import (
	"encoding/json"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Compatibility routing is inspection only, with no source/preparation calls.
// Every valid unsupported template and every project caveat uses the complete
// reference loop. They are never relabeled ErrCheckOnly or submitted to SQL.
func readySQLCompatible(t authz.FilterTemplate, items []service.Bead) bool {
	if t.NoPlugins || t.Request.Action != authz.Read || t.Request.ResourceKind != authz.Bead || t.Request.Method != "bead.ready" {
		return false
	}
	var supported func(authz.FilterExpr) bool
	supported = func(f authz.FilterExpr) bool {
		switch f.Op {
		case authz.True, authz.False, authz.PrincipalKind, authz.SponsorKind:
			return true
		case authz.All, authz.Any:
			for _, a := range f.Args {
				if !supported(a) {
					return false
				}
			}
			return true
		}
		return false
	}
	if !supported(t.Permit) || !supported(t.Forbid) {
		return false
	}
	for _, f := range t.Restrictions {
		if !supported(f) {
			return false
		}
	}
	for p := &t.Request.Caller.Principal; p != nil; p = p.Sponsor {
		// Invalid identities are ordinary reference denials. In particular,
		// do not bind unvalidated kind bytes into an engine text parameter:
		// PostgreSQL cannot encode NUL, even when every Prepared bit is false.
		switch p.Kind {
		case "anonymous", "user", "agent", "service", "plugin", "system":
		default:
			return false
		}
		if _, present := p.Caveats["projects"]; present {
			return false
		}
	}
	for _, b := range items {
		if authz.CanonicalRef(model.RefByID(b.ID, model.Natural64(0))) != nil || strings.Contains(b.ID, "/") {
			return false
		}
	}
	return true
}

func readyStorageExpr(f authz.FilterExpr) driver.SelectionExpr {
	out := driver.SelectionExpr{Kind: f.Kind}
	switch f.Op {
	case authz.True:
		out.Op = driver.SelectionTrue
	case authz.False:
		out.Op = driver.SelectionFalse
	case authz.PrincipalKind:
		out.Op = driver.SelectionPrincipalKind
	case authz.SponsorKind:
		out.Op = driver.SelectionSponsorKind
	case authz.All, authz.Any:
		out.Op = driver.SelectionAll
		if f.Op == authz.Any {
			out.Op = driver.SelectionAny
		}
		for _, a := range f.Args {
			out.Args = append(out.Args, readyStorageExpr(a))
		}
	default:
		panic("unsupported template passed the closed Ready gate")
	}
	return out
}

// Called after reference demand replay, never before it. No project read or
// absence semantics are synthesized. Invalid/unknown caveats deny exactly as
// caveatsAllow; project caveats never reach this helper.
func readyStorageCaveats(p model.PrincipalRef) driver.SelectionExpr {
	all := driver.SelectionExpr{Op: driver.SelectionAll}
	deny := driver.SelectionExpr{Op: driver.SelectionFalse}
	for current := &p; current != nil; current = current.Sponsor {
		for key, value := range current.Caveats {
			raw, err := json.Marshal(value)
			if err != nil || string(raw) == "null" {
				return deny
			}
			switch key {
			case "actions":
				var actions []string
				if json.Unmarshal(raw, &actions) != nil {
					return deny
				}
				found := false
				for _, action := range actions {
					if action == string(authz.Read) {
						found = true
					}
				}
				if !found {
					return deny
				}
			case "refs":
				var refs []model.Ref
				if json.Unmarshal(raw, &refs) != nil {
					return deny
				}
				ids := driver.SelectionExpr{Op: driver.SelectionIDs}
				for _, ref := range refs {
					if authz.CanonicalRef(ref) != nil {
						return deny
					}
					ids.IDs = append(ids.IDs, ref.ID)
				}
				all.Args = append(all.Args, ids)
			default:
				return deny
			}
		}
	}
	return all
}

func readyStoragePredicate(t authz.FilterTemplate) driver.SelectionExpr {
	out := driver.SelectionExpr{Op: driver.SelectionAll}
	out.Args = append(out.Args, readyStorageCaveats(t.Request.Caller.Principal))
	for _, f := range t.Restrictions {
		out.Args = append(out.Args, readyStorageExpr(f))
	}
	out.Args = append(out.Args,
		driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{readyStorageExpr(t.Forbid)}},
		readyStorageExpr(t.Permit),
	)
	return out
}
