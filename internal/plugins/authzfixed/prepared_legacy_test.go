package authzfixed

// Frozen 8dab45e Match/finite Eval/Compile oracle. Do not route it through new
// preparation or optimized Eval; the existing independent CP7 truth table is
// retained separately. This is test-only reference code, not a second policy.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

func legacyMatch(t authz.FilterTemplate, ctx context.Context, s authz.Slice, r authz.Resource) (bool, error) {
	if err := t.Validate(); err != nil {
		return false, legacyUnavailable(err)
	}
	if t.NoPlugins {
		if legacyScoped(authz.CheckRequest{Caller: t.Request.Caller}) {
			return false, &authz.Error{Code: "authz_not_installed"}
		}
		return true, nil
	}
	if r.Kind != t.Request.ResourceKind {
		return false, nil
	}
	req := authz.CheckRequest{Caller: t.Request.Caller, Action: t.Request.Action, Resource: r, Method: t.Request.Method}
	ok, err := legacyPrepare(ctx, s, t.Namespace, req)
	if err != nil || !ok {
		return false, err
	}
	ok, err = legacyCaveatsAllow(ctx, s, req)
	if err != nil || !ok {
		return false, err
	}
	for _, f := range t.Restrictions {
		ok, err = legacyEvalExpr(ctx, s, req, f)
		if err != nil {
			return false, legacyUnavailable(err)
		}
		if !ok {
			return false, nil
		}
	}
	deny, err := legacyEvalExpr(ctx, s, req, t.Forbid)
	if err != nil {
		return false, legacyUnavailable(err)
	}
	if deny {
		return false, nil
	}
	ok, err = legacyEvalExpr(ctx, s, req, t.Permit)
	if err != nil {
		return false, legacyUnavailable(err)
	}
	return ok, nil
}

func legacyPrincipalKind(k string) bool {
	switch k {
	case "anonymous", "user", "agent", "service", "plugin", "system":
		return true
	}
	return false
}

func legacyPrepare(ctx context.Context, s authz.Slice, n authz.Namespace, r authz.CheckRequest) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, legacyUnavailable(err)
	}
	if s == nil || !legacyNamespace(s.Namespace(), n) {
		return false, legacyUnavailable(errors.New("authorization namespace mismatch"))
	}
	if err := n.Validate(); err != nil {
		return false, legacyUnavailable(err)
	}
	c := r.Caller
	p := c.Principal
	if !authz.ValidID(c.VerificationID) || c.CapturedEpoch.Validate() != nil || !authz.ValidID(p.ID) || !legacyPrincipalKind(p.Kind) {
		return false, nil
	}
	if p.Sponsor != nil && (p.Kind != "agent" || p.Sponsor.Sponsor != nil || !authz.ValidID(p.Sponsor.ID) || (p.Sponsor.Kind != "user" && p.Sponsor.Kind != "service")) {
		return false, nil
	}
	if r.Resource.Kind != authz.Bead && r.Resource.Kind != authz.Project && r.Resource.Kind != authz.History {
		return false, nil
	}
	if authz.CanonicalRef(r.Resource.Ref) != nil {
		return false, nil
	}
	if r.Resource.Kind == authz.Bead && strings.Contains(r.Resource.Ref.ID, "/") {
		return false, nil
	}
	for _, d := range []authz.Dependency{authz.ContextDependency(n), authz.ACLDependency(n), {Kind: "sentinel", Key: "plugins"}, {Kind: "sentinel", Key: "ontology"}} {
		if err := s.Record(ctx, d); err != nil {
			return false, legacyUnavailable(err)
		}
	}
	ok, err := s.CheckContext(ctx, c)
	if err != nil {
		return false, legacyUnavailable(err)
	}
	return ok, nil
}

func legacySubject(r authz.CheckRequest, b authz.SubjectBinding) (authz.SubjectRef, bool) {
	p := &r.Caller.Principal
	if b == authz.SponsorSubject {
		p = p.Sponsor
	}
	if p == nil || p.Kind != "user" {
		return authz.SubjectRef{}, false
	}
	return authz.SubjectRef{Kind: "user", ID: p.ID}, true
}

func legacyTargetProject(ctx context.Context, s authz.Slice, r authz.Resource) (model.Ref, bool, error) {
	if r.Kind == authz.Project {
		return r.Ref, true, nil
	}
	if r.Kind != authz.Bead {
		return model.Ref{}, false, nil
	}
	for _, key := range []string{"bead/" + r.Ref.ID, "closure/" + r.Ref.ID} {
		if err := s.Record(ctx, authz.Dependency{Kind: "sentinel", Key: key}); err != nil {
			return model.Ref{}, false, err
		}
	}
	p, err := s.ProjectForBead(ctx, r.Ref)
	if err != nil || !p.Found {
		return model.Ref{}, false, err
	}
	if !authz.ValidID(p.Name) {
		return model.Ref{}, false, errors.New("invalid stored project name")
	}
	if err := authz.CanonicalRef(p.Ref); err != nil {
		return model.Ref{}, false, err
	}
	return p.Ref, true, nil
}

func legacyHasGrant(ctx context.Context, s authz.Slice, sub authz.SubjectRef, scope authz.Scope, role string) (bool, error) {
	d, err := authz.GrantDependency(s.Namespace(), scope)
	if err != nil {
		return false, err
	}
	if err := s.Record(ctx, d); err != nil {
		return false, err
	}
	return s.HasGrant(ctx, sub, scope, role)
}

func legacyEvalExpr(ctx context.Context, s authz.Slice, r authz.CheckRequest, f authz.FilterExpr) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	switch f.Op {
	case authz.False:
		return false, nil
	case authz.True:
		return true, nil
	case authz.All, authz.Any:
		for _, a := range f.Args {
			v, err := legacyEvalExpr(ctx, s, r, a)
			if err != nil {
				return false, err
			}
			if f.Op == authz.All && !v {
				return false, nil
			}
			if f.Op == authz.Any && v {
				return true, nil
			}
		}
		return f.Op == authz.All, nil
	case authz.PrincipalKind:
		return r.Caller.Principal.Kind == f.Kind, nil
	case authz.SponsorKind:
		p := r.Caller.Principal.Sponsor
		return p != nil && p.Kind == f.Kind, nil
	case authz.GrantExists:
		sub, ok := legacySubject(r, f.Subject)
		if !ok {
			return false, nil
		}
		scope := authz.Scope{Kind: authz.AuthorityScope, Ref: s.Namespace().Authority}
		if f.Scope == authz.ProjectBindingScope {
			p, found, err := legacyTargetProject(ctx, s, r.Resource)
			if err != nil || !found {
				return false, err
			}
			scope = authz.Scope{Kind: authz.ProjectScope, Ref: p}
		}
		return legacyHasGrant(ctx, s, sub, scope, f.Role)
	case authz.ActiveProject:
		p, ok, err := legacyTargetProject(ctx, s, r.Resource)
		if err != nil || !ok {
			return false, err
		}
		return s.ProjectActive(ctx, p)
	case authz.TeamInvolvement:
		sub, ok := legacySubject(r, f.Subject)
		if !ok {
			return false, nil
		}
		p, found, err := legacyTargetProject(ctx, s, r.Resource)
		if err != nil || !found {
			return false, err
		}
		teams, err := s.Groups(ctx, sub)
		if err != nil {
			return false, err
		}
		for _, t := range teams {
			if err := authz.CanonicalRef(t); err != nil {
				return false, err
			}
			member, err := legacyHasGrant(ctx, s, sub, authz.Scope{Kind: authz.TeamScope, Ref: t}, "member")
			if err != nil {
				return false, err
			}
			if !member {
				continue
			}
			active, err := s.TeamActive(ctx, t)
			if err != nil {
				return false, err
			}
			if !active {
				continue
			}
			involved, err := legacyHasGrant(ctx, s, authz.SubjectRef{Kind: "team", ID: t.ID}, authz.Scope{Kind: authz.ProjectScope, Ref: p}, "involved")
			if err != nil {
				return false, err
			}
			if involved {
				return true, nil
			}
		}
		return false, nil
	}
	return false, fmt.Errorf("unsupported filter node %q", f.Op)
}

// The finite caveat boundary accepts actions ([]string), projects and refs
// ([]Ref). Omission is unrestricted; a present empty list permits nothing.
// Unknown fields/forms fail closed, including unsupported provider caveats.
// These are internal normalized values, not a new token or wire schema.
func legacyCaveatsAllow(ctx context.Context, s authz.Slice, r authz.CheckRequest) (bool, error) {
	for p := &r.Caller.Principal; p != nil; p = p.Sponsor {
		for k, v := range p.Caveats {
			switch k {
			case "actions":
				b, err := json.Marshal(v)
				if err != nil {
					return false, nil
				}
				var xs []string
				if string(b) == "null" || json.Unmarshal(b, &xs) != nil {
					return false, nil
				}
				found := false
				for _, x := range xs {
					if x == string(r.Action) {
						found = true
					}
				}
				if !found {
					return false, nil
				}
			case "projects", "refs":
				b, err := json.Marshal(v)
				if err != nil {
					return false, nil
				}
				var xs []model.Ref
				if string(b) == "null" || json.Unmarshal(b, &xs) != nil {
					return false, nil
				}
				target := r.Resource.Ref
				if k == "projects" {
					var ok bool
					target, ok, err = legacyTargetProject(ctx, s, r.Resource)
					if err != nil {
						return false, legacyUnavailable(err)
					}
					if !ok {
						return false, nil
					}
				}
				found := false
				for _, x := range xs {
					if authz.CanonicalRef(x) != nil {
						return false, nil
					}
					if x.ID == target.ID {
						found = true
					}
				}
				if !found {
					return false, nil
				}
			default:
				return false, nil
			}
		}
	}
	return true, nil
}

func legacyFixedCompile(n authz.Namespace, req authz.ListRequest) (authz.FilterTemplate, error) {
	req, err := legacyFreezeList(req)
	if err != nil {
		return authz.FilterTemplate{}, err
	}
	f := authz.FilterExpr{Op: authz.False}
	user := authz.FilterExpr{Op: authz.PrincipalKind, Kind: "user"}
	switch {
	case req.Action == authz.Read && req.ResourceKind == authz.Bead && legacyReadMethod(req.Method):
		f = user
	case req.Action == authz.ImportBulkHistory && req.ResourceKind == authz.History && req.Method == "":
		f = authz.And(user, authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.AuthorityBinding, Role: "admin"})
	case req.Action == authz.Modify && ((req.ResourceKind == authz.Project && req.Method == "") || (req.ResourceKind == authz.Bead && legacyWriteMethod(req.Method))):
		f = authz.And(authz.FilterExpr{Op: authz.PrincipalKind, Kind: "agent"}, authz.FilterExpr{Op: authz.SponsorKind, Kind: "user"}, authz.FilterExpr{Op: authz.ActiveProject}, authz.Or(
			authz.FilterExpr{Op: authz.GrantExists, Subject: authz.SponsorSubject, Scope: authz.ProjectBindingScope, Role: "involved"},
			authz.FilterExpr{Op: authz.TeamInvolvement, Subject: authz.SponsorSubject},
		))
	}
	t := authz.FilterTemplate{Namespace: n, Request: req, Permit: f, Forbid: authz.FilterExpr{Op: authz.False}}
	return t, t.Validate()
}

func legacyReadMethod(m string) bool {
	switch m {
	case "", "bead.get", "bead.list", "bead.ready", "view.run":
		return true
	}
	return false
}
func legacyWriteMethod(m string) bool {
	return m == "bead.claim" || m == "bead.claimNext" || m == "bead.update"
}

func legacyFixedEval(n authz.Namespace, ctx context.Context, s authz.Slice, reqs []authz.CheckRequest) ([]authz.Decision, error) {
	out := make([]authz.Decision, len(reqs))
	for i, q := range reqs {
		t, err := legacyFixedCompile(n, authz.ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind, Method: q.Method})
		if err != nil {
			return nil, err
		}
		ok, err := legacyMatch(t, ctx, s, q.Resource)
		if err != nil {
			return nil, err
		}
		out[i].Effect = authz.NoDecision
		if ok {
			out[i].Effect = authz.Allow
			out[i].Reasons = []string{"cp7." + string(q.Action)}
		}
	}
	return out, nil
}

func legacyNamespace(a, b authz.Namespace) bool {
	return a.Validate() == nil && b.Validate() == nil && a.StoreID == b.StoreID && a.Incarnation == b.Incarnation && a.InstallationID == b.InstallationID && a.Authority.ID == b.Authority.ID
}
func legacyScoped(r authz.CheckRequest) bool {
	depth := 0
	for p := &r.Caller.Principal; p != nil; p = p.Sponsor {
		if depth > 1 || len(p.Caveats) > 0 {
			return true
		}
		depth++
	}
	return false
}
func legacyUnavailable(err error) error {
	return &authz.Error{Code: "authz_unavailable", Retryable: true, Cause: err}
}
func legacyFreezeList(r authz.ListRequest) (authz.ListRequest, error) {
	if p := r.Caller.Principal.Sponsor; p != nil && p.Sponsor != nil {
		return authz.ListRequest{}, errors.New("nested sponsor is unsupported")
	}
	b, err := json.Marshal(r.Caller.Principal)
	if err != nil {
		return authz.ListRequest{}, err
	}
	if err = json.Unmarshal(b, &r.Caller.Principal); err != nil {
		return authz.ListRequest{}, err
	}
	return r, nil
}
