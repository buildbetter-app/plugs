package authz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// FilterOp is a closed, engine-neutral logical vocabulary, not a user policy
// language. Only trusted compiled-in components construct these expressions.
// A driver must reject an unknown/unsupported node rather than post-filter a
// short page or partial count. TeamInvolvement is the explicit one-hop semi-join
// member(User,T) AND active(T) AND involved(Team T,P), never a recursive walk.
type FilterOp string

const (
	False           FilterOp = "false"
	True            FilterOp = "true"
	All             FilterOp = "all"
	Any             FilterOp = "any"
	PrincipalKind   FilterOp = "principal_kind"
	SponsorKind     FilterOp = "sponsor_kind"
	GrantExists     FilterOp = "grant_exists"
	ActiveProject   FilterOp = "active_project"
	TeamInvolvement FilterOp = "team_involvement"
)

type SubjectBinding string

const (
	CallerSubject  SubjectBinding = "caller"
	SponsorSubject SubjectBinding = "sponsor"
)

type ScopeBinding string

const (
	AuthorityBinding    ScopeBinding = "authority"
	ProjectBindingScope ScopeBinding = "target_project"
)

type FilterExpr struct {
	Op      FilterOp
	Args    []FilterExpr
	Kind    string
	Subject SubjectBinding
	Scope   ScopeBinding
	Role    string
}

func And(args ...FilterExpr) FilterExpr { return FilterExpr{Op: All, Args: args} }
func Or(args ...FilterExpr) FilterExpr  { return FilterExpr{Op: Any, Args: args} }

func (f FilterExpr) Validate() error {
	leaf := len(f.Args) == 0
	switch f.Op {
	case All, Any:
		if f.Kind != "" || f.Subject != "" || f.Scope != "" || f.Role != "" {
			return errors.New("invalid boolean filter fields")
		}
		for _, a := range f.Args {
			if err := a.Validate(); err != nil {
				return err
			}
		}
		return nil
	case False, True, ActiveProject:
		if leaf && f.Kind == "" && f.Subject == "" && f.Scope == "" && f.Role == "" {
			return nil
		}
	case PrincipalKind, SponsorKind:
		if leaf && validPrincipalKind(f.Kind) && f.Subject == "" && f.Scope == "" && f.Role == "" {
			return nil
		}
	case GrantExists:
		if leaf && f.Kind == "" && (f.Subject == CallerSubject || f.Subject == SponsorSubject) && (f.Scope == AuthorityBinding || f.Scope == ProjectBindingScope) && ValidID(f.Role) {
			return nil
		}
	case TeamInvolvement:
		if leaf && f.Kind == "" && (f.Subject == CallerSubject || f.Subject == SponsorSubject) && f.Scope == "" && f.Role == "" {
			return nil
		}
	}
	return fmt.Errorf("unsupported or invalid filter node %q", f.Op)
}

// FilterTemplate is bound per request, never to cached effective grants. The
// finite expressions retain live relations, including empty enumerations.
// Both permits and forbids are necessary: a forbid always wins. Restrictions
// (code-link filters and caveats) intersect, and can never supply a permit.
type FilterTemplate struct {
	NoPlugins      bool
	Namespace      Namespace
	Request        ListRequest
	Permit, Forbid FilterExpr
	Restrictions   []FilterExpr
	prepared       *PreparedRequest
}

func (t FilterTemplate) Validate() error {
	if err := t.Permit.Validate(); err != nil {
		return err
	}
	if err := t.Forbid.Validate(); err != nil {
		return err
	}
	if t.NoPlugins {
		if t.Permit.Op != True || t.Forbid.Op != False || len(t.Restrictions) != 0 {
			return errors.New("invalid unrestricted filter")
		}
		return nil
	}
	if err := t.Namespace.Validate(); err != nil {
		return err
	}
	for _, f := range t.Restrictions {
		if err := f.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Match is the reference interpreter for future engine adapters. It does not
// execute SQL or certify a transaction. Every call validates current context
// and facts in the supplied snapshot, including after revocation.
func (t FilterTemplate) Match(ctx context.Context, s Slice, r Resource) (bool, error) {
	if err := t.Validate(); err != nil {
		return false, unavailable(err)
	}
	if t.NoPlugins {
		if scoped(CheckRequest{Caller: t.Request.Caller}) {
			return false, &Error{Code: "authz_not_installed"}
		}
		return true, nil
	}
	if r.Kind != t.Request.ResourceKind {
		return false, nil
	}
	if t.prepared != nil && reflect.DeepEqual(t.Request, t.prepared.request) {
		return t.matchPrepared(ctx, s, r, t.prepared, snapshotPreparation(ctx, s))
	}
	req := CheckRequest{Caller: t.Request.Caller, Action: t.Request.Action, Resource: r, Method: t.Request.Method}
	ok, err := prepare(ctx, s, t.Namespace, req)
	if err != nil || !ok {
		return false, err
	}
	ok, err = caveatsAllow(ctx, s, req)
	if err != nil || !ok {
		return false, err
	}
	for _, f := range t.Restrictions {
		ok, err = evalExpr(ctx, s, req, f)
		if err != nil {
			return false, unavailable(err)
		}
		if !ok {
			return false, nil
		}
	}
	deny, err := evalExpr(ctx, s, req, t.Forbid)
	if err != nil {
		return false, unavailable(err)
	}
	if deny {
		return false, nil
	}
	ok, err = evalExpr(ctx, s, req, t.Permit)
	if err != nil {
		return false, unavailable(err)
	}
	return ok, nil
}

func (t FilterTemplate) matchPrepared(ctx context.Context, s Slice, r Resource, p *PreparedRequest, state *Preparation) (bool, error) {
	if r.Kind != p.request.ResourceKind {
		return false, nil
	}
	if t.prepared != nil && t.prepared.freshObservations {
		state = nil
	}
	ok, err := p.prepare(ctx, s, t.Namespace, r, state)
	if err != nil || !ok {
		return false, err
	}
	ok, err = p.caveatsAllow(ctx, s, r)
	if err != nil || !ok {
		return false, err
	}
	req := CheckRequest{Caller: p.request.Caller, Action: p.request.Action, Resource: r, Method: p.request.Method}
	for _, f := range t.Restrictions {
		ok, err = evalExpr(ctx, s, req, f)
		if err != nil {
			return false, unavailable(err)
		}
		if !ok {
			return false, nil
		}
	}
	deny, err := evalExpr(ctx, s, req, t.Forbid)
	if err != nil {
		return false, unavailable(err)
	}
	if deny {
		return false, nil
	}
	ok, err = evalExpr(ctx, s, req, t.Permit)
	if err != nil {
		return false, unavailable(err)
	}
	return ok, nil
}

func validPrincipalKind(k string) bool {
	switch k {
	case "anonymous", "user", "agent", "service", "plugin", "system":
		return true
	}
	return false
}

func prepare(ctx context.Context, s Slice, n Namespace, r CheckRequest) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, unavailable(err)
	}
	if s == nil || !sameNamespace(s.Namespace(), n) {
		return false, unavailable(errors.New("authorization namespace mismatch"))
	}
	if err := n.Validate(); err != nil {
		return false, unavailable(err)
	}
	c := r.Caller
	p := c.Principal
	if !ValidID(c.VerificationID) || c.CapturedEpoch.Validate() != nil || !ValidID(p.ID) || !validPrincipalKind(p.Kind) {
		return false, nil
	}
	if p.Sponsor != nil && (p.Kind != "agent" || p.Sponsor.Sponsor != nil || !ValidID(p.Sponsor.ID) || (p.Sponsor.Kind != "user" && p.Sponsor.Kind != "service")) {
		return false, nil
	}
	if r.Resource.Kind != Bead && r.Resource.Kind != Project && r.Resource.Kind != History {
		return false, nil
	}
	if CanonicalRef(r.Resource.Ref) != nil {
		return false, nil
	}
	if r.Resource.Kind == Bead && !validBeadReference(s, r.Resource.Ref) {
		return false, nil
	}
	for _, d := range []Dependency{ContextDependency(n), ACLDependency(n), {Kind: "sentinel", Key: "plugins"}, {Kind: "sentinel", Key: "ontology"}} {
		if err := s.Record(ctx, d); err != nil {
			return false, unavailable(err)
		}
	}
	ok, err := s.CheckContext(ctx, c)
	if err != nil {
		return false, unavailable(err)
	}
	return ok, nil
}

func subject(r CheckRequest, b SubjectBinding) (SubjectRef, bool) {
	p := &r.Caller.Principal
	if b == SponsorSubject {
		p = p.Sponsor
	}
	if p == nil || p.Kind != "user" {
		return SubjectRef{}, false
	}
	return SubjectRef{Kind: "user", ID: p.ID}, true
}

func targetProject(ctx context.Context, s Slice, r Resource) (model.Ref, bool, error) {
	if r.Kind == Project {
		return r.Ref, true, nil
	}
	if r.Kind != Bead {
		return model.Ref{}, false, nil
	}
	if !sourceBeadReference(s, r.Ref) {
		for _, key := range []string{"bead/" + r.Ref.ID, "closure/" + r.Ref.ID} {
			if err := s.Record(ctx, Dependency{"sentinel", key}); err != nil {
				return model.Ref{}, false, err
			}
		}
	}
	p, err := s.ProjectForBead(ctx, r.Ref)
	if err != nil || !p.Found {
		return model.Ref{}, false, err
	}
	if !ValidID(p.Name) {
		return model.Ref{}, false, errors.New("invalid stored project name")
	}
	if err := CanonicalRef(p.Ref); err != nil {
		return model.Ref{}, false, err
	}
	return p.Ref, true, nil
}

func hasGrant(ctx context.Context, s Slice, sub SubjectRef, scope Scope, role string) (bool, error) {
	d, err := GrantDependency(s.Namespace(), scope)
	if err != nil {
		return false, err
	}
	if err := s.Record(ctx, d); err != nil {
		return false, err
	}
	return s.HasGrant(ctx, sub, scope, role)
}

func evalExpr(ctx context.Context, s Slice, r CheckRequest, f FilterExpr) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	switch f.Op {
	case False:
		return false, nil
	case True:
		return true, nil
	case All, Any:
		for _, a := range f.Args {
			v, err := evalExpr(ctx, s, r, a)
			if err != nil {
				return false, err
			}
			if f.Op == All && !v {
				return false, nil
			}
			if f.Op == Any && v {
				return true, nil
			}
		}
		return f.Op == All, nil
	case PrincipalKind:
		return r.Caller.Principal.Kind == f.Kind, nil
	case SponsorKind:
		p := r.Caller.Principal.Sponsor
		return p != nil && p.Kind == f.Kind, nil
	case GrantExists:
		sub, ok := subject(r, f.Subject)
		if !ok {
			return false, nil
		}
		scope := Scope{Kind: AuthorityScope, Ref: s.Namespace().Authority}
		if f.Scope == ProjectBindingScope {
			p, found, err := targetProject(ctx, s, r.Resource)
			if err != nil || !found {
				return false, err
			}
			scope = Scope{Kind: ProjectScope, Ref: p}
		}
		return hasGrant(ctx, s, sub, scope, f.Role)
	case ActiveProject:
		p, ok, err := targetProject(ctx, s, r.Resource)
		if err != nil || !ok {
			return false, err
		}
		return s.ProjectActive(ctx, p)
	case TeamInvolvement:
		sub, ok := subject(r, f.Subject)
		if !ok {
			return false, nil
		}
		p, found, err := targetProject(ctx, s, r.Resource)
		if err != nil || !found {
			return false, err
		}
		teams, err := s.Groups(ctx, sub)
		if err != nil {
			return false, err
		}
		for _, t := range teams {
			if err := CanonicalRef(t); err != nil {
				return false, err
			}
			member, err := hasGrant(ctx, s, sub, Scope{Kind: TeamScope, Ref: t}, "member")
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
			involved, err := hasGrant(ctx, s, SubjectRef{Kind: "team", ID: t.ID}, Scope{Kind: ProjectScope, Ref: p}, "involved")
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
func caveatsAllow(ctx context.Context, s Slice, r CheckRequest) (bool, error) {
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
					target, ok, err = targetProject(ctx, s, r.Resource)
					if err != nil {
						return false, unavailable(err)
					}
					if !ok {
						return false, nil
					}
				}
				found := false
				for _, x := range xs {
					if CanonicalRef(x) != nil {
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

// FreezeList copies mutable principal/caveat inputs at the compilation boundary.
func FreezeList(r ListRequest) (ListRequest, error) {
	if p := r.Caller.Principal.Sponsor; p != nil && p.Sponsor != nil {
		return ListRequest{}, errors.New("nested sponsor is unsupported")
	}
	b, err := json.Marshal(r.Caller.Principal)
	if err != nil {
		return ListRequest{}, err
	}
	if err = json.Unmarshal(b, &r.Caller.Principal); err != nil {
		return ListRequest{}, err
	}
	return r, nil
}

func sameTemplateBinding(a, b FilterTemplate) bool {
	return sameNamespace(a.Namespace, b.Namespace) && reflect.DeepEqual(a.Request, b.Request)
}
