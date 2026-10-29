package authzfixed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

// This read-only test double deliberately stores independent tuple/source data.
// It is not a transactional driver or a measurement of actual SQL behavior.
type slice struct {
	data          Facts
	issued        map[string]authz.Caller
	contextActive bool
	epoch         model.Revision
	beads         map[string]authz.ProjectBinding
	recorded      map[authz.Dependency]bool
	fail          string
}

func (s *slice) Namespace() authz.Namespace { return s.data.Namespace }
func (s *slice) Record(_ context.Context, d authz.Dependency) error {
	if s.fail == "record" {
		return errors.New("record unavailable")
	}
	s.recorded[d] = true
	return nil
}
func (s *slice) CheckContext(_ context.Context, c authz.Caller) (bool, error) {
	if s.fail == "context" {
		return false, errors.New("context unavailable")
	}
	issued, ok := s.issued[c.VerificationID]
	a, ae := authz.FreezeList(authz.ListRequest{Caller: issued})
	b, be := authz.FreezeList(authz.ListRequest{Caller: c})
	return ok && ae == nil && be == nil && s.contextActive && reflect.DeepEqual(a.Caller.Principal, b.Caller.Principal) && c.CapturedEpoch.String() == issued.CapturedEpoch.String() && c.CapturedEpoch.String() == s.epoch.String(), nil
}
func (s *slice) HasGrant(_ context.Context, sub authz.SubjectRef, scope authz.Scope, role string) (bool, error) {
	if s.fail == "grant" {
		return false, errors.New("grant unavailable")
	}
	for _, g := range s.data.Grants {
		if g.Subject == sub && g.Scope.Kind == scope.Kind && g.Scope.Ref.ID == scope.Ref.ID && g.Role == role {
			s.recorded[authz.Dependency{Kind: "record", Key: "tuple/" + authz.Encode(sub.Kind, sub.ID, string(scope.Kind), scope.Ref.ID, role)}] = true
			return true, nil
		}
	}
	return false, nil
}
func (s *slice) Groups(_ context.Context, u authz.SubjectRef) ([]model.Ref, error) {
	if s.fail == "groups" {
		return nil, errors.New("groups unavailable")
	}
	var out []model.Ref
	for _, g := range s.data.Grants {
		if g.Subject == u && g.Scope.Kind == authz.TeamScope && g.Role == "member" {
			out = append(out, g.Scope.Ref)
		}
	}
	return out, nil
}
func (s *slice) TeamActive(_ context.Context, r model.Ref) (bool, error) {
	if s.fail == "team" {
		return false, errors.New("team unavailable")
	}
	for _, x := range s.data.Teams {
		if x.Ref.ID == r.ID {
			s.recorded[authz.Dependency{Kind: "record", Key: r.ID}] = true
			return x.Active, nil
		}
	}
	return false, nil
}
func (s *slice) ProjectActive(_ context.Context, r model.Ref) (bool, error) {
	if s.fail == "project" {
		return false, errors.New("project unavailable")
	}
	for _, x := range s.data.Projects {
		if x.Ref.ID == r.ID {
			s.recorded[authz.Dependency{Kind: "record", Key: r.ID}] = true
			return x.Active, nil
		}
	}
	return false, nil
}
func (s *slice) ProjectForBead(_ context.Context, r model.Ref) (authz.ProjectBinding, error) {
	if s.fail == "assignment" {
		return authz.ProjectBinding{}, errors.New("projection integrity failure")
	}
	p := s.beads[r.ID]
	s.recorded[authz.Dependency{Kind: "record", Key: r.ID}] = true
	return p, nil
}

func caller(kind string, sponsor *model.PrincipalRef) authz.Caller {
	return authz.Caller{Principal: model.PrincipalRef{Kind: kind, ID: kind + "-caller", Sponsor: sponsor}, VerificationID: "verification", CapturedEpoch: model.Natural64(1)}
}
func user() model.PrincipalRef { return model.PrincipalRef{Kind: "user", ID: "u"} }
func newSlice(c authz.Caller) *slice {
	return &slice{data: facts(), issued: map[string]authz.Caller{c.VerificationID: c}, contextActive: true, epoch: model.Natural64(1), beads: map[string]authz.ProjectBinding{"b": {Name: "web/api", Ref: ref("project"), Found: true}}, recorded: map[authz.Dependency]bool{}}
}
func rules(t *testing.T) *RuleSet {
	t.Helper()
	r, err := New(ns())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCP7ExhaustiveCompiledMembership(t *testing.T) {
	r := rules(t)
	u := user()
	svc := model.PrincipalRef{Kind: "service", ID: "svc"}
	principals := []authz.Caller{caller("user", nil), caller("agent", &u), caller("agent", nil), caller("agent", &svc), caller("service", nil), caller("plugin", nil), caller("system", nil), caller("anonymous", nil)}
	checked := 0
	for bits := 0; bits < 256; bits++ {
		for pi, c := range principals {
			s := newSlice(c)
			s.data.Teams[0].Active = bits&8 != 0
			s.data.Projects[0].Active = bits&16 != 0
			if bits&1 != 0 {
				s.data.Grants = append(s.data.Grants, grant("user", "u", authz.ProjectScope, "project", "involved"))
			}
			if bits&2 != 0 {
				s.data.Grants = append(s.data.Grants, grant("user", "u", authz.TeamScope, "team", "member"))
			}
			if bits&4 != 0 {
				s.data.Grants = append(s.data.Grants, grant("team", "team", authz.ProjectScope, "project", "involved"))
			}
			if bits&32 != 0 {
				s.data.Grants = append(s.data.Grants, grant("user", "teammate", authz.ProjectScope, "project", "involved"))
			}
			if bits&64 != 0 {
				s.data.Grants = append(s.data.Grants, grant("user", "teammate", authz.TeamScope, "team", "member"))
			}
			if bits&128 != 0 {
				s.data.Grants = append(s.data.Grants, grant("user", c.Principal.ID, authz.AuthorityScope, "authority", "admin"))
			}
			if err := s.data.Validate(); err != nil {
				t.Fatal(err)
			}
			involved := bits&16 != 0 && (bits&1 != 0 || bits&2 != 0 && bits&4 != 0 && bits&8 != 0)
			cases := []struct {
				action     authz.Action
				kind       authz.ResourceKind
				id, method string
				want       bool
			}{
				{authz.Read, authz.Bead, "b", "", c.Principal.Kind == "user"},
				{authz.Read, authz.Bead, "projectless", "", c.Principal.Kind == "user"},
				{authz.Modify, authz.Project, "project", "", pi == 1 && involved},
				{authz.Modify, authz.Project, "unknown-project", "", false},
				{authz.Modify, authz.Bead, "b", "bead.claim", pi == 1 && involved},
				{authz.Modify, authz.Bead, "b", "bead.claimNext", pi == 1 && involved},
				{authz.Modify, authz.Bead, "b", "bead.update", pi == 1 && involved},
				{authz.ImportBulkHistory, authz.History, "authority", "", c.Principal.Kind == "user" && bits&128 != 0},
			}
			for _, tc := range cases {
				q := authz.CheckRequest{Caller: c, Action: tc.action, Resource: authz.Resource{Kind: tc.kind, Ref: ref(tc.id)}, Method: tc.method}
				d, err := r.Eval(context.Background(), s, []authz.CheckRequest{q})
				if err != nil {
					t.Fatal(err)
				}
				f, err := r.Compile(authz.ListRequest{Caller: c, Action: tc.action, ResourceKind: tc.kind, Method: tc.method})
				if err != nil {
					t.Fatal(err)
				}
				member, err := f.Match(context.Background(), s, q.Resource)
				if err != nil {
					t.Fatal(err)
				}
				if member != tc.want || (d[0].Effect == authz.Allow) != tc.want {
					t.Fatalf("bits=%08b principal=%d action=%s kind=%s method=%s: eval=%s member=%v want=%v", bits, pi, tc.action, tc.kind, tc.method, d[0].Effect, member, tc.want)
				}
				checked++
			}
		}
	}
	t.Logf("%d comparisons against independent CP7/direct-one-hop truth table", checked)
}

func TestCompiledFilterRevocationAndRecordedMisses(t *testing.T) {
	u := user()
	c := caller("agent", &u)
	s := newSlice(c)
	r := rules(t)
	s.data.Grants = []authz.Grant{grant("user", "u", authz.TeamScope, "team", "member"), grant("team", "team", authz.ProjectScope, "project", "involved")}
	f, err := r.Compile(authz.ListRequest{Caller: c, Action: authz.Modify, ResourceKind: authz.Bead, Method: "bead.claimNext"})
	if err != nil {
		t.Fatal(err)
	}
	check := func(want bool) {
		t.Helper()
		ok, err := f.Match(context.Background(), s, authz.Resource{Kind: authz.Bead, Ref: ref("b")})
		if err != nil || ok != want {
			t.Fatalf("membership=%v want=%v error=%v", ok, want, err)
		}
	}
	check(true)
	for name, revoke := range map[string]func(){
		"membership":       func() { s.data.Grants = s.data.Grants[1:] },
		"explicit edge":    func() { s.data.Grants = s.data.Grants[:1] },
		"team active":      func() { s.data.Teams[0].Active = false },
		"project active":   func() { s.data.Projects[0].Active = false },
		"context":          func() { s.contextActive = false },
		"context epoch":    func() { s.epoch = model.Natural64(2) },
		"project registry": func() { s.beads["b"] = authz.ProjectBinding{} },
	} {
		t.Run(name, func(t *testing.T) {
			old := *s
			old.data = facts()
			old.data.Grants = append([]authz.Grant(nil), s.data.Grants...)
			old.beads = map[string]authz.ProjectBinding{"b": s.beads["b"]}
			revoke()
			check(false)
			*s = old
			check(true)
		})
	}
	// No membership is still a guarded complete predicate; a later insertion
	// cannot be represented by locking only returned team rows.
	s.data.Grants = nil
	s.recorded = map[authz.Dependency]bool{}
	check(false)
	for _, d := range []authz.Dependency{authz.ACLDependency(ns()), authz.ContextDependency(ns()), {Kind: "sentinel", Key: "bead/b"}, {Kind: "sentinel", Key: "closure/b"}, {Kind: "sentinel", Key: "plugins"}, {Kind: "sentinel", Key: "ontology"}} {
		if !s.recorded[d] {
			t.Errorf("missing dependency %+v", d)
		}
	}
	d, _ := authz.GrantDependency(ns(), authz.Scope{Kind: authz.ProjectScope, Ref: ref("project")})
	if !s.recorded[d] {
		t.Error("grant miss was not recorded")
	}
	// Reuse the original template after inserting the formerly absent edge.
	s.data.Grants = []authz.Grant{grant("user", "u", authz.ProjectScope, "project", "involved")}
	check(true)
}

func TestMissingOrStaleContextNeverSubstitutesSponsor(t *testing.T) {
	u := user()
	c := caller("agent", &u)
	r := rules(t)
	for _, mutate := range []func(*authz.Caller){
		func(c *authz.Caller) { c.VerificationID = "" }, func(c *authz.Caller) { c.VerificationID = "unknown" }, func(c *authz.Caller) { c.Principal.Sponsor = nil }, func(c *authz.Caller) { c.Principal.Sponsor = &model.PrincipalRef{Kind: "user", ID: "replacement"} }, func(c *authz.Caller) { c.CapturedEpoch = model.Natural64(2) },
	} {
		s := newSlice(c)
		s.data.Grants = []authz.Grant{grant("user", "u", authz.ProjectScope, "project", "involved")}
		bad := c
		mutate(&bad)
		f, err := r.Compile(authz.ListRequest{Caller: bad, Action: authz.Modify, ResourceKind: authz.Project})
		if err != nil {
			t.Fatal(err)
		}
		ok, err := f.Match(context.Background(), s, authz.Resource{Kind: authz.Project, Ref: ref("project")})
		if err != nil || ok {
			t.Fatalf("missing/stale context permitted: %v %v", ok, err)
		}
	}
}

func TestReadErrorsAreNotAbsence(t *testing.T) {
	u := user()
	c := caller("agent", &u)
	r := rules(t)
	for _, name := range []string{"record", "context", "grant", "groups", "team", "project", "assignment"} {
		t.Run(name, func(t *testing.T) {
			s := newSlice(c)
			s.data.Grants = []authz.Grant{grant("user", "u", authz.TeamScope, "team", "member"), grant("team", "team", authz.ProjectScope, "project", "involved")}
			s.fail = name
			q := authz.CheckRequest{Caller: c, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: ref("b")}, Method: "bead.claimNext"}
			d, err := (authz.Chain{Namespace: ns(), Rules: []authz.RuleSet{r}}).Check(context.Background(), s, []authz.CheckRequest{q})
			var e *authz.Error
			if !errors.As(err, &e) || e.Code != "authz_unavailable" || !e.Retryable || d != nil {
				t.Fatalf("error became a decision: %+v %v", d, err)
			}
		})
	}
}

func TestCaveatIntersectionAndFrozenBinding(t *testing.T) {
	u := user()
	r := rules(t)
	cases := []struct {
		name    string
		caveats map[string]any
		want    bool
	}{
		{"omitted", nil, true}, {"allowed", map[string]any{"actions": []string{"modify"}, "projects": []model.Ref{ref("project")}, "refs": []model.Ref{ref("b")}}, true},
		{"empty actions", map[string]any{"actions": []string{}}, false}, {"empty projects", map[string]any{"projects": []model.Ref{}}, false}, {"wrong project", map[string]any{"projects": []model.Ref{ref("other")}}, false},
		{"wrong action", map[string]any{"actions": []string{"read"}}, false}, {"wrong ref", map[string]any{"refs": []model.Ref{ref("other")}}, false}, {"unsupported", map[string]any{"admin": true}, false},
		{"null", map[string]any{"projects": nil}, false}, {"key ref", map[string]any{"refs": []model.Ref{model.RefByKey("b")}}, false}, {"malformed", map[string]any{"actions": true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := caller("agent", &u)
			c.Principal.Caveats = tc.caveats
			s := newSlice(c)
			s.data.Grants = []authz.Grant{grant("user", "u", authz.ProjectScope, "project", "involved")}
			f, err := r.Compile(authz.ListRequest{Caller: c, Action: authz.Modify, ResourceKind: authz.Bead, Method: "bead.update"})
			if err != nil {
				t.Fatal(err)
			}
			ok, err := f.Match(context.Background(), s, authz.Resource{Kind: authz.Bead, Ref: ref("b")})
			if err != nil || ok != tc.want {
				current, ce := s.CheckContext(context.Background(), f.Request.Caller)
				a, _ := json.Marshal(s.issued[c.VerificationID])
				b, _ := json.Marshal(f.Request.Caller)
				t.Fatalf("match=%v err=%v context=%v/%v issued=%s compiled=%s", ok, err, current, ce, a, b)
			}
			s.data.Grants = nil
			ok, err = f.Match(context.Background(), s, authz.Resource{Kind: authz.Bead, Ref: ref("b")})
			if err != nil || ok {
				t.Fatal("caveats expanded permission")
			}
		})
	}
	// Compilation owns a deep copy, including nested sponsor caveats.
	c := caller("agent", &u)
	c.Principal.Caveats = map[string]any{"actions": []string{"modify"}}
	f, err := r.Compile(authz.ListRequest{Caller: c, Action: authz.Modify, ResourceKind: authz.Project})
	if err != nil {
		t.Fatal(err)
	}
	c.Principal.Caveats["actions"] = []string{"read"}
	if fmt.Sprint(f.Request.Caller.Principal.Caveats["actions"]) != "[modify]" {
		t.Fatal("compiled binding aliases mutable caller")
	}
}

func TestUnsupportedActionsDeny(t *testing.T) {
	u := user()
	c := caller("agent", &u)
	s := newSlice(c)
	s.data.Grants = []authz.Grant{grant("user", "u", authz.ProjectScope, "project", "involved")}
	r := rules(t)
	chain := authz.Chain{Namespace: ns(), Rules: []authz.RuleSet{r}}
	for _, m := range []string{"bead.create", "bead.reparent", "bead.delete", "bead.release", "bead.heartbeat", "bead.close", "tx.apply", "admin.restore.begin", "plugin.apply", "x.fixed.grant.put", "x.fixed.team.set-active"} {
		q := authz.CheckRequest{Caller: c, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: ref("b")}, Method: m}
		ds, err := chain.Check(context.Background(), s, []authz.CheckRequest{q})
		if err != nil || ds[0].Effect != authz.Deny {
			t.Fatalf("%s: %+v %v", m, ds, err)
		}
		f, err := chain.Compile(authz.ListRequest{Caller: c, Action: q.Action, ResourceKind: q.Resource.Kind, Method: m})
		if err != nil {
			t.Fatal(err)
		}
		ok, err := f.Match(context.Background(), s, q.Resource)
		if err != nil || ok {
			t.Fatalf("compiled unsupported method %s: %v %v", m, ok, err)
		}
	}
	for _, a := range []authz.Action{"create", "admin", "transition", "write", "approve", "plan"} {
		q := authz.CheckRequest{Caller: c, Action: a, Resource: authz.Resource{Kind: authz.Project, Ref: ref("project")}}
		ds, err := chain.Check(context.Background(), s, []authz.CheckRequest{q})
		if err != nil || ds[0].Effect != authz.Deny {
			t.Fatalf("%s: %+v %v", a, ds, err)
		}
	}
}

func TestFiniteComponentClaimUpdateBoundary(t *testing.T) {
	u := user()
	c := caller("agent", &u)
	s := newSlice(c)
	r := rules(t)
	s.data.Grants = []authz.Grant{grant("user", "u", authz.TeamScope, "team", "member"), grant("team", "team", authz.ProjectScope, "project", "involved")}
	chain := authz.Chain{Namespace: ns(), Rules: []authz.RuleSet{r}}
	before := model.Record{Kind: "bead", Ref: ref("b"), Revision: model.Natural64(1), Body: map[string]any{"project": "web/api", "parent": map[string]any{"id": "parent"}, "title": "before"}}
	for _, method := range []string{"bead.claim", "bead.claimNext", "bead.update"} {
		q := authz.CheckRequest{Caller: c, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: ref("b")}, Method: method}
		ds, err := chain.Check(context.Background(), s, []authz.CheckRequest{q})
		if err != nil || ds[0].Effect != authz.Allow {
			t.Fatalf("%+v %v", ds, err)
		}
		after := before
		after.Body = map[string]any{"project": "web/api", "parent": map[string]any{"id": "parent"}, "title": "after"}
		if err := authz.ValidateBeadWrite(method, before, after, c.Principal, c, []authz.Action{authz.Modify}); err != nil {
			t.Fatal(err)
		}
		after.Body["project"] = "unauthorized-project"
		if authz.ValidateBeadWrite(method, before, after, c.Principal, c, nil) == nil {
			t.Fatal("post-image moved outside authorized project")
		}
	}
	// A modification permit never implies permission to return bead contents.
	read := authz.CheckRequest{Caller: c, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: ref("b")}}
	ds, err := chain.Check(context.Background(), s, []authz.CheckRequest{read})
	if err != nil || ds[0].Effect != authz.Deny {
		t.Fatal("Agent write supplied read permission")
	}
	// A single already-compiled candidate filter sees current revocation.
	f, err := chain.Compile(authz.ListRequest{Caller: c, Action: authz.Modify, ResourceKind: authz.Bead, Method: "bead.claimNext"})
	if err != nil {
		t.Fatal(err)
	}
	s.data.Grants = nil
	ok, err := f.Match(context.Background(), s, read.Resource)
	if err != nil || ok {
		t.Fatal("compiled membership reused revoked involvement")
	}
}

func TestProjectlessUnknownAndInactiveAssignment(t *testing.T) {
	u := user()
	r := rules(t)
	for _, binding := range []authz.ProjectBinding{{}, {Name: "missing-registry", Found: false}, {Name: "web/api", Ref: ref("project"), Found: true}} {
		for _, c := range []authz.Caller{caller("user", nil), caller("agent", &u)} {
			s := newSlice(c)
			s.beads["b"] = binding
			s.data.Projects[0].Active = false
			s.data.Grants = []authz.Grant{grant("user", "u", authz.ProjectScope, "project", "involved"), grant("user", "u", authz.AuthorityScope, "authority", "admin")}
			for _, a := range []authz.Action{authz.Read, authz.Modify} {
				method := ""
				if a == authz.Modify {
					method = "bead.update"
				}
				q := authz.CheckRequest{Caller: c, Action: a, Resource: authz.Resource{Kind: authz.Bead, Ref: ref("b")}, Method: method}
				ds, err := r.Eval(context.Background(), s, []authz.CheckRequest{q})
				want := a == authz.Read && c.Principal.Kind == "user"
				if err != nil || (ds[0].Effect == authz.Allow) != want {
					t.Fatalf("binding=%+v kind=%s action=%s decision=%+v err=%v", binding, c.Principal.Kind, a, ds, err)
				}
			}
			if c.Principal.Kind == "agent" {
				q := authz.CheckRequest{Caller: c, Action: authz.ImportBulkHistory, Resource: authz.Resource{Kind: authz.History, Ref: ref("authority")}}
				ds, err := r.Eval(context.Background(), s, []authz.CheckRequest{q})
				if err != nil || ds[0].Effect == authz.Allow {
					t.Fatal("Agent inherited admin sponsor's User permission")
				}
			}
		}
	}
}

func TestSponsorCaveatsAlsoIntersect(t *testing.T) {
	u := user()
	u.Caveats = map[string]any{"actions": []string{"read"}}
	c := caller("agent", &u)
	c.Principal.Caveats = map[string]any{"actions": []string{"modify"}}
	s := newSlice(c)
	s.data.Grants = []authz.Grant{grant("user", "u", authz.ProjectScope, "project", "involved")}
	f, err := rules(t).Compile(authz.ListRequest{Caller: c, Action: authz.Modify, ResourceKind: authz.Project})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := f.Match(context.Background(), s, authz.Resource{Kind: authz.Project, Ref: ref("project")}); err != nil || ok {
		t.Fatal("Agent caveat expanded sponsor restriction")
	}
}
