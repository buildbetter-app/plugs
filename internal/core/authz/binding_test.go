package authz

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestCheckPreparedAndTargetBindingsMatchLegacy(t *testing.T) {
	for _, tc := range []struct {
		name  string
		chain Chain
	}{
		{"permit", Chain{Rules: []RuleSet{permit()}}},
		{"forbid", Chain{Rules: []RuleSet{permit(), forbid()}}},
		{"restriction", Chain{Links: []Link{link{effect: NoDecision, restriction: &FilterExpr{Op: False}}}, Rules: []RuleSet{permit()}}},
		{"terminal", Chain{Links: []Link{link{effect: Allow}}}},
		{"illegal_terminal", Chain{Links: []Link{link{effect: Allow}}, Rules: []RuleSet{permit()}}},
		{"error", Chain{Links: []Link{link{err: errors.New("component unavailable")}}}},
		{"panic", Chain{Links: []Link{link{panic: true}}}},
		{"cardinality", Chain{Rules: []RuleSet{rule{short: true}}}},
		{"deadline", Chain{Links: []Link{link{wait: true}}, Budget: time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.chain
			c.Namespace = namespace()
			for _, caveats := range []map[string]any{nil, {"actions": []string{}}, {"actions": []any{"read", nil}}, {"refs": []model.Ref{ref("b"), model.RefByKey("invalid")}}, {"projects": []model.Ref{}}} {
				q := request()
				q.Caller.Principal.Caveats = caveats
				resources := []Resource{q.Resource, {Kind: Bead, Ref: ref("second")}, {Kind: Bead, Ref: model.RefByKey("invalid")}}
				requests := make([]CheckRequest, len(resources))
				for i, resource := range resources {
					requests[i] = q
					requests[i].Resource = resource
				}
				p, err := PrepareRequest(ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: Bead, Method: q.Method})
				if err != nil {
					t.Fatal(err)
				}
				want, we := legacyChainCheck(c, context.Background(), chainSlice{true}, requests)
				for _, batch := range []*PreparedChecks{PrepareChecks(requests), p.Checks(resources)} {
					got, ge := c.CheckPrepared(context.Background(), chainSlice{true}, batch)
					if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(preparedErrorShape(ge), preparedErrorShape(we)) {
						t.Fatalf("prepared=%+v/%v legacy=%+v/%v", got, ge, want, we)
					}
				}
			}
		})
	}
}

func TestTargetBindingRejectsKindChangeBeforeObservations(t *testing.T) {
	q := request()
	p, err := PrepareRequest(ListRequest{Caller: q.Caller, Action: Read, ResourceKind: Bead})
	if err != nil {
		t.Fatal(err)
	}
	s := &preparedBindingSlice{deps: map[Dependency]bool{}}
	c := Chain{Namespace: namespace(), Rules: []RuleSet{permit()}}
	_, err = c.CheckPrepared(context.Background(), s, p.Checks([]Resource{q.Resource, {Kind: Project, Ref: ref("project")}}))
	var e *Error
	if !errors.As(err, &e) || e.Code != "authz_unavailable" || len(s.deps) != 0 || s.contexts != 0 {
		t.Fatalf("changed kind was admitted or observed: error=%v deps=%v contexts=%d", err, s.deps, s.contexts)
	}
}

func bindingTemplate(t *testing.T) FilterTemplate {
	t.Helper()
	q := request()
	q.Caller.Principal = model.PrincipalRef{Kind: "agent", ID: "agent", Sponsor: &model.PrincipalRef{Kind: "user", ID: "u", Caveats: map[string]any{"actions": []string{"read"}}}, Caveats: map[string]any{"actions": []string{"read"}, "refs": []model.Ref{ref("b")}}}
	return FilterTemplate{Namespace: namespace(), Request: ListRequest{Caller: q.Caller, Action: Read, ResourceKind: Bead, Method: "bead.get"},
		Permit: And(Or(FilterExpr{Op: True}, FilterExpr{Op: False}), FilterExpr{Op: True}),
		Forbid: Or(FilterExpr{Op: False}, FilterExpr{Op: False}), Restrictions: []FilterExpr{And(FilterExpr{Op: True}), {Op: True}}}
}

func TestOpaqueFilterOwnsDescriptionAndBinding(t *testing.T) {
	template := bindingTemplate(t)
	sealed, err := template.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := sealed.Binding()
	if err != nil || len(digest) != 64 {
		t.Fatalf("binding=%q error=%v", digest, err)
	}
	expected := sealed.Description()
	// Mutate both source inputs and a separately returned recursive description.
	template.Request.Caller.Principal.Sponsor.ID = "changed"
	template.Request.Caller.Principal.Caveats["refs"].([]model.Ref)[0] = ref("changed")
	template.Permit.Args[0].Args[0].Op = False
	template.Forbid.Args[0].Op = True
	template.Restrictions[0].Args[0].Op = False
	copy := sealed.Description()
	copy.Namespace.Authority = ref("changed")
	copy.Request.Caller.Principal.Caveats["refs"].([]any)[0] = nil
	copy.Request.Caller.Principal.Sponsor.Caveats["actions"].([]any)[0] = "modify"
	copy.Permit.Args[0].Args[0].Op = False
	copy.Forbid.Args[0].Op = True
	copy.Restrictions[0].Args[0].Op = False
	if got, err := sealed.Binding(); err != nil || got != digest || !reflect.DeepEqual(sealed.Description(), expected) {
		t.Fatalf("mutation changed sealed binding or description: %q/%v", got, err)
	}
	resource := Resource{Kind: Bead, Ref: ref("b")}
	if allowed, err := sealed.Match(context.Background(), chainSlice{true}, resource); err != nil || !allowed {
		t.Fatalf("mutation changed sealed permit: %v/%v", allowed, err)
	}
	if allowed, err := template.Match(context.Background(), chainSlice{true}, resource); err != nil || allowed {
		t.Fatalf("public template did not retain mutation semantics: %v/%v", allowed, err)
	}
}

func TestOpaqueBindingCoversCallerNamespaceAndOrderedPolicy(t *testing.T) {
	base, err := bindingTemplate(t).Prepare()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := base.Binding()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*FilterTemplate)
	}{
		{"store", func(f *FilterTemplate) { f.Namespace.StoreID = "other" }},
		{"incarnation", func(f *FilterTemplate) { f.Namespace.Incarnation = "other" }},
		{"installation", func(f *FilterTemplate) { f.Namespace.InstallationID = "other" }},
		{"authority", func(f *FilterTemplate) { f.Namespace.Authority = ref("other") }},
		{"principal", func(f *FilterTemplate) { f.Request.Caller.Principal.ID = "other" }},
		{"sponsor", func(f *FilterTemplate) { f.Request.Caller.Principal.Sponsor.ID = "other" }},
		{"receipt", func(f *FilterTemplate) { f.Request.Caller.VerificationID = "other" }},
		{"epoch", func(f *FilterTemplate) { f.Request.Caller.CapturedEpoch = model.Natural64(2) }},
		{"action", func(f *FilterTemplate) { f.Request.Action = Modify }},
		{"kind", func(f *FilterTemplate) { f.Request.ResourceKind = Project }},
		{"method", func(f *FilterTemplate) { f.Request.Method = "bead.ready" }},
		{"caveat_empty", func(f *FilterTemplate) { f.Request.Caller.Principal.Caveats["actions"] = []string{} }},
		{"caveat_omitted", func(f *FilterTemplate) { delete(f.Request.Caller.Principal.Caveats, "refs") }},
		{"permit_order", func(f *FilterTemplate) {
			f.Permit.Args[0].Args[0], f.Permit.Args[0].Args[1] = f.Permit.Args[0].Args[1], f.Permit.Args[0].Args[0]
		}},
		{"forbid", func(f *FilterTemplate) { f.Forbid = FilterExpr{Op: True} }},
		{"restrictions", func(f *FilterTemplate) { f.Restrictions[1] = FilterExpr{Op: False} }},
		{"no_plugins", func(f *FilterTemplate) {
			f.NoPlugins = true
			f.Request.Caller.Principal.Caveats = nil
			f.Request.Caller.Principal.Sponsor.Caveats = nil
			f.Permit = FilterExpr{Op: True}
			f.Forbid = FilterExpr{Op: False}
			f.Restrictions = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			description := base.Description()
			tc.edit(&description)
			changed, err := description.Prepare()
			if err != nil {
				t.Fatal(err)
			}
			got, err := changed.Binding()
			if err != nil || got == digest {
				t.Fatalf("changed binding was conflated: %q/%v", got, err)
			}
		})
	}
	seen := map[string]bool{}
	for _, number := range []string{"1", "1.0", "1e0", "9007199254740992", "9007199254740993"} {
		d := base.Description()
		d.Request.Caller.Principal.Sponsor.Caveats["exact"] = json.Number(number)
		f, err := d.Prepare()
		if err != nil {
			t.Fatal(err)
		}
		got, err := f.Binding()
		if err != nil || seen[got] {
			t.Fatalf("exact caveat number %s was conflated: %q/%v", number, got, err)
		}
		seen[got] = true
	}
	// Object insertion order and equivalent native/decoded Ref forms are not
	// different normalized callers. Epoch equality remains mathematical.
	d := base.Description()
	d.Request.Caller.CapturedEpoch = model.Natural("1.0")
	p, err := PrepareRequest(d.Request)
	if err != nil {
		t.Fatal(err)
	}
	f, err := p.Filter(d)
	if err != nil {
		t.Fatal(err)
	}
	other, err := f.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := other.Binding(); err != nil || got != digest {
		t.Fatalf("equal normalized epoch changed binding: %q/%v", got, err)
	}
}

func TestOpaqueNoPluginRetainsEarlyBehavior(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := Chain{Budget: -time.Second}
	// A blank caller is intentionally ignored by the legacy no-plugin branch.
	template, err := c.Compile(ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := template.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := f.Match(ctx, nil, Resource{}); err != nil || !ok {
		t.Fatalf("no-plugin path acquired validation: %v/%v", ok, err)
	}
	q := request()
	p, err := PrepareRequest(ListRequest{Caller: q.Caller, Action: Read, ResourceKind: Bead})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CompilePrepared(p); err != nil {
		t.Fatal(err)
	}
	if ds, err := c.CheckPrepared(ctx, nil, p.Checks([]Resource{{}})); err != nil || len(ds) != 1 || ds[0].Effect != Allow {
		t.Fatalf("no-plugin prepared branch acquired validation: %+v/%v", ds, err)
	}
	q.Caller.Principal.Caveats = map[string]any{"bad": func() {}}
	_, err = c.Compile(ListRequest{Caller: q.Caller})
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "authz_not_installed" {
		t.Fatalf("scoped no-plugin failure changed: %v", err)
	}
}

type bindingErrorSlice struct {
	chainSlice
	failure string
	deps    []Dependency
}

type projectDependencySlice struct {
	chainSlice
	deps       []Dependency
	failKey    string
	bodyReads  int
	projectErr error
}

func (s *projectDependencySlice) Record(_ context.Context, dep Dependency) error {
	s.deps = append(s.deps, dep)
	if dep.Key == s.failKey {
		return errors.New("sentinel observation failed")
	}
	return nil
}

func (s *projectDependencySlice) ProjectForBead(context.Context, model.Ref) (ProjectBinding, error) {
	s.bodyReads++
	return ProjectBinding{Name: "project", Ref: ref("project"), Found: true}, s.projectErr
}

func TestProjectDependencyFallbackKeepsOrderAndStopsBeforeBody(t *testing.T) {
	resource := Resource{Kind: Bead, Ref: ref("target")}
	for _, fail := range []string{"bead/target", "closure/target", ""} {
		t.Run(fail, func(t *testing.T) {
			s := &projectDependencySlice{failKey: fail}
			_, found, err := targetProject(context.Background(), s, resource)
			want := []Dependency{{Kind: "sentinel", Key: "bead/target"}}
			if fail != "bead/target" {
				want = append(want, Dependency{Kind: "sentinel", Key: "closure/target"})
			}
			if !reflect.DeepEqual(s.deps, want) || (err != nil) != (fail != "") || found != (fail == "") {
				t.Fatalf("dependencies=%v error=%v found=%v", s.deps, err, found)
			}
			if (s.bodyReads == 1) != (fail == "") {
				t.Fatalf("read body after dependency failure: reads=%d failure=%q", s.bodyReads, fail)
			}
		})
	}
}

func (s *bindingErrorSlice) Record(_ context.Context, dep Dependency) error {
	s.deps = append(s.deps, dep)
	if s.failure == "record" {
		return errors.New("record observation unavailable")
	}
	return nil
}

func (s *bindingErrorSlice) CheckContext(context.Context, Caller) (bool, error) {
	if s.failure == "context" {
		return false, errors.New("context unavailable")
	}
	return s.failure != "revoked", nil
}

func (s *bindingErrorSlice) ProjectForBead(context.Context, model.Ref) (ProjectBinding, error) {
	if s.failure == "project" {
		return ProjectBinding{}, errors.New("project source unavailable")
	}
	return ProjectBinding{Found: true, Ref: ref("project")}, nil
}

func TestOpaqueFilterMatchesUnpreparedInterpreterFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		caveats map[string]any
		sponsor bool
	}{
		{"omitted", nil, false},
		{"empty", map[string]any{"actions": []string{}}, false},
		{"null", map[string]any{"actions": nil}, false},
		{"wrong_type", map[string]any{"actions": true}, false},
		{"mixed_null", map[string]any{"actions": []any{"read", nil}}, false},
		{"unknown", map[string]any{"unknown": true}, false},
		{"empty_projects", map[string]any{"projects": []model.Ref{}}, false},
		{"invalid_ref_tail", map[string]any{"refs": []model.Ref{ref("b"), model.RefByKey("bad")}}, false},
		{"sponsor_intersection", map[string]any{"actions": []string{}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, failure := range []string{"", "record", "context", "revoked", "project"} {
				q := request()
				q.Caller.Principal.Caveats = tc.caveats
				if tc.sponsor {
					q.Caller.Principal = model.PrincipalRef{Kind: "agent", ID: "agent", Sponsor: &model.PrincipalRef{Kind: "user", ID: "u", Caveats: tc.caveats}}
				}
				// An unattached template invokes the original prepare and
				// caveatsAllow interpreter, independently of PreparedFilter.
				legacy := FilterTemplate{Namespace: namespace(), Request: ListRequest{Caller: q.Caller, Action: Read, ResourceKind: Bead}, Permit: FilterExpr{Op: True}, Forbid: FilterExpr{Op: False}}
				sealed, err := legacy.Prepare()
				if err != nil {
					t.Fatal(err)
				}
				old, current := &bindingErrorSlice{failure: failure}, &bindingErrorSlice{failure: failure}
				want, we := legacy.Match(context.Background(), old, q.Resource)
				got, ge := sealed.Match(context.Background(), current, q.Resource)
				if got != want || !reflect.DeepEqual(preparedErrorShape(ge), preparedErrorShape(we)) || !reflect.DeepEqual(current.deps, old.deps) {
					t.Fatalf("failure=%s sealed=%v/%v legacy=%v/%v observations=%v/%v", failure, got, ge, want, we, current.deps, old.deps)
				}
			}
		})
	}
}
