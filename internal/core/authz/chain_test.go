package authz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type chainSlice struct{ valid bool }

func (s chainSlice) Namespace() Namespace                               { return namespace() }
func (s chainSlice) Record(context.Context, Dependency) error           { return nil }
func (s chainSlice) CheckContext(context.Context, Caller) (bool, error) { return s.valid, nil }
func (s chainSlice) HasGrant(context.Context, SubjectRef, Scope, string) (bool, error) {
	return false, nil
}
func (s chainSlice) Groups(context.Context, SubjectRef) ([]model.Ref, error) { return nil, nil }
func (s chainSlice) TeamActive(context.Context, model.Ref) (bool, error)     { return false, nil }
func (s chainSlice) ProjectActive(context.Context, model.Ref) (bool, error)  { return false, nil }
func (s chainSlice) ProjectForBead(context.Context, model.Ref) (ProjectBinding, error) {
	return ProjectBinding{}, nil
}

type link struct {
	effect      Effect
	restriction *FilterExpr
	err         error
	wait        bool
	short       bool
	checkOnly   bool
	panic       bool
}

func (l link) Check(ctx context.Context, _ Slice, qs []CheckRequest) ([]Step, error) {
	if l.panic {
		panic("broken link")
	}
	if l.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if l.err != nil {
		return nil, l.err
	}
	if l.short {
		return nil, nil
	}
	ds := make([]Step, len(qs))
	for i := range ds {
		ds[i] = Step{Effect: l.effect, Restriction: l.restriction}
	}
	return ds, nil
}
func (l link) Filter(ListRequest) (FilterExpr, error) {
	if l.checkOnly {
		return FilterExpr{}, ErrCheckOnly
	}
	if l.err != nil {
		return FilterExpr{}, l.err
	}
	if l.restriction != nil {
		return *l.restriction, nil
	}
	if l.effect == Deny || l.effect == NoDecision {
		return FilterExpr{Op: False}, nil
	}
	return FilterExpr{Op: True}, nil
}

type rule struct {
	decisions      []Decision
	err            error
	short          bool
	permit, forbid FilterExpr
}

func (r rule) Eval(_ context.Context, _ Slice, qs []CheckRequest) ([]Decision, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.short {
		return nil, nil
	}
	out := make([]Decision, len(qs))
	for i := range out {
		out[i] = r.decisions[i%len(r.decisions)]
	}
	return out, nil
}
func (r rule) Compile(q ListRequest) (FilterTemplate, error) {
	return FilterTemplate{Namespace: namespace(), Request: q, Permit: r.permit, Forbid: r.forbid}, r.err
}

func request() CheckRequest {
	return CheckRequest{Caller: Caller{Principal: model.PrincipalRef{Kind: "user", ID: "u"}, VerificationID: "verified", CapturedEpoch: model.Natural64(1)}, Action: Read, Resource: Resource{Kind: Bead, Ref: ref("b")}}
}
func permit() rule {
	return rule{decisions: []Decision{{Effect: Allow, Reasons: []string{"permit"}}}, permit: FilterExpr{Op: True}, forbid: FilterExpr{Op: False}}
}
func forbid() rule {
	return rule{decisions: []Decision{{Effect: Deny, Reasons: []string{"forbid"}}}, permit: FilterExpr{Op: False}, forbid: FilterExpr{Op: True}}
}

func TestChainTerminalOnlyAndDefaultDeny(t *testing.T) {
	cases := []struct {
		name  string
		links []Link
		rules []RuleSet
		want  Effect
		code  string
	}{
		{"last code allow", []Link{link{effect: NoDecision}, link{effect: Allow}}, nil, Allow, ""},
		{"early allow", []Link{link{effect: Allow}, link{effect: NoDecision}}, nil, "", "authz_link_allow_not_last"},
		{"code allow before rules", []Link{link{effect: Allow}}, []RuleSet{permit()}, "", "authz_link_allow_not_last"},
		{"pass only", []Link{link{effect: NoDecision}}, nil, Deny, ""},
		{"code deny sticky", []Link{link{effect: Deny}, link{effect: Allow}}, nil, Deny, ""},
		{"rules permit", []Link{link{effect: NoDecision}}, []RuleSet{permit()}, Allow, ""},
		{"forbid first", nil, []RuleSet{forbid(), permit()}, Deny, ""},
		{"forbid last", nil, []RuleSet{permit(), forbid()}, Deny, ""},
		{"link restriction", []Link{link{effect: NoDecision, restriction: &FilterExpr{Op: False}}}, []RuleSet{permit()}, Deny, ""},
		{"link error", []Link{link{err: errors.New("unavailable")}}, nil, "", "authz_unavailable"},
		{"ruleset error", nil, []RuleSet{rule{err: errors.New("unavailable")}}, "", "authz_unavailable"},
		{"bad link count", []Link{link{short: true}}, nil, "", "authz_unavailable"},
		{"bad rules count", nil, []RuleSet{rule{short: true}}, "", "authz_unavailable"},
		{"panic", []Link{link{panic: true}}, nil, "", "authz_unavailable"},
		{"unknown effect", []Link{link{effect: "surprise"}}, nil, "", "authz_unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Chain{Namespace: namespace(), Links: tc.links, Rules: tc.rules}
			ds, err := c.Check(context.Background(), chainSlice{true}, []CheckRequest{request()})
			if tc.code != "" {
				var e *Error
				if !errors.As(err, &e) || e.Code != tc.code {
					t.Fatalf("%+v %v", ds, err)
				}
				return
			}
			if err != nil || ds[0].Effect != tc.want {
				t.Fatalf("%+v %v", ds, err)
			}
		})
	}
}

func TestPolicyErrorsForbidWinsPermitSkippedAndReported(t *testing.T) {
	cases := []struct {
		name     string
		decision Decision
		want     Effect
	}{
		{"errored forbid", Decision{Effect: NoDecision, Errors: []PolicyError{{PolicyID: "broken-forbid", Effect: Deny, Message: "broken"}}}, Deny},
		{"errored permit", Decision{Effect: Allow, Reasons: []string{"broken-permit"}, Errors: []PolicyError{{PolicyID: "broken-permit", Effect: Allow, Message: "broken"}}}, Deny},
		{"another permit", Decision{Effect: Allow, Reasons: []string{"broken-permit", "good-permit"}, Errors: []PolicyError{{PolicyID: "broken-permit", Effect: Allow, Message: "broken"}}}, Allow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			alerts := 0
			c := Chain{Namespace: namespace(), Rules: []RuleSet{rule{decisions: []Decision{tc.decision}}}, Alert: func(PolicyError) { alerts++ }}
			ds, err := c.Check(context.Background(), chainSlice{true}, []CheckRequest{request()})
			if err != nil || ds[0].Effect != tc.want || len(ds[0].Errors) != 1 || alerts != 1 {
				t.Fatalf("%+v %v alerts=%d", ds, err, alerts)
			}
		})
	}
}

func TestChainBudgetAndInvalidContext(t *testing.T) {
	c := Chain{Namespace: namespace(), Links: []Link{link{wait: true}}, Budget: time.Millisecond}
	_, err := c.Check(context.Background(), chainSlice{true}, []CheckRequest{request()})
	var e *Error
	if !errors.As(err, &e) || e.Code != "authz_unavailable" || !e.Retryable {
		t.Fatalf("%v", err)
	}
	c.Budget = 101 * time.Millisecond
	if _, err := c.Check(context.Background(), chainSlice{true}, nil); err == nil {
		t.Fatal("budget ceiling not enforced")
	}
	c = Chain{Namespace: namespace(), Rules: []RuleSet{permit()}}
	ds, err := c.Check(context.Background(), chainSlice{false}, []CheckRequest{request()})
	if err != nil || ds[0].Effect != Deny {
		t.Fatalf("invalid context allowed: %+v %v", ds, err)
	}
}

func TestChainCompiledForbidAndIntersection(t *testing.T) {
	for _, c := range []Chain{
		{Namespace: namespace(), Rules: []RuleSet{permit()}},
		{Namespace: namespace(), Rules: []RuleSet{permit(), forbid()}},
		{Namespace: namespace(), Links: []Link{link{effect: NoDecision, restriction: &FilterExpr{Op: False}}}, Rules: []RuleSet{permit()}},
		{Namespace: namespace(), Links: []Link{link{effect: Allow}}},
		{Namespace: namespace(), Links: []Link{link{effect: NoDecision}}},
	} {
		q := request()
		ds, err := c.Check(context.Background(), chainSlice{true}, []CheckRequest{q})
		if err != nil {
			t.Fatal(err)
		}
		f, err := c.Compile(ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind})
		if err != nil {
			t.Fatal(err)
		}
		ok, err := f.Match(context.Background(), chainSlice{true}, q.Resource)
		if err != nil || ok != (ds[0].Effect == Allow) {
			t.Fatalf("compiled=%v decision=%+v error=%v", ok, ds, err)
		}
	}
	c := Chain{Namespace: namespace(), Links: []Link{link{checkOnly: true}}}
	q := request()
	if _, err := c.Compile(ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind}); !errors.Is(err, ErrCheckOnly) {
		t.Fatal("check-only filter must explicitly refuse")
	}
}

func TestFilterRejectsUnknownAndMalformedNodes(t *testing.T) {
	for _, f := range []FilterExpr{{Op: "sql"}, {Op: True, Role: "admin"}, {Op: All, Kind: "user"}, {Op: GrantExists, Subject: CallerSubject, Scope: "all"}, {Op: SponsorKind, Kind: "team"}} {
		if f.Validate() == nil {
			t.Errorf("invalid filter accepted: %+v", f)
		}
	}
}

type mutatingLink struct{}

func (mutatingLink) Check(_ context.Context, _ Slice, qs []CheckRequest) ([]Step, error) {
	qs[0].Caller.Principal.Kind = "service"
	qs[0].Caller.Principal.Caveats["actions"] = []string{"admin"}
	qs[0].Resource.Ref = ref("replaced")
	return []Step{{Effect: NoDecision}}, nil
}
func (mutatingLink) Filter(q ListRequest) (FilterExpr, error) {
	q.Caller.Principal.Kind = "service"
	q.Caller.Principal.Caveats["actions"] = []string{"admin"}
	return FilterExpr{Op: True}, nil
}
func TestCodeLinksCannotReplaceAuthenticatedBinding(t *testing.T) {
	q := request()
	q.Caller.Principal.Caveats = map[string]any{"actions": []string{"read"}}
	c := Chain{Namespace: namespace(), Links: []Link{mutatingLink{}}, Rules: []RuleSet{permit()}}
	ds, err := c.Check(context.Background(), chainSlice{true}, []CheckRequest{q})
	if err != nil || ds[0].Effect != Allow {
		t.Fatalf("%+v %v", ds, err)
	}
	if q.Caller.Principal.Kind != "user" || q.Resource.Ref.ID != "b" || q.Caller.Principal.Caveats["actions"].([]string)[0] != "read" {
		t.Fatal("link mutated host request")
	}
	f, err := c.Compile(ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := f.Match(context.Background(), chainSlice{true}, q.Resource)
	if err != nil || !ok {
		t.Fatal("compiled callback replaced authenticated context")
	}
}

func TestInvalidTargetsAndNamespaceFailClosed(t *testing.T) {
	c := Chain{Namespace: namespace(), Rules: []RuleSet{permit()}}
	for _, r := range []Resource{{Kind: "team", Ref: ref("b")}, {Kind: Bead, Ref: model.RefByKey("b")}, {Kind: Bead, Ref: model.RefByID("b", model.Natural64(1))}, {Kind: Bead, Ref: ref("b/other")}, {Kind: Bead, Ref: ref("")}} {
		q := request()
		q.Resource = r
		ds, err := c.Check(context.Background(), chainSlice{true}, []CheckRequest{q})
		if err != nil || ds[0].Effect != Deny {
			t.Fatalf("invalid target allowed: %+v %v", ds, err)
		}
	}
	q := request()
	f, err := c.Compile(ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind})
	if err != nil {
		t.Fatal(err)
	}
	f.Namespace.Incarnation = "different"
	if ok, err := f.Match(context.Background(), chainSlice{true}, q.Resource); err == nil || ok {
		t.Fatal("cross-incarnation template accepted")
	}
}
