package authz

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func preparedErrorShape(err error) any {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return struct {
			Code      string
			Retryable bool
			Cause     any
		}{e.Code, e.Retryable, preparedErrorShape(e.Cause)}
	}
	return reflect.TypeOf(err).String() + ":" + err.Error()
}

func TestPreparedChainDifferentialLegacyDecisionsAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		chain Chain
	}{
		{"permit", Chain{Rules: []RuleSet{permit()}}},
		{"forbid_then_permit", Chain{Rules: []RuleSet{forbid(), permit()}}},
		{"permit_then_forbid", Chain{Rules: []RuleSet{permit(), forbid()}}},
		{"restriction", Chain{Links: []Link{link{effect: NoDecision, restriction: &FilterExpr{Op: False}}}, Rules: []RuleSet{permit()}}},
		{"terminal_allow", Chain{Links: []Link{link{effect: NoDecision}, link{effect: Allow}}}},
		{"sticky_deny", Chain{Links: []Link{link{effect: Deny}, link{effect: Allow}}}},
		{"early_allow_p83", Chain{Links: []Link{link{effect: Allow}, link{effect: NoDecision}}}},
		{"allow_before_rules_p83", Chain{Links: []Link{link{effect: Allow}}, Rules: []RuleSet{permit()}}},
		{"check_only", Chain{Links: []Link{link{effect: NoDecision, checkOnly: true}}}},
		{"link_error", Chain{Links: []Link{link{err: errors.New("link unavailable")}}}},
		{"link_panic", Chain{Links: []Link{link{panic: true}}}},
		{"link_cardinality", Chain{Links: []Link{link{short: true}}}},
		{"link_unknown_effect", Chain{Links: []Link{link{effect: "unknown"}}}},
		{"rule_error", Chain{Rules: []RuleSet{rule{err: errors.New("rules unavailable")}}}},
		{"rule_cardinality", Chain{Rules: []RuleSet{rule{short: true}}}},
		{"errored_forbid", Chain{Rules: []RuleSet{rule{decisions: []Decision{{Effect: NoDecision, Errors: []PolicyError{{PolicyID: "bad", Effect: Deny, Message: "broken"}}}}, permit: FilterExpr{Op: False}, forbid: FilterExpr{Op: False}}, permit()}}},
		{"errored_permit", Chain{Rules: []RuleSet{rule{decisions: []Decision{{Effect: Allow, Reasons: []string{"bad"}, Errors: []PolicyError{{PolicyID: "bad", Effect: Allow, Message: "broken"}}}}, permit: FilterExpr{Op: False}, forbid: FilterExpr{Op: False}}}}},
		{"timeout", Chain{Links: []Link{link{wait: true}}, Budget: time.Millisecond}},
		{"invalid_budget", Chain{Rules: []RuleSet{permit()}, Budget: 101 * time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.chain
			c.Namespace = namespace()
			reqs := []CheckRequest{request(), request()}
			reqs[1].Resource.Ref = ref("second")
			want, wantErr := legacyChainCheck(c, context.Background(), chainSlice{true}, reqs)
			got, gotErr := c.Check(context.Background(), chainSlice{true}, reqs)
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(preparedErrorShape(gotErr), preparedErrorShape(wantErr)) {
				t.Fatalf("prepared=%+v/%v legacy=%+v/%v", got, gotErr, want, wantErr)
			}
			// Compile uses the frozen legacy path too; check-only/P83 refusal
			// cannot become an optimized membership success.
			q := ListRequest{Caller: reqs[0].Caller, Action: Read, ResourceKind: Bead}
			old, oldErr := legacyChainCompile(c, q)
			current, currentErr := c.Compile(q)
			if !reflect.DeepEqual(preparedErrorShape(currentErr), preparedErrorShape(oldErr)) {
				t.Fatalf("compiled prepared=%v legacy=%v", currentErr, oldErr)
			}
			if oldErr == nil {
				current.prepared = nil
				if !reflect.DeepEqual(current, old) {
					t.Fatalf("compiled descriptions differ: %+v / %+v", current, old)
				}
			}
		})
	}
}

type preparedBindingSlice struct {
	chainSlice
	contexts int
	deps     map[Dependency]bool
}

func (s *preparedBindingSlice) CheckContext(_ context.Context, c Caller) (bool, error) {
	s.contexts++
	return c.Principal.ID == "u", nil
}
func (s *preparedBindingSlice) Record(_ context.Context, dep Dependency) error {
	s.deps[dep] = true
	return nil
}

func TestPreparedBatchSeparatesEveryBindingAndInvalidResource(t *testing.T) {
	q := request()
	reqs := []CheckRequest{q, q, q, q, q, q, q}
	reqs[1].Resource.Ref = ref("second")
	reqs[2].Caller.Principal.ID = "other-user" // Same receipt, different caller.
	reqs[3].Action = Modify
	reqs[4].Resource.Kind = Project
	reqs[5].Method = "bead.get"
	reqs[6].Resource.Ref = model.RefByKey("invalid")
	c := Chain{Namespace: namespace(), Rules: []RuleSet{permit()}}
	old := &preparedBindingSlice{deps: map[Dependency]bool{}}
	current := &preparedBindingSlice{deps: map[Dependency]bool{}}
	want, wantErr := legacyChainCheck(c, context.Background(), old, reqs)
	got, gotErr := c.Check(context.Background(), current, reqs)
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(preparedErrorShape(gotErr), preparedErrorShape(wantErr)) || !reflect.DeepEqual(current.deps, old.deps) {
		t.Fatalf("prepared=%+v/%v legacy=%+v/%v dependencies=%v/%v", got, gotErr, want, wantErr, current.deps, old.deps)
	}
	if current.contexts != 5 || got[2].Effect != Deny || got[6].Effect != Deny {
		t.Fatalf("full bindings conflated or invalid resource reached context: contexts=%d decisions=%+v", current.contexts, got)
	}
}

type preparedIsolationLink struct {
	t      *testing.T
	mutate bool
}

func (l preparedIsolationLink) Check(_ context.Context, _ Slice, qs []CheckRequest) ([]Step, error) {
	out := make([]Step, len(qs))
	for i, q := range qs {
		if q.Caller.Principal.Caveats["actions"].([]any)[0] != "modify" || q.Caller.Principal.Sponsor.ID != "u" {
			l.t.Fatal("component input leaked a sibling or prior link mutation")
		}
		out[i].Effect = NoDecision
	}
	if l.mutate {
		qs[0].Caller.Principal.Caveats["actions"].([]any)[0] = "read"
		qs[0].Caller.Principal.Sponsor.ID = "changed"
		if qs[1].Caller.Principal.Caveats["actions"].([]any)[0] != "modify" || qs[1].Caller.Principal.Sponsor.ID != "u" {
			l.t.Fatal("two requests share mutable component input")
		}
	}
	return out, nil
}
func (preparedIsolationLink) Filter(ListRequest) (FilterExpr, error) {
	return FilterExpr{Op: True}, nil
}

func TestPreparedLinkInputsRemainIndependent(t *testing.T) {
	q := request()
	q.Caller.Principal = model.PrincipalRef{Kind: "agent", ID: "agent", Sponsor: &model.PrincipalRef{Kind: "user", ID: "u"}, Caveats: map[string]any{"actions": []string{"modify"}}}
	q.Action = Modify
	c := Chain{Namespace: namespace(), Links: []Link{preparedIsolationLink{t, true}, preparedIsolationLink{t, false}}, Rules: []RuleSet{permit()}}
	want, wantErr := legacyChainCheck(c, context.Background(), chainSlice{true}, []CheckRequest{q, q})
	got, gotErr := c.Check(context.Background(), chainSlice{true}, []CheckRequest{q, q})
	if !reflect.DeepEqual(got, want) || gotErr != nil || wantErr != nil {
		t.Fatalf("prepared=%+v/%v legacy=%+v/%v", got, gotErr, want, wantErr)
	}
	if q.Caller.Principal.Caveats["actions"].([]string)[0] != "modify" || q.Caller.Principal.Sponsor.ID != "u" {
		t.Fatal("host caller was mutated")
	}
}

func TestPreparedPrincipalPreservesExactNumbersAndOwnsMutableInputs(t *testing.T) {
	principal := model.PrincipalRef{Kind: "agent", ID: "agent", Sponsor: &model.PrincipalRef{Kind: "user", ID: "u", Caveats: map[string]any{"exact": json.Number("1.0")}}, Caveats: map[string]any{"numbers": []any{json.Number("9007199254740993"), json.Number("1e0")}}}
	r := ListRequest{Caller: Caller{Principal: principal}}
	p, err := PrepareRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	want, err := FreezeList(r)
	if err != nil {
		t.Fatal(err)
	}
	principal.Caveats["numbers"].([]any)[0] = json.Number("9007199254740992")
	principal.Sponsor.ID = "changed"
	copy := p.Request()
	copy.Caller.Principal.Caveats["numbers"].([]any)[1] = json.Number("1")
	copy.Caller.Principal.Sponsor.Caveats["exact"] = json.Number("1")
	if !reflect.DeepEqual(p.Request(), want) {
		t.Fatalf("mutable or rounded prepared principal: %+v want %+v", p.Request(), want)
	}
}

func TestPreparedBatchCapturesCallerInputsBeforeReuse(t *testing.T) {
	q := request()
	q.Caller.Principal.Caveats = map[string]any{"actions": []string{"read"}}
	want, err := FreezeList(ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind, Method: q.Method})
	if err != nil {
		t.Fatal(err)
	}
	batch := PrepareChecks([]CheckRequest{q, q})
	q.Caller.Principal.Caveats["actions"].([]string)[0] = "modify"
	for i := 0; i < batch.Len(); i++ {
		binding, err := batch.Binding(i)
		if err != nil || !reflect.DeepEqual(binding.Request(), want) {
			t.Fatalf("batch binding %d did not capture host input: %v/%v", i, binding, err)
		}
	}
}

func TestPreparedNoPluginAndMarshalFailureOrderMatchesLegacy(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["cycle"] = cyclic
	for _, tc := range []struct {
		name    string
		caveats map[string]any
	}{
		{"unscoped", nil},
		{"scoped_empty_actions", map[string]any{"actions": []string{}}},
		{"unsupported", map[string]any{"unknown": true}},
		{"cyclic", cyclic},
		{"function", map[string]any{"bad": func() {}}},
		{"nan", map[string]any{"bad": math.NaN()}},
	} {
		for _, installed := range []bool{false, true} {
			name := tc.name + "/no_plugins"
			if installed {
				name = tc.name + "/installed"
			}
			t.Run(name, func(t *testing.T) {
				c := Chain{Namespace: namespace()}
				if installed {
					c.Rules = []RuleSet{permit()}
				}
				q := request()
				q.Caller.Principal.Caveats = tc.caveats
				want, wantErr := legacyChainCheck(c, context.Background(), chainSlice{true}, []CheckRequest{q})
				got, gotErr := c.Check(context.Background(), chainSlice{true}, []CheckRequest{q})
				if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(preparedErrorShape(gotErr), preparedErrorShape(wantErr)) {
					t.Fatalf("prepared=%+v/%v legacy=%+v/%v", got, gotErr, want, wantErr)
				}
			})
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := Chain{Budget: -time.Second}
	got, err := c.Check(ctx, nil, []CheckRequest{{}})
	if err != nil || len(got) != 1 || got[0].Effect != Allow {
		t.Fatalf("uninstalled unscoped positive control performed eager validation: %+v/%v", got, err)
	}
}
