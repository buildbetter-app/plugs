package authzfixed

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

type preparedFactSlice struct {
	*slice
	preparation authz.Preparation
}

func (s *preparedFactSlice) AuthorizationPreparation() *authz.Preparation { return &s.preparation }

func fixedPreparedError(err error) any {
	if err == nil {
		return nil
	}
	var e *authz.Error
	if errors.As(err, &e) {
		return struct {
			Code      string
			Retryable bool
			Cause     any
		}{e.Code, e.Retryable, fixedPreparedError(e.Cause)}
	}
	return reflect.TypeOf(err).String() + ":" + err.Error()
}

func preparedMatrixSlice(c authz.Caller, bits int) *slice {
	s := newSlice(c)
	s.data.Teams[0].Active = bits&8 != 0
	s.data.Projects[0].Active = bits&16 != 0
	for _, row := range []struct {
		bit int
		g   authz.Grant
	}{
		{1, grant("user", "u", authz.ProjectScope, "project", "involved")},
		{2, grant("user", "u", authz.TeamScope, "team", "member")},
		{4, grant("team", "team", authz.ProjectScope, "project", "involved")},
		{32, grant("user", "teammate", authz.ProjectScope, "project", "involved")},
		{64, grant("user", "teammate", authz.TeamScope, "team", "member")},
		{128, grant("user", c.Principal.ID, authz.AuthorityScope, "authority", "admin")},
	} {
		if bits&row.bit != 0 {
			s.data.Grants = append(s.data.Grants, row.g)
		}
	}
	return s
}

func TestPreparedCP7DifferentialFrozenInterpreter(t *testing.T) {
	r := rules(t)
	u := user()
	svc := model.PrincipalRef{Kind: "service", ID: "svc"}
	principals := []authz.Caller{caller("user", nil), caller("agent", &u), caller("agent", nil), caller("agent", &svc), caller("service", nil), caller("plugin", nil), caller("system", nil), caller("anonymous", nil)}
	comparisons := 0
	for bits := 0; bits < 256; bits++ {
		for _, c := range principals {
			var requests []authz.CheckRequest
			for _, row := range []struct {
				action     authz.Action
				kind       authz.ResourceKind
				id, method string
			}{
				{authz.Read, authz.Bead, "b", ""},
				{authz.Read, authz.Bead, "projectless", "bead.list"},
				{authz.Modify, authz.Project, "project", ""},
				{authz.Modify, authz.Project, "unknown-project", ""},
				{authz.Modify, authz.Bead, "b", "bead.claim"},
				{authz.Modify, authz.Bead, "b", "bead.claimNext"},
				{authz.Modify, authz.Bead, "b", "bead.update"},
				{authz.ImportBulkHistory, authz.History, "authority", ""},
				{authz.Modify, authz.Bead, "b", "bead.close"},
				{"transition", authz.Project, "project", ""},
			} {
				requests = append(requests, authz.CheckRequest{Caller: c, Action: row.action, Resource: authz.Resource{Kind: row.kind, Ref: ref(row.id)}, Method: row.method})
			}
			old := preparedMatrixSlice(c, bits)
			current := &preparedFactSlice{slice: preparedMatrixSlice(c, bits)}
			want, wantErr := legacyFixedEval(ns(), context.Background(), old, requests)
			got, gotErr := r.Eval(context.Background(), current, requests)
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(fixedPreparedError(gotErr), fixedPreparedError(wantErr)) || !reflect.DeepEqual(current.recorded, old.recorded) {
				t.Fatalf("bits=%08b kind=%s prepared=%+v/%v legacy=%+v/%v dependencies=%v/%v", bits, c.Principal.Kind, got, gotErr, want, wantErr, current.recorded, old.recorded)
			}
			for _, q := range requests {
				binding := authz.ListRequest{Caller: c, Action: q.Action, ResourceKind: q.Resource.Kind, Method: q.Method}
				before, e := legacyFixedCompile(ns(), binding)
				if e != nil {
					t.Fatal(e)
				}
				after, e := r.Compile(binding)
				if e != nil {
					t.Fatal(e)
				}
				want, we := legacyMatch(before, context.Background(), preparedMatrixSlice(c, bits), q.Resource)
				got, ge := after.Match(context.Background(), &preparedFactSlice{slice: preparedMatrixSlice(c, bits)}, q.Resource)
				if got != want || !reflect.DeepEqual(fixedPreparedError(ge), fixedPreparedError(we)) {
					t.Fatalf("bits=%08b request=%+v prepared=%v/%v legacy=%v/%v", bits, q, got, ge, want, we)
				}
				comparisons++
			}
		}
	}
	t.Logf("%d compiled membership and full-batch decisions compared with frozen 8dab45e interpreter; existing independent 16384-case truth table retained", comparisons)
}

func TestPreparedCaveatPresenceAndSponsorDifferential(t *testing.T) {
	for _, tc := range []struct {
		name    string
		caveats map[string]any
		sponsor map[string]any
		want    bool
	}{
		{"omitted", nil, nil, true},
		{"all_present", map[string]any{"actions": []string{"modify"}, "projects": []model.Ref{ref("project")}, "refs": []model.Ref{ref("b")}}, nil, true},
		{"empty_actions", map[string]any{"actions": []string{}}, nil, false},
		{"empty_projects", map[string]any{"projects": []model.Ref{}}, nil, false},
		{"empty_refs", map[string]any{"refs": []model.Ref{}}, nil, false},
		{"null_actions", map[string]any{"actions": nil}, nil, false},
		{"null_projects", map[string]any{"projects": nil}, nil, false},
		{"null_refs", map[string]any{"refs": nil}, nil, false},
		{"malformed_actions", map[string]any{"actions": true}, nil, false},
		{"mixed_action_type", map[string]any{"actions": []any{"modify", 2}}, nil, false},
		// Preserve the legacy []string codec's null-to-empty-string behavior.
		{"matching_action_then_null", map[string]any{"actions": []any{"modify", nil}}, nil, true},
		{"malformed_projects", map[string]any{"projects": "project"}, nil, false},
		{"trailing_key_ref", map[string]any{"refs": []model.Ref{ref("b"), model.RefByKey("bad")}}, nil, false},
		{"trailing_branched_project", map[string]any{"projects": []model.Ref{ref("project"), model.RefByID("other", model.Natural64(1))}}, nil, false},
		{"literal_ref", map[string]any{"refs": []model.Ref{{ID: "b"}}}, nil, true},
		{"equivalent_zero_branch", map[string]any{"refs": []model.Ref{model.RefByID("b", model.Natural("0.0"))}}, nil, true},
		{"unknown_provider", map[string]any{"provider": map[string]any{"limit": 1}}, nil, false},
		{"sponsor_allows", map[string]any{"refs": []model.Ref{ref("b")}}, map[string]any{"projects": []model.Ref{ref("project")}, "actions": []string{"modify"}}, true},
		{"sponsor_empty", nil, map[string]any{"refs": []model.Ref{}}, false},
		{"sponsor_intersection", map[string]any{"actions": []string{"modify"}}, map[string]any{"actions": []string{"read"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := user()
			u.Caveats = tc.sponsor
			c := caller("agent", &u)
			c.Principal.Caveats = tc.caveats
			q := authz.CheckRequest{Caller: c, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: ref("b")}, Method: "bead.update"}
			old := preparedMatrixSlice(c, 17)
			current := &preparedFactSlice{slice: preparedMatrixSlice(c, 17)}
			want, wantErr := legacyFixedEval(ns(), context.Background(), old, []authz.CheckRequest{q, q})
			got, gotErr := rules(t).Eval(context.Background(), current, []authz.CheckRequest{q, q})
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(fixedPreparedError(gotErr), fixedPreparedError(wantErr)) || !reflect.DeepEqual(current.recorded, old.recorded) || (got[0].Effect == authz.Allow) != tc.want {
				t.Fatalf("prepared=%+v/%v legacy=%+v/%v want_allowed=%v dependencies=%v/%v", got, gotErr, want, wantErr, tc.want, current.recorded, old.recorded)
			}
		})
	}
}

func TestPreparedResourceFailuresAndErrorOrderDifferential(t *testing.T) {
	for _, failure := range []string{"", "record", "context", "assignment", "grant", "groups", "team", "project"} {
		for _, resource := range []authz.Resource{{Kind: authz.Bead, Ref: ref("b")}, {Kind: authz.Bead, Ref: model.RefByKey("bad")}, {Kind: authz.Bead, Ref: ref("bad/id")}, {Kind: "unknown", Ref: ref("b")}} {
			t.Run(fmt.Sprintf("%s/%s/%s", failure, resource.Kind, resource.Ref.ID), func(t *testing.T) {
				u := user()
				c := caller("agent", &u)
				old := preparedMatrixSlice(c, 30) // Only the active one-hop team path.
				current := &preparedFactSlice{slice: preparedMatrixSlice(c, 30)}
				old.fail, current.fail = failure, failure
				q := authz.CheckRequest{Caller: c, Action: authz.Modify, Resource: resource, Method: "bead.claimNext"}
				want, wantErr := legacyFixedEval(ns(), context.Background(), old, []authz.CheckRequest{q})
				got, gotErr := rules(t).Eval(context.Background(), current, []authz.CheckRequest{q})
				if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(fixedPreparedError(gotErr), fixedPreparedError(wantErr)) || !reflect.DeepEqual(current.recorded, old.recorded) {
					t.Fatalf("prepared=%+v/%v legacy=%+v/%v dependencies=%v/%v", got, gotErr, want, wantErr, current.recorded, old.recorded)
				}
			})
		}
	}
	for _, refs := range [][]model.Ref{{}, {ref("project"), model.RefByKey("bad")}} {
		t.Run(fmt.Sprintf("project_caveat_resolves_before_deny_%d", len(refs)), func(t *testing.T) {
			u := user()
			c := caller("agent", &u)
			c.Principal.Caveats = map[string]any{"projects": refs}
			old, current := preparedMatrixSlice(c, 17), &preparedFactSlice{slice: preparedMatrixSlice(c, 17)}
			old.fail, current.fail = "assignment", "assignment"
			q := authz.CheckRequest{Caller: c, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: ref("b")}, Method: "bead.update"}
			want, we := legacyFixedEval(ns(), context.Background(), old, []authz.CheckRequest{q})
			got, ge := rules(t).Eval(context.Background(), current, []authz.CheckRequest{q})
			if ge == nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(fixedPreparedError(ge), fixedPreparedError(we)) || !reflect.DeepEqual(current.recorded, old.recorded) {
				t.Fatalf("prepared=%+v/%v legacy=%+v/%v", got, ge, want, we)
			}
		})
	}
}

func TestPreparedTemplateMutationAndFreshAuthority(t *testing.T) {
	u := user()
	c := caller("agent", &u)
	binding := authz.ListRequest{Caller: c, Action: authz.Modify, ResourceKind: authz.Bead, Method: "bead.update"}
	resource := authz.Resource{Kind: authz.Bead, Ref: ref("b")}
	for _, tc := range []struct {
		name string
		edit func(*authz.FilterTemplate)
	}{
		{"namespace", func(f *authz.FilterTemplate) { f.Namespace.Incarnation = "different" }},
		{"action", func(f *authz.FilterTemplate) { f.Request.Action = authz.Read }},
		{"method", func(f *authz.FilterTemplate) { f.Request.Method = "bead.get" }},
		{"kind", func(f *authz.FilterTemplate) { f.Request.ResourceKind = authz.Project }},
		{"receipt", func(f *authz.FilterTemplate) { f.Request.Caller.VerificationID = "different" }},
		{"epoch", func(f *authz.FilterTemplate) { f.Request.Caller.CapturedEpoch = model.Natural64(2) }},
		{"caveat", func(f *authz.FilterTemplate) {
			f.Request.Caller.Principal.Caveats = map[string]any{"actions": []string{}}
		}},
		{"sponsor", func(f *authz.FilterTemplate) { f.Request.Caller.Principal.Sponsor.ID = "different" }},
		{"forbid", func(f *authz.FilterTemplate) { f.Forbid = authz.FilterExpr{Op: authz.True} }},
		{"invalid_expr", func(f *authz.FilterTemplate) { f.Permit.Op = "unsupported" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old, err := legacyFixedCompile(ns(), binding)
			if err != nil {
				t.Fatal(err)
			}
			current, err := rules(t).Compile(binding)
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&old)
			tc.edit(&current)
			want, we := legacyMatch(old, context.Background(), preparedMatrixSlice(c, 17), resource)
			got, ge := current.Match(context.Background(), &preparedFactSlice{slice: preparedMatrixSlice(c, 17)}, resource)
			if got != want || !reflect.DeepEqual(fixedPreparedError(ge), fixedPreparedError(we)) {
				t.Fatalf("prepared=%v/%v legacy=%v/%v", got, ge, want, we)
			}
		})
	}
	f, err := rules(t).Compile(binding)
	if err != nil {
		t.Fatal(err)
	}
	s := &preparedFactSlice{slice: preparedMatrixSlice(c, 17)}
	if ok, err := f.Match(context.Background(), s, resource); err != nil || !ok {
		t.Fatalf("legitimate positive control=%v/%v", ok, err)
	}
	s.data.Grants = nil
	if ok, err := f.Match(context.Background(), s, resource); err != nil || ok {
		t.Fatalf("prepared context cached an effective grant=%v/%v", ok, err)
	}
	fresh := &preparedFactSlice{slice: preparedMatrixSlice(c, 17)}
	fresh.contextActive = false
	if ok, err := f.Match(context.Background(), fresh, resource); err != nil || ok {
		t.Fatalf("compiled parsing reused revoked fresh context=%v/%v", ok, err)
	}
	// An equivalent mathematical epoch retains the baseline equality domain.
	fresh = &preparedFactSlice{slice: preparedMatrixSlice(c, 17)}
	fresh.epoch = model.Natural("1e0")
	if ok, err := f.Match(context.Background(), fresh, resource); err != nil || !ok {
		t.Fatalf("equivalent current epoch changed permission=%v/%v", ok, err)
	}
}

func TestPreparedBatchKeepsResourceFactsIndependent(t *testing.T) {
	u := user()
	c := caller("agent", &u)
	old, current := preparedMatrixSlice(c, 17), &preparedFactSlice{slice: preparedMatrixSlice(c, 17)}
	old.beads["other"] = authz.ProjectBinding{Name: "other", Ref: ref("other-project"), Found: true}
	current.beads["other"] = old.beads["other"]
	var requests []authz.CheckRequest
	for _, id := range []string{"b", "other", "projectless", "b"} {
		requests = append(requests, authz.CheckRequest{Caller: c, Action: authz.Modify, Resource: authz.Resource{Kind: authz.Bead, Ref: ref(id)}, Method: "bead.update"})
	}
	want, we := legacyFixedEval(ns(), context.Background(), old, requests)
	got, ge := rules(t).Eval(context.Background(), current, requests)
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(fixedPreparedError(ge), fixedPreparedError(we)) || !reflect.DeepEqual(current.recorded, old.recorded) {
		t.Fatalf("prepared=%+v/%v legacy=%+v/%v dependencies=%v/%v", got, ge, want, we, current.recorded, old.recorded)
	}
	if got[0].Effect != authz.Allow || got[1].Effect != authz.NoDecision || got[2].Effect != authz.NoDecision || got[3].Effect != authz.Allow {
		t.Fatalf("one resource supplied another's permit: %+v", got)
	}
}

func TestPreparedReusedBatchFreshSnapshotDifferential(t *testing.T) {
	for _, provider := range []bool{false, true} {
		for _, useEval := range []bool{false, true} {
			t.Run(fmt.Sprintf("provider=%v/eval=%v", provider, useEval), func(t *testing.T) {
				c := caller("user", nil)
				q := authz.CheckRequest{Caller: c, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: ref("b")}, Method: "bead.get"}
				batch := authz.PrepareChecks([]authz.CheckRequest{q})
				p, err := batch.Binding(0)
				if err != nil {
					t.Fatal(err)
				}
				r := rules(t)
				filter, err := r.CompilePrepared(p)
				if err != nil {
					t.Fatal(err)
				}
				for _, state := range []string{"active", "epoch_changed", "revoked", "context_error", "restored"} {
					old, current := newSlice(c), newSlice(c)
					if state == "epoch_changed" {
						old.epoch, current.epoch = model.Natural64(2), model.Natural64(2)
					}
					if state == "revoked" {
						old.contextActive, current.contextActive = false, false
					}
					if state == "context_error" {
						old.fail, current.fail = "context", "context"
					}
					var facts authz.Slice = current
					if provider {
						facts = &preparedFactSlice{slice: current}
					}
					want, we := legacyFixedEval(ns(), context.Background(), old, []authz.CheckRequest{q})
					var got []authz.Decision
					var ge error
					if useEval {
						got, ge = r.EvalPrepared(context.Background(), facts, batch)
					} else {
						var allowed bool
						allowed, ge = batch.Match(context.Background(), facts, 0, filter)
						if ge == nil {
							got = []authz.Decision{{Effect: authz.NoDecision}}
							if allowed {
								got[0] = authz.Decision{Effect: authz.Allow, Reasons: []string{"cp7.read"}}
							}
						}
					}
					if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(fixedPreparedError(ge), fixedPreparedError(we)) || !reflect.DeepEqual(current.recorded, old.recorded) {
						t.Fatalf("%s: prepared=%+v/%v legacy=%+v/%v dependencies=%v/%v", state, got, ge, want, we, current.recorded, old.recorded)
					}
				}
			})
		}
	}
}

func TestPreparedFixedEvalPreservesEarlierFactError(t *testing.T) {
	c := caller("user", nil)
	q := authz.CheckRequest{Caller: c, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: ref("b")}, Method: "bead.get"}
	bad := q
	bad.Caller.Principal.Caveats = map[string]any{"bad": func() {}}
	old, current := newSlice(c), &preparedFactSlice{slice: newSlice(c)}
	old.fail, current.fail = "context", "context"
	want, we := legacyFixedEval(ns(), context.Background(), old, []authz.CheckRequest{q, bad})
	got, ge := rules(t).Eval(context.Background(), current, []authz.CheckRequest{q, bad})
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(fixedPreparedError(ge), fixedPreparedError(we)) {
		t.Fatalf("later malformed binding displaced earlier context error: prepared=%+v/%v legacy=%+v/%v", got, ge, want, we)
	}
}
