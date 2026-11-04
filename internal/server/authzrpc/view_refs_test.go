package authzrpc

import (
	"context"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This extra configured plane permits a known agent/sponsor shape. Its
// ordinary interpreter retains the core caveat semantics independently from
// the storage lowering, so sponsor intersections can affect an allowed row.
type w10AgentRead struct{ namespace authz.Namespace }

func (r w10AgentRead) Compile(q authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{Namespace: r.namespace, Request: q,
		Permit: authz.And(authz.FilterExpr{Op: authz.PrincipalKind, Kind: "agent"}, authz.FilterExpr{Op: authz.SponsorKind, Kind: "user"}),
		Forbid: authz.FilterExpr{Op: authz.False}}, nil
}
func (r w10AgentRead) Eval(ctx context.Context, s authz.Slice, qs []authz.CheckRequest) ([]authz.Decision, error) {
	out := make([]authz.Decision, len(qs))
	for i, q := range qs {
		template, err := r.Compile(authz.ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind, Method: q.Method})
		if err != nil {
			return nil, err
		}
		ok, err := template.Match(ctx, s, q.Resource)
		if err != nil {
			return nil, err
		}
		out[i].Effect = authz.NoDecision
		if ok {
			out[i].Effect = authz.Allow
		}
	}
	return out, nil
}

func TestW10FixedRefsFullPolicyIntersectionAcrossEngines(t *testing.T) {
	w9Engines(t, func(t *testing.T, store driver.Store) {
		f := newW9Fixture(t, store)
		f.seed(t, []driver.Record{w9Record("a", "bug", "visible-name", "a", "todo"), w9Record("b", "bug", "hidden-name", "b", "todo"), w9Record("c", "bug", "visible-name", "c", "todo")})
		f.store.allowScan = true
		f.app.Checker.chain.Rules = append(f.app.Checker.chain.Rules, w10AgentRead{f.base.Namespace})
		cases := []struct {
			name               string
			principal, sponsor map[string]any
			agent, compiled    bool
			want               []string
		}{
			{name: "user_omitted", compiled: true, want: []string{"a", "c"}},
			{name: "user_fixed_refs", principal: map[string]any{"refs": []model.Ref{w9Ref("a"), w9Ref("b")}}, compiled: true, want: []string{"a"}},
			{name: "user_empty", principal: map[string]any{"refs": []model.Ref{}}, compiled: true, want: []string{}},
			{name: "sponsor_intersection", agent: true, principal: map[string]any{"refs": []model.Ref{w9Ref("a"), w9Ref("b")}}, sponsor: map[string]any{"refs": []model.Ref{w9Ref("b"), w9Ref("c")}}, compiled: true, want: []string{"b"}},
			{name: "sponsor_empty", agent: true, sponsor: map[string]any{"refs": []model.Ref{}}, compiled: true, want: []string{}},
			{name: "sponsor_omitted", agent: true, compiled: true, want: []string{"a", "b", "c"}},
			{name: "actions_fallback", principal: map[string]any{"actions": []string{"read"}}, want: []string{"a", "c"}},
			{name: "projects_fallback", principal: map[string]any{"projects": []model.Ref{w9Ref("project-canonical-visible")}}, want: []string{"a", "c"}},
			{name: "unknown_fallback", principal: map[string]any{"unknown": []model.Ref{w9Ref("a")}}, want: []string{}},
			{name: "null_fallback", principal: map[string]any{"refs": nil}, want: []string{}},
			{name: "malformed_fallback", principal: map[string]any{"refs": []string{"a"}}, want: []string{}},
			{name: "noncanonical_fallback", principal: map[string]any{"refs": []model.Ref{model.RefByID("a", model.Natural64(1))}}, want: []string{}},
			{name: "sponsor_projects_fallback", agent: true, sponsor: map[string]any{"projects": []model.Ref{w9Ref("project-canonical-visible")}}, want: []string{"a", "c"}},
			{name: "sponsor_null_fallback", agent: true, sponsor: map[string]any{"refs": nil}, want: []string{}},
			{name: "empty_refs_unsupported_sponsor_fallback", agent: true, principal: map[string]any{"refs": []model.Ref{}}, sponsor: map[string]any{"unknown": true}, want: []string{}},
			{name: "slash_ref_fallback", principal: map[string]any{"refs": []model.Ref{w9Ref("a/b")}}, want: []string{}},
			{name: "mixed_slash_refs_fallback", principal: map[string]any{"refs": []model.Ref{w9Ref("a"), w9Ref("a/b")}}, want: []string{"a"}},
			{name: "sponsor_mixed_slash_refs_fallback", agent: true, principal: map[string]any{"refs": []model.Ref{w9Ref("a"), w9Ref("b")}}, sponsor: map[string]any{"refs": []model.Ref{w9Ref("b"), w9Ref("a/b")}}, want: []string{"b"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				by := w9Reader
				by.Caveats = tc.principal
				if tc.agent {
					by = model.PrincipalRef{Kind: "agent", ID: "agent:worker", Caveats: tc.principal, Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:direct", Caveats: tc.sponsor}}
				}
				cfg := viewops.Config{Selection: f.app.Service.ViewSelection(), OntologyRevision: model.Natural64(1)}
				bind, fences := cfg.Selection.Bind, 0
				cfg.Selection.Bind = func(ctx context.Context, by model.PrincipalRef, inline string) (*viewops.SelectionAuthority, error) {
					authority, err := bind(ctx, by, inline)
					if err == nil && authority != nil {
						fence := authority.Fence
						authority.Fence = func(ctx context.Context) error { fences++; return fence(ctx) }
					}
					return authority, err
				}
				v, err := viewops.New(f.store, f.lease, cfg)
				if err != nil {
					t.Fatal(err)
				}
				req := viewops.RunRequest{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}}}
				before := f.store.selects
				count, err := v.Run(context.Background(), by, req)
				if err != nil || count.Count == nil || *count.Count != len(tc.want) {
					t.Fatalf("count=%+v err=%v want%v", count, err, tc.want)
				}
				req.CountOnly = false
				page, err := v.Run(context.Background(), by, req)
				if err != nil || !reflect.DeepEqual(w9IDs(page), tc.want) {
					t.Fatalf("page=%+v err=%v want%v", page, err, tc.want)
				}
				calls := f.store.selects - before
				if tc.compiled && calls != 2 || !tc.compiled && calls != 0 {
					t.Fatalf("compiled=%t selects%d", tc.compiled, calls)
				}
				if fences != 2 {
					t.Fatalf("count/page final fences%d want2", fences)
				}
			})
		}
	})
}

func TestW10FixedRefsKeepFinalAuthorityFence(t *testing.T) {
	w9Engines(t, func(t *testing.T, store driver.Store) {
		f := newW9Fixture(t, store)
		f.seed(t, []driver.Record{w9Record("a", "bug", "visible-name", "a", "todo")})
		by := w9Reader
		by.Caveats = map[string]any{"refs": []model.Ref{w9Ref("a")}}
		f.store.afterSelect = func(driver.SelectionResult) {
			changed := f.base
			changed.Grants = append(append([]authz.Grant{}, changed.Grants...), authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: by.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: w9Ref("project-canonical-visible")}, Role: "involved"})
			if err := f.facts.Replace(changed); err != nil {
				t.Fatal(err)
			}
		}
		_, err := f.views(t, viewops.Config{}).Run(context.Background(), by, viewops.RunRequest{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}}})
		w9Code(t, err, "authz_unavailable")
		if f.store.selects != 1 || f.store.queries != 0 {
			t.Fatalf("final-fence route selects%d queries%d", f.store.selects, f.store.queries)
		}
	})
}

func TestW10FixedRefsRetainVectorBudget(t *testing.T) {
	w9Engines(t, func(t *testing.T, store driver.Store) {
		f := newW9Fixture(t, store)
		f.seed(t, []driver.Record{w9Record("a", "bug", "visible-name", "a", "todo"), w9Record("b", "bug", "hidden-name", "b", "todo")})
		f.store.allowScan = true
		for _, n := range []int{driver.SelectionMaxNodes, driver.SelectionMaxNodes + 1} {
			refs := make([]model.Ref, n)
			for i := range refs {
				refs[i] = w9Ref("a")
			}
			by := w9Reader
			by.Caveats = map[string]any{"refs": refs}
			before := f.store.selects
			count, err := f.views(t, viewops.Config{}).Run(context.Background(), by, viewops.RunRequest{CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}}})
			if err != nil || count.Count == nil || *count.Count != 1 {
				t.Fatalf("vector%d count=%+v err=%v", n, count, err)
			}
			wantSelects := 0
			if n == driver.SelectionMaxNodes {
				wantSelects = 1
			}
			if got := f.store.selects - before; got != wantSelects {
				t.Fatalf("vector%d selects%d want%d", n, got, wantSelects)
			}
		}
	})
}
