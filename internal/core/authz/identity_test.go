package authz

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
func namespace() Namespace {
	return Namespace{StoreID: "store", Incarnation: "inc", InstallationID: "fixed", Authority: ref("authority")}
}

func TestCanonicalRefsAndInjectiveIdentities(t *testing.T) {
	n := namespace()
	n.Authority = model.Ref{ID: "authority"}
	if !sameNamespace(n, namespace()) {
		t.Fatal("equivalent ID-arm/zero-branch namespace representations differ")
	}
	for _, r := range []model.Ref{{}, model.RefByID("", model.Natural64(0)), model.RefByKey("key"), model.RefByID("p", model.Natural64(1)), ref(" p"), ref("p\x00q"), ref(string([]byte{0xff}))} {
		if CanonicalRef(r) == nil {
			t.Errorf("accepted invalid reference %+v", r)
		}
	}
	seen := map[string]bool{}
	for _, id := range []string{"p", "a:b", "web/api", "é", "s1_abc"} {
		for _, kind := range []ScopeKind{AuthorityScope, TeamScope, ProjectScope} {
			s, err := ScopeComponent(namespace(), Scope{Kind: kind, Ref: ref(id)})
			if err != nil {
				t.Fatal(err)
			}
			if seen[s] || strings.ContainsAny(s, "/\x00") {
				t.Fatalf("invalid/aliased key %q", s)
			}
			seen[s] = true
		}
	}
	if Encode("ab", "c") == Encode("a", "bc") || Encode("é") != "323ac3a9" {
		t.Fatal("encoding must use injective UTF-8 byte lengths")
	}
}

func TestNoPluginAllowsWithoutFacts(t *testing.T) {
	c := Chain{}
	d, err := c.Check(context.Background(), nil, []CheckRequest{{}})
	if err != nil || len(d) != 1 || d[0].Effect != Allow {
		t.Fatalf("%+v %v", d, err)
	}
	f, err := c.Compile(ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := f.Match(context.Background(), nil, Resource{})
	if err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	r := ListRequest{Caller: Caller{Principal: model.PrincipalRef{Kind: "agent", ID: "a", Caveats: map[string]any{"actions": []string{"read"}}}}}
	if _, err := c.Compile(r); err == nil {
		t.Fatal("scoped token must refuse without authz")
	}
}

type sourceReferenceSlice struct {
	chainSlice
	declared string
	deps     []Dependency
}

func (s *sourceReferenceSlice) SourceBeadReference(target model.Ref) bool {
	return target.ID == s.declared
}
func (s *sourceReferenceSlice) Record(_ context.Context, dep Dependency) error {
	s.deps = append(s.deps, dep)
	return nil
}
func (s *sourceReferenceSlice) ProjectForBead(_ context.Context, target model.Ref) (ProjectBinding, error) {
	return ProjectBinding{Name: "demo", Ref: ref("project"), Found: target.ID == s.declared}, nil
}
func (s *sourceReferenceSlice) ProjectActive(_ context.Context, project model.Ref) (bool, error) {
	return project.ID == "project", nil
}

func TestSourceBeadIdentityRequiresHostBindingAndRetainsPolicy(t *testing.T) {
	for _, test := range []struct {
		name    string
		target  model.Ref
		host    bool
		context bool
		caveats map[string]any
		forbid  bool
		want    bool
	}{
		{name: "ordinary-slice-refuses-source", target: ref("workflow:item/declared"), context: true},
		{name: "bound-source", target: ref("workflow:item/declared"), host: true, context: true, want: true},
		{name: "unknown-source", target: ref("workflow:item/unknown"), host: true, context: true},
		{name: "nonzero-branch", target: model.RefByID("workflow:item/declared", model.Natural64(1)), host: true, context: true},
		{name: "expired-context", target: ref("workflow:item/declared"), host: true},
		{name: "empty-action-caveat", target: ref("workflow:item/declared"), host: true, context: true, caveats: map[string]any{"actions": []string{}}},
		{name: "different-ref-caveat", target: ref("workflow:item/declared"), host: true, context: true, caveats: map[string]any{"refs": []model.Ref{ref("ordinary")}}},
		{name: "exact-ref-caveat", target: ref("workflow:item/declared"), host: true, context: true, caveats: map[string]any{"refs": []model.Ref{ref("workflow:item/declared")}}, want: true},
		{name: "policy-forbid", target: ref("workflow:item/declared"), host: true, context: true, forbid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			bound := &sourceReferenceSlice{chainSlice: chainSlice{test.context}, declared: "workflow:item/declared"}
			var facts Slice = chainSlice{test.context}
			if test.host {
				facts = bound
			}
			q := request()
			q.Resource.Ref, q.Caller.Principal.Caveats = test.target, test.caveats
			chain := Chain{Namespace: namespace(), Rules: []RuleSet{permit()}}
			if test.forbid {
				chain.Rules = append(chain.Rules, forbid())
			}
			checks := []CheckRequest{q}
			for _, api := range []string{"check", "prepared", "filter", "sealed"} {
				var allowed bool
				var err error
				if api == "check" || api == "prepared" {
					var decisions []Decision
					if api == "check" {
						decisions, err = chain.Check(ctx, facts, checks)
					} else {
						decisions, err = chain.CheckPrepared(ctx, facts, PrepareChecks(checks))
					}
					allowed = len(decisions) == 1 && decisions[0].Effect == Allow
				} else {
					var filter FilterTemplate
					filter, err = chain.Compile(ListRequest{Caller: q.Caller, Action: q.Action, ResourceKind: q.Resource.Kind, Method: q.Method})
					// Exercise the real project lookup without constructing a core
					// bead/closure sentinel for a separately guarded source target.
					filter.Permit = And(filter.Permit, FilterExpr{Op: ActiveProject})
					if err == nil && api == "filter" {
						allowed, err = filter.Match(ctx, facts, q.Resource)
					} else if err == nil {
						var sealed *PreparedFilter
						sealed, err = filter.Prepare()
						if err == nil {
							allowed, err = sealed.Match(ctx, facts, q.Resource)
						}
					}
				}
				if err != nil || allowed != test.want {
					t.Fatalf("%s allowed=%v want=%v error=%v", api, allowed, test.want, err)
				}
			}
			for _, dep := range bound.deps {
				if dep.Kind == "sentinel" && (strings.HasPrefix(dep.Key, "bead/") || strings.HasPrefix(dep.Key, "closure/")) {
					t.Fatalf("source identity invented an ordinary record guard: %+v", dep)
				}
			}
		})
	}
}
