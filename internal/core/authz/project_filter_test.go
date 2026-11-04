package authz

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type projectGrant struct {
	subject SubjectRef
	kind    ScopeKind
	scope   string
	role    string
}

type projectFilterSlice struct {
	valid, active                   bool
	project                         ProjectBinding
	grants                          map[projectGrant]bool
	groups                          []model.Ref
	records, contexts, lookups      int
	grantReads, groupReads, actives int
	err                             error
}

func (s *projectFilterSlice) Namespace() Namespace { return namespace() }
func (s *projectFilterSlice) Record(context.Context, Dependency) error {
	s.records++
	return nil
}
func (s *projectFilterSlice) CheckContext(context.Context, Caller) (bool, error) {
	s.contexts++
	return s.valid, nil
}
func (s *projectFilterSlice) HasGrant(_ context.Context, sub SubjectRef, scope Scope, role string) (bool, error) {
	s.grantReads++
	return s.grants[projectGrant{sub, scope.Kind, scope.Ref.ID, role}], s.err
}
func (s *projectFilterSlice) Groups(context.Context, SubjectRef) ([]model.Ref, error) {
	s.groupReads++
	return s.groups, s.err
}
func (s *projectFilterSlice) TeamActive(context.Context, model.Ref) (bool, error) {
	s.actives++
	return s.active, s.err
}
func (s *projectFilterSlice) ProjectActive(context.Context, model.Ref) (bool, error) {
	s.actives++
	return s.active, s.err
}
func (s *projectFilterSlice) ProjectForBead(context.Context, model.Ref) (ProjectBinding, error) {
	s.lookups++
	return s.project, nil
}

func projectTemplate() FilterTemplate {
	q := request()
	return FilterTemplate{Namespace: namespace(), Request: ListRequest{Caller: q.Caller, ResourceKind: Bead, Action: Read, Method: "bead.ready"}, Permit: FilterExpr{Op: True}, Forbid: FilterExpr{Op: False}}
}

func projectSlice() *projectFilterSlice {
	return &projectFilterSlice{valid: true, active: true,
		project: ProjectBinding{Name: "display-name", Ref: ref("registry-id"), Found: true},
		groups:  []model.Ref{ref("team")}, grants: map[projectGrant]bool{
			{SubjectRef{"user", "u"}, ProjectScope, "registry-id", "involved"}:    true,
			{SubjectRef{"user", "u"}, TeamScope, "team", "member"}:                true,
			{SubjectRef{"team", "team"}, ProjectScope, "registry-id", "involved"}: true,
		}}
}

func TestProjectFilterMatchesFinitePolicy(t *testing.T) {
	expressions := []FilterExpr{
		{Op: False}, {Op: True}, {Op: All}, {Op: Any},
		{Op: PrincipalKind, Kind: "user"}, {Op: PrincipalKind, Kind: "agent"},
		{Op: SponsorKind, Kind: "user"}, {Op: ActiveProject},
		{Op: GrantExists, Subject: CallerSubject, Scope: ProjectBindingScope, Role: "involved"},
		{Op: GrantExists, Subject: SponsorSubject, Scope: ProjectBindingScope, Role: "involved"},
		{Op: GrantExists, Subject: CallerSubject, Scope: AuthorityBinding, Role: "admin"},
		{Op: TeamInvolvement, Subject: CallerSubject},
		{Op: TeamInvolvement, Subject: SponsorSubject},
	}
	for i, permit := range expressions {
		for j, deny := range expressions {
			for _, agent := range []bool{false, true} {
				template := projectTemplate()
				if agent {
					template.Request.Caller.Principal = model.PrincipalRef{Kind: "agent", ID: "worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "u"}}
				}
				template.Permit, template.Forbid = permit, deny
				template.Restrictions = []FilterExpr{And(FilterExpr{Op: True}, Or(FilterExpr{Op: ActiveProject}, FilterExpr{Op: False}))}
				s := projectSlice()
				bound, err := template.BindProject(context.Background(), s, s.project)
				want, wantErr := template.Match(context.Background(), projectSlice(), Resource{Kind: Bead, Ref: ref("b")})
				got, gotErr := bound.Allows(context.Background(), ref("b"), s.project)
				if err != nil || gotErr != nil || wantErr != nil || got != want || bound.Uniform() != want {
					t.Fatalf("permit%d forbid%d agent%t got=%t/%v/%v want=%t/%v", i, j, agent, got, err, gotErr, want, wantErr)
				}
			}
		}
	}
}

func TestProjectFilterCaveatsAndOwnership(t *testing.T) {
	for _, tc := range []struct {
		name             string
		caveats, sponsor map[string]any
		want             []model.Ref
		uniform          bool
	}{
		{name: "omitted", uniform: true},
		{name: "actions", caveats: map[string]any{"actions": []string{"read"}}, uniform: true},
		{name: "wrong_action", caveats: map[string]any{"actions": []string{"modify"}}, want: []model.Ref{}},
		{name: "empty_actions", caveats: map[string]any{"actions": []string{}}, want: []model.Ref{}},
		{name: "registered_id", caveats: map[string]any{"projects": []model.Ref{ref("registry-id")}}, uniform: true},
		{name: "display_is_not_id", caveats: map[string]any{"projects": []model.Ref{ref("display-name")}}, want: []model.Ref{}},
		{name: "empty_projects", caveats: map[string]any{"projects": []model.Ref{}}, want: []model.Ref{}},
		{name: "empty_refs", caveats: map[string]any{"refs": []model.Ref{}}, want: []model.Ref{}},
		{name: "null_refs", caveats: map[string]any{"refs": nil}, want: []model.Ref{}},
		{name: "malformed_refs", caveats: map[string]any{"refs": []string{"b"}}, want: []model.Ref{}},
		{name: "invalid_after_hit", caveats: map[string]any{"refs": []model.Ref{ref("b"), model.RefByKey("bad")}}, want: []model.Ref{}},
		{name: "unknown", caveats: map[string]any{"provider": "unknown"}, want: []model.Ref{}},
		{name: "deduplicated_sorted", caveats: map[string]any{"refs": []model.Ref{ref("c"), ref("b"), ref("b")}}, want: []model.Ref{ref("b"), ref("c")}},
		{name: "sponsor_intersection", caveats: map[string]any{"refs": []model.Ref{ref("b"), ref("c")}}, sponsor: map[string]any{"refs": []model.Ref{ref("a"), ref("b")}}, want: []model.Ref{ref("b")}},
		{name: "sponsor_empty", sponsor: map[string]any{"refs": []model.Ref{}}, want: []model.Ref{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			template := projectTemplate()
			template.Request.Caller.Principal = model.PrincipalRef{Kind: "agent", ID: "worker", Caveats: tc.caveats, Sponsor: &model.PrincipalRef{Kind: "user", ID: "u", Caveats: tc.sponsor}}
			s := projectSlice()
			bound, err := template.BindProject(context.Background(), s, s.project)
			if err != nil {
				t.Fatal(err)
			}
			refs, restricted := bound.References()
			if bound.Uniform() != tc.uniform || restricted == tc.uniform || !reflect.DeepEqual(refs, tc.want) {
				t.Fatalf("uniform%t refs%v restricted%t want%v", bound.Uniform(), refs, restricted, tc.want)
			}
			for _, id := range []string{"a", "b", "c", "other"} {
				want, we := template.Match(context.Background(), projectSlice(), Resource{Kind: Bead, Ref: ref(id)})
				got, ge := bound.Allows(context.Background(), ref(id), s.project)
				if we != nil || ge != nil || got != want {
					t.Fatalf("%s got%t/%v want%t/%v", id, got, ge, want, we)
				}
			}
			// Neither the caller's original list nor a returned list can mutate it.
			if len(refs) != 0 {
				refs[0] = ref("changed")
				again, _ := bound.References()
				if !reflect.DeepEqual(again, tc.want) {
					t.Fatal("returned refs alias bound predicate")
				}
			}
		})
	}
}

func TestProjectFilterFailClosedAndCurrentProject(t *testing.T) {
	ctx := context.Background()
	s := projectSlice()
	template := projectTemplate()
	bound, err := template.BindProject(ctx, s, s.project)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []ProjectBinding{{}, {Name: s.project.Name, Ref: ref("moved"), Found: true}, {Name: "historical-name", Ref: s.project.Ref, Found: true}} {
		if yes, err := bound.Allows(ctx, ref("b"), project); yes || !errors.Is(err, ErrProjectFilterScope) {
			t.Fatal("out-of-scope current project was silently filtered instead of refusing")
		}
	}
	moved := projectSlice()
	moved.project = ProjectBinding{Name: "other", Ref: ref("other-project"), Found: true}
	if allowed, err := template.Match(ctx, moved, Resource{Kind: Bead, Ref: ref("b")}); !allowed || err != nil {
		t.Fatal("full policy should still permit the moved bead; specialization must refuse scope")
	}
	for _, bead := range []model.Ref{model.RefByKey("b"), model.RefByID("b", model.Natural64(1)), ref("plugin/item"), {}} {
		if yes, err := bound.Allows(ctx, bead, s.project); yes || err != nil {
			t.Fatal("invalid bead ref authorized")
		}
	}
	template.Permit = FilterExpr{Op: All, Args: []FilterExpr{{Op: False}, {Op: "unknown"}}}
	if _, err := template.BindProject(ctx, s, s.project); err == nil {
		t.Fatal("unknown node hidden under false")
	}
	template = projectTemplate()
	template.Permit = FilterExpr{Op: ActiveProject}
	s.err = errors.New("source unavailable")
	if _, err := template.BindProject(ctx, s, s.project); err == nil {
		t.Fatal("policy error became allow/absence")
	}
	// Caveat denial precedes policy reads and their possible failures.
	template.Request.Caller.Principal.Caveats = map[string]any{"refs": []model.Ref{}}
	if out, err := template.BindProject(ctx, s, s.project); err != nil || out.Uniform() {
		t.Fatal("empty refs reached failing policy")
	}
	s.err, s.valid = nil, false
	if out, err := projectTemplate().BindProject(ctx, s, s.project); err != nil || out.Uniform() {
		t.Fatal("invalid current context authorized")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := bound.Allows(canceled, ref("b"), s.project); err == nil {
		t.Fatal("cancellation ignored")
	}
	if _, err := projectTemplate().BindProject(canceled, s, s.project); err == nil {
		t.Fatal("bind cancellation ignored")
	}
}

func TestProjectFilterActionMethodAndOwnedBinding(t *testing.T) {
	for _, action := range []Action{Read, Modify} {
		template := projectTemplate()
		template.Request.Action = action
		if action == Modify {
			template.Request.Method = "bead.claimNext"
		}
		template.Request.Caller.Principal.Caveats = map[string]any{"actions": []string{string(action)}, "refs": []model.Ref{ref("b")}}
		s := projectSlice()
		bound, err := template.BindProject(context.Background(), s, s.project)
		if err != nil {
			t.Fatal(err)
		}
		template.Request.Caller.Principal.Caveats["refs"].([]model.Ref)[0] = ref("changed")
		if ok, _ := bound.Allows(context.Background(), ref("b"), s.project); !ok {
			t.Fatal("sealed request was mutated")
		}
		if s.contexts != 1 || s.lookups != 0 {
			t.Fatalf("context=%d project-lookups=%d", s.contexts, s.lookups)
		}
	}
	if _, err := (Chain{Namespace: namespace(), Links: []Link{link{checkOnly: true}}}).Compile(projectTemplate().Request); !errors.Is(err, ErrCheckOnly) {
		t.Fatal("check-only chain was admitted")
	}
}
