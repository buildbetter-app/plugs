package authzfixed

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
func ns() authz.Namespace {
	return authz.Namespace{StoreID: "store", Incarnation: "inc", InstallationID: "fixed", Authority: ref("authority")}
}
func facts() Facts {
	return Facts{Namespace: ns(), Teams: []Team{{Ref: ref("team"), InstallationID: "fixed", Active: true}}, Projects: []Project{{Ref: ref("project"), InstallationID: "fixed", Name: "web/api", Active: true}}}
}
func grant(kind, id string, scope authz.ScopeKind, target, role string) authz.Grant {
	return authz.Grant{Subject: authz.SubjectRef{Kind: kind, ID: id}, Scope: authz.Scope{Kind: scope, Ref: ref(target)}, Role: role}
}

func TestFourGrantForms(t *testing.T) {
	f := facts()
	f.Grants = []authz.Grant{grant("user", "u", authz.AuthorityScope, "authority", "admin"), grant("user", "u", authz.ProjectScope, "project", "involved"), grant("user", "u", authz.TeamScope, "team", "member"), grant("team", "team", authz.ProjectScope, "project", "involved")}
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}
	f.Grants = append(f.Grants, f.Grants[0])
	if f.Validate() == nil {
		t.Fatal("duplicate grant accepted")
	}
	for _, g := range []authz.Grant{grant("team", "team", authz.AuthorityScope, "authority", "admin"), grant("user", "u", authz.ProjectScope, "project", "writer"), grant("user", "u", authz.TeamScope, "team", "involved"), grant("agent", "a", authz.ProjectScope, "project", "involved"), grant("team", "unknown", authz.ProjectScope, "project", "involved")} {
		f := facts()
		f.Grants = []authz.Grant{g}
		if f.Validate() == nil {
			t.Errorf("accepted %+v", g)
		}
	}
}

func TestCanonicalFactAndRegistryValidation(t *testing.T) {
	for name, mutate := range map[string]func(*Facts){
		"empty namespace":  func(f *Facts) { f.Namespace.StoreID = "" },
		"authority key":    func(f *Facts) { f.Namespace.Authority = model.RefByKey("authority") },
		"authority branch": func(f *Facts) { f.Namespace.Authority = model.RefByID("authority", model.Natural64(1)) },
		"foreign team":     func(f *Facts) { f.Teams[0].InstallationID = "foreign" },
		"foreign project":  func(f *Facts) { f.Projects[0].InstallationID = "foreign" },
		"scope collision":  func(f *Facts) { f.Teams[0].Ref = f.Namespace.Authority },
		"duplicate team":   func(f *Facts) { f.Teams = append(f.Teams, f.Teams[0]) },
		"duplicate ref":    func(f *Facts) { p := f.Projects[0]; p.Name = "other"; f.Projects = append(f.Projects, p) },
		"duplicate name":   func(f *Facts) { p := f.Projects[0]; p.Ref = ref("other"); f.Projects = append(f.Projects, p) },
		"blank project":    func(f *Facts) { f.Projects[0].Name = "" },
		"padded project":   func(f *Facts) { f.Projects[0].Name = " web/api" },
		"invalid UTF8":     func(f *Facts) { f.Projects[0].Name = string([]byte{0xff}) },
		"unknown scope": func(f *Facts) {
			f.Grants = []authz.Grant{grant("user", "u", authz.ProjectScope, "missing", "involved")}
		},
		"scope kind lie": func(f *Facts) { f.Grants = []authz.Grant{grant("user", "u", authz.ProjectScope, "team", "involved")} },
		"cross branch": func(f *Facts) {
			g := grant("user", "u", authz.ProjectScope, "project", "involved")
			g.Scope.Ref = model.RefByID("project", model.Natural64(2))
			f.Grants = []authz.Grant{g}
		},
		"nested team": func(f *Facts) { f.Grants = []authz.Grant{grant("team", "team", authz.TeamScope, "team", "member")} },
	} {
		t.Run(name, func(t *testing.T) {
			f := facts()
			mutate(&f)
			if f.Validate() == nil {
				t.Fatal("invalid fact accepted")
			}
		})
	}
	before := facts()
	after := facts()
	after.Projects[0].Active = false
	after.Projects = append(after.Projects, Project{Ref: ref("second"), InstallationID: "fixed", Name: "Web/API", Active: true})
	if err := ValidateRegistryUpdate(before, after); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Facts){func(f *Facts) { f.Projects = nil }, func(f *Facts) { f.Projects[0].Name = "renamed" }, func(f *Facts) { f.Projects[0].Ref = ref("remapped") }, func(f *Facts) { f.Namespace.Authority = ref("new-authority") }} {
		after := facts()
		mutate(&after)
		if ValidateRegistryUpdate(before, after) == nil {
			t.Fatal("immutable registry association changed")
		}
	}
	// Inactive records may be stored; live predicates determine involvement.
	inactive := facts()
	inactive.Teams[0].Active = false
	inactive.Projects[0].Active = false
	inactive.Grants = []authz.Grant{grant("user", "u", authz.TeamScope, "team", "member"), grant("team", "team", authz.ProjectScope, "project", "involved")}
	if err := inactive.Validate(); err != nil {
		t.Fatal(err)
	}
}
