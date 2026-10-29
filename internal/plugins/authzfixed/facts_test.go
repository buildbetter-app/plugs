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
