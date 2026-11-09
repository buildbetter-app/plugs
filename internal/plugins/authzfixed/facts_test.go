package authzfixed

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestScopedSelectionUsesSameSourceRegistryAndOpaqueIdentities(t *testing.T) {
	t.Run("complete_empty_domains", func(t *testing.T) {
		empty := Facts{Namespace: ns()}
		body, err := FactsSourceBytes(empty)
		if err != nil {
			t.Fatal(err)
		}
		keys, _ := SelectionRows(empty.Namespace)
		source := driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &keys.Source}, Present: true, Digest: driver.SelectionDigest(body)}
		rows, err := SelectionProjection(empty, source)
		if err != nil || len(rows) != 3 {
			t.Fatalf("empty source admission: %v", err)
		}
		for i, kind := range []driver.SelectionFactKind{driver.SelectionScopedGrantFact, driver.SelectionProjectFact, driver.SelectionTeamFact} {
			index := rows[i].Index
			if index == nil || index.Facts == nil || index.Facts.Kind != kind || len(index.Facts.Rows) != 0 || index.Source.Digest != source.Digest {
				t.Fatal("empty source lost a complete same-source domain")
			}
		}
	})
	f := facts() // project ID=project; exact registry name=web/api.
	f.Grants = []authz.Grant{
		grant("user", "team", authz.ProjectScope, "project", "involved"),
		grant("team", "team", authz.ProjectScope, "project", "involved"),
		grant("user", "u", authz.TeamScope, "team", "member"),
		grant("user", "u", authz.AuthorityScope, "authority", "admin"),
	}
	body, err := FactsSourceBytes(f)
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := SelectionRows(f.Namespace)
	source := driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &keys.Source}, Revision: model.Natural64(7), Present: true, Digest: driver.SelectionDigest(body)}
	rows, err := SelectionProjection(f, source)
	if err != nil || len(rows) != 3 || rows[0].Index == nil || rows[0].Index.Facts.Kind != driver.SelectionScopedGrantFact {
		t.Fatalf("missing complete scoped domain: %v", err)
	}
	seen := map[string]bool{}
	for _, fact := range rows[0].Index.Facts.Rows {
		if fact.ScopeRef == nil {
			t.Fatal("scope reference metadata absent")
		}
		matched := false
		for _, grant := range f.Grants {
			identity, _ := authz.GrantIdentity(f.Namespace, grant)
			dependency, _ := authz.GrantDependency(f.Namespace, grant.Scope)
			scope, _ := authz.ScopeComponent(f.Namespace, grant.Scope)
			if fact.Subject != authz.Encode(grant.Subject.Kind, grant.Subject.ID) || fact.Scope != scope || fact.Role != grant.Role {
				continue
			}
			if fact.ScopeKind != driver.SelectionScopeKind(grant.Scope.Kind) || !driver.SelectionSameRef(*fact.ScopeRef, grant.Scope.Ref) || identity == "" || dependency.Key == "" {
				t.Fatal("opaque and semantic grant identities diverged")
			}
			if grant.Scope.Kind == authz.ProjectScope {
				if fact.ProjectName == nil || *fact.ProjectName != "web/api" || *fact.ProjectName == fact.ScopeRef.ID {
					t.Fatal("project name was synthesized from scope ID")
				}
			} else if fact.ProjectName != nil {
				t.Fatal("nonproject scope acquired a project target")
			}
			seen[fact.Subject+fact.Scope+fact.Role], matched = true, true
		}
		if !matched {
			t.Fatal("projection invented an unrelated grant")
		}
	}
	if len(seen) != len(f.Grants) {
		t.Fatal("user/team equal-text IDs collapsed")
	}
	// Every redundant column must be justified by the same complete source,
	// including its immutable registry rather than just a grant's shape.
	for name, edit := range map[string]func(*driver.SelectionSource){
		"wrong digest":   func(s *driver.SelectionSource) { s.Digest = driver.SelectionDigest([]byte("other")) },
		"missing source": func(s *driver.SelectionSource) { s.Present = false },
		"foreign source": func(s *driver.SelectionSource) { key := keys.Source; key.Key += "/other"; s.Key.Derived = &key },
	} {
		t.Run(name, func(t *testing.T) {
			wrong := source
			edit(&wrong)
			if rows, err := SelectionProjection(f, wrong); err == nil || rows != nil {
				t.Fatal("unrelated source certified semantic columns")
			}
		})
	}
	mutated := f
	mutated.Projects = append([]Project(nil), f.Projects...)
	mutated.Projects[0].Name = "other/name"
	if rows, err := SelectionProjection(mutated, source); err == nil || rows != nil {
		t.Fatal("registry metadata rebound to old source bytes")
	}
	rows[0].Index.Facts.Rows[0].ScopeRef.ID = "changed owned output"
	if !reflect.DeepEqual(f.Grants[0].Scope.Ref, ref("project")) {
		t.Fatal("projection aliases source scope refs")
	}
}

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

func TestSelectionFactLimitDoesNotChangeSourceAdmission(t *testing.T) {
	f := facts()
	f.Grants = make([]authz.Grant, driver.SelectionSummaryCap+1)
	for i := range f.Grants {
		f.Grants[i] = grant("user", fmt.Sprintf("user-%06d", i), authz.ProjectScope, "project", "involved")
	}
	body, err := FactsSourceBytes(f)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := SelectionRows(f.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := SelectionProjection(f, driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &keys.Source}, Present: true, Digest: driver.SelectionDigest(body)})
	if err != nil || len(rows) != 3 || rows[0].Index != nil {
		t.Fatalf("optional projection cap rejected valid source: rows=%d error=%v", len(rows), err)
	}
	var grants driver.SelectionFacts
	if err := json.Unmarshal(rows[0].Value, &grants); err != nil || len(grants.Rows) != len(f.Grants) {
		t.Fatal("large ordinary grant set was truncated")
	}
	for _, row := range rows[1:] {
		if row.Index == nil || row.Index.Facts == nil {
			t.Fatal("supported complete domain lost its annotation")
		}
	}
}
