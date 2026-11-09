package authzfixed

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type Team struct {
	Ref            model.Ref
	InstallationID string
	Active         bool
}
type Project struct {
	Ref                  model.Ref
	InstallationID, Name string
	Active               bool
}

// Facts is a validation input for a complete installation snapshot, not a
// management API or a permission cache. A host records/maintains source facts.
type Facts struct {
	Namespace authz.Namespace
	Teams     []Team
	Projects  []Project
	Grants    []authz.Grant
}

func (f Facts) Validate() error {
	if err := f.Namespace.Validate(); err != nil {
		return err
	}
	kinds := map[string]authz.ScopeKind{f.Namespace.Authority.ID: authz.AuthorityScope}
	names := map[string]bool{}
	for _, t := range f.Teams {
		if authz.CanonicalRef(t.Ref) != nil || t.InstallationID != f.Namespace.InstallationID || kinds[t.Ref.ID] != "" {
			return fmt.Errorf("invalid, duplicate or foreign team")
		}
		kinds[t.Ref.ID] = authz.TeamScope
	}
	for _, p := range f.Projects {
		if authz.CanonicalRef(p.Ref) != nil || p.InstallationID != f.Namespace.InstallationID || !authz.ValidID(p.Name) || kinds[p.Ref.ID] != "" || names[p.Name] {
			return fmt.Errorf("invalid, duplicate or foreign project registry binding")
		}
		kinds[p.Ref.ID] = authz.ProjectScope
		names[p.Name] = true
	}
	tuples := map[string]bool{}
	for _, g := range f.Grants {
		if g.Subject.Validate() != nil || g.Scope.Validate() != nil || kinds[g.Scope.Ref.ID] != g.Scope.Kind {
			return fmt.Errorf("invalid or unknown grant scope/subject")
		}
		valid := g.Subject.Kind == "user" && ((g.Scope.Kind == authz.AuthorityScope && g.Role == "admin") || (g.Scope.Kind == authz.ProjectScope && g.Role == "involved") || (g.Scope.Kind == authz.TeamScope && g.Role == "member"))
		if g.Subject.Kind == "team" {
			valid = kinds[g.Subject.ID] == authz.TeamScope && g.Scope.Kind == authz.ProjectScope && g.Role == "involved"
		}
		if !valid {
			return fmt.Errorf("unsupported grant form")
		}
		key, err := authz.GrantIdentity(f.Namespace, g)
		if err != nil {
			return err
		}
		if tuples[key] {
			return fmt.Errorf("duplicate grant tuple")
		}
		tuples[key] = true
	}
	return nil
}

// ValidateRegistryUpdate allows append/activity changes only. No name rename,
// remap or deletion is silently supplied by a projection or destination grant.
func ValidateRegistryUpdate(before, after Facts) error {
	if err := before.Validate(); err != nil {
		return err
	}
	if err := after.Validate(); err != nil {
		return err
	}
	if before.Namespace.StoreID != after.Namespace.StoreID || before.Namespace.Incarnation != after.Namespace.Incarnation || before.Namespace.InstallationID != after.Namespace.InstallationID || before.Namespace.Authority.ID != after.Namespace.Authority.ID {
		return fmt.Errorf("authority/installation changes are unsupported")
	}
	byName := map[string]string{}
	for _, p := range after.Projects {
		byName[p.Name] = p.Ref.ID
	}
	for _, p := range before.Projects {
		if byName[p.Name] != p.Ref.ID {
			return fmt.Errorf("project rename, remap or deletion is unsupported")
		}
	}
	return nil
}

// FactsSourceBytes is the canonical complete installed source, including empty
// domains. It owns its slices and has the same bytes for a reordered input.
func FactsSourceBytes(f Facts) ([]byte, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	f.Teams = append([]Team{}, f.Teams...)
	f.Projects = append([]Project{}, f.Projects...)
	f.Grants = append([]authz.Grant{}, f.Grants...)
	sort.Slice(f.Teams, func(i, j int) bool { return f.Teams[i].Ref.ID < f.Teams[j].Ref.ID })
	sort.Slice(f.Projects, func(i, j int) bool { return f.Projects[i].Ref.ID < f.Projects[j].Ref.ID })
	sort.Slice(f.Grants, func(i, j int) bool {
		a, _ := authz.GrantIdentity(f.Namespace, f.Grants[i])
		b, _ := authz.GrantIdentity(f.Namespace, f.Grants[j])
		return a < b
	})
	return json.Marshal(f)
}

// SelectionSubject and SelectionScope retain the kind in the opaque tuple.
// A user and team with the same ID, or different scope kinds, never alias.
func SelectionSubject(s authz.SubjectRef) string { return authz.Encode(s.Kind, s.ID) }
func SelectionScope(n authz.Namespace, s authz.Scope) (string, error) {
	return authz.ScopeComponent(n, s)
}

// FactsRows names one complete row per domain, not principal x bead materialized
// decisions. The root is durable source data; all annotations bind its exact
// bytes and observed revision in the publishing transaction.
type FactsRows struct {
	Source, Grants, Projects, Teams driver.DerivedRowKey
	AttributeScope                  driver.Sentinel
}

func SelectionRows(n authz.Namespace) (FactsRows, error) {
	if err := n.Validate(); err != nil {
		return FactsRows{}, err
	}
	id := driver.SelectionDigest([]byte(authz.Encode(n.StoreID, n.Incarnation, n.InstallationID, n.Authority.ID)))
	prefix := "authzfixed/v1/" + id + "/"
	scope, err := driver.NewClosureSentinel("authzfixed-" + id)
	if err != nil {
		return FactsRows{}, err
	}
	return FactsRows{
		Source:         driver.DerivedRowKey{Table: driver.DerivedGrant, Key: prefix + "source"},
		Grants:         driver.DerivedRowKey{Table: driver.DerivedGrant, Key: prefix + "grants"},
		Projects:       driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: prefix + "projects"},
		Teams:          driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: prefix + "teams"},
		AttributeScope: scope,
	}, nil
}

// SelectionProjection supplies semantic tuples from the same validated source
// used by the interpreter. Storage validates envelopes, not authorization JSON.
func SelectionProjection(f Facts, source driver.SelectionSource) ([]driver.DerivedRow, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	keys, err := SelectionRows(f.Namespace)
	if err != nil {
		return nil, err
	}
	// Redundant semantic columns are derived only from this exact, complete
	// installed source. A matching shape or caller-supplied digest is not a
	// registry proof. Storage separately reobserves the actual source envelope
	// in the publishing transaction and restamps its revision at commit.
	canonical, err := FactsSourceBytes(f)
	if err != nil {
		return nil, err
	}
	actualKey, keyErr := driver.EncodeLockKey(source.Key)
	expectedKey, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockDerived, Derived: &keys.Source})
	if keyErr != nil || actualKey != expectedKey || !source.Present || source.Digest != driver.SelectionDigest(canonical) {
		return nil, fmt.Errorf("selection facts require the complete same installed source")
	}
	projects := make(map[string]string, len(f.Projects))
	for _, project := range f.Projects {
		projects[project.Ref.ID] = project.Name
	}
	sets := []driver.SelectionFacts{
		{Kind: driver.SelectionScopedGrantFact, Rows: []driver.SelectionFact{}},
		{Kind: driver.SelectionProjectFact, Rows: []driver.SelectionFact{}},
		{Kind: driver.SelectionTeamFact, Rows: []driver.SelectionFact{}},
	}
	for _, grant := range f.Grants {
		scope, err := SelectionScope(f.Namespace, grant.Scope)
		if err != nil {
			return nil, err
		}
		ref := grant.Scope.Ref
		fact := driver.SelectionFact{Kind: driver.SelectionScopedGrantFact, Subject: SelectionSubject(grant.Subject), Scope: scope, Role: grant.Role, ScopeKind: driver.SelectionScopeKind(grant.Scope.Kind), ScopeRef: &ref}
		if grant.Scope.Kind == authz.ProjectScope {
			name, found := projects[grant.Scope.Ref.ID]
			if !found || name == "" {
				return nil, fmt.Errorf("selection project scope is absent from the same registry")
			}
			fact.ProjectName = &name
		}
		sets[0].Rows = append(sets[0].Rows, fact)
	}
	for _, project := range f.Projects {
		sets[1].Rows = append(sets[1].Rows, driver.SelectionFact{Kind: driver.SelectionProjectFact, Name: project.Name, Active: project.Active})
	}
	for _, team := range f.Teams {
		sets[2].Rows = append(sets[2].Rows, driver.SelectionFact{Kind: driver.SelectionTeamFact, Name: team.Ref.ID, Active: team.Active})
	}
	rows := make([]driver.DerivedRow, len(sets))
	for i, key := range []driver.DerivedRowKey{keys.Grants, keys.Projects, keys.Teams} {
		// Canonical byte order makes exact source/delta observations independent
		// of management input ordering, without changing the policy's set rules.
		sort.Slice(sets[i].Rows, func(a, b int) bool {
			x, _ := json.Marshal(sets[i].Rows[a])
			y, _ := json.Marshal(sets[i].Rows[b])
			return string(x) < string(y)
		})
		value, err := json.Marshal(sets[i])
		if err != nil {
			return nil, err
		}
		row := driver.DerivedRow{Key: key.Key, Value: value}
		row.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key, Incarnation: f.Namespace.Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value), Source: source, Facts: &sets[i]}
		if err := row.Index.Validate(key.Table, row); err != nil {
			// Facts.Validate owns source admission. Optional selector layout
			// limits (including query caps) cannot reject valid installations
			// or replacements; unannotated rows leave coverage unavailable.
			row.Index = nil
		}
		rows[i] = row.Clone()
	}
	return rows, nil
}
