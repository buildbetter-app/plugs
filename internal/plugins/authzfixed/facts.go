package authzfixed

import (
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
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
