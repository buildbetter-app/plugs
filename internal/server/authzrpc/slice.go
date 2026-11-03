package authzrpc

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type slice struct {
	snapshot              Snapshot
	facts                 authzfixed.Facts
	expected              authz.Caller
	expectedNormalized    bool
	authorizationRevision model.Revision
	authorizationBound    bool
	preparation           authz.Preparation
}

var _ authz.Slice = (*slice)(nil)

func (s *slice) Namespace() authz.Namespace { return s.facts.Namespace }

func (s *slice) AuthorizationPreparation() *authz.Preparation { return &s.preparation }

func (s *slice) Record(ctx context.Context, dep authz.Dependency) error {
	if s == nil || s.snapshot == nil {
		return fmt.Errorf("authorization snapshot is unavailable")
	}
	switch dep.Kind {
	case "sentinel":
		value, err := driver.NewSentinel(dep.Key)
		if err != nil {
			return err
		}
		_, err = s.snapshot.Observe(ctx, []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: value}})
		return err
	case "read_fact":
		value, err := driver.NewReadFact(dep.Key)
		if err != nil {
			return err
		}
		_, err = s.snapshot.Observe(ctx, []driver.LockKey{{Kind: driver.LockReadFact, Fact: value}})
		return err
	case "record":
		// Grant/team/project tuples are host-owned fact records. The source
		// validates their complete relation; a tuple miss is an ordinary false
		// fact and must not become a read error. Bead IDs, however, are read
		// from the actual snapshot so assignment checks cannot be fabricated.
		if dep.Key == "" || strings.Contains(dep.Key, "/") || strings.ContainsAny(dep.Key, " \t\r\n") {
			return fmt.Errorf("unsupported authorization record dependency %q", dep.Key)
		}
		_, err := s.snapshot.Get(ctx, []model.Ref{model.RefByID(dep.Key, model.Natural64(0))})
		return err
	default:
		return fmt.Errorf("unsupported authorization dependency %q", dep.Kind)
	}
}

func (s *slice) CheckContext(_ context.Context, caller authz.Caller) (bool, error) {
	if s == nil {
		return false, fmt.Errorf("authorization slice is nil")
	}
	principalMatches := s.expectedNormalized && reflect.DeepEqual(s.expected.Principal, caller.Principal)
	if !principalMatches {
		principalMatches = samePrincipal(s.expected.Principal, caller.Principal)
	}
	return principalMatches &&
		s.expected.VerificationID == caller.VerificationID &&
		s.expected.CapturedEpoch.String() == caller.CapturedEpoch.String(), nil
}

func (s *slice) HasGrant(_ context.Context, subject authz.SubjectRef, scope authz.Scope, role string) (bool, error) {
	for _, grant := range s.facts.Grants {
		if grant.Subject == subject && grant.Scope.Kind == scope.Kind && grant.Scope.Ref.ID == scope.Ref.ID && grant.Role == role {
			return true, nil
		}
	}
	return false, nil
}

func (s *slice) Groups(_ context.Context, subject authz.SubjectRef) ([]model.Ref, error) {
	groups := make([]model.Ref, 0)
	for _, grant := range s.facts.Grants {
		if grant.Subject == subject && grant.Scope.Kind == authz.TeamScope && grant.Role == "member" {
			groups = append(groups, grant.Scope.Ref)
		}
	}
	return groups, nil
}

func (s *slice) TeamActive(_ context.Context, ref model.Ref) (bool, error) {
	for _, team := range s.facts.Teams {
		if team.Ref.ID == ref.ID {
			return team.Active, nil
		}
	}
	return false, nil
}

func (s *slice) ProjectActive(_ context.Context, ref model.Ref) (bool, error) {
	for _, project := range s.facts.Projects {
		if project.Ref.ID == ref.ID {
			return project.Active, nil
		}
	}
	return false, nil
}

func (s *slice) projectByName(name string) (model.Ref, bool) {
	if s == nil || name == "" {
		return model.Ref{}, false
	}
	for _, project := range s.facts.Projects {
		if project.Name == name {
			return project.Ref, true
		}
	}
	return model.Ref{}, false
}

func (s *slice) ProjectForBead(ctx context.Context, ref model.Ref) (authz.ProjectBinding, error) {
	rows, err := s.snapshot.Get(ctx, []model.Ref{ref})
	if err != nil {
		return authz.ProjectBinding{}, err
	}
	if len(rows) == 0 || rows[0].Kind != "bead" {
		return authz.ProjectBinding{}, nil
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		return authz.ProjectBinding{}, err
	}
	if doc.Project == "" {
		return authz.ProjectBinding{}, nil
	}
	for _, project := range s.facts.Projects {
		if project.Name == doc.Project {
			return authz.ProjectBinding{Name: project.Name, Ref: project.Ref, Found: true}, nil
		}
	}
	// A stored name with no immutable registry binding is an explicit miss;
	// authorization never invents a project reference from the name.
	return authz.ProjectBinding{Name: doc.Project, Found: false}, nil
}
