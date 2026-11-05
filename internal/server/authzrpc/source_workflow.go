package authzrpc

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// The trusted Writer identifies active changes only after the installed source
// provider has admitted them. Their immutable project binding is still resolved
// through the SAME tracked authority registry. Original bead method policies,
// including enclosing tx.apply and each close, remain in the installed chain.
type sourceWriteSlice struct {
	authz.Slice
	projects map[string]authz.ProjectBinding
}

func (s *sourceWriteSlice) ProjectForBead(ctx context.Context, ref model.Ref) (authz.ProjectBinding, error) {
	if project, ok := s.projects[ref.ID]; ok {
		return project, ctx.Err()
	}
	return s.Slice.ProjectForBead(ctx, ref)
}

func sourceAuthorizationSlice(base *slice, stage port.AuthorizationStage, proposal port.ProposedChange) (authz.Slice, error) {
	projects := map[string]authz.ProjectBinding{}
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "workflow_item" {
			continue
		}
		if change.Resource.Kind != "workflow_item" || !workflowItemRef(change.Resource.Ref) || authz.CanonicalRef(change.Resource.Ref) != nil {
			return nil, forbidden("unsupported writer resource")
		}
		project, ok := base.projectByName(proposal.Meta.Project)
		if !ok {
			return nil, forbidden("source workflow project is not authorized")
		}
		if stage != port.AuthorizeReplay {
			for _, raw := range []json.RawMessage{change.Before, change.After} {
				if len(raw) == 0 || string(raw) == "null" {
					continue
				} // a completed run has no live after body
				var image struct {
					ID      string `json:"id"`
					Project string `json:"project"`
				}
				if json.Unmarshal(raw, &image) != nil || image.ID != change.Resource.Ref.ID || image.Project != proposal.Meta.Project {
					return nil, forbidden("source workflow project assignment changed")
				}
			}
		}
		projects[change.Resource.Ref.ID] = authz.ProjectBinding{Name: proposal.Meta.Project, Ref: project, Found: true}
	}
	if len(projects) == 0 {
		return base, nil
	}
	return &sourceWriteSlice{Slice: base, projects: projects}, nil
}

func (s *GuardedService) sourceRuntimeInstalled() bool {
	installed, ok := s.delegate.(interface{ SourceRuntimeInstalled() bool })
	return ok && installed.SourceRuntimeInstalled()
}
