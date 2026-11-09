package authzrpc

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Link payloads use current endpoint authority even when the source content is
// historical. Explicit external identities are opaque, so they never trigger a
// local lookup or inherit the visibility of a same-spelled local record.
func (s *GuardedService) projectLinkTargets(ctx context.Context, by model.PrincipalRef, beads []service.Bead) error {
	hasLinks := false
	for _, bead := range beads {
		hasLinks = hasLinks || len(bead.Links) != 0
	}
	if !hasLinks {
		return nil
	}
	current, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return err
	}
	defer closeReader()
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return authzError(err)
	}
	authority, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return hierarchyUnavailable()
	}
	visible := func(ref model.Ref, project string) (bool, error) {
		if workflowItemRef(ref) {
			allowed, err := s.checker.workflowRead(ctx, caller, authority, "bead.get", project)
			if err != nil || !allowed {
				return false, err
			}
			item, err := s.delegate.Get(ctx, by, service.GetRequest{ID: ref, Project: project})
			if err != nil {
				var refusal *model.Error
				if errors.As(err, &refusal) && refusal.Data.Code == "not_found" {
					return false, nil
				}
				return false, err
			}
			return item.Bead.ID == ref.ID && item.Bead.Project == project, nil
		}
		row, err := hierarchySource(ctx, current, ref)
		if err != nil || row == nil {
			return false, err
		}
		return s.checker.hierarchyRead(ctx, caller, authority, ref)
	}
	for i := range beads {
		bead := &beads[i]
		links := make([]workflowplugin.RelationView, 0, len(bead.Links))
		for _, link := range bead.Links {
			kind, err := link.TargetKind.Normalized()
			if err != nil || model.ValidateLinkTarget(kind, link.To) != nil {
				return hierarchyUnavailable()
			}
			if link.From != bead.ID {
				allowed, err := visible(model.RefByID(link.From, model.Natural64(0)), bead.Project)
				if err != nil {
					return err
				}
				if !allowed {
					continue
				}
			}
			if kind != model.LinkTargetExternal && link.To.ID != bead.ID {
				allowed, err := visible(link.To, bead.Project)
				if err != nil {
					return err
				}
				if !allowed {
					continue
				}
			}
			links = append(links, link)
		}
		bead.Links = links
	}
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authority, snapshot)
	if err != nil {
		return err
	}
	return s.checker.ValidateReadAuthorityBinding(ctx, by, binding)
}
