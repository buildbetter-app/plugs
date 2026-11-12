package authzrpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Blocked keeps authorization on the graph's pinned reader, before both
// reason disclosure and the subject page boundary. No point-read substitute
// can splice grants from a different graph snapshot into the result.
func (s *GuardedService) Blocked(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BlockedPage, error) {
	if s == nil || s.delegate == nil || s.checker == nil {
		return service.BlockedPage{}, hierarchyUnavailable()
	}
	delegate, ok := s.delegate.(interface {
		BlockedWithFilter(context.Context, model.PrincipalRef, service.ReadyRequest, service.ReadyAuthorizer) (service.BlockedPage, error)
	})
	if !ok {
		return service.BlockedPage{}, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
	}
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return service.BlockedPage{}, authzError(err)
	}
	var authority *slice
	var binding string
	var revision model.Revision
	page, err := delegate.BlockedWithFilter(ctx, by, req, func(ctx context.Context, reader driver.Reader, bead service.Bead) (bool, error) {
		if authority == nil {
			loaded, err := s.checker.load(ctx, reader, caller)
			if err != nil {
				return false, hierarchyUnavailable()
			}
			authority, revision = loaded, loaded.authorizationRevision
			binding, err = s.checker.readAuthorityBinding(ctx, caller, authority, reader)
			if err != nil {
				return false, err
			}
			if req.Position != nil && (!req.Position.AuthorizationBound || req.Position.AuthorizationRevision != revision) {
				return false, &model.Error{Data: model.ErrorData{Code: "cursor_invalid"}}
			}
		}
		if bead.Kind == "workflow.item" {
			if s.sourceRuntimeInstalled() {
				return s.sourceRead(ctx, caller, authority, model.RefByID(bead.ID, model.Natural64(0)), bead.Project, "bead.get")
			}
			return s.checker.workflowRead(ctx, caller, authority, "bead.get", bead.Project)
		}
		return s.checker.hierarchyRead(ctx, caller, authority, model.RefByID(bead.ID, model.Natural64(0)))
	})
	if err != nil {
		return service.BlockedPage{}, err
	}
	if authority != nil {
		if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
			return service.BlockedPage{}, err
		}
	}
	if page.Next != nil {
		page.Next.AuthorizationBound, page.Next.AuthorizationRevision = true, revision
	}
	return page, nil
}
