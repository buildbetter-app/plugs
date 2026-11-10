package authzrpc

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// GateList uses the existing bead read boundary before the selected gate
// limit. Inaccessible gates never consume slots, and read failures propagate.
func (s *GuardedService) GateList(ctx context.Context, by model.PrincipalRef, req service.GateListRequest) (service.BeadPage, error) {
	delegate, ok := s.delegate.(interface {
		GateListWithFilter(context.Context, model.PrincipalRef, service.GateListRequest, service.GateVisibility) (service.BeadPage, error)
	})
	if !ok {
		return service.BeadPage{}, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
	}
	_, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return service.BeadPage{}, err
	}
	defer closeReader()
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return service.BeadPage{}, authzError(err)
	}
	authority, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return service.BeadPage{}, authzError(err)
	}
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authority, snapshot)
	if err != nil {
		return service.BeadPage{}, err
	}
	page, err := delegate.GateListWithFilter(ctx, by, req, func(ctx context.Context, bead *service.Bead) (bool, error) {
		result, err := s.Get(ctx, by, service.GetRequest{ID: model.RefByID(bead.ID, model.Natural64(0)), Project: req.Project, Agent: req.Agent})
		if err != nil {
			var typed *model.Error
			if errors.As(err, &typed) && typed.Data.Code == "not_found" {
				return false, nil
			}
			return false, err
		}
		bead.Parent = result.Bead.Parent
		bead.ParentHidden = result.Bead.ParentHidden
		return true, nil
	})
	if err != nil {
		return service.BeadPage{}, err
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return service.BeadPage{}, err
	}
	return page, nil
}
