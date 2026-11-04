package authzrpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *GuardedService) compiledClaimPage(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (out service.BeadPage, resultErr error) {
	domain, ok := s.delegate.(readyOrderDomain)
	if !ok {
		return service.BeadPage{}, driver.ErrUnsupported
	}
	reader, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return service.BeadPage{}, err
	}
	defer closeReader()
	selector, ok := reader.(driver.ReadyOrderReader)
	if !ok {
		return service.BeadPage{}, driver.ErrUnsupported
	}
	caller, authority, filter, err := s.readyProjectFilter(ctx, by, snapshot, req.Project, "bead.claimNext", authz.Modify)
	defer func() {
		resultErr = s.fenceReadyFallback(ctx, by, caller, authority, snapshot, resultErr)
		if resultErr != nil {
			out = service.BeadPage{}
		}
	}()
	if err != nil {
		return service.BeadPage{}, err
	}
	refs, _ := filter.References()
	query := req
	query.Position = nil
	if query.Limit == 0 {
		query.Limit = 100
	}
	plan, err := domain.ReadyOrderPlan(query, reader, filter.Uniform(), refs)
	if err != nil {
		return service.BeadPage{}, err
	}
	workflowPhase := false
	if p := req.Position; p != nil {
		if p.AsOf.Incarnation != plan.Incarnation || p.Offset < 0 {
			return service.BeadPage{}, &model.Error{Data: model.ErrorData{Code: "cursor_invalid"}}
		}
		if p.ClaimNextAuthorized {
			workflowPhase = p.Workflow
			if !workflowPhase && p.AsOf.Revision.String() == plan.ContentRevision.String() {
				plan.Offset = uint64(p.Offset)
			}
		}
	}
	if workflowPhase {
		plan.Limit = 1
	}
	// W7 derives every group's count and seed pivot from the RAW scope before
	// intersecting Modify refs. The offset is in that authorized rotated order.
	selected, err := selector.SelectReadyOrder(ctx, plan)
	if err != nil {
		return service.BeadPage{}, err
	}
	page := service.BeadPage{Items: []service.Bead{}, AsOf: service.AsOf{Revision: plan.ContentRevision, Incarnation: plan.Incarnation}}
	if !workflowPhase {
		page.Items, _, err = service.HydrateReadyOrder(ctx, reader, selected.Keys)
		if err != nil {
			return service.BeadPage{}, err
		}
		for _, item := range page.Items {
			if item.Project != req.Project {
				return service.BeadPage{}, hierarchyUnavailable()
			}
		}
		if selected.More {
			page.Next = &service.PagePosition{Offset: int(plan.Offset) + len(page.Items), AsOf: page.AsOf, ClaimNextAuthorized: true}
		}
	}
	if page.Next == nil {
		workflow, binding, err := domain.WorkflowReadyItems(ctx, by, query)
		if err != nil {
			return service.BeadPage{}, err
		}
		page.WorkflowSnapshot = binding
		offset := 0
		if workflowPhase {
			p := req.Position
			if p.WorkflowSnapshot == nil || !p.WorkflowSnapshot.Valid() {
				return service.BeadPage{}, &model.Error{Data: model.ErrorData{Code: "cursor_invalid"}}
			}
			if p.WorkflowSnapshot.Equal(binding) {
				offset = p.Offset
			}
			if offset > len(workflow) {
				return service.BeadPage{}, &model.Error{Data: model.ErrorData{Code: "cursor_invalid"}}
			}
		}
		end := min(len(workflow), offset+query.Limit-len(page.Items))
		page.Items = append(page.Items, workflow[offset:end]...)
		if end < len(workflow) {
			page.Next = &service.PagePosition{Offset: end, AsOf: page.AsOf, Workflow: true, WorkflowSnapshot: binding, ClaimNextAuthorized: true}
		}
	}
	// This proof covers only candidate selection. Every actual claim continues
	// through the existing Writer, including its four attempts and race policy.
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authority, snapshot)
	if err != nil {
		return service.BeadPage{}, err
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return service.BeadPage{}, err
	}
	return page, nil
}
