package rpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// BlockedService is additive: minimal service implementations keep their
// existing registered methods without pretending to supply a graph listing.
type BlockedService interface {
	Blocked(context.Context, model.PrincipalRef, service.ReadyRequest) (service.BlockedPage, error)
}

func dispatchBlocked(ctx context.Context, svc BlockedService, c Call, p serviceParams, limit int) (any, error) {
	position, err := c.paging.decode(c, p)
	if err != nil {
		return nil, err
	}
	value, err := svc.Blocked(ctx, c.Principal, service.ReadyRequest{Project: c.Meta.Project, Agent: c.Meta.Agent, Filter: p.Filter.domain(), Limit: limit, Position: position})
	if err != nil {
		return nil, err
	}
	out, err := c.paging.page(c, p, position, service.BeadPage{Items: []service.Bead{}, AsOf: value.AsOf, Next: value.Next, WorkflowSnapshot: value.WorkflowSnapshot, ReadyMembership: value.Membership})
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(value.Items))
	for _, item := range value.Items {
		items = append(items, map[string]any{"bead": beadJSON(item.Bead), "blockedBy": item.BlockedBy, "blockedByCount": item.BlockedByCount, "hiddenBlockers": item.HiddenBlockers})
	}
	out["items"] = items
	return out, nil
}
