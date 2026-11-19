package service

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// ReadyCount is the complete result of the existing ready predicate. It has no
// item projection or cursor, and a zero Count is still a successful answer.
type ReadyCount struct {
	AsOf  AsOf
	Count int
}

// ReadyCount reuses the same source-bound ready path as listing. A supplied
// limit is validated, then applies only to listing, never to the total.
func (s *Service) ReadyCount(ctx context.Context, by model.PrincipalRef, req ReadyRequest) (ReadyCount, error) {
	return CountReadyPages(ctx, by, req, func(ctx context.Context, by model.PrincipalRef, req ReadyRequest) (BeadPage, error) {
		return s.ReadyWithFilter(ctx, by, req, nil)
	})
}

// CountReadyPages lets an authorization adapter count its existing authorized
// ready sequence. It does not implement another predicate, bypass a cap, cache
// authority, or publish a partial total when any page refuses. Source bindings
// must agree through the terminal page, including the complete ready order.
func CountReadyPages(ctx context.Context, by model.PrincipalRef, req ReadyRequest, read func(context.Context, model.PrincipalRef, ReadyRequest) (BeadPage, error)) (ReadyCount, error) {
	if req.Cursor != "" || req.Position != nil {
		return ReadyCount{}, failure("invalid_params", "countOnly cannot be combined with a cursor")
	}
	if _, err := pageOptions(req.Project, req.ViewID, req.Cursor, req.Filter, req.Limit); err != nil {
		return ReadyCount{}, err
	}
	req.Limit = 1000
	var first *BeadPage
	total := 0
	type positionKey struct {
		offset   int
		workflow bool
	}
	positions := map[positionKey]bool{}
	seen := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return ReadyCount{}, err
		}
		page, err := read(ctx, by, req)
		if err != nil {
			return ReadyCount{}, err
		}
		if first == nil {
			binding := page
			first = &binding
		} else if page.AsOf.Incarnation != first.AsOf.Incarnation || page.AsOf.Revision.String() != first.AsOf.Revision.String() || page.ReadyMembership != first.ReadyMembership || !page.WorkflowSnapshot.Equal(first.WorkflowSnapshot) {
			return ReadyCount{}, failure("cursor_invalid", "ready membership changed while counting; restart")
		}
		for _, item := range page.Items {
			if seen[item.ID] {
				return ReadyCount{}, failure("cursor_invalid", "ready sequence repeated an item while counting; restart")
			}
			seen[item.ID] = true
			total++
		}
		if page.Next == nil {
			return ReadyCount{AsOf: first.AsOf, Count: total}, nil
		}
		if len(page.Items) == 0 {
			return ReadyCount{}, failure("cursor_invalid", "ready sequence did not advance while counting; restart")
		}
		position := *page.Next
		key := positionKey{position.Offset, position.Workflow}
		if positions[key] || req.Position != nil && position.Workflow == req.Position.Workflow && position.Offset <= req.Position.Offset {
			return ReadyCount{}, failure("cursor_invalid", "ready position did not advance while counting; restart")
		}
		positions[key] = true
		req.Position = &position
	}
}
