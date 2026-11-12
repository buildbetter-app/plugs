package authzrpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Cycles authorizes each member against the same current authority used for
// this report. Only placeholders/counts cross the boundary for hidden members.
func (s *GuardedService) Cycles(ctx context.Context, by model.PrincipalRef, req service.CyclesRequest) (service.CyclesResult, error) {
	content, snapshot, _, err := s.current(ctx, nil)
	if err != nil {
		return service.CyclesResult{}, err
	}
	closed := false
	closeContent := func() error {
		if closed {
			return nil
		}
		closed = true
		return content.Close()
	}
	defer closeContent()
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return service.CyclesResult{}, authzError(err)
	}
	authority, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return service.CyclesResult{}, hierarchyUnavailable()
	}
	readable := func(ctx context.Context, ref model.Ref) (bool, error) {
		if workflowItemRef(ref) {
			if s.sourceRuntimeInstalled() {
				return s.sourceRead(ctx, caller, authority, ref, req.Project, "bead.cycles")
			}
			return s.checker.workflowRead(ctx, caller, authority, "bead.cycles", req.Project)
		}
		decisions, err := s.checker.chain.Check(ctx, authority, []authz.CheckRequest{{
			Caller: caller, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: ref}, Method: "bead.cycles",
		}})
		if err != nil {
			return false, authzError(err)
		}
		if len(decisions) != 1 {
			return false, hierarchyUnavailable()
		}
		return decisions[0].Effect == authz.Allow, nil
	}
	var result service.CyclesResult
	if producer, ok := s.delegate.(interface {
		ReadCyclesFrom(context.Context, model.PrincipalRef, driver.Reader, service.CyclesRequest, func(context.Context, model.Ref) (bool, error)) (service.CyclesResult, error)
	}); ok {
		result, err = producer.ReadCyclesFrom(ctx, by, content, req, readable)
	} else {
		result, err = service.ReadCycles(ctx, content, req, readable)
	}
	if err != nil {
		return service.CyclesResult{}, err
	}
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authority, snapshot)
	if err != nil {
		return service.CyclesResult{}, err
	}
	// No lazy content read or reader close may run after the final authority,
	// cancellation and lease fences. A close failure publishes no report.
	if err := closeContent(); err != nil {
		return service.CyclesResult{}, hierarchyUnavailable()
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return service.CyclesResult{}, err
	}
	// The final authority reader is closed before validation returns. Lease
	// loss during that read/close must still fence this response.
	select {
	case <-s.lease.Lost():
		return service.CyclesResult{}, &model.Error{Data: model.ErrorData{Code: "busy", Retryable: true, Hint: "active storage lease was lost"}}
	default:
	}
	if err := ctx.Err(); err != nil {
		return service.CyclesResult{}, err
	}
	return result, nil
}
