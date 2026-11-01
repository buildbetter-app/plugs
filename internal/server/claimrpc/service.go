// Package claimrpc binds the canonical claim-recovery and quiet heartbeat
// methods without widening the general service binding. It deliberately
// exposes no invented reclaim verb: system:core uses the same cataloged
// release method.
package claimrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

type Service interface {
	ReleaseWithOutcome(context.Context, model.PrincipalRef, service.ReleaseRequest, model.WriteMeta) (port.WriteOutcome, error)
}

type heartbeatService interface {
	HeartbeatWithOutcome(context.Context, model.PrincipalRef, service.HeartbeatRequest, model.WriteMeta) (port.HeartbeatOutcome, error)
}

type params struct {
	ID     model.Ref `json:"id"`
	Reason string    `json:"reason"`
	Meta   wire.Meta `json:"_meta"`
}

// Bind adds only the already-cataloged bead.release descriptor. It fails
// before changing the registry when the descriptor or binding is unavailable.
func Bind(registry *rpc.Registry, svc Service) error {
	if registry == nil || svc == nil {
		return fmt.Errorf("nil claim service or registry")
	}
	if err := registry.Register("bead.release", func(ctx context.Context, c rpc.Call) (any, error) {
		var p params
		if err := wire.DecodeInto(c.Params, &p); err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		meta := model.WriteMeta{Method: c.Method, AgentLabel: c.Meta.Agent, Project: c.Meta.Project, IdempotencyKey: c.Meta.IdempotencyKey, DryRun: c.Meta.DryRun, Return: c.Meta.Return, Traceparent: c.Meta.Traceparent, Agent: c.Meta.Agent}
		v, err := svc.ReleaseWithOutcome(ctx, c.Principal, service.ReleaseRequest{ID: p.ID, Reason: p.Reason}, meta)
		if err != nil {
			return nil, err
		}
		results := v.Commit.Results
		if results == nil {
			results = []model.OpResult{}
		}
		warnings := v.Commit.Warnings
		if warnings == nil {
			warnings = []model.Warning{}
		}
		out := map[string]any{"revision": v.Commit.Revision, "results": results, "warnings": warnings}
		if v.Commit.WouldCommit {
			out["wouldCommit"] = true
		}
		if v.Commit.Violations != nil {
			out["violations"] = v.Commit.Violations
		}
		return rpc.Outcome{Result: out, Replayed: v.Replayed}, nil
	}); err != nil {
		return err
	}
	if heartbeat, ok := any(svc).(heartbeatService); ok {
		return registry.Register("bead.heartbeat", func(ctx context.Context, c rpc.Call) (any, error) {
			var p params
			if err := wire.DecodeInto(c.Params, &p); err != nil {
				return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
			}
			meta := model.WriteMeta{Method: c.Method, AgentLabel: c.Meta.Agent, Project: c.Meta.Project, IdempotencyKey: c.Meta.IdempotencyKey, DryRun: c.Meta.DryRun, Return: c.Meta.Return, Traceparent: c.Meta.Traceparent, Agent: c.Meta.Agent}
			v, err := heartbeat.HeartbeatWithOutcome(ctx, c.Principal, service.HeartbeatRequest{ID: p.ID}, meta)
			if err != nil {
				return nil, err
			}
			return rpc.Outcome{Result: map[string]any{"leaseExpiresAt": v.LeaseExpiresAt.UTC().Format(time.RFC3339Nano)}, Replayed: v.Replayed}, nil
		})
	}
	return nil
}
