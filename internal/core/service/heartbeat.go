package service

import (
	"context"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

type heartbeatWriter interface {
	Heartbeat(context.Context, model.PrincipalRef, model.Ref, model.WriteMeta) (port.HeartbeatOutcome, error)
}

type heartbeatAuthorizationCapability interface {
	HeartbeatAuthorizationAvailable() bool
}

// HeartbeatWithOutcome is the quiet lease-only service seam. It deliberately
// bypasses Writer.Apply: the writer stores the refreshed deadline and its
// keyed replay response in the replica-local checkpoint metadata plane.
func (s *Service) HeartbeatWithOutcome(ctx context.Context, by model.PrincipalRef, req HeartbeatRequest, meta model.WriteMeta) (port.HeartbeatOutcome, error) {
	if err := namedRef(req.ID); err != nil {
		return port.HeartbeatOutcome{}, err
	}
	if err := s.admit(ctx, by); err != nil {
		return port.HeartbeatOutcome{}, err
	}
	if strings.TrimSpace(meta.Project) == "" {
		return port.HeartbeatOutcome{}, failure("project_required", "supply a project")
	}
	if meta.IdempotencyKey == "" {
		return port.HeartbeatOutcome{}, failure("invalid_params", "heartbeat requires an idempotency key")
	}
	if by.Kind == "anonymous" && meta.AgentLabel == "" {
		meta.AgentLabel = meta.Agent
	}
	writer, ok := s.writer.(heartbeatWriter)
	if !ok {
		return port.HeartbeatOutcome{}, failure("internal_error", "heartbeat is unsupported by this writer")
	}
	// A host that reports an installed authorization chain must expose the
	// writer capability that invokes it before heartbeat target inspection.
	// Missing support fails closed. With no installed chain, the explicit
	// anonymous/no-plugin mode remains allowed.
	if s.authorizationInstalled != nil && s.authorizationInstalled() {
		capability, supported := s.writer.(heartbeatAuthorizationCapability)
		if !supported || !capability.HeartbeatAuthorizationAvailable() {
			return port.HeartbeatOutcome{}, failure("authz_unavailable", "heartbeat authorization is unavailable")
		}
	}
	outcome, err := writer.Heartbeat(ctx, by, req.ID, meta)
	if err != nil {
		return port.HeartbeatOutcome{}, mapped(err)
	}
	return outcome, nil
}
