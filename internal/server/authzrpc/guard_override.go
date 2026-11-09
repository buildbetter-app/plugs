package authzrpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// AuthorizeGuardOverride uses the installed chain's distinct action on each
// actual close target. The finite fixed component has no override permit;
// its ordinary modify grants cannot satisfy this decision. Transition guards
// still obtain their independent observation through the ontology adapter.
func (c *Checker) AuthorizeGuardOverride(ctx context.Context, _ port.AuthorizationStage, by model.PrincipalRef, targets []model.ResourceRef, reader port.SnapshotReader) error {
	if by.Kind != "user" {
		return forbidden("guard override requires a user")
	}
	for _, target := range targets {
		if target.Kind != "bead" || authz.CanonicalRef(target.Ref) != nil {
			return forbidden("invalid guard override target")
		}
		allowed, err := c.decision(ctx, reader, by, authz.OverrideGuard, authz.Bead, target.Ref, "bead.close")
		if err != nil {
			return err
		}
		if !allowed {
			return forbidden("override_guard was not allowed")
		}
	}
	return nil
}

var _ port.GuardOverrideAuthorizer = (*Checker)(nil)
