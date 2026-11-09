package pipeline

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// ProtectedAuthorizer checks the exact management operation against current
// authority and authenticated context. All consumed policy/source facts must
// be read through reader, so their presence and revisions join publication's
// lock guard. A replay is a fresh authorization decision, never a stored grant.
type ProtectedAuthorizer interface {
	AuthorizeProtected(context.Context, port.AuthorizationStage, model.PrincipalRef, ProtectedApplyRequest, model.WriteMeta, port.SnapshotReader) (port.EventContext, error)
	// Policy facts are protected by the complete recorded lock set at this
	// point. Recheck only the live authentication context, without reloading
	// a host source whose publication may be waiting for those same locks.
	RevalidateProtectedContext(context.Context, model.PrincipalRef) error
}

func (w *Writer) authorizeProtected(ctx context.Context, stage port.AuthorizationStage, by model.PrincipalRef, req ProtectedApplyRequest, meta model.WriteMeta, reader port.SnapshotReader) (port.EventContext, error) {
	if w.cfg.protectedAuthorizer == nil {
		if w.cfg.authorizer != nil {
			return port.EventContext{}, failure("forbidden", "installed authorization has no protected ontology.apply capability")
		}
		return port.EventContext{Cause: model.EventCause{Root: meta.IdempotencyKey, Depth: model.Natural64(0), Chain: []string{}}, Chain: []string{}}, nil
	}
	return w.cfg.protectedAuthorizer.AuthorizeProtected(ctx, stage, by, req, meta, reader)
}

func (w *Writer) revalidateProtectedContext(ctx context.Context, by model.PrincipalRef) error {
	if w.cfg.protectedAuthorizer != nil {
		return w.cfg.protectedAuthorizer.RevalidateProtectedContext(ctx, by)
	}
	return nil
}
