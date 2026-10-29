package service

import (
	"context"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// ReleaseRequest is the canonical bead.release domain request. Recovery is
// selected only for the trusted system:core principal; it is never a caller
// controlled flag and ordinary callers cannot bypass claim ownership.
type ReleaseRequest struct {
	ID     model.Ref
	Reason string
}

type claimReleaseWriter interface {
	ApplyClaimRelease(context.Context, model.PrincipalRef, model.Ref, string, model.WriteMeta) (port.WriteOutcome, error)
}

// ReleaseWithOutcome lowers bead.release through the protected claim-recovery
// writer seam. A system:core call is the durable expired-claim reclaimer;
// every other caller must release its own live claim.
func (s *Service) ReleaseWithOutcome(ctx context.Context, by model.PrincipalRef, req ReleaseRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if err := namedRef(req.ID); err != nil {
		return port.WriteOutcome{}, err
	}
	if err := s.admit(ctx, by); err != nil {
		return port.WriteOutcome{}, err
	}
	if strings.TrimSpace(meta.Project) == "" {
		return port.WriteOutcome{}, failure("project_required", "supply a project")
	}
	writer, ok := s.writer.(claimReleaseWriter)
	if !ok {
		return port.WriteOutcome{}, failure("internal_error", "claim release is unsupported by this writer")
	}
	v, err := writer.ApplyClaimRelease(ctx, by, req.ID, req.Reason, meta)
	return v, mapped(err)
}
