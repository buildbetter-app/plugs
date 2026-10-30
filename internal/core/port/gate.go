package port

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// GateApproveRequest is the domain input for gate.approve. Transport metadata
// such as the idempotency key stays in WriteMeta; callers cannot supply an
// application actor separate from the authenticated PrincipalRef.
type GateApproveRequest struct {
	ID     model.Ref
	Expect []model.Precondition
}

// GateReportRequest is the domain input for a plugin-resolved gate.report.
// Details are resolver-owned evidence and are not interpreted by core.
type GateReportRequest struct {
	ID      model.Ref
	State   string
	Details map[string]any
	Expect  []model.Precondition
}

// GateResult is the canonical domain shape for both gate.approve and
// gate.report. It deliberately mirrors the frozen OpenRPC GateResult schema;
// transports may wrap it with response-only replay metadata.
type GateResult struct {
	Commit    model.Commit  `json:"commit"`
	State     string        `json:"state"`
	Approvals model.Natural `json:"approvals"`
}

// GateOutcome carries the response-only replay bit without adding it to the
// durable GateResult wire shape.
type GateOutcome struct {
	Result   GateResult
	Replayed bool
}

// GateService is the narrow core port that an RPC adapter may bind. An
// implementation must perform the compare-and-set against the gate snapshot
// in the same Writer commit that records the approval/report and any slot
// transition; this interface does not authorize a second write path.
type GateService interface {
	ApproveGate(context.Context, model.PrincipalRef, GateApproveRequest, model.WriteMeta) (GateOutcome, error)
	ReportGate(context.Context, model.PrincipalRef, GateReportRequest, model.WriteMeta) (GateOutcome, error)
}
