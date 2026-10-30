package service

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// ApproveGate records one manual approval through the shared Writer. The gate
// evaluator runs in the Writer's ontology normalizer after its idempotency
// lookup, so a keyed replay never re-evaluates a now-terminal gate.
func (s *Service) ApproveGate(ctx context.Context, by model.PrincipalRef, req GateApproveRequest, meta model.WriteMeta) (GateOutcome, error) {
	meta.GateAction = "approve"
	meta.GateState = ""
	meta.GateDetails = nil
	cs, err := gateChangeSet(req.ID, req.Expect)
	if err != nil {
		return GateOutcome{}, err
	}
	// GateResult must be recoverable from the durable receipt even when the
	// caller asks for a minimal ordinary commit, so retain the full record in
	// the canonical nested commit.
	meta.Return = "full"
	v, err := s.apply(ctx, by, "gate.approve", cs, meta, true)
	if err != nil {
		return GateOutcome{}, err
	}
	result, err := gateResult(v.Commit)
	if err != nil {
		return GateOutcome{}, err
	}
	return GateOutcome{Result: result, Replayed: v.Replayed}, nil
}

// ReportGate records a plugin resolver outcome through the same Writer path.
// Details remain resolver-owned evidence and are retained in the gate bead.
func (s *Service) ReportGate(ctx context.Context, by model.PrincipalRef, req GateReportRequest, meta model.WriteMeta) (GateOutcome, error) {
	meta.GateAction = "report"
	meta.GateState = req.State
	meta.GateDetails = req.Details
	cs, err := gateChangeSet(req.ID, req.Expect)
	if err != nil {
		return GateOutcome{}, err
	}
	meta.Return = "full"
	v, err := s.apply(ctx, by, "gate.report", cs, meta, true)
	if err != nil {
		return GateOutcome{}, err
	}
	result, err := gateResult(v.Commit)
	if err != nil {
		return GateOutcome{}, err
	}
	return GateOutcome{Result: result, Replayed: v.Replayed}, nil
}

func gateChangeSet(id model.Ref, expect []model.Precondition) (model.ChangeSet, error) {
	if err := namedRef(id); err != nil {
		return model.ChangeSet{}, err
	}
	// The Writer looks up and reauthorizes a durable receipt before requiring
	// the live bead. For fresh writes its tracked read is checked under the
	// transaction's record lock, alongside the caller's explicit expectations.
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: id, Fields: map[string]any{}}}}, Expect: expect}, nil
}

func gateResult(commit model.Commit) (GateResult, error) {
	if len(commit.Results) != 1 || len(commit.Results[0].Record) == 0 {
		return GateResult{}, failure("internal_error", "gate commit is missing its final bead image")
	}
	doc, err := pipeline.DecodeDocument(commit.Results[0].Record)
	if err != nil {
		return GateResult{}, failure("internal_error", "gate commit contains an invalid bead image")
	}
	state, ok := doc.Fields[gateStateField].(string)
	if !ok || state == "" {
		return GateResult{}, failure("internal_error", "gate commit is missing gate_state")
	}
	// The evaluator's count can differ from the retained observations: it
	// excludes anonymous observations and collapses sponsor identities. Persist
	// that count in the same bead image so replay never consults current rules.
	raw, present := doc.Fields[gateApprovalCountField]
	data, marshalErr := json.Marshal(raw)
	var approvals model.Natural
	if !present || marshalErr != nil || json.Unmarshal(data, &approvals) != nil {
		return GateResult{}, failure("internal_error", "gate commit is missing its evaluated approval count")
	}
	return GateResult{Commit: commit, State: state, Approvals: approvals}, nil
}

var _ port.GateService = (*Service)(nil)
