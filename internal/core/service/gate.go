package service

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
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
	cs, autoCAS, err := s.gateChangeSet(ctx, by, req.ID, req.Expect)
	if err != nil {
		return GateOutcome{}, err
	}
	meta.GateAutoCAS = autoCAS
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
	cs, autoCAS, err := s.gateChangeSet(ctx, by, req.ID, req.Expect)
	if err != nil {
		return GateOutcome{}, err
	}
	meta.GateAutoCAS = autoCAS
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

func (s *Service) gateChangeSet(ctx context.Context, by model.PrincipalRef, id model.Ref, expect []model.Precondition) (model.ChangeSet, bool, error) {
	if err := namedRef(id); err != nil {
		return model.ChangeSet{}, false, err
	}
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return model.ChangeSet{}, false, err
	}
	defer r.Close()
	records, err := r.Get(ctx, []model.Ref{id})
	if err != nil {
		return model.ChangeSet{}, false, mapped(err)
	}
	if len(records) != 1 || records[0].Kind != "bead" {
		return model.ChangeSet{}, false, failure("not_found", "gate bead not found")
	}
	if _, err := pipeline.DecodeDocument(records[0].Body); err != nil {
		return model.ChangeSet{}, false, failure("internal_error", "invalid persisted gate bead")
	}
	revision := records[0].Revision
	hasRead := false
	for _, precondition := range expect {
		if precondition.Ref.ID == id.ID && precondition.Ref.Branch.IsZero() && precondition.Revision != nil && precondition.Revision.String() == revision.String() && precondition.Field == "" && precondition.Hash == "" {
			hasRead = true
			break
		}
	}
	autoCAS := !hasRead
	if autoCAS {
		expect = append(append([]model.Precondition(nil), expect...), model.Precondition{Ref: id, Revision: &revision})
	}
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: id, Fields: map[string]any{}}}}, Expect: expect}, autoCAS, nil
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
	approvals := []ontology.GateApproval{}
	if raw, present := doc.Fields[gateApprovalsField]; present {
		data, marshalErr := json.Marshal(raw)
		if marshalErr != nil || json.Unmarshal(data, &approvals) != nil {
			return GateResult{}, failure("internal_error", "gate commit contains invalid approvals")
		}
	}
	return GateResult{Commit: commit, State: state, Approvals: model.Natural64(uint64(len(approvals)))}, nil
}

var _ port.GateService = (*Service)(nil)
