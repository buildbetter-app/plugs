package service

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ClaimNextCandidateEligible evaluates the real claim transition on the supplied
// ready snapshot. It grants no write: the Writer still checks the actual
// prepared image and current authority before publication. The terminal probe
// supplies its post-head transaction reader, so a denied transition cannot
// keep an otherwise empty queue restarting against an unchanged ready set.
func (s *Service) ClaimNextCandidateEligible(ctx context.Context, by model.PrincipalRef, ref model.Ref, reader driver.Reader) (bool, error) {
	if reader == nil || s == nil || s.ontology == nil {
		return false, failure("authz_unavailable", "claim eligibility snapshot is unavailable")
	}
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil {
		return false, mapped(err)
	}
	if len(rows) == 0 {
		return false, nil
	}
	if len(rows) != 1 || rows[0].Kind != "bead" {
		return false, failure("internal_error", "invalid claim eligibility target")
	}
	doc, err := decodeDocument(rows[0].Body)
	if err != nil {
		return false, err
	}
	target, ok := s.ontology.effective(doc.Project).SlotStatus(doc.Type, ontology.Claim)
	if !ok {
		return false, nil
	}
	fields := make(map[string]any, len(doc.Fields))
	for name, value := range doc.Fields {
		fields[name] = value
	}
	fields["status"] = target
	doc.Fields = fields
	after, err := json.Marshal(doc)
	if err != nil {
		return false, err
	}
	view := reader.Snapshot()
	if view.At == nil {
		return false, failure("internal_error", "claim eligibility reader is unpinned")
	}
	proposal := port.ProposedChange{
		Principal: by, BaseRevision: *view.At,
		Meta: model.WriteMeta{Method: "bead.claimNext", Project: doc.Project},
		Changes: []port.Change{{
			Resource: model.ResourceRef{Kind: "bead", Ref: ref},
			Op:       model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{}}},
			Before:   rows[0].Body, After: after, SetBy: map[string]string{"fields.status": "core"},
		}},
	}
	if err := s.ontology.Validate(ctx, proposal, reader); err != nil {
		var refusal *model.Error
		if errors.As(err, &refusal) && refusal.Data.Code == "ontology_guard_failed" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
