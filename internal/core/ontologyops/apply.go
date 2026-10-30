package ontologyops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Apply loads one retained, ready plan and lowers it to the canonical
// protected Writer. The service repeats every read-side guard immediately
// before building complete stored images; the Writer repeats those guards
// while holding the one transaction's locks.
func (s *Service) Apply(ctx context.Context, by model.PrincipalRef, req ApplyRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if s.cfg.ProtectedApplier == nil {
		return port.WriteOutcome{}, failure("method_not_found", "ontology.apply is not bound by this composition root")
	}
	meta.Method = "ontology.apply"
	if meta.AgentLabel == "" {
		meta.AgentLabel = meta.Agent
	}
	if by.Kind != "anonymous" {
		meta.AgentLabel = ""
	}
	if req.PlanDigest == "" || len(req.PlanDigest) != 64 || !isLowerHex(req.PlanDigest) {
		return port.WriteOutcome{}, failure("invalid_params", "apply requires a canonical plan digest")
	}
	plan, retained, err := s.loadPlan(req.PlanDigest)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	if !equalPreconditions(req.Expect, plan.Expect) {
		return port.WriteOutcome{}, failure("ontology_plan_stale", "apply expect does not exactly match the retained plan")
	}
	if !plan.Ready {
		return port.WriteOutcome{}, failure("ontology_plan_stale", "only a ready ontology plan can be applied")
	}
	if err := s.validatePlanPrincipal(ctx, by, plan); err != nil {
		return port.WriteOutcome{}, err
	}
	if err := s.revalidate(ctx, by, plan); err != nil {
		if !isCode(err, "ontology_plan_stale") || len(retained.protected) == 0 {
			return port.WriteOutcome{}, err
		}
		var protected pipeline.ProtectedApplyRequest
		if decodeErr := json.Unmarshal(retained.protected, &protected); decodeErr != nil {
			return port.WriteOutcome{}, err
		}
		_, candidate, candidateErr := candidateSnapshot(plan)
		if candidateErr != nil {
			return port.WriteOutcome{}, candidateErr
		}
		meta.Method = "ontology.apply"
		meta.Project = plan.Project
		if meta.Return == "" {
			meta.Return = "full"
		}
		// A stale plan may still be retried after a response was lost: the
		// Writer can return its durable receipt before checking the old base
		// revision. Never use that replay escape hatch for a new operation,
		// because stale validating-pre bindings or other guards must not be
		// bypassed by applying the retained bytes again.
		receipt, receiptErr := s.protectedReceiptPresent(ctx, by, meta)
		if receiptErr != nil {
			return port.WriteOutcome{}, receiptErr
		}
		if !receipt {
			return port.WriteOutcome{}, err
		}
		outcome, applyErr := s.cfg.ProtectedApplier.ApplyProtected(ctx, by, protected, meta)
		if applyErr != nil {
			return port.WriteOutcome{}, applyErr
		}
		return outcome, s.refreshRuntime(ctx, by, meta, candidate, outcome.Replayed)
	}
	protected, candidate, err := s.protectedRequest(ctx, by, plan)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	meta.Method = "ontology.apply"
	meta.Project = plan.Project
	if meta.Return == "" {
		meta.Return = "full"
	}
	outcome, err := s.cfg.ProtectedApplier.ApplyProtected(ctx, by, protected, meta)
	if err == nil {
		err = s.refreshRuntime(ctx, by, meta, candidate, outcome.Replayed)
	}
	return outcome, err
}

func (s *Service) refreshRuntime(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta, candidate ontology.Snapshot, replayed bool) error {
	if meta.DryRun || s.cfg.OntologyRefresher == nil {
		return nil
	}
	if replayed {
		// A receipt certifies a historical commit, not the current source.
		// Another apply may have superseded its candidate, so publish the
		// latest durable source rather than the retained plan's snapshot.
		_, reader, err := s.reader(ctx, by, nil)
		if err != nil {
			return err
		}
		defer reader.Close()
		current, err := s.source(ctx, by, reader, meta.Project)
		if err != nil {
			return err
		}
		if err := current.valid(); err != nil {
			return err
		}
		candidate = current.compiled
	}
	s.cfg.OntologyRefresher.Refresh(candidate)
	return nil
}

func (s *Service) protectedReceiptPresent(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta) (bool, error) {
	if meta.IdempotencyKey == "" {
		return false, nil
	}
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		return false, failure("invalid_params", err.Error())
	}
	_, reader, err := s.reader(ctx, by, nil)
	if err != nil {
		return false, err
	}
	defer reader.Close()
	agent := meta.AgentLabel
	if by.Kind != "anonymous" {
		agent = ""
	}
	entry, err := reader.IdempotencyByKey(ctx, principal, agent, "ontology.apply", meta.IdempotencyKey)
	if err != nil {
		return false, mapped(err)
	}
	return entry != nil, nil
}

func (s *Service) loadPlan(wantDigest string) (PlannedRevision, retainedPlan, error) {
	s.mu.Lock()
	retained, exists := s.plans[wantDigest]
	s.mu.Unlock()
	if !exists {
		return PlannedRevision{}, retainedPlan{}, failure("ontology_plan_stale", "plan is absent, evicted, refused or from a previous process")
	}
	plan, err := decodePlan(retained.bytes)
	if err != nil {
		return PlannedRevision{}, retainedPlan{}, failure("ontology_plan_stale", "retained ontology plan is unreadable")
	}
	if plan.Digest != wantDigest {
		return PlannedRevision{}, retainedPlan{}, failure("ontology_plan_stale", "retained ontology plan digest does not match its key")
	}
	computed, err := planDigest(plan)
	if err != nil || computed != wantDigest {
		return PlannedRevision{}, retainedPlan{}, failure("ontology_plan_stale", "retained ontology plan failed its digest check")
	}
	return plan, retained, nil
}

func (s *Service) validatePlanPrincipal(ctx context.Context, by model.PrincipalRef, plan PlannedRevision) error {
	want, err := digest(plan.Principal)
	if err != nil {
		return err
	}
	got, err := digest(by)
	if err != nil || got != want {
		return failure("ontology_plan_stale", "plan belongs to a different principal context")
	}
	return s.admit(ctx, by)
}

func isCode(err error, code model.ErrorCode) bool {
	var target *model.Error
	return errors.As(err, &target) && target.Data.Code == code
}

func equalPreconditions(left, right []model.Precondition) bool {
	if left == nil {
		left = []model.Precondition{}
	}
	if right == nil {
		right = []model.Precondition{}
	}
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

type fullBead struct {
	record driver.Record
	doc    pipeline.Document
}

func (s *Service) protectedRequest(ctx context.Context, by model.PrincipalRef, plan PlannedRevision) (pipeline.ProtectedApplyRequest, ontology.Snapshot, error) {
	meta, r, err := s.reader(ctx, by, nil)
	if err != nil {
		return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, err
	}
	defer r.Close()
	if meta.StoreID == "" || r.Snapshot().At == nil || r.Snapshot().At.String() != plan.BaseRevision.String() {
		return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("ontology_plan_stale", "protected apply snapshot changed")
	}
	current, err := s.source(ctx, by, r, plan.Project)
	if err != nil {
		return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, err
	}
	if !sameRef(current.guard.Ref, plan.Source.Ref) || current.guard.Revision.String() != plan.Source.Revision.String() || current.guard.BodyHash != plan.Source.BodyHash || current.guard.Digest != plan.Source.Digest {
		return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("ontology_plan_stale", "protected ontology source changed")
	}
	sourceRecords, err := r.Get(ctx, []model.Ref{current.guard.Ref})
	if err != nil {
		return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, mapped(err)
	}
	if len(sourceRecords) != 1 || sourceRecords[0].Kind != "ontology" || sourceRecords[0].Revision.String() != plan.Source.Revision.String() || bytesDigest(sourceRecords[0].Body) != plan.Source.BodyHash {
		return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("ontology_plan_stale", "protected ontology source image changed")
	}
	candidateBody, candidate, err := candidateSnapshot(plan)
	if err != nil {
		return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, err
	}
	beads, err := readFullBeads(ctx, r)
	if err != nil {
		return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, err
	}
	byID := make(map[string]fullBead, len(beads))
	for _, bead := range beads {
		if _, exists := byID[bead.record.Ref.ID]; exists {
			return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("ontology_plan_stale", "complete bead inventory contains duplicate IDs")
		}
		byID[bead.record.Ref.ID] = bead
	}
	if len(byID) != len(plan.Migration.Rows) {
		return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("ontology_plan_stale", "complete bead row inventory changed")
	}
	rows := make([]pipeline.ProtectedRecordChange, 0, len(plan.Migration.Rows))
	for _, row := range plan.Migration.Rows {
		actual, exists := byID[row.Before.State.ID]
		if !exists || actual.record.Revision.String() != row.Before.Revision.String() {
			return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("ontology_plan_stale", "planned bead row revision changed")
		}
		beforeState := beadState(actual.doc)
		if !equalJSON(beforeState, row.Before.State) {
			return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("ontology_plan_stale", "planned bead row image changed")
		}
		desired := actual.doc
		if row.After.State.ID != actual.doc.ID || row.After.State.Project != actual.doc.Project || row.After.State.Type == "" || row.After.State.Fields == nil {
			return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("ontology_plan_stale", "planned bead after image is incomplete")
		}
		desired.Type = row.After.State.Type
		desired.Fields = cloneFields(row.After.State.Fields)
		if desired.Fields == nil {
			return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("ontology_plan_stale", "planned bead after fields are missing")
		}
		desired.Fields["status"] = row.After.State.Status
		afterBody, err := json.Marshal(desired)
		if err != nil {
			return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, err
		}
		// Preserve the exact before bytes for an unchanged row. This keeps the
		// protected writer from manufacturing a body-only change when only the
		// ontology rules changed.
		if equalJSON(beforeState, row.After.State) {
			afterBody = append([]byte(nil), actual.record.Body...)
		}
		if _, err := pipeline.DecodeDocument(afterBody); err != nil {
			return pipeline.ProtectedApplyRequest{}, ontology.Snapshot{}, failure("invalid_params", "planned bead after image cannot be decoded")
		}
		paths := []string{}
		if !bytes.Equal(actual.record.Body, afterBody) {
			paths = []string{"/fields", "/type"}
		}
		rows = append(rows, pipeline.ProtectedRecordChange{Ref: actual.record.Ref, Kind: "bead", FromRevision: actual.record.Revision, Before: append([]byte(nil), actual.record.Body...), After: afterBody, ChangedPaths: paths, Operation: model.OpPatch})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Ref.ID < rows[j].Ref.ID })
	sourceAfter := append([]byte(nil), candidateBody...)
	return pipeline.ProtectedApplyRequest{
		PlanDigest:   plan.Digest,
		BaseRevision: plan.BaseRevision,
		Source: pipeline.ProtectedRecordChange{
			Ref: current.guard.Ref, Kind: "ontology", FromRevision: current.guard.Revision,
			Before: append([]byte(nil), sourceRecords[0].Body...), After: sourceAfter,
			ChangedPaths: []string{"/document"}, Operation: model.OpPatch,
		},
		Rows: rows,
	}, candidate, nil
}

func candidateSnapshot(plan PlannedRevision) ([]byte, ontology.Snapshot, error) {
	candidateBody, err := service.MarshalOntologySource(plan.Document)
	if err != nil {
		return nil, ontology.Snapshot{}, failure("invalid_params", "retained ontology candidate cannot be encoded")
	}
	candidate, err := service.DecodeOntologySource(candidateBody)
	if err != nil {
		return nil, ontology.Snapshot{}, failure("ontology_plan_stale", "retained ontology candidate cannot be decoded")
	}
	snapshot, problems := ontology.Compile(candidate.Base, candidate.Overlays, candidate.Plugins)
	if len(problems) != 0 {
		return nil, ontology.Snapshot{}, failure("ontology_plan_stale", "retained ontology candidate no longer compiles")
	}
	return candidateBody, snapshot, nil
}

func readFullBeads(ctx context.Context, r driver.Reader) ([]fullBead, error) {
	result := []fullBead{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			return nil, mapped(err)
		}
		if r.Snapshot().At == nil || page.AsOf.String() != r.Snapshot().At.String() {
			return nil, failure("ontology_plan_stale", "bead enumeration changed storage snapshot")
		}
		for _, record := range page.Items {
			if record.Kind != "bead" || !record.Ref.IsID() || !record.Ref.Branch.IsZero() {
				return nil, failure("invalid_params", "unsupported stored bead identity")
			}
			doc, err := decodeRow(record.Body)
			if err != nil || doc.ID != record.Ref.ID {
				return nil, failure("invalid_params", "stored bead image is invalid")
			}
			result = append(result, fullBead{record: record, doc: doc})
		}
		if page.NextCursor == nil {
			return result, nil
		}
		if cursor != nil && page.NextCursor.Index.String() == cursor.Index.String() {
			return nil, failure("invalid_params", "bead enumeration did not advance")
		}
		cursor = page.NextCursor
	}
}

func beadState(doc pipeline.Document) ontology.BeadState {
	fields := cloneFields(doc.Fields)
	status, _ := fields["status"].(string)
	delete(fields, "status")
	template, _ := fields["template"].(bool)
	return ontology.BeadState{ID: doc.ID, Project: doc.Project, Type: doc.Type, Status: status, Template: template, Fields: fields}
}

func cloneFields(fields map[string]any) map[string]any {
	if fields == nil {
		return nil
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return nil
	}
	var copy map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&copy) != nil {
		return nil
	}
	return copy
}

func equalJSON(left, right any) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func isLowerHex(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}
