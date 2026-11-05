package ontologyops

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Plan replaces the requested base or project overlay document. The frozen
// wire method has no directives parameter, so directive-bearing documents are
// explicitly refused by the real ontology decoder, never silently ignored.
func (s *Service) Plan(ctx context.Context, by model.PrincipalRef, req PlanRequest) (PlanResult, error) {
	meta, r, err := s.reader(ctx, by, nil)
	if err != nil {
		return PlanResult{}, err
	}
	defer r.Close()
	current, err := s.source(ctx, by, r, req.Project)
	if err != nil {
		return PlanResult{}, err
	}
	if err = current.valid(); err != nil {
		return PlanResult{}, err
	}
	if err := s.authorizeSource(ctx, by, current); err != nil {
		return PlanResult{}, err
	}
	if s.cfg.ValidatingPre == nil {
		return PlanResult{}, failure("invalid_params", "planning requires the actual validating-pre inventory or an explicit no-bindings host declaration")
	}
	if err := s.checkExpect(ctx, by, r, req.Expect); err != nil {
		return PlanResult{}, err
	}
	document, err := ontology.Decode(req.Document)
	if err != nil {
		return PlanResult{}, failure("invalid_params", "candidate ontology cannot be decoded: "+err.Error())
	}
	// Clone through the adopted source codec; neither caller edits nor plugin
	// callbacks may retarget the candidate after compilation.
	raw, err := service.MarshalOntologySource(current.document)
	if err != nil {
		return PlanResult{}, err
	}
	candidate, err := service.DecodeOntologySource(raw)
	if err != nil {
		return PlanResult{}, err
	}
	if req.Project == "" {
		candidate.Base = document
	} else {
		if candidate.Overlays == nil {
			candidate.Overlays = map[string]ontology.Document{}
		}
		candidate.Overlays[req.Project] = document
	}
	compiled, problems := ontology.Compile(candidate.Base, candidate.Overlays, candidate.Plugins)
	if len(problems) != 0 {
		return PlanResult{}, failure("invalid_params", "candidate ontology does not compile")
	}
	if err := validateKeyContinuity(current.document, current.compiled, candidate, compiled); err != nil {
		return PlanResult{}, err
	}
	revision, err := current.guard.Revision.Uint64()
	if err != nil || revision == math.MaxUint64 {
		return PlanResult{}, failure("invalid_params", "ontology source revision has no supported successor")
	}
	plan, err := ontologymigration.Build(ctx, ontologymigration.Request{
		Current:   ontologymigration.Version{Revision: current.guard.Revision, Snapshot: current.compiled},
		Candidate: ontologymigration.Version{Revision: model.Natural64(revision + 1), Snapshot: compiled},
	}, rowSource{s: s, by: by, r: r, current: current, candidate: compiled, graph: &storedGraph{}}, s.cfg.ValidatingPre)
	if err != nil {
		return PlanResult{}, mapped(err)
	}
	artifact := PlannedRevision{StoreID: meta.StoreID, Incarnation: r.Snapshot().Incarnation, BaseRevision: *r.Snapshot().At, Principal: by, Project: req.Project, Source: current.guard, Document: candidate, Expect: req.Expect, Migration: plan.Report(), Ready: plan.Ready()}
	if artifact.Expect == nil {
		artifact.Expect = []model.Precondition{}
	}
	artifact.Digest, err = digest(artifact)
	if err != nil {
		return PlanResult{}, err
	}
	encoded, err := json.Marshal(artifact)
	if err != nil {
		return PlanResult{}, err
	}
	artifact, err = decodePlan(encoded)
	if err != nil {
		return PlanResult{}, err
	}
	// Re-read authoritative state after callbacks. A source or row change
	// during planning produces a stale refusal, not an apparently current plan.
	if err := s.revalidate(ctx, by, artifact); err != nil {
		return PlanResult{}, err
	}
	if artifact.Ready {
		var protectedBytes []byte
		if s.cfg.ProtectedApplier != nil {
			protected, _, protectedErr := s.protectedRequest(ctx, by, artifact)
			if protectedErr != nil {
				return PlanResult{}, protectedErr
			}
			protectedBytes, err = json.Marshal(protected)
			if err != nil {
				return PlanResult{}, err
			}
		}
		s.mu.Lock()
		if _, exists := s.plans[artifact.Digest]; !exists {
			if len(s.order) == s.cfg.MaxPlans {
				delete(s.plans, s.order[0])
				s.order = s.order[1:]
			}
			s.order = append(s.order, artifact.Digest)
			s.plans[artifact.Digest] = retainedPlan{bytes: encoded, protected: protectedBytes}
		}
		s.mu.Unlock()
	}
	violations := []model.Violation{}
	for _, refusal := range artifact.Migration.Refusals {
		violations = append(violations, model.Violation{Guard: string(refusal.Code), Message: refusal.Message, Details: map[string]any{"row": refusal.Row, "reason": refusal.Reason}})
	}
	for _, row := range artifact.Migration.Rows {
		for _, finding := range row.Findings {
			if finding.Disposition == "" {
				violations = append(violations, model.Violation{Guard: string(finding.Violation.Code), Message: finding.Violation.Message, Details: map[string]any{"id": finding.ID, "row": row.Before.State.ID, "field": finding.Violation.Field, "subscriber": finding.Subscriber}})
			}
		}
	}
	return PlanResult{PlanDigest: artifact.Digest, BaseRevision: artifact.BaseRevision, Changes: artifact, Violations: violations, Warnings: []model.Warning{}, RequiresHuman: true}, nil
}

func decodePlan(raw []byte) (PlannedRevision, error) {
	var plan PlannedRevision
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	err := decoder.Decode(&plan)
	return plan, err
}

func planDigest(plan PlannedRevision) (string, error) {
	copy := plan
	copy.Digest = ""
	return digest(copy)
}

// Planned retrieves a detached admitted report for this exact caller and
// rechecks its source bytes/revision/digest, complete rows, CAS, incarnation and
// validating-pre manifest. It is a read seam, never an apply permit. Retention
// is process-local and bounded; restart or eviction requires replanning.
func (s *Service) Planned(ctx context.Context, by model.PrincipalRef, digest string) (PlannedRevision, error) {
	s.mu.Lock()
	retained, exists := s.plans[digest]
	s.mu.Unlock()
	if !exists {
		return PlannedRevision{}, failure("ontology_plan_stale", "plan is absent, evicted, refused or from a previous process")
	}
	plan, err := decodePlan(retained.bytes)
	if err != nil {
		return PlannedRevision{}, err
	}
	if plan.Digest != digest {
		return PlannedRevision{}, failure("ontology_plan_stale", "retained ontology plan digest does not match its key")
	}
	if err := s.revalidate(ctx, by, plan); err != nil {
		return PlannedRevision{}, err
	}
	return plan, nil
}

func (s *Service) revalidate(ctx context.Context, by model.PrincipalRef, plan PlannedRevision) error {
	computed, err := planDigest(plan)
	if err != nil || computed != plan.Digest {
		return failure("ontology_plan_stale", "retained ontology plan failed its digest check")
	}
	want, err := digest(plan.Principal)
	if err != nil {
		return err
	}
	got, err := digest(by)
	if err != nil || got != want {
		return failure("ontology_plan_stale", "plan belongs to a different principal context")
	}
	meta, r, err := s.reader(ctx, by, nil)
	if err != nil {
		return err
	}
	defer r.Close()
	if meta.StoreID != plan.StoreID || r.Snapshot().Incarnation != plan.Incarnation {
		return failure("ontology_plan_stale", "plan belongs to a different store incarnation")
	}
	src, err := s.source(ctx, by, r, plan.Project)
	if err != nil {
		return err
	}
	if !sameRef(src.guard.Ref, plan.Source.Ref) || src.guard.Revision.String() != plan.Source.Revision.String() || src.guard.BodyHash != plan.Source.BodyHash || src.guard.Digest != plan.Source.Digest {
		return failure("ontology_plan_stale", "protected ontology source changed")
	}
	if err := s.authorizeSource(ctx, by, src); err != nil {
		return err
	}
	if err := s.checkExpect(ctx, by, r, plan.Expect); err != nil {
		return err
	}
	rows, err := (rowSource{s: s, by: by, r: r, current: src}).Inventory(ctx)
	if err != nil {
		return err
	}
	if rows.RowsDigest != plan.Migration.Inventory.RowsDigest || rows.Count.String() != plan.Migration.Inventory.Count.String() {
		return failure("ontology_plan_stale", "complete bead row inventory changed")
	}
	if s.cfg.ValidatingPre == nil {
		return failure("ontology_plan_stale", "validating-pre binding inventory is unavailable")
	}
	manifest, err := s.cfg.ValidatingPre.Inventory(ctx, plan.Migration.Candidate)
	if err != nil {
		return mapped(err)
	}
	manifestDigest, err := ontologymigration.BindingsDigest(manifest.Candidate, manifest.Bindings)
	if err != nil || manifestDigest != manifest.Digest || manifest.Digest != plan.Migration.PreInventory.Digest || manifest.Candidate != plan.Migration.Candidate {
		return failure("ontology_plan_stale", "validating-pre binding inventory changed")
	}
	return s.admit(ctx, by)
}

// A3 permits display-name edits and migration-like definition changes, but
// elements are archived, never removed or reused. Candidate-local compilation
// cannot detect a key that disappeared or was resurrected between revisions.
// Compare the base separately, then each effective project: removing an
// overlay must not silently unarchive a base element in that project.
func validateKeyContinuity(beforeSource service.OntologySource, before ontology.Snapshot, afterSource service.OntologySource, after ontology.Snapshot) error {
	projects := map[string]bool{"": true}
	for project := range beforeSource.Overlays {
		projects[project] = true
	}
	for project := range afterSource.Overlays {
		projects[project] = true
	}
	ordered := make([]string, 0, len(projects))
	for project := range projects {
		ordered = append(ordered, project)
	}
	sort.Strings(ordered)
	for _, project := range ordered {
		old, next := before.Effective(project).Document(), after.Effective(project).Document()
		path := "base"
		if project != "" {
			path = "projects." + project
		}
		for _, err := range []error{
			definitionContinuity(old.Statuses, next.Statuses, func(v ontology.Status) bool { return v.Archived }, path+".statuses"),
			definitionContinuity(old.Workflows, next.Workflows, func(v ontology.Workflow) bool { return v.Archived }, path+".workflows"),
			definitionContinuity(old.Types, next.Types, func(v ontology.Type) bool { return v.Archived }, path+".types"),
			definitionContinuity(old.GateKinds, next.GateKinds, func(v ontology.GateKind) bool { return v.Archived }, path+".gate_kinds"),
		} {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func definitionContinuity[T any](before, after map[string]T, archived func(T) bool, path string) error {
	keys := make([]string, 0, len(before))
	for key := range before {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		next, exists := after[key]
		if !exists {
			return failure("invalid_params", path+"."+key+": elements must be archived, never removed")
		}
		if archived(before[key]) && !archived(next) {
			return failure("invalid_params", path+"."+key+": archived keys cannot be reused")
		}
	}
	return nil
}

func (s *Service) checkExpect(ctx context.Context, by model.PrincipalRef, r driver.Reader, expect []model.Precondition) error {
	for _, condition := range expect {
		if condition.Validate() != nil || !condition.Ref.IsID() || !condition.Ref.Branch.IsZero() {
			return failure("invalid_params", "ontology plan CAS requires resolved branch-zero references")
		}
		rows, err := r.Get(ctx, []model.Ref{condition.Ref})
		if err != nil {
			return mapped(err)
		}
		if len(rows) != 1 {
			return failure("ontology_plan_stale", "expected record is absent")
		}
		project := ""
		if rows[0].Kind == "bead" {
			doc, err := decodeRow(rows[0].Body)
			if err != nil {
				return failure("invalid_params", "expected bead image is invalid")
			}
			project = doc.Project
		}
		if err := s.authorize(ctx, by, condition.Ref, rows[0].Kind, project); err != nil {
			return err
		}
		if condition.Revision != nil {
			if condition.Revision.String() != rows[0].Revision.String() {
				return failure("ontology_plan_stale", "expected record revision changed")
			}
			continue
		}
		if rows[0].Kind != "bead" || condition.Field == "" || condition.Hash == "" {
			return failure("invalid_params", "field CAS is supported only for bead fields; use revision CAS for protected sources")
		}
		doc, err := pipeline.DecodeDocument(rows[0].Body)
		if err != nil {
			return failure("invalid_params", "expected bead image is invalid")
		}
		hash, err := pipeline.FieldHash(doc.Fields[strings.TrimPrefix(condition.Field, "fields.")])
		if err != nil {
			return err
		}
		if hash != condition.Hash {
			return failure("ontology_plan_stale", "expected field hash changed")
		}
	}
	return nil
}
