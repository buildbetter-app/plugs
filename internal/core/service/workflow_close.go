package service

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type workflowCloseFactsKey struct{}

// These observations belong to one service Apply. The workflow guard joins
// their exact checkpoint expectation to publication; clients cannot supply
// them. Source preparation replaces them with its staged final checkpoint.
type workflowCloseFacts struct {
	children map[string]bool
}

func withWorkflowCloseFacts(ctx context.Context) context.Context {
	return context.WithValue(ctx, workflowCloseFactsKey{}, &workflowCloseFacts{})
}

func setWorkflowCloseFacts(ctx context.Context, children map[string]bool) {
	facts, _ := ctx.Value(workflowCloseFactsKey{}).(*workflowCloseFacts)
	if facts == nil {
		return
	}
	facts.children = children
}

func workflowHasOpenChildren(ctx context.Context, id string) bool {
	facts, _ := ctx.Value(workflowCloseFactsKey{}).(*workflowCloseFacts)
	if facts == nil {
		return false
	}
	return facts.children[id]
}

func (s *Service) sourceOrdinaryCloseGuard(ctx context.Context, staged *workflowplugin.Provider, by model.PrincipalRef, doc pipeline.Document, op model.Op, meta model.WriteMeta) error {
	blocked, children, _, err := staged.OrdinaryCloseSnapshot(ctx, by, []string{doc.ID}, meta)
	if err != nil {
		return err
	}
	if blocked[doc.ID] {
		return failure("ontology_guard_failed", "an active source dependency is not satisfied")
	}
	return s.ontology.validateWorkflowCloseChildren(doc, closeRequestStatus(op, meta), children[doc.ID])
}

// Source operations run against successive unpublished provider images. Check
// the child fact at each close prefix as well as in final ontology validation.
// Reuse the ontology evaluator so only a declared no_open_children guard can
// refuse: a guard-free transition remains valid. Other guards retain their
// normal complete, post-mutator evaluation by the Writer.
func (o *Ontology) validateWorkflowCloseChildren(doc pipeline.Document, requested *string, open bool) error {
	if !open {
		return nil
	}
	e := o.effective(doc.Project)
	target, ok := e.LifecycleStatus(doc.Type, ontology.Close, requested)
	if !ok {
		return nil // The ordinary ontology phase reports invalid selection.
	}
	before := state(doc)
	after := before
	after.Status = target
	after.Fields = copyFields(before.Fields)
	after.Fields["status"] = target
	clear := false
	facts := ontology.GuardFacts{TargetStatus: requested, NoOpenChildren: &clear}
	var violations []ontology.Violation
	if before.Status == target {
		violations = ontology.ValidateCloseSettlement(e, &before, &after, facts)
	} else {
		violations = ontology.Validate(e, &before, &after, "bead.close", facts)
	}
	for _, v := range violations {
		if v.Guard == ontology.NoOpenChildren {
			return violationsError([]ontology.Violation{v})
		}
	}
	return nil
}

func closeRequestStatus(op model.Op, meta model.WriteMeta) *string {
	if op.Kind == model.OpClose && op.Close != nil {
		return op.Close.TargetStatus
	}
	return meta.TargetStatus
}
