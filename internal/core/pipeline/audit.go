package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const auditScanLimit = 20000

type auditDependent struct {
	ref      model.Ref
	revision model.Revision
}

type auditTransition struct {
	decision   port.ConditionalDecision
	dependents []auditDependent
}

type auditPlan struct {
	context     port.EventContext
	transitions map[string]auditTransition
}

func statusChanged(p *pending) bool {
	if p == nil || p.deleted || len(p.before) == 0 {
		return false
	}
	before, err := DecodeDocument(p.before)
	return err == nil && !reflect.DeepEqual(before.Fields["status"], p.doc.Fields["status"])
}

// prepareAudit runs after mutators, ontology, claims and actual-change
// detection. Every observation joins the existing sorted commit guard. No
// callback or live ontology lookup runs after locks are acquired.
func (w *Writer) prepareAudit(ctx context.Context, p *prepared) error {
	gate := isGateMethod(p.proposal.Meta.Method)
	// A close's optional reason is the last operation on its coalesced image.
	// Pin the lifecycle event instead of letting legacy projection classify
	// that final comment as the whole transition. Same-status comments retain
	// the legacy path, and conditional notices still require the audit source.
	ordinaryClose := p.proposal.Meta.Method == "bead.close" && len(p.proposal.Changes) == 2 && statusChanged(p.writes[p.proposal.Changes[0].Resource.Ref.ID])
	if len(p.changed) == 0 || (!w.cfg.auditSet && p.release == nil && !gate && !ordinaryClose && len(p.closes) == 0) {
		return nil
	}
	coreRecovery := p.release != nil && p.release.reclaim
	if p.release != nil && !coreRecovery && w.cfg.authorizer != nil && w.cfg.eventContext == nil {
		return failure("invalid_params", "claim release requires trusted event context for an installed authorizer")
	}
	if gate && w.cfg.authorizer != nil && w.cfg.eventContext == nil {
		return failure("invalid_params", "gate events require trusted event context for an installed authorizer")
	}
	p.audit = &auditPlan{context: port.EventContext{Cause: model.EventCause{Root: p.proposal.Meta.IdempotencyKey, Depth: model.Natural64(0), Chain: []string{}}, Chain: []string{}}, transitions: map[string]auditTransition{}}
	if w.cfg.eventContext != nil && !coreRecovery {
		proposal, err := clone(p.proposal)
		if err != nil {
			return err
		}
		value, err := invoke(ctx, 500*time.Millisecond, func(call context.Context) (port.EventContext, error) {
			return w.cfg.eventContext.EventContext(call, proposal, p.snapshot.child())
		})
		if err != nil {
			return err
		}
		p.audit.context, err = clone(value)
		if err != nil {
			return failure("invalid_params", "invalid trusted event context")
		}
	}
	provenance := model.EventProvenance{Method: p.proposal.Meta.Method, SetBy: map[string]string{}, Cause: p.audit.context.Cause, Chain: p.audit.context.Chain}
	if err := provenance.Validate(); err != nil {
		return failure("invalid_params", "invalid trusted event context: "+err.Error())
	}
	if p.release != nil {
		// Release/reclaim is itself the durable audit boundary. It does not run
		// conditional blocker analysis, but it still gets a complete descriptor.
		return nil
	}
	if !w.cfg.auditSet {
		return nil
	}
	blockers := map[string]bool{}
	for id, pending := range p.writes {
		if p.changed[id] && statusChanged(pending) {
			blockers[id] = true
		}
	}
	if len(blockers) == 0 {
		return nil
	}
	graph, err := p.auditGraph(ctx)
	if err != nil {
		return err
	}
	edges := map[string][]auditDependent{}
	for id, row := range graph {
		seen := map[string]bool{}
		for _, link := range row.doc.Links {
			if link.TargetKind == model.LinkTargetExternal || link.Kind != "conditional-blocks" || !blockers[link.To.ID] {
				continue
			}
			if !link.To.IsID() || !link.To.Branch.IsZero() || seen[link.To.ID] || id == link.To.ID {
				return failure("invalid_params", "invalid affected conditional edge")
			}
			seen[link.To.ID] = true
			// Query guards the predicate, including misses/inserts. Point reads
			// also guard the exact membership image and its source revision.
			if _, err := p.snapshot.Get(ctx, []model.Ref{row.ref}); err != nil {
				return err
			}
			edges[link.To.ID] = append(edges[link.To.ID], auditDependent{row.ref, row.revision})
		}
	}
	count := len(p.changed)
	ids := make([]string, 0, len(edges))
	for id := range edges {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		pending := p.writes[id]
		after, err := json.Marshal(pending.doc)
		if err != nil {
			return err
		}
		change, err := clone(port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: pending.ref}, Op: model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: pending.ref, Fields: map[string]any{}}}, Before: pending.before, After: after, SetBy: cloneMap(pending.setBy)})
		if err != nil {
			return err
		}
		decision, err := invoke(ctx, 500*time.Millisecond, func(call context.Context) (port.ConditionalDecision, error) {
			return w.cfg.audit.DecideConditional(call, change, p.snapshot.child())
		})
		if err != nil {
			return err
		}
		decision, err = clone(decision)
		if err != nil {
			return failure("invalid_params", "invalid conditional ontology decision")
		}
		if err := p.checkDecision(ctx, pending, decision); err != nil {
			return err
		}
		// A false condition on an open state is ordinary readiness, not an
		// outcome notice. Completed/failure satisfies and emits no notice.
		if !decision.After.Terminal || decision.After.Satisfied {
			continue
		}
		method := p.lifecycleMethod(pending)
		if method != "bead.close" && method != "bead.cancel" || method == "bead.close" && (decision.After.Category != "completed" || decision.After.Outcome != "success") || method == "bead.cancel" && decision.After.Category != "canceled" {
			return failure("invalid_params", "unsatisfied terminal decision requires its trusted close or cancel slot")
		}
		dependents := edges[id]
		sort.Slice(dependents, func(i, j int) bool { return dependents[i].ref.ID < dependents[j].ref.ID })
		count += len(dependents)
		if count > model.MaxChangesetEvents {
			return failure("scope_too_large", "complete atomic event plan exceeds 1000 descriptors")
		}
		p.audit.transitions[id] = auditTransition{decision, dependents}
	}
	return nil
}

type auditRow struct {
	ref      model.Ref
	revision model.Revision
	doc      Document
}

func (p *prepared) auditGraph(ctx context.Context) (map[string]auditRow, error) {
	graph := map[string]auditRow{}
	var cursor *driver.Cursor
	for {
		page, err := p.snapshot.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		for _, record := range page.Items {
			doc, err := DecodeDocument(record.Body)
			if err != nil || doc.ID != record.Ref.ID || !record.Ref.IsID() || !record.Ref.Branch.IsZero() || record.Kind != "bead" {
				return nil, failure("internal_error", "invalid bead in conditional edge snapshot")
			}
			if _, exists := graph[doc.ID]; exists {
				return nil, failure("internal_error", "conditional edge scan did not advance")
			}
			graph[doc.ID] = auditRow{record.Ref, record.Revision, doc}
			if len(graph) > auditScanLimit {
				return nil, failure("scope_too_large", "conditional edge scan exceeds 20000 beads")
			}
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			return nil, failure("internal_error", "conditional edge scan did not advance")
		}
		cursor = page.NextCursor
	}
	for id, pending := range p.writes {
		if pending.deleted {
			delete(graph, id)
		} else {
			graph[id] = auditRow{pending.ref, pending.revision, pending.doc}
		}
	}
	if len(graph) > auditScanLimit {
		return nil, failure("scope_too_large", "final conditional edge scan exceeds 20000 beads")
	}
	return graph, nil
}

func (p *prepared) checkDecision(ctx context.Context, pending *pending, d port.ConditionalDecision) error {
	before, err := DecodeDocument(pending.before)
	if err != nil {
		return err
	}
	if d.Before.Status != before.Fields["status"] || d.After.Status != pending.doc.Fields["status"] {
		return failure("invalid_params", "conditional decision does not match final blocker status images")
	}
	if before.Project != pending.doc.Project || before.Type != pending.doc.Type || (d.Ontology.Project == nil) != (pending.doc.Project == "") || d.Ontology.Project != nil && d.Ontology.Project.ID != pending.doc.Project {
		return failure("invalid_params", "conditional decision must use the blocker's own unchanged project and type")
	}
	rows, err := p.snapshot.Get(ctx, []model.Ref{d.Ontology.Ref})
	if err != nil {
		return err
	}
	if !protectedName(d.Ontology.Ref.ID) || len(rows) != 1 || rows[0].Ref.ID != d.Ontology.Ref.ID || rows[0].Kind != "ontology" || rows[0].Revision.String() != d.Ontology.Revision.String() {
		return failure("invalid_params", "conditional ontology pin does not identify its tracked source")
	}
	return nil
}

func (p *prepared) events(revision model.Revision, changes []model.ChangeRecord) ([]model.EventDescriptor, []string, error) {
	if p.audit == nil {
		return nil, nil, nil
	}
	events := make([]model.EventDescriptor, 0, len(changes))
	for index, change := range changes {
		pending := p.writes[change.Ref.ID]
		kind, err := p.ordinaryEvent(pending, change.Op)
		if err != nil {
			return nil, nil, err
		}
		slot := ""
		if p.release == nil {
			if canonical, ok := ontology.SlotForMethod(p.lifecycleMethod(pending)); ok {
				slot = string(canonical)
			}
		}
		provenance := model.EventProvenance{Method: p.proposal.Meta.Method, Slot: slot, SetBy: cloneMap(pending.setBy), Cause: p.audit.context.Cause, Chain: p.audit.context.Chain}
		resourceKind := pending.kind
		if resourceKind == "" {
			resourceKind = "bead"
		}
		events = append(events, model.EventDescriptor{Index: model.Natural64(uint64(len(events))), ChangeIndex: model.Natural64(uint64(index)), Type: kind, Resource: model.AuditResource{Kind: resourceKind, Ref: change.Ref}, Provenance: provenance})
		gate := model.DurableEventType("")
		switch p.proposal.Meta.Method {
		case "gate.approve":
			if gateApprovalAdded(pending) {
				gate = model.EventGateApproved
			}
		case "gate.report":
			gate = model.EventGateReported
		}
		if gate != "" {
			// A resolved gate produces the workflow slot event first and the
			// gate event second, both anchored to the same atomic bead image.
			// A pending approval has no slot transition, so its gate event is
			// the single ordinary event for the change.
			if !statusChanged(pending) {
				events[len(events)-1].Type = gate
			} else {
				events = append(events, model.EventDescriptor{Index: model.Natural64(uint64(len(events))), ChangeIndex: model.Natural64(uint64(index)), Type: gate, Resource: model.AuditResource{Kind: "bead", Ref: change.Ref}, Provenance: provenance})
			}
		}
		transition := p.audit.transitions[change.Ref.ID]
		for _, dependent := range transition.dependents {
			at := dependent.revision
			if p.changed[dependent.ref.ID] {
				at = revision
			}
			details := &model.ConditionUnmetDetails{Dependent: model.EventImage{Ref: dependent.ref, Revision: at}, BlockerBefore: model.EventImage{Ref: change.Ref, Revision: change.FromRevision}, BlockerAfter: model.EventImage{Ref: change.Ref, Revision: revision}, Edge: model.ConditionalEdge{Dependent: dependent.ref, Blocker: change.Ref, Kind: "conditional-blocks", Revision: at}, Ontology: transition.decision.Ontology, Before: transition.decision.Before, After: transition.decision.After}
			events = append(events, model.EventDescriptor{Index: model.Natural64(uint64(len(events))), ChangeIndex: model.Natural64(uint64(index)), Type: model.EventDependencyConditionUnmet, Resource: model.AuditResource{Kind: "bead", Ref: dependent.ref}, Provenance: provenance, Details: details})
		}
	}
	entry := model.Changeset{Revision: revision, Changes: changes, Chain: p.audit.context.Chain, Events: events}
	if err := entry.ValidateEvents(); err != nil {
		return nil, nil, failure("invalid_params", "invalid complete event plan: "+err.Error())
	}
	return model.CloneEventDescriptors(events), append([]string{}, p.audit.context.Chain...), nil
}

func (p *prepared) ordinaryEvent(pending *pending, op model.OpKind) (model.DurableEventType, error) {
	if p.release != nil {
		if p.release.reclaim {
			return model.EventBeadLeaseReclaimed, nil
		}
		return model.EventBeadReleased, nil
	}
	if statusChanged(pending) {
		method := p.lifecycleMethod(pending)
		if event, ok := map[string]model.DurableEventType{"bead.claim": model.EventBeadClaimed, "bead.claimNext": model.EventBeadClaimed, "bead.close": model.EventBeadClosed, "bead.fail": model.EventBeadFailed, "bead.cancel": model.EventBeadCanceled, "bead.reopen": model.EventBeadReopened, "bead.defer": model.EventBeadDeferred}[method]; ok {
			return event, nil
		}
	}
	if pending != nil && pending.kind == "memory" {
		if op == model.OpDelete {
			return model.EventMemoryDeleted, nil
		}
		if op == model.OpCreate || op == model.OpPatch || op == model.OpSetMeta {
			return model.EventMemorySaved, nil
		}
		return "", fmt.Errorf("unsupported memory event operation %q", op)
	}
	event, ok := map[model.OpKind]model.DurableEventType{model.OpCreate: model.EventBeadCreated, model.OpPatch: model.EventBeadUpdated, model.OpSetMeta: model.EventBeadUpdated, model.OpReparent: model.EventBeadReparented, model.OpComment: model.EventBeadCommented, model.OpLink: model.EventBeadLinked, model.OpUnlink: model.EventBeadUnlinked, model.OpDelete: model.EventBeadDeleted, model.OpClose: model.EventBeadCommented}[op]
	if !ok {
		return "", fmt.Errorf("unsupported ordinary event operation %q", op)
	}
	return event, nil
}
