package service

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// Ontology adapts pure workflow rules to the pipeline's tracked snapshot port.
// Permission is inert without an authorization provider; gate decisions use
// the canonical pure evaluator and remain within the single Writer proposal.
type Ontology struct {
	mu                   sync.RWMutex
	resolve              func(string) ontology.Effective
	now                  func() time.Time
	permissionObserver   TransitionPermissionObserver
	permissionConfigured bool
	// Configured resolvers are never advertised as immutable no-plugin sources.
	defaultOnly bool
}

// TransitionPermissionRequest carries the existing A3 permission guard's
// action and actual target status to a trusted host source. It is never RPC
// input and does not map transition onto an ordinary write permission.
type TransitionPermissionRequest struct {
	Principal            model.PrincipalRef
	Resource             model.ResourceRef
	Project, Type        string
	Method, Action       string
	FromStatus, ToStatus string
}

// TransitionPermissionObservation is one installation/decision observation
// from the same tracked snapshot. An installed chain with no permission
// observation denies; absence of an authorization plugin allows (AZ9).
type TransitionPermissionObservation struct {
	AuthzInstalled bool
	Permission     *bool
}

// TransitionPermissionSource is the distinct capability of an installed
// write authority. Generic modify authorization cannot supply this decision.
type TransitionPermissionSource interface {
	ObserveTransitionPermission(context.Context, TransitionPermissionRequest, port.SnapshotReader) (TransitionPermissionObservation, error)
}

type TransitionPermissionObserver func(context.Context, TransitionPermissionRequest, port.SnapshotReader) (TransitionPermissionObservation, error)

type OntologyOption func(*Ontology)

// WithOntologyClock supplies the host clock for ancestor scheduling checks.
func WithOntologyClock(now func() time.Time) OntologyOption {
	return func(o *Ontology) {
		if now != nil {
			o.now = now
		}
	}
}

// WithTransitionPermissionObserver binds only trusted composition state. The
// source must record installation and authority reads on the supplied reader.
// Explicit nil means authorization is installed but its transition adapter is
// unavailable, so permission guards fail closed. Omitting the option retains
// the explicit no-authorization composition.
func WithTransitionPermissionObserver(observer TransitionPermissionObserver) OntologyOption {
	return func(o *Ontology) {
		o.permissionObserver, o.permissionConfigured = observer, true
	}
}

func NewOntology(resolve func(string) ontology.Effective, options ...OntologyOption) *Ontology {
	defaultOnly := resolve == nil
	if resolve == nil {
		base := ontology.Default()
		resolve = func(string) ontology.Effective { return base }
	}
	o := &Ontology{resolve: resolve, now: time.Now, defaultOnly: defaultOnly}
	for _, option := range options {
		if option != nil {
			option(o)
		}
	}
	return o
}

// Refresh atomically replaces the compiled ontology used by all subsequent
// pipeline and domain decisions. A compiled snapshot owns detached immutable
// maps, so swapping the resolver is sufficient to publish the new runtime
// view without exposing a partially updated ontology.
func (o *Ontology) Refresh(snapshot ontology.Snapshot) {
	o.mu.Lock()
	o.resolve = snapshot.Effective
	o.defaultOnly = false
	o.mu.Unlock()
}

// ImmutableDefault returns the same built-in ontology used by this instance's
// writer and service while it remains in no-plugin mode. A view continuation
// must refuse after Refresh instead of applying new category membership to its
// earlier content snapshot.
func (o *Ontology) ImmutableDefault() (ontology.Effective, bool) {
	if o == nil {
		return ontology.Effective{}, false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if !o.defaultOnly {
		return ontology.Effective{}, false
	}
	return o.resolve(""), true
}

func (o *Ontology) effective(project string) ontology.Effective {
	o.mu.RLock()
	resolve := o.resolve
	o.mu.RUnlock()
	return resolve(project)
}

// A detached JSON proposal represents a missing raw-message image as null.
func hasDocument(raw []byte) bool {
	return len(raw) != 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func violationsError(violations []ontology.Violation) error {
	if len(violations) == 0 {
		return nil
	}
	return &model.Error{Data: model.ErrorData{Code: violations[0].Code, Details: map[string]any{"violations": violations}}}
}

// CloseSlots is pure trusted lowering context, not a permission or guard.
// The Writer tracks the ontology sentinel and validates ordered guards only
// after mutators, against prefix and final images.
func (o *Ontology) CloseSlots(project, beadType string) (string, string, bool) {
	return o.CloseStatuses(project, beadType, nil)
}

// CloseStatuses extends selection without breaking existing omitted-slot hooks.
func (o *Ontology) CloseStatuses(project, beadType string, requested *string) (string, string, bool) {
	e := o.effective(project)
	initial, hasInitial := e.InitialStatus(beadType)
	target, hasTarget := e.LifecycleStatus(beadType, ontology.Close, requested)
	return initial, target, hasInitial && hasTarget
}

// CreateStatus is selection only. Ordered/final validation owns all guards.
func (o *Ontology) CreateStatus(project, beadType string, requested *string) (string, bool) {
	return o.effective(project).CreateStatus(beadType, requested)
}

func requestedStatus(proposal port.ProposedChange, change port.Change) *string {
	if change.Op.Create != nil {
		return change.Op.Create.TargetStatus
	}
	if change.Op.Close != nil {
		return change.Op.Close.TargetStatus
	}
	if proposal.Meta.Method == "bead.close" || proposal.Meta.Method == "bead.reopen" {
		return proposal.Meta.TargetStatus
	}
	return nil
}

func statusResource(resource model.ResourceRef) [3]string {
	return [3]string{resource.Kind, resource.Ref.ID, resource.Ref.Branch.String()}
}

func operationMethod(proposal port.ProposedChange, change port.Change) string {
	if proposal.Meta.Method == "tx.apply" && change.Op.Kind == model.OpClose {
		return "bead.close"
	}
	return proposal.Meta.Method
}

func (o *Ontology) Normalize(ctx context.Context, proposal port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isGateMethod(proposal.Meta.Method) && proposal.Meta.GateAction == "" {
		return nil, failure("invalid_params", "gate method requires trusted gate lowering context")
	}
	patches := []port.Patch{}
	// Atomic images already contain every ordered operation. Only the last
	// close may select their final status; ValidateOrdered separately evaluates
	// every captured prefix, including earlier targets and their guards.
	lastClose := map[[3]string]int{}
	for i, change := range proposal.Changes {
		if change.Op.Kind == model.OpClose {
			lastClose[statusResource(change.Resource)] = i
		}
	}
	for i, change := range proposal.Changes {
		if change.Resource.Kind != "bead" {
			continue
		}
		var memberInput map[string]any
		if change.Op.Create != nil {
			memberInput = change.Op.Create.Fields
		} else if change.Op.Patch != nil {
			memberInput = change.Op.Patch.Fields
		}
		if err := violationsError(ontology.ValidateAwaitMemberInput(memberInput, change.Op.Patch != nil)); err != nil {
			return nil, err
		}
		if !hasDocument(change.After) {
			continue
		}
		if change.Op.Kind == model.OpClose && lastClose[statusResource(change.Resource)] != i {
			continue
		}
		doc, err := decodeDocument(change.After)
		if err != nil {
			return nil, err
		}
		if patch, selected := normalizeAwaitWaiters(change, doc.Fields); selected {
			doc.Fields["waiters"] = []any{}
			patches = append(patches, patch)
		}
		if proposal.Meta.GateAction != "" {
			if len(proposal.Changes) != 1 || !isGateAction(proposal.Meta.GateAction) || proposal.Meta.Method != "gate."+proposal.Meta.GateAction {
				return nil, failure("invalid_params", "invalid gate lowering context")
			}
			if !hasDocument(change.Before) {
				return nil, failure("invalid_params", "gate operation requires an existing bead")
			}
			before, err := decodeDocument(change.Before)
			if err != nil {
				return nil, failure("internal_error", "invalid prior gate bead")
			}
			for _, fields := range []map[string]any{before.Fields, doc.Fields} {
				for name := range fields {
					if isGateField(name) && (!reflect.DeepEqual(before.Fields[name], doc.Fields[name]) || change.SetBy["fields."+name] != "") {
						return nil, failure("field_owned", "gate fields are owned by core")
					}
				}
			}
			patch, err := o.normalizeGate(proposal, change.Resource, doc)
			if err != nil {
				return nil, err
			}
			patches = append(patches, patch)
			continue
		}
		before := state(doc)
		after := before
		e := o.effective(doc.Project)
		if !hasDocument(change.Before) {
			var violations []ontology.Violation
			after, violations = ontology.Defaults(e, before)
			if err := violationsError(violations); err != nil {
				return nil, err
			}
		} else if slot, lifecycle := ontology.SlotForMethod(operationMethod(proposal, change)); lifecycle && !proposal.SameClaim && (change.Op.Kind == model.OpPatch || change.Op.Kind == model.OpClose) {
			target, ok := e.LifecycleStatus(doc.Type, slot, requestedStatus(proposal, change))
			if !ok {
				return nil, failure("invalid_params", "lifecycle slot is unavailable for this type")
			}
			after.Status = target
		}
		paths := map[string]json.RawMessage{}
		if after.Status != before.Status {
			raw, err := json.Marshal(after.Status)
			if err != nil {
				return nil, err
			}
			paths["fields.status"] = raw
		}
		for name, value := range after.Fields {
			old, exists := before.Fields[name]
			if !exists || !reflect.DeepEqual(old, value) {
				raw, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				paths["fields."+name] = raw
			}
		}
		if len(paths) != 0 {
			patches = append(patches, port.Patch{Resource: change.Resource, Paths: paths})
		}
	}
	return patches, nil
}

const (
	gateKindField          = "gate_kind"
	gateStateField         = "gate_state"
	gateApprovalsField     = "gate_approvals"
	gateApprovalCountField = "gate_approval_count"
	gateDetailsField       = "gate_details"
)

func isGateAction(action string) bool {
	return action == "approve" || action == "report"
}

func isGateMethod(method string) bool {
	return method == "gate.approve" || method == "gate.report"
}

func isGateField(name string) bool {
	switch name {
	case gateKindField, gateStateField, gateApprovalsField, gateApprovalCountField, gateDetailsField:
		return true
	default:
		return false
	}
}

func gateLifecycleMethod(doc pipeline.Document) string {
	state, _ := doc.Fields[gateStateField].(string)
	switch state {
	case ontology.GateStateResolved:
		return "bead.close"
	case ontology.GateStateFailed:
		return "bead.fail"
	default:
		return ""
	}
}

func decodeGateState(doc pipeline.Document) (ontology.GateState, error) {
	kind, ok := doc.Fields[gateKindField].(string)
	if !ok || kind == "" {
		return ontology.GateState{}, failure("invalid_params", "gate_kind must be a nonempty string")
	}
	state := ontology.GateState{Kind: kind, Type: doc.Type, State: ontology.GateStatePending, Approvals: []ontology.GateApproval{}}
	if raw, present := doc.Fields[gateStateField]; present {
		value, ok := raw.(string)
		if !ok {
			return ontology.GateState{}, failure("invalid_params", "gate_state must be a string")
		}
		state.State = value
	}
	if raw, present := doc.Fields[gateApprovalsField]; present {
		data, err := json.Marshal(raw)
		if err != nil {
			return ontology.GateState{}, failure("invalid_params", "gate_approvals must be an array")
		}
		if err := json.Unmarshal(data, &state.Approvals); err != nil || state.Approvals == nil {
			return ontology.GateState{}, failure("invalid_params", "gate_approvals must be an array")
		}
	}
	return state, nil
}

func (o *Ontology) normalizeGate(proposal port.ProposedChange, resource model.ResourceRef, doc pipeline.Document) (port.Patch, error) {
	state, err := decodeGateState(doc)
	if err != nil {
		return port.Patch{}, err
	}
	effective := o.effective(doc.Project)
	var decision ontology.GateDecision
	switch proposal.Meta.GateAction {
	case "approve":
		decision, err = effective.ApproveGate(state, proposal.Principal, proposal.Principal.Kind != "anonymous")
		if err == nil && decision.Added && decision.Approval != nil {
			decision.Approval.Token = proposal.RequestID
		}
	case "report":
		decision, err = effective.ReportGate(state, ontology.Outcome(proposal.Meta.GateState))
	default:
		return port.Patch{}, failure("invalid_params", "unknown gate action")
	}
	if err != nil {
		return port.Patch{}, err
	}
	approvals := append([]ontology.GateApproval{}, state.Approvals...)
	if decision.Added {
		if decision.Approval == nil {
			return port.Patch{}, failure("internal_error", "gate approval decision omitted its approval")
		}
		approvals = append(approvals, *decision.Approval)
	}
	paths := map[string]json.RawMessage{}
	for name, value := range map[string]any{gateStateField: decision.State, gateApprovalsField: approvals, gateApprovalCountField: decision.Approvals} {
		data, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return port.Patch{}, marshalErr
		}
		paths["fields."+name] = data
	}
	if proposal.Meta.GateAction == "report" && proposal.Meta.GateDetails != nil {
		data, marshalErr := json.Marshal(proposal.Meta.GateDetails)
		if marshalErr != nil {
			return port.Patch{}, failure("invalid_params", "gate details must be JSON")
		}
		paths["fields."+gateDetailsField] = data
	}
	if decision.Transition != nil {
		data, marshalErr := json.Marshal(decision.Transition.Status)
		if marshalErr != nil {
			return port.Patch{}, marshalErr
		}
		paths["fields.status"] = data
	}
	return port.Patch{Resource: resource, Paths: paths}, nil
}

func (o *Ontology) Validate(ctx context.Context, proposal port.ProposedChange, reader port.SnapshotReader) error {
	return o.validate(ctx, proposal, reader, nil)
}

func (o *Ontology) validate(ctx context.Context, proposal port.ProposedChange, reader port.SnapshotReader, prefix map[string]storedBead) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	method := proposal.Meta.Method
	if proposal.SameClaim {
		// Core proved the same live holder before callbacks. There is no slot
		// transition, but final schema, ownership and status-touch checks still
		// apply to every mutator change through the ordinary update rules.
		if method != "bead.claim" && method != "bead.claimNext" {
			return failure("internal_error", "same-claim marker requires a claim")
		}
		method = "bead.update"
	}
	settlement := o.closeSettlement(proposal)
	// All changes for a new resource share its absent committed pre-image.
	// Preserve its typed create selector across later ordinary operations; a
	// different resource or a generic fields.status patch supplies no authority.
	createTargets := map[[3]string]*string{}
	_, lifecycle := ontology.SlotForMethod(proposal.Meta.Method)
	for _, change := range proposal.Changes {
		if change.Op.Create != nil {
			createTargets[statusResource(change.Resource)] = change.Op.Create.TargetStatus
		}
		if change.Op.Kind == model.OpClose || change.Op.Create != nil && change.Op.Create.TargetStatus != nil {
			lifecycle = true
		}
	}
	if !lifecycle && isGateMethod(proposal.Meta.Method) {
		// A resolved gate lowers to the close/fail slot inside Normalize. The
		// public method remains gate.* so the service must still collect the
		// graph facts required by that terminal transition.
		for _, change := range proposal.Changes {
			if !hasDocument(change.After) {
				continue
			}
			doc, err := decodeDocument(change.After)
			if err != nil {
				return failure("internal_error", "invalid proposed gate bead")
			}
			if gateLifecycleMethod(doc) != "" {
				lifecycle = true
				break
			}
		}
	}
	var graph map[string]storedBead
	graphNeeded, err := hierarchyGraphNeeded(proposal)
	if err != nil {
		// Decode malformed proposal images before any graph read. This keeps
		// the refusal typed and no-write even when the pinned store is large or
		// unavailable for a full guard scan.
		return err
	}
	if lifecycle || graphNeeded {
		graph = prefix
		if graph == nil {
			var complete bool
			graph, complete, err = indexedGuardGraph(ctx, proposal, reader)
			if err != nil {
				return err
			}
			if !complete {
				graph, err = scanBeads(ctx, reader)
				if err != nil {
					return err
				}
			}
		}
		// Guards evaluate the whole atomic post-image, including dependencies
		// changed in the same changeset. The query tracks graph/blocking, so a
		// concurrent insertion cannot turn an observed absence into an allow.
		for _, change := range proposal.Changes {
			if change.Resource.Kind != "bead" {
				continue
			}
			if !hasDocument(change.After) {
				delete(graph, change.Resource.Ref.ID)
				continue
			}
			doc, err := decodeDocument(change.After)
			if err != nil {
				return failure("internal_error", "invalid proposed bead")
			}
			graph[doc.ID] = storedBead{doc: doc}
		}
	}
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" || !hasDocument(change.After) {
			continue
		}
		doc, err := decodeDocument(change.After)
		if err != nil {
			return err
		}
		after := state(doc)
		var before *ontology.BeadState
		if hasDocument(change.Before) {
			old, err := decodeDocument(change.Before)
			if err != nil {
				return failure("internal_error", "invalid prior bead")
			}
			v := state(old)
			before = &v
		}
		facts := ontology.GuardFacts{SetBy: change.SetBy, TargetStatus: requestedStatus(proposal, change)}
		if before == nil && change.Op.Create == nil && change.Op.Close == nil {
			facts.TargetStatus = createTargets[statusResource(change.Resource)]
		}
		for path := range change.SetBy {
			facts.Touched = append(facts.Touched, path)
		}
		// Include the request's original paths even if a patch writes the same
		// value. Final-image comparison alone would let direct status edits pass.
		if change.Op.Patch != nil {
			for name := range change.Op.Patch.Fields {
				if before == nil && name == "status" {
					return failure("ontology_use_slot_verb", "status must be changed through a lifecycle verb")
				}
				facts.Touched = append(facts.Touched, "fields."+name)
			}
		}
		if graph != nil {
			noBlockers, err := o.noOpenBlockers(doc, graph)
			if err == nil && noBlockers && (method == "bead.claim" || method == "bead.claimNext") {
				noBlockers, err = o.readyAncestors(doc, graph)
			}
			if err != nil {
				return err
			}
			noChildren := o.noOpenChildren(doc, graph)
			if operationMethod(proposal, change) == "bead.close" && workflowHasOpenChildren(ctx, doc.ID) {
				noChildren = false
			}
			facts.NoOpenBlockers, facts.NoOpenChildren = &noBlockers, &noChildren
			facts.Evidence = map[string]bool{}
			for _, attachment := range doc.Attachments {
				if attachment.URI != "" || attachment.ContentRef != "" {
					facts.Evidence[attachment.Kind] = true
				}
			}
		}
		// The slot patch and optional reason comment share the final target
		// post-image; neither introduces an independent status transition.
		effectiveMethod := method
		closeSettlement := settlement
		if change.Op.Kind == model.OpClose {
			if proposal.Meta.Method != "tx.apply" {
				return failure("invalid_params", "close operation requires tx.apply")
			}
			effectiveMethod = "bead.close"
			single := proposal
			single.Changes = []port.Change{change}
			closeSettlement = o.closeSettlement(single)
		}
		if err := o.observeTransitionGuards(ctx, proposal, change, before, after, effectiveMethod, closeSettlement, reader, &facts); err != nil {
			return err
		}
		var violations []ontology.Violation
		if closeSettlement {
			violations = ontology.ValidateCloseSettlement(o.effective(doc.Project), before, &after, facts)
		} else {
			violations = ontology.Validate(o.effective(doc.Project), before, &after, effectiveMethod, facts)
		}
		if err := violationsError(violations); err != nil {
			return err
		}
	}
	return o.validateHierarchyParents(proposal, graph)
}

func (o *Ontology) observeTransitionGuards(ctx context.Context, proposal port.ProposedChange, change port.Change, before *ontology.BeadState, after ontology.BeadState, method string, settlement bool, reader port.SnapshotReader, facts *ontology.GuardFacts) error {
	guardMethod := method
	effective := o.effective(after.Project)
	fromStatus := ""
	if before == nil {
		// A typed noninitial create evaluates a real initial-to-target
		// transition despite having no stored pre-image. Initial create has
		// no transition and keeps its existing creation permission semantics.
		initial, ok := effective.InitialStatus(after.Type)
		if !ok || facts.TargetStatus == nil || after.Status == initial {
			return nil
		}
		fromStatus = initial
	} else {
		fromStatus = before.Status
		if isGateMethod(guardMethod) {
			guardMethod = gateLifecycleMethod(pipeline.Document{Fields: after.Fields})
		}
		if _, lifecycle := ontology.SlotForMethod(guardMethod); !lifecycle {
			return nil
		}
	}
	guards, _ := effective.Guards(after.Type, fromStatus, after.Status)
	if settlement {
		// Ordinary Guards deliberately rejects same-status transitions. Close
		// settlement instead evaluates every applicable broad/source rule, as
		// ValidateCloseSettlement does, without inventing a transition.
		document := effective.Document()
		workflow := document.Workflows[document.Types[after.Type].Workflow]
		for _, rule := range workflow.Transitions {
			if rule.To != after.Status {
				continue
			}
			matches := len(rule.From) == 0
			for _, source := range rule.From {
				matches = matches || source == fromStatus
			}
			if matches {
				guards = append(guards, rule.Guards...)
			}
		}
	}
	permission := false
	for _, guard := range guards {
		switch guard.Kind {
		case ontology.Permission:
			permission = true
		}
	}
	if !permission {
		return nil
	}
	o.mu.RLock()
	observer, configured := o.permissionObserver, o.permissionConfigured
	o.mu.RUnlock()
	if !configured {
		return nil
	}
	facts.AuthzInstalled = true
	if observer == nil {
		return nil
	}
	if reader == nil {
		return failure("authz_unavailable", "transition permission snapshot is unavailable")
	}
	observation, err := observer(ctx, TransitionPermissionRequest{
		Principal: proposal.Principal, Resource: change.Resource,
		Project: after.Project, Type: after.Type, Method: guardMethod, Action: "transition",
		FromStatus: fromStatus, ToStatus: after.Status,
	}, reader)
	if err != nil {
		return err
	}
	facts.AuthzInstalled = observation.AuthzInstalled
	if observation.Permission != nil {
		permission := *observation.Permission
		facts.Permission = &permission
	}
	return nil
}

// Recognize only the named empty slot patch and optional same-target reason.
// Before comes from the pipeline's guarded read; mutators cannot supply it.
// The core still owns lease admission, CAS and final change/no-op detection.
func (o *Ontology) closeSettlement(p port.ProposedChange) bool {
	if p.Meta.Method == "tx.apply" && len(p.Changes) == 1 && p.Changes[0].Op.Kind == model.OpClose && p.Changes[0].Op.Close != nil {
		c := p.Changes[0]
		if c.Resource.Kind != "bead" || !hasDocument(c.Before) {
			return false
		}
		before, err := decodeDocument(c.Before)
		if err != nil {
			return false
		}
		target, ok := o.effective(before.Project).LifecycleStatus(before.Type, ontology.Close, requestedStatus(p, c))
		return ok && state(before).Status == target
	}
	if p.Meta.Method != "bead.close" || len(p.Changes) < 1 || len(p.Changes) > 2 {
		return false
	}
	first := p.Changes[0]
	if first.Resource.Kind != "bead" || first.Op.Kind != model.OpPatch || first.Op.Patch == nil || len(first.Op.Patch.Fields) != 0 || !hasDocument(first.Before) {
		return false
	}
	before, err := decodeDocument(first.Before)
	if err != nil {
		return false
	}
	target, ok := o.effective(before.Project).LifecycleStatus(before.Type, ontology.Close, requestedStatus(p, first))
	if !ok || state(before).Status != target {
		return false
	}
	if len(p.Changes) == 2 {
		reason := p.Changes[1]
		if reason.Resource.Kind != "bead" || reason.Resource.Ref.ID != first.Resource.Ref.ID || reason.Resource.Ref.Branch.String() != first.Resource.Ref.Branch.String() || reason.Op.Kind != model.OpComment || reason.Op.Comment == nil {
			return false
		}
	}
	return true
}

var _ port.Ontology = (*Ontology)(nil)
