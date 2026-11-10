package ontology

import (
	"bytes"
	"encoding/json"
	"math/big"
	"reflect"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const (
	gateKindField          = "gate_kind"
	gateStateField         = "gate_state"
	gateApprovalsField     = "gate_approvals"
	gateApprovalCountField = "gate_approval_count"
	gateDetailsField       = "gate_details"
)

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

// SlotForMethod recognizes only named lifecycle verbs. A caller-controlled
// status patch is never sufficient authority to choose a transition.
func SlotForMethod(method string) (Slot, bool) {
	switch method {
	case "bead.claim", "bead.claimNext":
		return Claim, true
	case "bead.close":
		return Close, true
	case "bead.reopen":
		return Reopen, true
	case "bead.fail":
		return Fail, true
	case "bead.cancel":
		return Cancel, true
	case "bead.defer":
		return Defer, true
	default:
		return "", false
	}
}

func methodForSlot(slot Slot) string {
	switch slot {
	case Claim:
		return "bead.claim"
	case Close:
		return "bead.close"
	case Reopen:
		return "bead.reopen"
	case Fail:
		return "bead.fail"
	case Cancel:
		return "bead.cancel"
	case Defer:
		return "bead.defer"
	default:
		return ""
	}
}

// SlotStatus is the lowering seam: service obtains the target without knowing
// concrete status keys, then submits a patch through the one Writer.Apply with
// its trusted WriteMeta.Method. Validate checks the same slot after mutators.
func (e Effective) SlotStatus(typ string, slot Slot) (string, bool) {
	t, ok := e.document.Types[typ]
	if !ok || t.Archived {
		return "", false
	}
	w, ok := e.document.Workflows[t.Workflow]
	target, exists := w.Slots[slot]
	s, found := e.document.Statuses[target]
	if !ok || w.Archived || !exists || !found || s.Archived {
		return "", false
	}
	return target, true
}

// Defaults copies a proposal and supplies only absent defaults. Plugin-owned
// fields must be set by their owning mutator, not by a generic default.
func Defaults(e Effective, state BeadState) (BeadState, []Violation) {
	copy, err := copyState(state)
	if err != nil {
		return BeadState{}, invalid("fields", "fields must contain JSON values")
	}
	t, ok := e.document.Types[copy.Type]
	if !ok || t.Archived {
		return BeadState{}, invalid("type", "unknown or archived type")
	}
	if copy.Status == "" {
		var ok bool
		copy.Status, ok = e.InitialStatus(copy.Type)
		if !ok {
			return BeadState{}, invalid("type", "initial status unavailable")
		}
	}
	if copy.Fields == nil {
		copy.Fields = map[string]any{}
	}
	if _, present := copy.Fields["priority"]; !present {
		copy.Fields["priority"] = DefaultPriority
	}
	if _, present := copy.Fields["template"]; !present {
		copy.Fields["template"] = copy.Template
	}
	for name, field := range t.Fields {
		if _, exists := copy.Fields[name]; !exists && field.Default != nil && field.Owner == "" {
			copy.Fields[name] = field.Default
		}
	}
	// Defaults from the compiled snapshot must not escape as mutable aliases.
	copy, err = copyState(copy)
	if err != nil {
		return BeadState{}, invalid("fields", "invalid defaults")
	}
	return copy, nil
}

// Transition lowers and evaluates a named slot without changing its input.
// Pipeline callers can use SlotStatus before mutators and Validate afterward.
func Transition(e Effective, before BeadState, slot Slot, facts GuardFacts) (BeadState, []Violation) {
	target, ok := e.SlotStatus(before.Type, slot)
	if !ok {
		return BeadState{}, invalid("status", "lifecycle slot is unavailable")
	}
	after, err := copyState(before)
	if err != nil {
		return BeadState{}, invalid("fields", "fields must contain JSON values")
	}
	after.Status = target
	violations := Validate(e, &before, &after, methodForSlot(slot), facts)
	if len(violations) != 0 {
		return BeadState{}, violations
	}
	return after, nil
}

// Validate checks the final proposal. method is WriteMeta.Method stamped by
// core service, never a field supplied inside an operation. Facts are obtained
// through tracked reads and include write-set post-images. Re-run with graph
// facts from the transaction; this function never performs a read or a write.
func Validate(e Effective, before, after *BeadState, method string, facts GuardFacts) []Violation {
	if after == nil {
		return nil
	} // deletion cannot introduce ontology drift
	if before != nil && before.ID != after.ID {
		return invalid("id", "bead identity is immutable")
	}
	gateMethod := isGateMethod(method)
	if gateMethod {
		switch after.Fields[gateStateField] {
		case GateStateResolved:
			method = "bead.close"
		case GateStateFailed:
			method = "bead.fail"
		default:
			method = "bead.update"
		}
	}
	slot, lifecycle := SlotForMethod(method)
	if before != nil && !lifecycle && (before.Status != after.Status || hasPath(facts.Touched, "status") || hasPath(facts.Touched, "fields.status")) {
		return []Violation{{Code: "ontology_use_slot_verb", Field: "status", Message: "status must be changed through a lifecycle verb"}}
	}
	var violations []Violation
	for _, name := range []string{gateKindField, gateStateField, gateApprovalsField, gateApprovalCountField, gateDetailsField} {
		value, exists := after.Fields[name]
		if gateMethod {
			continue
		}
		if before == nil {
			if !exists {
				continue
			}
			if name == gateKindField {
				kind, ok := value.(string)
				if _, active := e.Gate(kind); !ok || !active {
					violations = append(violations, invalid("fields."+name, "unknown or archived gate kind")...)
				}
				continue
			}
		} else {
			old, present := before.Fields[name]
			if present == exists && sameValue(old, value) && !hasPath(facts.Touched, "fields."+name) {
				continue
			}
		}
		violations = append(violations, Violation{Code: "ontology_use_slot_verb", Field: "fields." + name, Message: "gate state must be changed through a gate verb"})
	}
	_, priorityViolations := Priority(*after)
	violations = append(violations, priorityViolations...)
	_, templateViolations := IsTemplate(*after)
	violations = append(violations, templateViolations...)
	t, ok := e.document.Types[after.Type]
	if !ok || t.Archived {
		return append(violations, invalid("type", "unknown or archived type")...)
	}
	if e.degraded[after.Type] {
		return append(violations, Violation{Code: "subscriber_unavailable", Field: "type", Message: "type is degraded: field owner unavailable"})
	}
	w := e.document.Workflows[t.Workflow]
	s, ok := e.document.Statuses[after.Status]
	if !ok || s.Archived || w.Archived || !contains(w.Statuses, after.Status) {
		violations = append(violations, invalid("status", "status is outside the active type workflow")...)
	}
	for _, name := range keys(after.Fields) {
		if name == "priority" || name == "template" {
			continue
		} // fixed kernel field, checked above
		if isGateField(name) {
			continue
		}
		field, ok := t.Fields[name]
		if !ok {
			violations = append(violations, Violation{Code: "ontology_unknown_field", Field: "fields." + name, Message: "unknown typed field"})
			continue
		}
		if !validValue(field, after.Fields[name]) {
			violations = append(violations, invalid("fields."+name, "value does not match field schema")...)
		}
	}
	for _, name := range keys(t.Fields) {
		field := t.Fields[name]
		value, exists := after.Fields[name]
		if field.Required && (!exists || value == nil) {
			violations = append(violations, invalid("fields."+name, "required field missing")...)
		}
		changed := exists
		if before != nil {
			old, present := before.Fields[name]
			changed = present != exists || !sameValue(old, value)
		}
		if changed && field.Owner != "" && facts.SetBy["fields."+name] != field.Owner {
			violations = append(violations, Violation{Code: "field_owned", Field: "fields." + name, Message: "field may only be written by its owner"})
		}
	}
	if before == nil {
		if lifecycle {
			violations = append(violations, invalid("status", "lifecycle operation requires an existing bead")...)
		}
		if after.Status != w.Initial {
			violations = append(violations, Violation{Code: "ontology_use_slot_verb", Field: "status", Message: "new beads must start at the workflow initial status"})
		}
		return violations
	}
	if !lifecycle {
		return violations
	}
	target, ok := e.SlotStatus(after.Type, slot)
	if !ok {
		return append(violations, invalid("status", "lifecycle slot is unavailable")...)
	}
	if before.Type != after.Type || before.Project != after.Project {
		return append(violations, invalid("status", "lifecycle operation cannot move project or change type")...)
	}
	if after.Status != target || before.Status == after.Status {
		return append(violations, invalid("status", "proposal does not enter the selected lifecycle slot")...)
	}
	if !contains(w.Statuses, before.Status) {
		return append(violations, invalid("status", "source is outside workflow")...)
	}
	if category, found := e.Category(before.Status); !found || (slot == Claim && !category.ReadyEligible()) {
		return append(violations, invalid("status", "claim requires an unstarted source")...)
	}
	matched, guardViolations := transitionGuards(w, before.Status, target, *after, facts)
	violations = append(violations, guardViolations...)
	if !matched {
		violations = append(violations, invalid("status", "transition is not allowed")...)
	}
	return violations
}

// ValidateCloseSettlement validates an already-closed row without inventing a
// transition. The adapter must first recognize the original empty close patch.
// Ordinary field/ownership/status-touch checks and every applicable close guard
// still run. Source-specific transition rules apply only to their named source;
// broad close guards also apply to settlement. Validate retains its strict
// same-slot rejection for ordinary lifecycle proposals.
func ValidateCloseSettlement(e Effective, before, after *BeadState, facts GuardFacts) []Violation {
	if before == nil || after == nil {
		return invalid("status", "close settlement requires an existing bead")
	}
	target, ok := e.SlotStatus(before.Type, Close)
	if !ok || before.Status != target || after.Status != target || before.Type != after.Type || before.Project != after.Project {
		return invalid("status", "close settlement must retain the selected close slot and workflow")
	}
	violations := Validate(e, before, after, "bead.update", facts)
	w := e.document.Workflows[e.document.Types[before.Type].Workflow]
	_, guards := transitionGuards(w, before.Status, target, *after, facts)
	return append(violations, guards...)
}

func transitionGuards(w Workflow, from, to string, after BeadState, facts GuardFacts) (bool, []Violation) {
	matched := false
	var violations []Violation
	if facts.OverridePolicy {
		// Force can never close a template, even when its workflow omitted
		// the optional not_template declaration.
		violations = evaluateGuard(Guard{Kind: NotTemplate}, after, facts)
	}
	for _, rule := range w.Transitions {
		if rule.To != to || (len(rule.From) != 0 && !contains(rule.From, from)) {
			continue
		}
		matched = true
		for _, guard := range rule.Guards {
			if facts.OverridePolicy && guard.Kind == NotTemplate {
				continue // the mandatory template check already ran above
			}
			violations = append(violations, evaluateGuard(guard, after, facts)...)
		}
	}
	return matched, violations
}

// DefaultPriority is the fixed v1 default in the P0 through P4 kernel domain.
const DefaultPriority = 2

// Priority reads the fixed kernel field, including its default. The service
// can use this value followed by bead ID to order ready work without duplicating
// parsing or range rules. JSON numbers remain lossless until the checked 0..4
// conversion; string, boolean, fractional and null values are refused.
func Priority(state BeadState) (int, []Violation) {
	value, present := state.Fields["priority"]
	if !present {
		return DefaultPriority, nil
	}
	if !validValue(Field{Type: "integer"}, value) {
		return 0, invalid("fields.priority", "priority must be an integer from P0 through P4")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return 0, invalid("fields.priority", "priority must be an integer from P0 through P4")
	}
	priority, err := model.Natural(string(data)).Uint64()
	if err != nil || priority > 4 {
		return 0, invalid("fields.priority", "priority must be an integer from P0 through P4")
	}
	return int(priority), nil
}

// IsTemplate validates the fixed kernel template flag. Adapters must preserve
// the raw stored field instead of discarding a failed boolean assertion. The
// Template member remains a trusted compatibility view for existing callers;
// it cannot override a contradictory raw value.
func IsTemplate(state BeadState) (bool, []Violation) {
	value, present := state.Fields["template"]
	if !present {
		return state.Template, nil
	}
	flag, ok := value.(bool)
	if !ok || (state.Template && !flag) {
		return false, invalid("fields.template", "template must be a boolean consistent with the record view")
	}
	return flag, nil
}

func evaluateGuard(guard Guard, after BeadState, facts GuardFacts) []Violation {
	if facts.OverridePolicy {
		switch guard.Kind {
		case FieldsRequired, EvidenceRequired, NoOpenChildren, NoOpenBlockers, GatesResolved:
			return nil
		}
	}
	failed := false
	switch guard.Kind {
	case FieldsRequired:
		for _, name := range guard.Fields {
			value, ok := after.Fields[name]
			failed = failed || !ok || value == nil || value == ""
		}
	case EvidenceRequired:
		for _, kind := range guard.Kinds {
			failed = failed || !facts.Evidence[kind]
		}
	case NoOpenChildren:
		failed = facts.NoOpenChildren == nil || !*facts.NoOpenChildren
	case NoOpenBlockers:
		failed = facts.NoOpenBlockers == nil || !*facts.NoOpenBlockers
	case NotTemplate:
		flag, problems := IsTemplate(after)
		failed = flag || len(problems) != 0
	case GatesResolved:
		for _, kind := range guard.Kinds {
			failed = failed || !facts.Gates[kind]
		}
	case Permission:
		failed = facts.AuthzInstalled && (facts.Permission == nil || !*facts.Permission)
	default:
		failed = true
	}
	if !failed {
		return nil
	}
	return []Violation{{Code: "ontology_guard_failed", Guard: guard.Kind, Message: "transition guard failed or required observation is missing"}}
}

// ValidateTransitionPermission is the permission-only check used for forced
// receipt replay. A stored result is never a current transition grant; replay
// need not rerun policy guards or subscriber callbacks.
func ValidateTransitionPermission(facts GuardFacts) []Violation {
	return evaluateGuard(Guard{Kind: Permission}, BeadState{}, facts)
}

func invalid(field, message string) []Violation {
	return []Violation{{Code: "invalid_params", Field: field, Message: message}}
}

func validValue(field Field, value any) bool {
	switch field.Type {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "enum":
		s, ok := value.(string)
		return ok && contains(field.Values, s)
	case "strings":
		switch values := value.(type) {
		case []string:
			return values != nil
		case []any:
			for _, v := range values {
				if _, ok := v.(string); !ok {
					return false
				}
			}
			return values != nil
		default:
			return false
		}
	case "integer", "number":
		data, err := json.Marshal(value)
		if err != nil || len(data) == 0 || (data[0] != '-' && (data[0] < '0' || data[0] > '9')) {
			return false
		}
		return field.Type == "number" || integralJSON(string(data))
	default:
		return false
	}
}

// integralJSON never expands an exponent. A short value such as 1e1000000000
// must not allocate a billion-digit integer during schema validation.
func integralJSON(number string) bool {
	exponent := new(big.Int)
	if at := strings.IndexAny(number, "eE"); at >= 0 {
		if _, ok := exponent.SetString(number[at+1:], 10); !ok {
			return false
		}
		number = number[:at]
	}
	fraction := 0
	if at := strings.IndexByte(number, '.'); at >= 0 {
		fraction = len(number) - at - 1
		number = number[:at] + number[at+1:]
	}
	number = strings.TrimLeft(number, "-0")
	if number == "" {
		return true
	}
	trailing := len(number) - len(strings.TrimRight(number, "0"))
	exponent.Add(exponent, big.NewInt(int64(trailing-fraction)))
	return exponent.Sign() >= 0
}

func copyState(state BeadState) (BeadState, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return BeadState{}, err
	}
	var copy BeadState
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	err = dec.Decode(&copy)
	if err == nil && state.Fields != nil && copy.Fields == nil {
		copy.Fields = map[string]any{}
	}
	return copy, err
}

func sameValue(left, right any) bool {
	if reflect.DeepEqual(left, right) {
		return true
	}
	l, err := json.Marshal(left)
	if err != nil {
		return false
	}
	r, err := json.Marshal(right)
	return err == nil && bytes.Equal(l, r)
}
