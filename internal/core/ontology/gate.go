package ontology

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const (
	GateCountHumans     = "humans"
	GateCountPrincipals = "principals"

	GateTimeoutEscalate = "escalate"
	GateTimeoutFail     = "fail"

	GateStatePending  = "pending"
	GateStateResolved = "resolved"
	GateStateFailed   = "failed"
	GateStateStale    = "stale"
)

// GateApproval is the durable approval observation supplied by a gate
// projection. Token is retained for audit/idempotency, but never contributes
// to the distinct-human count.
type GateApproval struct {
	Principal model.PrincipalRef `json:"principal"`
	Token     string             `json:"token,omitempty"`
}

// GateState is the read image needed by the pure gate evaluator. Type is the
// gate bead's ontology type and is required only when a resolution needs to
// lower to a workflow slot.
type GateState struct {
	Kind      string         `json:"kind"`
	Type      string         `json:"type"`
	State     string         `json:"state"`
	Approvals []GateApproval `json:"approvals,omitempty"`
}

type GateTransition struct {
	Slot   Slot   `json:"slot"`
	Status string `json:"status,omitempty"`
}

// GateDecision is the response from the pure phase. The caller appends
// Approval only when Added is true, then persists the resulting gate image in
// its one CAS-protected Writer transaction.
type GateDecision struct {
	State      string          `json:"state"`
	Approvals  uint32          `json:"approvals"`
	Added      bool            `json:"added"`
	Approval   *GateApproval   `json:"approval,omitempty"`
	Transition *GateTransition `json:"transition,omitempty"`
}

// Gate returns a detached active gate definition. A caller cannot mutate a
// compiled snapshot through Params.
func (e Effective) Gate(kind string) (GateKind, bool) {
	definition, ok := e.document.GateKinds[kind]
	if !ok || definition.Archived {
		return GateKind{}, false
	}
	data, err := json.Marshal(definition)
	if err != nil {
		return GateKind{}, false
	}
	var copy GateKind
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&copy); err != nil {
		return GateKind{}, false
	}
	return copy, true
}

// ApproveGate applies the manual approval phase and, once the threshold is
// reached, resolves the gate into its workflow close slot. It performs no
// storage I/O and never mutates state.
func (e Effective) ApproveGate(state GateState, principal model.PrincipalRef, authenticated bool) (GateDecision, error) {
	if state.Kind == "" {
		return GateDecision{}, gateFailure("invalid_params", "gate kind is required")
	}
	kind, ok := e.Gate(state.Kind)
	if !ok {
		return GateDecision{}, gateFailure("invalid_params", "unknown or archived gate kind")
	}
	decision, err := EvaluateApproval(kind, state, principal, authenticated)
	if err != nil {
		return GateDecision{}, err
	}
	return e.withTransitionStatus(state, decision)
}

// ReportGate applies a resolver result. Manual gates must use ApproveGate so
// a plugin cannot bypass the distinct-human threshold.
func (e Effective) ReportGate(state GateState, outcome Outcome) (GateDecision, error) {
	if state.Kind == "" {
		return GateDecision{}, gateFailure("invalid_params", "gate kind is required")
	}
	kind, ok := e.Gate(state.Kind)
	if !ok {
		return GateDecision{}, gateFailure("invalid_params", "unknown or archived gate kind")
	}
	decision, err := ResolveGate(kind, state, outcome)
	if err != nil {
		return GateDecision{}, err
	}
	return e.withTransitionStatus(state, decision)
}

func (e Effective) withTransitionStatus(state GateState, decision GateDecision) (GateDecision, error) {
	if decision.Transition == nil {
		return decision, nil
	}
	if state.Type == "" {
		return GateDecision{}, gateFailure("invalid_params", "gate type is required for a resolved transition")
	}
	target, ok := e.SlotStatus(state.Type, decision.Transition.Slot)
	if !ok {
		return GateDecision{}, gateFailure("ontology_guard_failed", "gate resolution slot is unavailable")
	}
	decision.Transition.Status = target
	return decision, nil
}

// EvaluateApproval is the storage-independent manual gate phase. Distinct
// identities are counted from the complete approval image, so duplicate
// tokens from one human are a no-op and never produce a second event.
func EvaluateApproval(kind GateKind, state GateState, principal model.PrincipalRef, authenticated bool) (GateDecision, error) {
	if state.Kind == "" {
		return GateDecision{}, gateFailure("invalid_params", "gate kind is required")
	}
	if !isManualResolver(kind.Resolver) {
		return GateDecision{}, gateFailure("invalid_params", "gate kind is not manual")
	}
	min, err := gateMinApprovals(kind)
	if err != nil {
		return GateDecision{}, gateFailure("invalid_params", err.Error())
	}
	if err := validateGateState(state, false); err != nil {
		return GateDecision{}, err
	}
	if _, err := principalChain(principal); err != nil {
		return GateDecision{}, gateFailure("invalid_params", "approval principal is invalid")
	}
	if min > 1 && (!authenticated || principal.Kind == "anonymous") {
		return GateDecision{}, gateFailure("forbidden", "multi-approval manual gates require authentication")
	}
	if !authenticated && principal.Kind != "anonymous" {
		return GateDecision{}, gateFailure("unauthenticated", "authenticated principal required")
	}

	counted := map[string]struct{}{}
	for _, approval := range state.Approvals {
		identity, err := gateIdentity(approval.Principal, gateCount(kind))
		if err != nil {
			return GateDecision{}, gateFailure("invalid_params", "stored approval has no valid counting identity")
		}
		counted[identity] = struct{}{}
	}
	identity, err := gateIdentity(principal, gateCount(kind))
	if err != nil {
		return GateDecision{}, gateFailure("forbidden", err.Error())
	}
	if _, exists := counted[identity]; exists {
		decision := GateDecision{State: gateStateOrPending(state.State), Approvals: uint32(len(counted))}
		// A durable image can be pending while already containing the threshold
		// (for example, after recovery from an older projection). Replaying a
		// duplicate approval must remain idempotent, but it must still expose the
		// deterministic terminal transition implied by that image.
		if decision.Approvals >= min {
			decision.State = GateStateResolved
			decision.Transition = &GateTransition{Slot: Close}
		}
		return decision, nil
	}
	counted[identity] = struct{}{}
	decision := GateDecision{
		State:     gateStateOrPending(state.State),
		Approvals: uint32(len(counted)),
		Added:     true,
		Approval:  &GateApproval{Principal: principal},
	}
	if decision.Approvals >= min {
		decision.State = GateStateResolved
		decision.Transition = &GateTransition{Slot: Close}
	}
	return decision, nil
}

// ResolveGate applies a non-manual resolver report. Success always selects
// close and failure always selects fail; missing slots are refused by the
// Effective wrapper before any caller can publish a partial transition.
func ResolveGate(kind GateKind, state GateState, outcome Outcome) (GateDecision, error) {
	if state.Kind == "" {
		return GateDecision{}, gateFailure("invalid_params", "gate kind is required")
	}
	if kind.Resolver == "" {
		return GateDecision{}, gateFailure("invalid_params", "gate resolver is required")
	}
	if isManualResolver(kind.Resolver) {
		return GateDecision{}, gateFailure("forbidden", "manual gates require gate.approve")
	}
	// A timeout leaves a gate stale until its resolver reports a new state, so
	// resolver reports may replace stale but never overwrite a terminal state.
	if err := validateGateState(state, true); err != nil {
		return GateDecision{}, err
	}
	approvals, err := distinctApprovalCount(state, gateCount(kind))
	if err != nil {
		return GateDecision{}, gateFailure("invalid_params", "stored approval has no valid counting identity")
	}
	decision := GateDecision{Approvals: approvals}
	switch outcome {
	case Success:
		decision.State = GateStateResolved
		decision.Transition = &GateTransition{Slot: Close}
	case Failure:
		decision.State = GateStateFailed
		decision.Transition = &GateTransition{Slot: Fail}
	default:
		return GateDecision{}, gateFailure("invalid_params", "gate outcome must be success or failure")
	}
	return decision, nil
}

func validateGateState(state GateState, allowStale bool) error {
	switch state.State {
	case "", GateStatePending:
		return nil
	case GateStateResolved, GateStateFailed:
		return gateFailure("precondition_failed", "gate is already resolved")
	case GateStateStale:
		if allowStale {
			return nil
		}
		return gateFailure("precondition_failed", "gate state is stale")
	default:
		return gateFailure("invalid_params", "unknown gate state")
	}
}

func gateStateOrPending(state string) string {
	if state == "" {
		return GateStatePending
	}
	return state
}

func gateCount(kind GateKind) string {
	if kind.Count == "" {
		return GateCountHumans
	}
	return kind.Count
}

func isManualResolver(resolver string) bool {
	return resolver == "manual" || resolver == "builtin.manual"
}

func gateMinApprovals(kind GateKind) (uint32, error) {
	param, present := kind.Params["min_approvals"]
	if present {
		parsed, err := naturalParam(param)
		if err != nil || parsed == 0 || parsed > uint64(^uint32(0)) {
			return 0, fmt.Errorf("min_approvals must be a positive integer")
		}
		return uint32(parsed), nil
	}
	return 1, nil
}

func naturalParam(value any) (uint64, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	var natural model.Natural
	if err := json.Unmarshal(data, &natural); err != nil {
		return 0, err
	}
	return natural.Uint64()
}

func parseGateTimeout(value string) (time.Duration, error) {
	return time.ParseDuration(strings.TrimSpace(value))
}

func gateIdentity(principal model.PrincipalRef, count string) (string, error) {
	chain, err := principalChain(principal)
	if err != nil {
		return "", err
	}
	// Anonymous mode has one principal. The transport may attach a transient
	// label, but that label must not manufacture additional approval identities.
	if chain[0].Kind == "anonymous" {
		return "anonymous", nil
	}
	if count == GateCountPrincipals {
		if chain[0].Kind == "system" {
			return "", fmt.Errorf("system principal cannot approve a user gate")
		}
		return principalKey(chain[0]), nil
	}
	if count != GateCountHumans {
		return "", fmt.Errorf("unknown gate count mode")
	}
	switch chain[0].Kind {
	case "user":
		return principalKey(chain[0]), nil
	case "agent", "service":
		last := chain[len(chain)-1]
		if last.Kind != "user" {
			return "", fmt.Errorf("agent or service has no sponsoring user")
		}
		return principalKey(last), nil
	default:
		return "", fmt.Errorf("principal kind %q cannot resolve to a human", chain[0].Kind)
	}
}

func principalChain(principal model.PrincipalRef) ([]model.PrincipalRef, error) {
	chain := []model.PrincipalRef{}
	seen := map[*model.PrincipalRef]bool{}
	current := &principal
	for current != nil {
		if seen[current] {
			return nil, fmt.Errorf("cyclic principal sponsor")
		}
		seen[current] = true
		if current.Kind == "" || current.ID == "" {
			return nil, fmt.Errorf("principal kind and id are required")
		}
		switch current.Kind {
		case "anonymous", "user", "agent", "service", "plugin", "system":
		default:
			return nil, fmt.Errorf("unknown principal kind %q", current.Kind)
		}
		chain = append(chain, *current)
		current = current.Sponsor
	}
	return chain, nil
}

func principalKey(principal model.PrincipalRef) string {
	return principal.Kind + ":" + principal.ID
}

func distinctApprovalCount(state GateState, count string) (uint32, error) {
	identities := map[string]struct{}{}
	for _, approval := range state.Approvals {
		identity, err := gateIdentity(approval.Principal, count)
		if err != nil {
			return 0, err
		}
		identities[identity] = struct{}{}
	}
	return uint32(len(identities)), nil
}

func gateFailure(code model.ErrorCode, hint string) error {
	return &model.Error{Data: model.ErrorData{Code: code, Hint: hint}}
}
