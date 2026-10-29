package ontology

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func manualGateEffective(t *testing.T, min uint32) Effective {
	t.Helper()
	doc := DefaultDocument()
	doc.GateKinds = map[string]GateKind{
		"human_approval": {
			Resolver: "builtin.manual",
			Params:   map[string]any{"min_approvals": json.Number(stringValue(model.Natural64(uint64(min))))},
		},
	}
	return compileForTest(t, doc, nil)
}

func user(id string) model.PrincipalRef {
	return model.PrincipalRef{Kind: "user", ID: id}
}

func sponsoredAgent(id string, sponsor model.PrincipalRef) model.PrincipalRef {
	return model.PrincipalRef{Kind: "agent", ID: id, Sponsor: &sponsor}
}

func gateState(approvals ...GateApproval) GateState {
	return GateState{Kind: "human_approval", Type: "issue", State: GateStatePending, Approvals: approvals}
}

func errorCode(t *testing.T, err error) model.ErrorCode {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	var typed *model.Error
	if !errors.As(err, &typed) {
		t.Fatalf("error is not typed: %v", err)
	}
	return typed.Data.Code
}

func TestManualApprovalCountsDistinctSponsorResolvedHumans(t *testing.T) {
	e := manualGateEffective(t, 2)
	first, err := e.ApproveGate(gateState(), user("user:one"), true)
	if err != nil || first.State != GateStatePending || first.Approvals != 1 || !first.Added || first.Transition != nil {
		t.Fatalf("first approval: %+v, %v", first, err)
	}

	duplicate, err := e.ApproveGate(gateState(*first.Approval), sponsoredAgent("agent:one", user("user:one")), true)
	if err != nil || duplicate.State != GateStatePending || duplicate.Approvals != 1 || duplicate.Added {
		t.Fatalf("duplicate sponsor approval: %+v, %v", duplicate, err)
	}

	second, err := e.ApproveGate(gateState(*first.Approval), sponsoredAgent("agent:two", user("user:two")), true)
	if err != nil || second.State != GateStateResolved || second.Approvals != 2 || !second.Added || second.Transition == nil || second.Transition.Slot != Close || second.Transition.Status != "done" {
		t.Fatalf("threshold approval: %+v, %v", second, err)
	}

	// A recovered pending image can already contain the threshold. A duplicate
	// approval stays a no-op but must not hide the deterministic close decision.
	recovered, err := e.ApproveGate(gateState(*first.Approval, *second.Approval), user("user:one"), true)
	if err != nil || recovered.State != GateStateResolved || recovered.Approvals != 2 || recovered.Added || recovered.Transition == nil || recovered.Transition.Slot != Close || recovered.Transition.Status != "done" {
		t.Fatalf("recovered threshold replay: %+v, %v", recovered, err)
	}
}

func TestManualApprovalRefusesAnonymousMultiApproval(t *testing.T) {
	e := manualGateEffective(t, 2)
	_, err := e.ApproveGate(gateState(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, false)
	if got := errorCode(t, err); got != "forbidden" {
		t.Fatalf("anonymous multi-approval code = %q", got)
	}

	one := manualGateEffective(t, 1)
	decision, err := one.ApproveGate(gateState(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, false)
	if err != nil || decision.State != GateStateResolved || decision.Approvals != 1 {
		t.Fatalf("single anonymous approval: %+v, %v", decision, err)
	}
}

func TestManualApprovalRefusesUnsponsoredHumanAndMalformedState(t *testing.T) {
	e := manualGateEffective(t, 1)
	_, err := e.ApproveGate(gateState(), model.PrincipalRef{Kind: "service", ID: "service:orphan"}, true)
	if got := errorCode(t, err); got != "forbidden" {
		t.Fatalf("unsponsored service code = %q", got)
	}
	_, err = e.ApproveGate(GateState{Kind: "human_approval", Type: "issue", State: GateStateResolved}, user("user:one"), true)
	if got := errorCode(t, err); got != "precondition_failed" {
		t.Fatalf("resolved gate code = %q", got)
	}
	_, err = e.ApproveGate(gateState(), user("user:one"), false)
	if got := errorCode(t, err); got != "unauthenticated" {
		t.Fatalf("non-anonymous unauthenticated code = %q", got)
	}
}

func TestGateReportSelectsCloseOrFailDeterministically(t *testing.T) {
	doc := DefaultDocument()
	doc.GateKinds = map[string]GateKind{"external": {Resolver: "plugin.external"}}
	snapshot, problems := Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	e := snapshot.Effective("")
	success, err := e.ReportGate(GateState{Kind: "external", Type: "issue", State: GateStatePending}, Success)
	if err != nil || success.State != GateStateResolved || success.Approvals != 0 || success.Transition == nil || success.Transition.Slot != Close || success.Transition.Status != "done" {
		t.Fatalf("success report: %+v, %v", success, err)
	}
	failure, err := e.ReportGate(GateState{Kind: "external", Type: "issue", State: GateStatePending}, Failure)
	if err != nil || failure.State != GateStateFailed || failure.Transition == nil || failure.Transition.Slot != Fail || failure.Transition.Status != "failed" {
		t.Fatalf("failure report: %+v, %v", failure, err)
	}
	_, err = e.ReportGate(GateState{Kind: "human_approval", Type: "issue", State: GateStatePending}, Success)
	if got := errorCode(t, err); got != "invalid_params" && got != "forbidden" {
		t.Fatalf("manual report code = %q", got)
	}
}

func TestGateResolutionRefusesWithoutAWorkflowSlot(t *testing.T) {
	e := manualGateEffective(t, 1)
	_, err := e.ApproveGate(GateState{Kind: "human_approval", Type: "missing", State: GateStatePending}, user("user:one"), true)
	if got := errorCode(t, err); got != "ontology_guard_failed" {
		t.Fatalf("missing target type code = %q", got)
	}
}

func TestGateKindCompilationAndDetachedDefinition(t *testing.T) {
	doc := DefaultDocument()
	doc.GateKinds = map[string]GateKind{
		"human_approval": {Resolver: "builtin.manual", Params: map[string]any{"min_approvals": json.Number("2")}, Timeout: "72h", OnTimeout: GateTimeoutEscalate},
	}
	e := compileForTest(t, doc, nil)
	definition, ok := e.Gate("human_approval")
	if !ok || definition.Params["min_approvals"] != json.Number("2") {
		t.Fatalf("gate definition not retained: %+v, %t", definition, ok)
	}
	definition.Params["min_approvals"] = json.Number("3")
	unchanged, _ := e.Gate("human_approval")
	if unchanged.Params["min_approvals"] != json.Number("2") {
		t.Fatal("gate params alias compiled snapshot")
	}

	for _, bad := range []GateKind{
		{Resolver: "builtin.manual", Params: map[string]any{"min_approvals": json.Number("0")}},
		{Resolver: "builtin.manual", Count: "unknown"},
		{Resolver: "builtin.manual", Timeout: "not-a-duration"},
	} {
		invalid := DefaultDocument()
		invalid.GateKinds = map[string]GateKind{"bad": bad}
		if _, problems := Compile(invalid, nil, nil); len(problems) == 0 {
			t.Fatalf("invalid gate kind compiled: %+v", bad)
		}
	}
	if _, err := Decode([]byte(`{"gate_kinds":{"bad":{"resolver":"builtin.manual","min_approvals":2}}}`)); err == nil {
		t.Fatal("accepted top-level min_approvals outside gate params")
	}
	external := DefaultDocument()
	external.GateKinds = map[string]GateKind{"external": {Resolver: "plugin.external", Params: map[string]any{"min_approvals": "resolver-owned"}}}
	if _, problems := Compile(external, nil, nil); len(problems) != 0 {
		t.Fatalf("opaque external resolver params were interpreted by core: %+v", problems)
	}
}

func TestManualApprovalCountsPrincipalsAndCanonicalizesAnonymous(t *testing.T) {
	doc := DefaultDocument()
	doc.GateKinds = map[string]GateKind{
		"principal_approval": {Resolver: "builtin.manual", Count: GateCountPrincipals, Params: map[string]any{"min_approvals": json.Number("2")}},
		"anonymous_approval": {Resolver: "builtin.manual", Params: map[string]any{"min_approvals": json.Number("1")}},
	}
	snapshot, problems := Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	e := snapshot.Effective("")
	principalState := GateState{Kind: "principal_approval", Type: "issue", State: GateStatePending, Approvals: []GateApproval{{Principal: sponsoredAgent("agent:one", user("user:one"))}}}
	decision, err := e.ApproveGate(principalState, sponsoredAgent("agent:two", user("user:two")), true)
	if err != nil || decision.State != GateStateResolved || decision.Approvals != 2 {
		t.Fatalf("principal counting: %+v, %v", decision, err)
	}
	anonymousState := GateState{Kind: "anonymous_approval", Type: "issue", State: GateStatePending}
	first, err := e.ApproveGate(anonymousState, model.PrincipalRef{Kind: "anonymous", ID: "request-a"}, false)
	if err != nil || !first.Added || first.Approvals != 1 {
		t.Fatalf("first anonymous approval: %+v, %v", first, err)
	}
	duplicate, err := e.ApproveGate(GateState{Kind: "anonymous_approval", Type: "issue", State: GateStatePending, Approvals: []GateApproval{*first.Approval}}, model.PrincipalRef{Kind: "anonymous", ID: "request-b"}, false)
	if err != nil || duplicate.Added || duplicate.Approvals != 1 {
		t.Fatalf("anonymous labels created a second identity: %+v, %v", duplicate, err)
	}
}

func TestGateReportRefreshesStaleAndRefusesMalformedApproval(t *testing.T) {
	doc := DefaultDocument()
	doc.GateKinds = map[string]GateKind{"external": {Resolver: "plugin.external"}}
	snapshot, problems := Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	e := snapshot.Effective("")
	refreshed, err := e.ReportGate(GateState{Kind: "external", Type: "issue", State: GateStateStale}, Success)
	if err != nil || refreshed.State != GateStateResolved {
		t.Fatalf("stale resolver report: %+v, %v", refreshed, err)
	}
	_, err = e.ReportGate(GateState{Kind: "external", Type: "issue", State: GateStatePending, Approvals: []GateApproval{{Principal: model.PrincipalRef{Kind: "service", ID: "orphan"}}}}, Success)
	if got := errorCode(t, err); got != "invalid_params" {
		t.Fatalf("malformed stored approval code = %q", got)
	}
}
