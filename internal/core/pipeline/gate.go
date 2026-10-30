package pipeline

import "encoding/json"

func isGateMethod(method string) bool {
	return method == "gate.approve" || method == "gate.report"
}

func gateLifecycleMethod(method string, pending *pending) string {
	if !isGateMethod(method) || pending == nil {
		return method
	}
	state, _ := pending.doc.Fields["gate_state"].(string)
	switch state {
	case "resolved":
		return "bead.close"
	case "failed":
		return "bead.fail"
	default:
		return ""
	}
}

// An evaluator may repair the count or close a recovered pending image on a
// duplicate approval. Only an appended observation earns gate.approved.
func gateApprovalAdded(p *pending) bool {
	before, err := DecodeDocument(p.before)
	if err != nil {
		return false
	}
	count := func(value any) int {
		data, err := json.Marshal(value)
		var approvals []json.RawMessage
		if err != nil || json.Unmarshal(data, &approvals) != nil {
			return 0
		}
		return len(approvals)
	}
	return count(p.doc.Fields["gate_approvals"]) > count(before.Fields["gate_approvals"])
}
