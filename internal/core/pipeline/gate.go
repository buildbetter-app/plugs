package pipeline

import "github.com/buildbetter-app/plugs/internal/core/model"

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

func gateEventType(method string) model.DurableEventType {
	switch method {
	case "gate.approve":
		return model.EventGateApproved
	case "gate.report":
		return model.EventGateReported
	default:
		return ""
	}
}
