package reverseconverter

import (
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// KindDisposition is the common write-admission classification used by the
// rollback window. It describes the record kind itself; field-level conversion
// validation remains the reverse converter's responsibility.
type KindDisposition string

const (
	// KindRepresentable is user content with a canonical Beads record mapping.
	KindRepresentable KindDisposition = "representable"
	// KindOperational is durable control or receipt metadata. It may be written
	// during the rollback window and is accounted as loss by the reverse report.
	KindOperational KindDisposition = "operational"
	// KindUnrepresentable is user content for which the reverse path has no
	// canonical Beads mapping.
	KindUnrepresentable KindDisposition = "unrepresentable"
)

// KindMapping resolves an actual, already-admitted reverse mapping. Callers
// must source this callback from the same reverse/export contract used for
// rollback; it is intentionally not a free-form example allowlist. The
// returned value is a canonical reverse-converter record kind (for example,
// "bead" or "memory").
type KindMapping func(kind string) (beadsKind string, ok bool)

// KindClassification is the detached result of ClassifyKind. BeadsKind is set
// when the kind has an actual canonical mapping; it is empty for operational or
// refused user-content kinds.
type KindClassification struct {
	Kind        string          `json:"kind"`
	BeadsKind   string          `json:"beads_kind,omitempty"`
	Disposition KindDisposition `json:"disposition"`
}

// ClassifyKind applies the canonical reverse converter vocabulary. The
// supportedKinds table is the same table used by archive validation and
// conversion, so adding a real reverse mapping updates both paths together.
func ClassifyKind(kind string) KindClassification {
	return ClassifyKindWithMapping(kind, nil)
}

// ClassifyKindWithMapping first consults an actual host-supplied reverse
// mapping, then falls back to the converter's canonical record-kind table.
// Mappings to a noncanonical Beads record kind are ignored and remain a typed
// refusal; this prevents a frozen example allowlist from weakening admission.
func ClassifyKindWithMapping(kind string, mapping KindMapping) KindClassification {
	classification := KindClassification{Kind: kind, Disposition: KindUnrepresentable}
	if mapping != nil {
		if beadsKind, ok := mapping(kind); ok && supportedKinds[beadsKind] {
			classification.BeadsKind = beadsKind
			classification.Disposition = KindRepresentable
			return classification
		}
	}
	if operationalKind(kind) {
		classification.Disposition = KindOperational
		return classification
	}
	if supportedKinds[kind] {
		classification.BeadsKind = kind
		classification.Disposition = KindRepresentable
	}
	return classification
}

func operationalKind(kind string) bool {
	switch kind {
	case "audit", "receipt", "workflow.audit", "workflow.receipt", "workflow.receipts", "workflow.audit-log",
		"instance_config", "restore-archive", "beads-archive":
		return true
	default:
		return false
	}
}

// RollbackWindowRefusal is the stable typed refusal shared by the pipeline and
// service write boundaries. The offending kind is always present in details so
// transports and operators do not have to parse a message string.
func RollbackWindowRefusal(kind string) error {
	return &model.Error{
		Message: fmt.Sprintf("rollback_window refuses unrepresentable user-content kind %q", kind),
		Data: model.ErrorData{
			Code:    model.CodeUnsupported,
			Hint:    "disable rollback_window only after the prepared rollback window closes",
			Details: map[string]any{"kind": kind},
		},
	}
}
