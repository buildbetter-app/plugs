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
	return (*Converter)(nil).ClassifyKind(kind)
}

// ClassifyKind derives admission from this converter's executable mappings.
// A kind label alone cannot register a mapping. A nil converter uses only the
// canonical vocabulary; native workflow kinds have no default mapping.
func (c *Converter) ClassifyKind(kind string) KindClassification {
	classification := KindClassification{Kind: kind, Disposition: KindUnrepresentable}
	if operationalKind(kind) {
		classification.Disposition = KindOperational
		return classification
	}
	if supportedKinds[kind] {
		classification.BeadsKind = kind
		classification.Disposition = KindRepresentable
	} else if c != nil {
		if mapping, ok := c.mappings[kind]; ok {
			classification.BeadsKind = mapping.BeadsKind
			classification.Disposition = KindRepresentable
		}
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
