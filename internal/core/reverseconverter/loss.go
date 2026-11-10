package reverseconverter

import "sort"

// StateLoss counts a persisted representation that Beads does not recreate.
// Kind is an actual record/public-view kind, when one exists. Family instead
// names a serialized provider family (runs, receipts, source.entities, ...);
// it is not an ontology kind. SourceTargetKind preserves an import plan's
// spelling separately from the current public view's Kind.
//
// FieldInstances counts scalar/null or empty-container occurrences inside
// each stored row body. Repeated fields count repeatedly. A schema occurrence
// is not necessarily a unique lost value: selected source content can survive
// in Beads fields or archived strings while its provider schema is absent.
type StateLoss struct {
	Kind             string `json:"kind,omitempty"`
	SourceTargetKind string `json:"source_target_kind,omitempty"`
	Family           string `json:"family,omitempty"`
	RecordCount      int    `json:"record_count"`
	FieldInstances   int    `json:"field_instances"`
	Disposition      string `json:"disposition"`
}

type NamespaceValueLoss struct {
	Key   string      `json:"key"`
	State []StateLoss `json:"state"`
}

// NamespaceLoss binds a complete persisted namespace census to the same
// snapshot as CoreKindLosses. Wrapper fields (revision/origin/envelope data)
// are separate from logical state rows and their field-instance counts.
type NamespaceLoss struct {
	Namespace             string               `json:"namespace"`
	Generation            uint64               `json:"generation"`
	PayloadBytes          int                  `json:"payload_bytes"`
	WrapperFieldInstances int                  `json:"wrapper_field_instances"`
	Values                []NamespaceValueLoss `json:"values"`
}

// CountFieldInstances operates on detached, losslessly decoded JSON values.
// Empty maps/arrays and null each occupy one field instance. Object keys and
// container nodes with children do not add instances of their own.
func CountFieldInstances(value any) int {
	switch value := value.(type) {
	case map[string]any:
		if len(value) == 0 {
			return 1
		}
		total := 0
		for _, child := range value {
			total += CountFieldInstances(child)
		}
		return total
	case []any:
		if len(value) == 0 {
			return 1
		}
		total := 0
		for _, child := range value {
			total += CountFieldInstances(child)
		}
		return total
	default:
		return 1
	}
}

// AddLossAccounting appends the trusted host's complete snapshot census. It
// never changes JSONL bytes, mappings, admission or the supported-kind policy.
func (report *Report) AddLossAccounting(core []StateLoss, namespaces []NamespaceLoss) {
	report.CoreKindLosses = append(report.CoreKindLosses, core...)
	report.NamespaceLosses = append(report.NamespaceLosses, namespaces...)
	for _, loss := range core {
		if loss.Kind != "" {
			addUnique(&report.PlugOnlyKinds, loss.Kind)
		}
	}
	for i := range report.NamespaceLosses {
		ns := &report.NamespaceLosses[i]
		for j := range ns.Values {
			value := &ns.Values[j]
			for _, loss := range value.State {
				if loss.Kind != "" && loss.Disposition == "unbound_native_state" {
					addUnique(&report.PlugOnlyKinds, loss.Kind)
				}
			}
			sort.Slice(value.State, func(a, b int) bool {
				x, y := value.State[a], value.State[b]
				if x.Family != y.Family {
					return x.Family < y.Family
				}
				if x.Kind != y.Kind {
					return x.Kind < y.Kind
				}
				return x.Disposition < y.Disposition
			})
		}
		sort.Slice(ns.Values, func(a, b int) bool { return ns.Values[a].Key < ns.Values[b].Key })
	}
	sort.Slice(report.CoreKindLosses, func(a, b int) bool { return report.CoreKindLosses[a].Kind < report.CoreKindLosses[b].Kind })
	sort.Slice(report.NamespaceLosses, func(a, b int) bool { return report.NamespaceLosses[a].Namespace < report.NamespaceLosses[b].Namespace })
	normalizeReport(report)
}
