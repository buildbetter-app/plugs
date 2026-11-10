package converterroundtrip

import (
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// reverseLossCensus uses only the already pinned reader. Provider family
// names describe serialized state, never fabricated registered content kinds.
func reverseLossCensus(r driver.CheckpointInventory, live map[string]driver.Record, plan *driver.SourcePlanePlanV1, options reverseconverter.Options) ([]reverseconverter.StateLoss, []reverseconverter.NamespaceLoss, error) {
	core := map[string]reverseconverter.StateLoss{}
	for _, row := range live {
		if row.Kind == "memory" {
			var body map[string]any
			if decodeReverseJSON(row.Body, &body) != nil {
				return nil, nil, ReverseRefusal{"invalid_memory_loss_census"}
			}
			if overlay, present := body["sourceOverlay"]; present {
				loss := core["memory/sourceOverlay"]
				loss.Kind, loss.Family, loss.Disposition = "memory", "source_overlay_attribution", "effective_memory_representation"
				loss.RecordCount++
				loss.FieldInstances += reverseconverter.CountFieldInstances(overlay)
				core["memory/sourceOverlay"] = loss
			}
			if metadata, present := body["metadata"]; present && body["source_id"] == nil && !strings.HasPrefix(row.Ref.ID, "source/memory/") {
				loss := core["memory/metadata"]
				loss.Kind, loss.Family, loss.Disposition = "memory", "metadata", "memory_metadata"
				loss.RecordCount++
				loss.FieldInstances += reverseconverter.CountFieldInstances(metadata)
				core["memory/metadata"] = loss
			}
			continue
		}

		if row.Kind != "instance_config" && row.Kind != "restore-archive" && row.Kind != "beads-archive" {
			continue // Other unmapped core records already refuse projection.
		}
		var body map[string]any
		if decodeReverseJSON(row.Body, &body) != nil || body == nil {
			return nil, nil, ReverseRefusal{"invalid_control_record_loss_census"}
		}
		loss := core[row.Kind]
		loss.Kind, loss.Disposition = row.Kind, "retained_control_state"
		if row.Kind == "beads-archive" {
			loss.Disposition = "retained_source_provenance"
		}
		loss.RecordCount++
		loss.FieldInstances += reverseconverter.CountFieldInstances(body)
		core[row.Kind] = loss
	}
	var coreLosses []reverseconverter.StateLoss
	for _, loss := range core {
		coreLosses = append(coreLosses, loss)
	}
	keys, err := r.CheckpointKeys()
	if err != nil {
		return nil, nil, err
	}
	var namespaces []reverseconverter.NamespaceLoss
	for _, key := range keys {
		cp, err := r.SourcePlaneCheckpoint(key)
		if err != nil {
			return nil, nil, err
		}
		ns := reverseconverter.NamespaceLoss{Namespace: key, Generation: cp.Generation, PayloadBytes: len(cp.Payload), Values: []reverseconverter.NamespaceValueLoss{}}
		var envelope map[string]any
		if decodeReverseJSON(cp.Payload, &envelope) != nil || envelope == nil {
			// An opaque checkpoint is explicitly visible as bytes, not claimed
			// to be an enumerated external plugin schema or a registered kind.
			ns.Values = append(ns.Values, reverseconverter.NamespaceValueLoss{Key: "$checkpoint", State: []reverseconverter.StateLoss{{Family: "opaque_payload", RecordCount: 1, FieldInstances: 1, Disposition: "opaque_checkpoint_bytes"}}})
			namespaces = append(namespaces, ns)
			continue
		}
		values, durable := envelope["values"].(map[string]any)
		if !durable {
			ns.Values = append(ns.Values, reverseconverter.NamespaceValueLoss{Key: "$checkpoint", State: []reverseconverter.StateLoss{{Family: "checkpoint_payload", RecordCount: 1, FieldInstances: reverseconverter.CountFieldInstances(envelope), Disposition: "unclassified_checkpoint_schema"}}})
			namespaces = append(namespaces, ns)
			continue
		}
		for name, value := range envelope {
			if name != "values" {
				ns.WrapperFieldInstances += reverseconverter.CountFieldInstances(value)
			}
		}
		for name, raw := range values {
			entry, ok := raw.(map[string]any)
			if !ok {
				return nil, nil, ReverseRefusal{"invalid_namespace_loss_census"}
			}
			for field, value := range entry {
				if field != "value" {
					ns.WrapperFieldInstances += reverseconverter.CountFieldInstances(value)
				}
			}
			valueLoss := reverseconverter.NamespaceValueLoss{Key: name, State: []reverseconverter.StateLoss{}}
			body, present := entry["value"]
			if !present {
				ns.Values = append(ns.Values, valueLoss) // Revision-only tombstone.
				continue
			}
			logical, workflow := body.(map[string]any)
			workflow = workflow && strings.HasPrefix(key, "plugin/workflow/") && strings.HasSuffix(key, "/durable") && name == "workflow/state"
			if !workflow {
				valueLoss.State = append(valueLoss.State, reverseconverter.StateLoss{Family: "durable_value", RecordCount: 1, FieldInstances: reverseconverter.CountFieldInstances(body), Disposition: "unclassified_plugin_schema"})
			} else {
				bound := plan != nil && len(plan.Providers) == 1 && plan.Providers[0].Namespace == key
				for family, rows := range logical {
					if family == "source" {
						if source, ok := rows.(map[string]any); ok {
							for name, rows := range source {
								valueLoss.State = append(valueLoss.State, reverseFamilyLosses("source."+name, rows, bound, plan, options)...)
							}
							continue
						}
					}
					valueLoss.State = append(valueLoss.State, reverseFamilyLosses(family, rows, bound, plan, options)...)
				}
			}
			ns.Values = append(ns.Values, valueLoss)
		}
		namespaces = append(namespaces, ns)
	}
	return coreLosses, namespaces, nil
}

func reverseFamilyLosses(family string, value any, bound bool, plan *driver.SourcePlanePlanV1, options reverseconverter.Options) []reverseconverter.StateLoss {
	var rows []any
	switch value := value.(type) {
	case map[string]any:
		for _, row := range value {
			rows = append(rows, row)
		}
	case []any:
		rows = value
	default:
		rows = []any{value}
	}
	groups := map[string]reverseconverter.StateLoss{}
	for _, row := range rows {
		disposition := "unbound_native_state"
		if bound {
			disposition = "retained_control_state"
		}
		if strings.HasPrefix(family, "source.") {
			disposition = "retained_source_provenance"
		}
		kind, sourceKind := "", ""
		if family == "items" {
			kind = "workflow.item" // Public ItemView.kind; runs have no Kind.
			if bound {
				sourceKind = "workflow:item" // Exact import-plan TargetKind.
				disposition = "selected_source_content"
				body, _ := row.(map[string]any)
				for _, entity := range plan.Entities {
					if entity.TargetID != body["id"] {
						continue
					}
					for _, rule := range plan.Dispositions {
						if rule.Key == entity.DispositionKey && rule.Ephemeral && !options.All {
							disposition = "default_excluded_source_content"
						}
					}
				}
			}
		}
		loss := groups[disposition]
		loss.Kind, loss.SourceTargetKind, loss.Family, loss.Disposition = kind, sourceKind, family, disposition
		loss.RecordCount++
		loss.FieldInstances += reverseconverter.CountFieldInstances(row)
		groups[disposition] = loss
	}
	result := make([]reverseconverter.StateLoss, 0, len(groups))
	for _, loss := range groups {
		result = append(result, loss)
	}
	return result
}

func attachReverseLossCensus(result *reverseconverter.Result, err error, core []reverseconverter.StateLoss, namespaces []reverseconverter.NamespaceLoss) {
	result.Report.AddLossAccounting(core, namespaces)
	if refused, ok := err.(*reverseconverter.Refusal); ok {
		refused.Report = result.Report
	}
}
