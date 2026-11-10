// Package reversetest supplies an executable, lossless memory mapping for
// synthetic native content controls. It is not a production workflow mapping.
package reversetest

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
)

func Converter(kinds ...string) (*reverseconverter.Converter, error) {
	var mappings []reverseconverter.RecordMapping
	for _, kind := range kinds {
		mappings = append(mappings, reverseconverter.RecordMapping{Kind: kind, BeadsKind: "memory", Convert: func(record reverseconverter.Record) (reverseconverter.Record, error) {
			encoded, err := json.Marshal(record)
			if err != nil {
				return reverseconverter.Record{}, err
			}
			key, value := record.Kind+"/"+record.ID, string(encoded)
			return reverseconverter.Record{Kind: "memory", ID: key,
				Body:   map[string]any{"key": key, "value": value, "classification": "public"},
				Source: reverseconverter.Provenance{Table: "config", Fields: map[string]any{"key": "kv.memory." + key, "value": value}},
			}, nil
		}})
	}
	return reverseconverter.NewConverter(mappings...)
}

// WorkflowDump reads actual checkpoint bytes, including every persisted run
// and item field. Operational receipt/audit accounting belongs to the separate
// loss-census lane, so those families are not fed to this content conversion.
func WorkflowDump(payload []byte) (reverseconverter.Dump, error) {
	var envelope struct {
		Values map[string]struct {
			Value json.RawMessage `json:"value"`
		} `json:"values"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return reverseconverter.Dump{}, err
	}
	var state struct {
		Runs  map[string]map[string]any `json:"runs"`
		Items map[string]map[string]any `json:"items"`
	}
	if err := json.Unmarshal(envelope.Values["workflow/state"].Value, &state); err != nil {
		return reverseconverter.Dump{}, err
	}
	dump := reverseconverter.Dump{Manifest: reverseconverter.Manifest{
		Format: reverseconverter.PlugDumpFormat, Schema: 1, Reference: reverseconverter.BeadsReference,
		StoreID: "synthetic-checkpoint", Machine: "test", MainVersion: 72, IgnoredVersion: 28,
		Scope: "offline-head-and-source-archives", History: "unavailable",
	}, Records: map[string][]reverseconverter.Record{}}
	for kind, records := range map[string]map[string]map[string]any{"workflow.run": state.Runs, "workflow.item": state.Items} {
		if len(records) == 0 {
			return reverseconverter.Dump{}, fmt.Errorf("checkpoint contains no %s content", kind)
		}
		for id, body := range records {
			project, ok := body["project"].(string)
			if !ok {
				return reverseconverter.Dump{}, fmt.Errorf("%s %s has no project", kind, id)
			}
			dump.Records[kind] = append(dump.Records[kind], reverseconverter.Record{Kind: kind, ID: id, Project: project, Body: body,
				Source: reverseconverter.Provenance{Table: "workflow/state/" + kind, Fields: body},
			})
		}
		dump.Manifest.Kinds = append(dump.Manifest.Kinds, reverseconverter.Kind{Name: kind, Version: 1, File: "records/" + kind + ".ndjson", Count: len(records)})
	}
	sort.Slice(dump.Manifest.Kinds, func(i, j int) bool { return dump.Manifest.Kinds[i].Name < dump.Manifest.Kinds[j].Name })
	return dump, nil
}
