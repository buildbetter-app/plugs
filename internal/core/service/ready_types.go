package service

import (
	"encoding/json"
	"slices"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

// Beads keeps orchestration and infrastructure rows out of default ready work.
// Imported source types can be represented by the ordinary Plug issue type, so
// the source alias is read from retained import metadata before membership is
// evaluated. An explicit type selector remains an opt-in for that alias.
var defaultReadyExcludedTypes = map[string]struct{}{
	"merge-request": {},
	"gate":          {},
	"molecule":      {},
	"rig":           {},
	"agent":         {},
	"role":          {},
	"message":       {},
}

func readySourceType(doc pipeline.Document) string {
	for _, key := range []string{"_plug_native_type", "beads.source.source_type"} {
		if raw, ok := doc.Metadata[key]; ok {
			var value string
			if json.Unmarshal(raw, &value) == nil && value != "" {
				return value
			}
		}
	}
	if raw, ok := doc.Metadata["beads.source_fields"]; ok {
		var fields map[string]any
		if json.Unmarshal(raw, &fields) == nil {
			if value, ok := fields["issue_type"].(string); ok && value != "" {
				return value
			}
		}
	}
	return doc.Type
}

func readyFilterForRow(row storedBead, filter *Filter) (*Filter, bool) {
	sourceType := readySourceType(row.doc)
	if filter == nil || len(filter.Types) == 0 {
		if _, excluded := defaultReadyExcludedTypes[sourceType]; excluded {
			return nil, false
		}
		return filter, true
	}
	if !slices.Contains(filter.Types, sourceType) || slices.Contains(filter.Types, row.doc.Type) {
		return filter, true
	}
	// A retained source alias may have been mapped to Plug's ordinary issue
	// type. Reuse the normal selector checks after proving the explicit alias.
	copy := *filter
	copy.Types = []string{row.doc.Type}
	return &copy, true
}
