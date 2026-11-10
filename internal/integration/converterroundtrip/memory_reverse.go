package converterroundtrip

import (
	"bytes"
	"context"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Original mirrors have already passed the raw archive conservation checks.
// Project their effective memory values on the same reader; never patch source
// evidence in storage or export a forgotten value from the retained archive.
func projectEffectiveMemories(ctx context.Context, reader driver.Reader, dump *reverseconverter.Dump, live map[string]driver.Record, joint bool) error {
	var memories []reverseconverter.Record
	for _, source := range dump.Records["memory"] {
		id := "source/memory/" + source.ID
		if joint {
			id = sourcePlaneMirrorID(reverseSourceRecord(source), true)
		}
		rows, err := reader.Get(ctx, []model.Ref{model.RefByID(id, model.Natural64(0))})
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return ReverseRefusal{"missing_memory_source"}
		}
		effective, document, origin, err := pipeline.ResolveMemoryRecord(ctx, reader, rows[0])
		if err != nil {
			return ReverseRefusal{"invalid_effective_memory"}
		}
		if document.Overlay != nil {
			if origin == nil || !origin.Visible {
				return ReverseRefusal{"invalid_memory_overlay"}
			}
			stored, ok := live[effective.Ref.ID]
			if !ok || stored.Kind != "memory" || !bytes.Equal(stored.Body, effective.Body) {
				return ReverseRefusal{"missing_memory_overlay"}
			}
			delete(live, effective.Ref.ID)
			if document.Overlay.Deleted {
				continue
			}
			source.Body = cloneRow(source.Body)
			source.Source.Fields = cloneRow(source.Source.Fields)
			source.Body["value"], source.Source.Fields["value"] = document.Content, document.Content
		}
		memories = append(memories, source)
	}
	for id, row := range live {
		if row.Kind != "memory" {
			continue
		}
		document, source, err := pipeline.DecodeMemoryRecord(row)
		if err != nil || source != nil || document.Overlay != nil {
			return ReverseRefusal{"unbound_memory_record"}
		}
		memories = append(memories, reverseconverter.Record{Kind: "memory", ID: document.Key, Body: map[string]any{"key": document.Key, "value": document.Content, "classification": "public"}, Source: reverseconverter.Provenance{Table: "config", Fields: map[string]any{"key": "kv.memory." + document.Key, "value": document.Content}}})
		delete(live, id)
	}
	var kinds []reverseconverter.Kind
	for _, kind := range dump.Manifest.Kinds {
		if kind.Name != "memory" {
			kinds = append(kinds, kind)
		}
	}
	if len(memories) == 0 {
		delete(dump.Records, "memory")
	} else {
		dump.Records["memory"] = memories
		kinds = append(kinds, reverseconverter.Kind{Name: "memory", Version: 1, File: "records/memory.ndjson", Count: len(memories)})
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i].Name < kinds[j].Name })
	dump.Manifest.Kinds = kinds
	return nil
}
