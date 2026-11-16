package service

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

func copyFields(fields map[string]any) map[string]any {
	result := make(map[string]any, len(fields))
	for name, value := range fields {
		result[name] = value
	}
	return result
}

// ValidateOrdered is used only by typed close batches. It stays in the ordinary
// ontology phase, after mutators. Captured request-prefix images cannot be
// supplied by RPC callers or subscribers. One tracked graph scan guards all
// prefixes and the final image, including query misses and dependency phantoms.
func (o *Ontology) ValidateOrdered(ctx context.Context, images []port.Change, final port.ProposedChange, reader port.SnapshotReader) error {
	if final.Meta.Method != "tx.apply" || len(images) != len(final.Changes) {
		return failure("invalid_params", "invalid ordered close proposal")
	}
	graph, err := scanBeads(ctx, reader)
	if err != nil {
		return err
	}
	latest := map[string]port.Change{}
	for _, change := range final.Changes {
		latest[change.Resource.Ref.ID] = change
	}
	validated := make([]port.Change, 0, len(images))
	for opIndex, image := range images {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := image.Resource.Ref.ID
		single := final
		single.Changes = []port.Change{image}
		change := &single.Changes[0]
		prior, exists := graph[id]
		if exists {
			change.Before, err = json.Marshal(prior.doc)
			if err != nil {
				return err
			}
		} else {
			change.Before = nil
		}
		change.SetBy = map[string]string{}
		if !hasDocument(image.After) {
			delete(graph, id)
			if err := o.validate(ctx, single, reader, graph); err != nil {
				return err
			}
			validated = append(validated, *change)
			continue
		}
		after, err := decodeDocument(image.After)
		if err != nil {
			return err
		}
		if image.Op.Kind != model.OpCreate && exists {
			// All non-field document edits (links, reasons, metadata and parent)
			// already came from Writer's ordered operation application. Carry only
			// earlier ontology defaults/slot fields through that immutable raw image.
			after.Fields = copyFields(prior.doc.Fields)
		}
		switch image.Op.Kind {
		case model.OpCreate:
			for name := range after.Fields {
				change.SetBy["fields."+name] = "request"
			}
		case model.OpPatch:
			for name, value := range image.Op.Patch.Fields {
				if value == nil {
					delete(after.Fields, name)
				} else {
					after.Fields[name] = value
				}
				change.SetBy["fields."+name] = "request"
			}
		case model.OpClose:
			selected, err := decodeDocument(image.After)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(after.Fields["status"], selected.Fields["status"]) {
				after.Fields["status"] = selected.Fields["status"]
				change.SetBy["fields.status"] = "request"
			}
		}
		// Mutators still run once, before ontology. Their final owned-field values
		// are evaluated by every applicable operation guard; request values retain
		// declaration order. A mutator cannot supply an earlier source image.
		last := latest[id]
		var finalDoc pipeline.Document
		if hasDocument(last.After) {
			finalDoc, err = decodeDocument(last.After)
			if err != nil {
				return err
			}
		}
		for path, setter := range last.SetBy {
			if setter == "request" {
				continue
			}
			name, ok := strings.CutPrefix(path, "fields.")
			if !ok {
				continue
			}
			if value, present := finalDoc.Fields[name]; present {
				after.Fields[name] = value
			} else {
				delete(after.Fields, name)
			}
			change.SetBy[path] = setter
		}
		change.After, err = json.Marshal(after)
		if err != nil {
			return err
		}
		patches, err := o.Normalize(ctx, single, reader)
		if err != nil {
			return err
		}
		for _, patch := range patches {
			if patch.Resource.Ref.ID != id || patch.Resource.Kind != "bead" {
				return failure("internal_error", "ordered ontology normalized another resource")
			}
			for path, raw := range patch.Paths {
				name, ok := strings.CutPrefix(path, "fields.")
				if !ok || name == "" || strings.ContainsAny(name, "./\x00") {
					return failure("internal_error", "invalid ordered ontology field")
				}
				var value any
				decoder := json.NewDecoder(strings.NewReader(string(raw)))
				decoder.UseNumber()
				if err := decoder.Decode(&value); err != nil {
					return err
				}
				if value == nil {
					delete(after.Fields, name)
				} else {
					after.Fields[name] = value
				}
				change.SetBy[path] = "request"
			}
		}
		change.After, err = json.Marshal(after)
		if err != nil {
			return err
		}
		graph[id] = storedBead{doc: after}
		if err := o.validate(ctx, single, reader, graph); err != nil {
			if refusal, ok := err.(*model.Error); ok && refusal.Data.Code == "ontology_guard_failed" {
				details := make(map[string]any, len(refusal.Data.Details)+2)
				for name, value := range refusal.Data.Details {
					details[name] = value
				}
				details["opIndex"] = opIndex
				details["ref"] = image.Resource.Ref
				refusal.Data.Details = details
			}
			return err
		}
		validated = append(validated, *change)
	}
	// Select one final validation image per resource. The last genuine close
	// supplies its ordered pre-image even when the record was created earlier
	// in this batch or a later comment/patch shares its atomic final image.
	checks := map[string]port.Change{}
	order := []string{}
	for _, change := range validated {
		id := change.Resource.Ref.ID
		previous, seen := checks[id]
		if !seen {
			order = append(order, id)
		}
		if !seen || change.Op.Kind == model.OpClose || previous.Op.Kind != model.OpClose && previous.Op.Kind != model.OpCreate {
			checks[id] = change
		}
	}
	complete := final
	complete.Changes = nil
	for _, id := range order {
		change := checks[id]
		last := latest[id]
		change.After = last.After
		// A resource's final body includes every earlier write, but its last
		// close must retain that operation's status-touch provenance. Otherwise
		// a repeated same-target close inherits an earlier close's status touch
		// and is rejected as a generic update during settlement validation.
		setBy := make(map[string]string, len(last.SetBy))
		for path, setter := range last.SetBy {
			setBy[path] = setter
		}
		if change.Op.Kind == model.OpClose {
			if setter, touched := change.SetBy["fields.status"]; touched {
				setBy["fields.status"] = setter
			} else {
				delete(setBy, "fields.status")
			}
		}
		change.SetBy = setBy
		if !hasDocument(last.After) {
			change.Op = last.Op
		}
		complete.Changes = append(complete.Changes, change)
		if hasDocument(last.After) {
			doc, err := pipeline.DecodeDocument(last.After)
			if err != nil {
				return err
			}
			graph[id] = storedBead{doc: doc}
		} else {
			delete(graph, id)
		}
	}
	return o.validate(ctx, complete, reader, graph)
}
