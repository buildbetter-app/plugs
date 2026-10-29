package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const scanLimit = 20000

type queryReader interface {
	Query(context.Context, driver.Query) (driver.Page[driver.Record], error)
}

type storedBead struct {
	record driver.Record
	doc    pipeline.Document
}

// The supported envelope has no parent relation. Rejecting unknown envelope
// members is essential: a full scan then proves the absence of children rather
// than silently ignoring a relation this slice cannot evaluate.
func decodeDocument(raw []byte) (pipeline.Document, error) {
	var doc pipeline.Document
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return doc, failure("invalid_params", "unsupported bead document shape")
	}
	if err := dec.Decode(new(any)); err != io.EOF || doc.ID == "" || doc.Type == "" || doc.Fields == nil {
		return doc, failure("invalid_params", "invalid bead document")
	}
	if v, exists := doc.Fields["status"]; exists {
		if _, ok := v.(string); !ok {
			return doc, failure("invalid_params", "status must be a string")
		}
	}
	if _, violations := ontology.IsTemplate(state(doc)); len(violations) != 0 {
		return doc, violationsError(violations)
	}
	if _, err := priority(doc); err != nil {
		return doc, err
	}
	return doc, nil
}

func priority(doc pipeline.Document) (int, error) {
	p, violations := ontology.Priority(state(doc))
	return p, violationsError(violations)
}

// scanBeads reads one pinned snapshot, including every page, and never reports
// a partial graph as complete. This bounded slice has no projection dependency.
func scanBeads(ctx context.Context, r queryReader) (map[string]storedBead, error) {
	graph := map[string]storedBead{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			return nil, mapped(err)
		}
		for _, record := range page.Items {
			doc, err := decodeDocument(record.Body)
			if err != nil || doc.ID != record.Ref.ID || !record.Ref.Branch.IsZero() {
				return nil, failure("internal_error", "invalid persisted bead")
			}
			if _, exists := graph[doc.ID]; exists {
				return nil, failure("internal_error", "duplicate bead in snapshot")
			}
			graph[doc.ID] = storedBead{record, doc}
			if len(graph) > scanLimit {
				return nil, failure("scope_too_large", "local graph scan exceeds 20000 beads")
			}
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			return nil, failure("internal_error", "driver returned a non-advancing page")
		}
		cursor = page.NextCursor
	}
	return graph, nil
}

func state(doc pipeline.Document) ontology.BeadState {
	fields := make(map[string]any, len(doc.Fields))
	for k, v := range doc.Fields {
		if k != "status" {
			fields[k] = v
		}
	}
	status, _ := doc.Fields["status"].(string)
	template, _ := doc.Fields["template"].(bool)
	return ontology.BeadState{ID: doc.ID, Project: doc.Project, Type: doc.Type, Status: status, Template: template, Fields: fields}
}

func (o *Ontology) noOpenBlockers(doc pipeline.Document, graph map[string]storedBead) (bool, error) {
	for _, link := range doc.Links {
		switch link.Kind {
		case "relates-to":
			continue
		case "blocks", "conditional-blocks":
			blocker, exists := graph[link.To.ID]
			if !exists || !link.To.IsID() || !link.To.Branch.IsZero() {
				return false, nil
			}
			s := state(blocker.doc)
			effective := o.effective(s.Project)
			satisfied := effective.SatisfiesBlocks(s.Status)
			if link.Kind == "conditional-blocks" {
				satisfied = effective.SatisfiesConditionalBlocks(s.Status)
			}
			if !satisfied {
				return false, nil
			}
		default:
			return false, failure("invalid_params", "dependency kind is not supported by the local slice")
		}
	}
	return true, nil
}

func beadValue(row storedBead) (Bead, error) {
	doc := row.doc
	metadata := map[string]any{}
	for k, raw := range doc.Metadata {
		var v any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return Bead{}, failure("internal_error", "invalid persisted metadata")
		}
		metadata[k] = v
	}
	return Bead{ID: doc.ID, Type: doc.Type, Project: doc.Project, Revision: row.record.Revision, Status: state(doc).Status, Fields: doc.Fields, Metadata: metadata}, nil
}

func sortedIDs(graph map[string]storedBead) []string {
	ids := make([]string, 0, len(graph))
	for id := range graph {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
