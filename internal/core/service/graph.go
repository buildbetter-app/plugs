package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type queryReader interface {
	Query(context.Context, driver.Query) (driver.Page[driver.Record], error)
}

type recordGetter interface {
	Get(context.Context, []driver.Ref) ([]driver.Record, error)
}

type storedBead struct {
	record driver.Record
	doc    pipeline.Document
}

// Strict envelope decoding preserves canonical direct parents; a malformed
// relation cannot become an observed absence in a guard or count query.
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
	if doc.Parent != nil {
		if err := doc.Parent.Validate(); err != nil || !doc.Parent.IsID() || doc.Parent.ID == "" || !doc.Parent.Branch.IsZero() {
			return doc, failure("invalid_params", "invalid stored parent")
		}
		if _, err := driver.NewClosureSentinel(doc.Parent.ID); err != nil {
			return doc, failure("invalid_params", "invalid stored parent")
		}
	}
	// Preserve member presence before Go zero values erase null/empty provenance.
	var envelope struct {
		Comments json.RawMessage `json:"comments"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		return doc, failure("invalid_params", "invalid bead document")
	}
	if len(envelope.Comments) != 0 {
		var comments []map[string]json.RawMessage
		if bytes.Equal(bytes.TrimSpace(envelope.Comments), []byte("null")) || json.Unmarshal(envelope.Comments, &comments) != nil {
			return doc, failure("invalid_params", "invalid stored comments")
		}
		for _, comment := range comments {
			for key := range comment {
				switch key {
				case "body", "principal", "time", "origin", "sourceAuthor", "sourceTime":
				default:
					return doc, failure("invalid_params", "unsupported stored comment member")
				}
			}
			for _, key := range []string{"body", "principal", "time"} {
				value, exists := comment[key]
				if !exists || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
					return doc, failure("invalid_params", "invalid stored comment member")
				}
			}
			origin, imported := comment["origin"]
			if !imported {
				if _, exists := comment["sourceAuthor"]; exists {
					return doc, failure("invalid_params", "legacy comment has source provenance")
				}
				if _, exists := comment["sourceTime"]; exists {
					return doc, failure("invalid_params", "legacy comment has source provenance")
				}
			} else {
				var value string
				if json.Unmarshal(origin, &value) != nil || value != "import" {
					return doc, failure("invalid_params", "invalid comment origin")
				}
				if at, exists := comment["sourceTime"]; !exists || bytes.Equal(bytes.TrimSpace(at), []byte("null")) {
					return doc, failure("invalid_params", "missing comment source time")
				}
				if author, exists := comment["sourceAuthor"]; exists {
					var value string
					if bytes.Equal(bytes.TrimSpace(author), []byte("null")) || json.Unmarshal(author, &value) != nil || value == "" {
						return doc, failure("invalid_params", "invalid comment source author")
					}
				}
			}
		}
	}
	for _, comment := range doc.Comments {
		if err := comment.Validate(); err != nil {
			return doc, failure("invalid_params", "invalid stored comment provenance")
		}
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
// a partial graph as complete. ClaimNext relies on this uncapped lazy walk so
// large ready sets remain traversable without a hidden terminal null.
func scanBeads(ctx context.Context, r queryReader) (map[string]storedBead, error) {
	return scanBeadsWithRewrite(ctx, r, nil)
}

// scanBeadsWithRewrite narrows the initial candidate query when a trusted
// deny-only rewrite is present, then loads the complete dependency closure
// needed by readiness (parents and every referenced edge). Missing closure
// rows remain absent and therefore retain the ordinary not-ready result.
func scanBeadsWithRewrite(ctx context.Context, r queryReader, rewrite *driver.QueryRewrite) (map[string]storedBead, error) {
	getter, canGet := r.(recordGetter)
	if rewrite != nil && rewrite.IDs != nil && !canGet {
		// A query-only test reader cannot supply the dependency closure that
		// readiness needs after candidate narrowing. Use the complete query path
		// rather than treating an absent parent or blocker as an observed row.
		return scanBeads(ctx, r)
	}
	graph := map[string]storedBead{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Rewrite: rewrite, Cursor: cursor, Limit: 10000})
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
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			return nil, failure("internal_error", "driver returned a non-advancing page")
		}
		cursor = page.NextCursor
	}
	if rewrite == nil || rewrite.IDs == nil || len(graph) == 0 {
		return graph, nil
	}
	for _, row := range graph {
		if hasWaitsFor(row.doc) {
			// waits-for evaluates the complete reverse child set through
			// noOpenChildren. Keep that graph-wide fact exact instead of
			// approximating it from a narrowed candidate query.
			return scanBeads(ctx, r)
		}
	}
	// A ready candidate can depend on a parent, blocker, or parent-child edge;
	// ancestors can have blockers of their own. Walk that finite reference
	// closure in batches so the candidate rewrite never changes readiness facts.
	queue := make([]model.Ref, 0, len(graph))
	seen := make(map[string]struct{}, len(graph))
	for id, row := range graph {
		seen[id] = struct{}{}
		for _, ref := range appendReadyRefs(nil, row.doc) {
			if _, exists := seen[ref.ID]; exists {
				continue
			}
			seen[ref.ID] = struct{}{}
			queue = append(queue, ref)
		}
	}
	for len(queue) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batchSize := len(queue)
		if batchSize > 1000 {
			batchSize = 1000
		}
		batch := queue[:batchSize]
		queue = queue[batchSize:]
		records, err := getter.Get(ctx, batch)
		if err != nil {
			return nil, mapped(err)
		}
		for _, record := range records {
			if record.Kind != "bead" {
				continue
			}
			doc, err := decodeDocument(record.Body)
			if err != nil || doc.ID != record.Ref.ID || !record.Ref.Branch.IsZero() {
				return nil, failure("internal_error", "invalid persisted bead")
			}
			if _, exists := graph[doc.ID]; exists {
				continue
			}
			graph[doc.ID] = storedBead{record, doc}
			if hasWaitsFor(doc) {
				return scanBeads(ctx, r)
			}
			for _, ref := range appendReadyRefs(nil, doc) {
				if _, exists := seen[ref.ID]; exists {
					continue
				}
				seen[ref.ID] = struct{}{}
				queue = append(queue, ref)
			}
		}
	}
	return graph, nil
}

func appendReadyRefs(refs []model.Ref, doc pipeline.Document) []model.Ref {
	if doc.Parent != nil && doc.Parent.IsID() && doc.Parent.Branch.IsZero() {
		refs = append(refs, *doc.Parent)
	}
	for _, link := range doc.Links {
		switch link.Kind {
		case "blocks", "conditional-blocks", "waits-for", "parent-child":
		default:
			continue
		}
		if link.To.IsID() && link.To.Branch.IsZero() {
			refs = append(refs, link.To)
		}
	}
	return refs
}

func hasWaitsFor(doc pipeline.Document) bool {
	for _, link := range doc.Links {
		if link.Kind == "waits-for" {
			return true
		}
	}
	return false
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
	terminal := func(target storedBead) bool {
		effective := o.effective(target.doc.Project)
		return effective.SatisfiesBlocks(state(target.doc).Status)
	}
	for _, link := range doc.Links {
		switch link.Kind {
		case "relates-to", "related", "discovered-from", "replies-to", "duplicates", "supersedes", "authored-by", "assigned-to", "approved-by", "attests", "tracks", "until", "caused-by", "validates", "delegated-from":
			continue
		case "blocks", "conditional-blocks", "waits-for":
			blocker, exists := graph[link.To.ID]
			if !exists || !link.To.IsID() || !link.To.Branch.IsZero() {
				return false, nil
			}
			s := state(blocker.doc)
			effective := o.effective(s.Project)
			satisfied := terminal(blocker)
			if link.Kind == "conditional-blocks" {
				satisfied = effective.SatisfiesConditionalBlocks(s.Status)
			}
			if link.Kind == "waits-for" && !satisfied {
				// A waits-for edge is a fanout gate. With no gate metadata on
				// the compact Plug link envelope, an open target is satisfied
				// once all of its explicit or linked children are terminal.
				satisfied = o.noOpenChildren(blocker.doc, graph)
			}
			if !satisfied {
				return false, nil
			}
		default:
			// Valid custom dependency kinds are retained but have no core
			// scheduling or blocker effect.
			continue
		}
	}
	return true, nil
}

// readyDependencies extends the close-guard blocker predicate with the
// scheduling-only hierarchy edges. Parent-child and an explicit parent affect
// ready membership, but they are not blockers for closing the child itself.
func (o *Ontology) readyDependencies(doc pipeline.Document, graph map[string]storedBead) (bool, error) {
	ready, err := o.noOpenBlockers(doc, graph)
	if err != nil || !ready {
		return ready, err
	}
	terminal := func(target storedBead) bool {
		effective := o.effective(target.doc.Project)
		return effective.SatisfiesBlocks(state(target.doc).Status)
	}
	for _, link := range doc.Links {
		if link.Kind != "parent-child" {
			continue
		}
		target, exists := graph[link.To.ID]
		if !exists || !link.To.IsID() || !link.To.Branch.IsZero() || !terminal(target) {
			return false, nil
		}
	}
	return o.readyAncestors(doc, graph)
}

// readyAncestors applies canonical parent scheduling to ready and fresh claims.
// It deliberately leaves close and settlement's direct blocker guard unchanged.
func (o *Ontology) readyAncestors(doc pipeline.Document, graph map[string]storedBead) (bool, error) {
	seen := map[string]bool{doc.ID: true}
	for doc.Parent != nil {
		parent := doc.Parent
		row, exists := graph[parent.ID]
		if !exists || !parent.IsID() || !parent.Branch.IsZero() || seen[parent.ID] {
			return false, nil
		}
		// An open parent alone is not a blocker. Deferred/backlog ancestors
		// and their live blockers suppress the entire subtree, using each
		// ancestor's own project ontology from the same complete snapshot.
		category, known := o.effective(row.doc.Project).Category(state(row.doc).Status)
		if !known || category == ontology.Backlog {
			return false, nil
		}
		unblocked, err := o.noOpenBlockers(row.doc, graph)
		if err != nil || !unblocked {
			return unblocked, err
		}
		seen[parent.ID] = true
		doc = row.doc
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
	parent := ""
	if doc.Parent != nil {
		parent = doc.Parent.ID
	}
	return Bead{ID: doc.ID, Type: doc.Type, Project: doc.Project, Revision: row.record.Revision, Status: state(doc).Status, Fields: doc.Fields, Metadata: metadata, Labels: append([]string(nil), doc.Labels...), Parent: parent}, nil
}

func beadPriority(bead Bead) (int, error) {
	fields := make(map[string]any, len(bead.Fields))
	for key, value := range bead.Fields {
		fields[key] = value
	}
	return priority(pipeline.Document{ID: bead.ID, Type: bead.Type, Project: bead.Project, Fields: fields})
}

func sortedIDs(graph map[string]storedBead) []string {
	ids := make([]string, 0, len(graph))
	for id := range graph {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
