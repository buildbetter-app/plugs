package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type queryReader interface {
	Query(context.Context, driver.Query) (driver.Page[driver.Record], error)
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
	for _, at := range []*time.Time{doc.CreatedAt, doc.StartedAt, doc.ClosedAt} {
		if at != nil && at.IsZero() {
			return doc, failure("invalid_params", "invalid stored lifecycle time")
		}
	}
	if doc.Parent != nil {
		if err := doc.Parent.Validate(); err != nil || !doc.Parent.IsID() || doc.Parent.ID == "" || !doc.Parent.Branch.IsZero() {
			return doc, failure("invalid_params", "invalid stored parent")
		}
		if _, err := driver.NewClosureSentinel(doc.Parent.ID); err != nil {
			return doc, failure("invalid_params", "invalid stored parent")
		}
	}
	for i, link := range doc.Links {
		kind, err := link.TargetKind.Normalized()
		if err != nil || model.ValidateLinkTarget(kind, link.To) != nil {
			return doc, failure("invalid_params", "invalid stored link target")
		}
		doc.Links[i].TargetKind = kind
	}
	parent, err := pipeline.HierarchyParent(doc)
	if err != nil {
		return doc, err
	}
	doc.Parent = parent
	// Preserve member presence before Go zero values erase null/empty provenance.
	var envelope struct {
		Comments  json.RawMessage `json:"comments"`
		CreatedAt json.RawMessage `json:"createdAt"`
		StartedAt json.RawMessage `json:"startedAt"`
		ClosedAt  json.RawMessage `json:"closedAt"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		return doc, failure("invalid_params", "invalid bead document")
	}
	for _, at := range []json.RawMessage{envelope.CreatedAt, envelope.StartedAt, envelope.ClosedAt} {
		if bytes.Equal(bytes.TrimSpace(at), []byte("null")) {
			return doc, failure("invalid_params", "stored lifecycle time must be an instant or absent")
		}
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

func (o *Ontology) noOpenBlockers(doc pipeline.Document, graph map[string]storedBead, work ...context.Context) (bool, error) {
	if blocked, _ := doc.Fields["blocked"].(bool); blocked {
		return false, nil
	}
	terminal := func(target storedBead) bool {
		effective := o.effective(target.doc.Project)
		return effective.SatisfiesBlocks(state(target.doc).Status)
	}
	for _, link := range doc.Links {
		readinessVisit(work)
		switch link.Kind {
		case "relates-to", "related", "discovered-from", "replies-to", "duplicates", "supersedes", "authored-by", "assigned-to", "approved-by", "attests", "tracks", "until", "caused-by", "validates", "delegated-from":
			continue
		case "blocks", "conditional-blocks", "waits-for":
			if link.TargetKind == model.LinkTargetExternal {
				// External blockers remain unknown, even if their exact ID
				// happens to name a terminal bead in the local graph.
				return false, nil
			}
			blocker, exists := graph[link.To.ID]
			if !exists || !link.To.IsID() || !link.To.Branch.IsZero() {
				// Keep the unresolved edge in the source image, but do not
				// infer that an external blocker is terminal. Readiness is
				// deliberately fail-closed until the target can be read.
				return false, nil
			}
			s := state(blocker.doc)
			effective := o.effective(s.Project)
			satisfied := terminal(blocker)
			if link.Kind == "conditional-blocks" {
				satisfied = effective.SatisfiesConditionalBlocks(s.Status)
			}
			if link.Kind == "waits-for" {
				satisfied = o.waitsForSatisfied(link, blocker, graph, work...)
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

// waitsForSatisfied evaluates the complete tracked graph already acquired by
// the ontology guard. Explicit metadata follows the pinned waits-for formula:
// an open child blocks unless any-children has a terminal child; also_blocks
// additionally requires the spawner itself to be terminal.
func (o *Ontology) waitsForSatisfied(link pipeline.Link, spawner storedBead, graph map[string]storedBead, work ...context.Context) bool {
	terminal := func(row storedBead) bool {
		return o.effective(row.doc.Project).SatisfiesBlocks(state(row.doc).Status)
	}
	if link.Metadata == nil {
		// Preserve the legacy terminal-spawner/all-children fallback.
		return terminal(spawner) || o.noOpenChildren(spawner.doc, graph, work...)
	}
	if link.Metadata.AlsoBlocks && !terminal(spawner) {
		return false
	}
	openChild, terminalChild := false, false
	for _, child := range graph {
		readinessVisit(work)
		member := child.doc.Parent != nil && child.doc.Parent.ID == spawner.doc.ID
		if !member {
			for _, edge := range child.doc.Links {
				readinessVisit(work)
				if edge.TargetKind != model.LinkTargetExternal && edge.Kind == "parent-child" && edge.To.ID == spawner.doc.ID {
					member = true
					break
				}
			}
		}
		if member {
			if terminal(child) {
				terminalChild = true
			} else {
				openChild = true
			}
		}
	}
	return !openChild || link.Metadata.Gate == "any-children" && terminalChild
}

// readyDependencies extends the close-guard blocker predicate with the
// scheduling-only hierarchy edges. Parent-child and an explicit parent affect
// ready membership, but they are not blockers for closing the child itself.
func (o *Ontology) readyDependencies(doc pipeline.Document, graph map[string]storedBead, work ...context.Context) (bool, error) {
	ready, err := o.noOpenBlockers(doc, graph, work...)
	if err != nil || !ready {
		return ready, err
	}
	for _, link := range doc.Links {
		readinessVisit(work)
		if link.Kind == "parent-child" && link.TargetKind == model.LinkTargetExternal {
			return false, nil
		}
	}
	return o.readyAncestors(doc, graph, work...)
}

// readyAncestors applies canonical parent scheduling to ready and fresh claims.
// It deliberately leaves close and settlement's direct blocker guard unchanged.
func (o *Ontology) readyAncestors(doc pipeline.Document, graph map[string]storedBead, work ...context.Context) (bool, error) {
	seen := map[string]bool{doc.ID: true}
	for {
		parent, err := pipeline.HierarchyParent(doc)
		if err != nil {
			return false, err
		}
		if parent == nil {
			return true, nil
		}
		readinessVisit(work)
		row, exists := graph[parent.ID]
		if !exists || !parent.IsID() || !parent.Branch.IsZero() || seen[parent.ID] {
			return false, nil
		}
		// An open parent alone is not a blocker. Deferred/backlog ancestors
		// and their live blockers suppress the entire subtree, using each
		// ancestor's own project ontology from the same complete snapshot.
		category, known := o.effective(row.doc.Project).Category(state(row.doc).Status)
		if !known || category.Backlog() {
			return false, nil
		}
		unblocked, err := o.noOpenBlockers(row.doc, graph, work...)
		if err != nil || !unblocked {
			return unblocked, err
		}
		seen[parent.ID] = true
		doc = row.doc
	}
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
	createdAt, _ := doc.CreationTime()
	bead := Bead{ID: doc.ID, Type: doc.Type, Project: doc.Project, Revision: row.record.Revision, Status: state(doc).Status, Fields: doc.Fields, Metadata: metadata, Labels: append([]string(nil), doc.Labels...), Parent: parent, Blocked: doc.Fields["blocked"] == true, CreatedAt: createdAt}
	for _, link := range doc.Links {
		bead.Links = append(bead.Links, workflowplugin.RelationView{From: doc.ID, To: link.To, Kind: link.Kind, Metadata: link.Metadata})
	}
	return bead, nil
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
