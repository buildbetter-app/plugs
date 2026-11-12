package service

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const indexedHierarchyLimit = 5000

// indexedGuardGraph builds the complete fact slice used by ontology guards.
// Deletion retains the full scan because custom/association incoming links are
// outside the inverse selector's closed domain. Unavailable or exhausted
// membership also returns to that scan; it is never treated as absence.
func indexedGuardGraph(ctx context.Context, proposal port.ProposedChange, reader port.SnapshotReader, ontologies ...*Ontology) (map[string]storedBead, bool, error) {
	var o *Ontology
	if len(ontologies) != 0 {
		o = ontologies[0]
	}
	indexed, ok := reader.(port.HierarchySnapshotReader)
	if !ok || len(proposal.Changes) == 0 || len(proposal.Changes) > driver.SourceLinkMaxExcluded {
		return nil, false, nil
	}
	graph, overlay, childParents := map[string]storedBead{}, map[string]storedBead{}, map[string]bool{}
	parents := map[string]bool{}
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" {
			continue
		}
		if !hasDocument(change.After) || change.Op.Kind == model.OpDelete {
			return nil, false, nil
		}
		doc, err := decodeDocument(change.After)
		if err != nil || doc.ID != change.Resource.Ref.ID || driver.ValidateHierarchyRef(change.Resource.Ref) != nil {
			return nil, false, failure("ontology_guard_failed", "invalid indexed guard image")
		}
		// Both old and new parents remain guard subjects after a move. Their
		// own current status decides whether children are required.
		for _, raw := range [][]byte{change.Before, change.After} {
			if !hasDocument(raw) {
				continue
			}
			image, err := decodeDocument(raw)
			if err != nil {
				return nil, false, err
			}
			if image.Parent != nil {
				parents[image.Parent.ID] = true
			}
		}
		overlay[doc.ID] = storedBead{doc: doc}
		if o.childGuardFactsNeeded(doc) {
			childParents[doc.ID] = true
		}
	}
	if len(overlay) == 0 || len(overlay) > driver.SourceLinkMaxExcluded {
		return nil, false, nil
	}
	excluded := make([]model.Ref, 0, len(overlay))
	for id, row := range overlay {
		graph[id] = row
		excluded = append(excluded, model.Ref{ID: id})
	}
	sort.Slice(excluded, func(i, j int) bool { return excluded[i].ID < excluded[j].ID })
	queue := make([]string, 0, len(overlay)+len(childParents))
	queued, expanded, childDone := map[string]bool{}, map[string]bool{}, map[string]bool{}
	images := 0
	enqueue := func(id string) bool {
		if id == "" || queued[id] {
			return true
		}
		if len(queued) >= indexedHierarchyLimit {
			return false
		}
		queued[id] = true
		queue = append(queue, id)
		return true
	}
	for id := range overlay {
		if !enqueue(id) {
			return nil, false, nil
		}
	}
	for id := range parents {
		if !enqueue(id) {
			return nil, false, nil
		}
	}
	add := func(record driver.Record) error {
		if record.Kind != "bead" || driver.ValidateHierarchyRef(record.Ref) != nil {
			return failure("ontology_guard_failed", "invalid indexed guard source")
		}
		doc, err := decodeDocument(record.Body)
		if err != nil || doc.ID != record.Ref.ID {
			return failure("ontology_guard_failed", "invalid indexed guard source document")
		}
		if _, changed := overlay[doc.ID]; changed {
			return failure("ontology_guard_failed", "indexed guard returned an excluded source")
		}
		if prior, exists := graph[doc.ID]; exists && (prior.record.Revision.String() != record.Revision.String() || !bytes.Equal(prior.record.Body, record.Body)) {
			return failure("internal_error", "indexed guard source changed within snapshot")
		}
		if _, exists := graph[doc.ID]; !exists && len(graph) >= indexedHierarchyLimit {
			return driver.ErrUnsupported
		}
		graph[doc.ID] = storedBead{record: record, doc: doc}
		if !enqueue(doc.ID) {
			return driver.ErrUnsupported
		}
		return nil
	}
	for len(queue) != 0 || len(childDone) < len(childParents) {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if len(queued) > indexedHierarchyLimit || len(childParents) > indexedHierarchyLimit || len(graph) > indexedHierarchyLimit {
			return nil, false, nil
		}
		for len(queue) != 0 {
			// Check before the next Get. A chain of ancestors or outgoing
			// links must not decode the whole store before a later fallback.
			if len(queued) > indexedHierarchyLimit || images >= indexedHierarchyLimit {
				return nil, false, nil
			}
			id := queue[0]
			queue = queue[1:]
			if _, exists := graph[id]; !exists {
				records, err := reader.Get(ctx, []model.Ref{{ID: id}})
				if err != nil {
					return nil, false, err
				}
				if len(records) > 1 || len(records) == 1 && records[0].Ref.ID != id {
					return nil, false, failure("internal_error", "indexed guard Get returned wrong source")
				}
				images += len(records)
				if len(records) != 0 {
					if err := add(records[0]); err != nil {
						if errors.Is(err, driver.ErrUnsupported) {
							return nil, false, nil
						}
						return nil, false, err
					}
				}
			}
			if expanded[id] {
				continue
			}
			expanded[id] = true
			row, exists := graph[id]
			if !exists {
				continue
			}
			if o.childGuardFactsNeeded(row.doc) {
				childParents[id] = true
			}
			if row.doc.Parent != nil {
				if !enqueue(row.doc.Parent.ID) {
					return nil, false, nil
				}
			}
			for _, link := range row.doc.Links {
				if link.TargetKind == model.LinkTargetExternal {
					continue
				}
				switch link.Kind {
				case "parent-child", "blocks", "conditional-blocks", "waits-for":
					if !enqueue(link.To.ID) {
						return nil, false, nil
					}
				}
				if link.Kind == "waits-for" {
					if len(childParents) >= indexedHierarchyLimit && !childParents[link.To.ID] {
						return nil, false, nil
					}
					childParents[link.To.ID] = true
				}
			}
		}
		for id := range childParents {
			if childDone[id] {
				continue
			}
			childDone[id] = true
			parent := model.Ref{ID: id}
			if driver.ValidateHierarchyRef(parent) != nil {
				return nil, false, failure("ontology_guard_failed", "invalid child guard target")
			}
			memberOverlay := 0
			for _, row := range overlay {
				member := row.doc.Parent != nil && row.doc.Parent.ID == id
				for _, link := range row.doc.Links {
					member = member || link.TargetKind != model.LinkTargetExternal && link.Kind == "parent-child" && link.To.ID == id
				}
				if member {
					memberOverlay++
				}
			}
			remaining := indexedHierarchyLimit - images - memberOverlay
			if remaining < 0 {
				return nil, false, nil
			}
			canonical, more, err := indexed.HierarchyChildrenExcluding(ctx, []model.Ref{parent}, excluded, remaining)
			if errors.Is(err, driver.ErrUnsupported) || more {
				return nil, false, nil
			}
			if err != nil {
				return nil, false, err
			}
			images += len(canonical)
			for _, record := range canonical {
				if err := add(record); err != nil {
					if errors.Is(err, driver.ErrUnsupported) {
						return nil, false, nil
					}
					return nil, false, err
				}
			}
			remaining = indexedHierarchyLimit - images
			if remaining < 0 {
				return nil, false, nil
			}
			legacy, more, err := indexed.InverseSourceLinks(ctx, []model.Ref{parent}, []driver.IncomingLinkKind{driver.IncomingLinkParentChild}, excluded, remaining)
			if errors.Is(err, driver.ErrUnsupported) || more {
				return nil, false, nil
			}
			if err != nil {
				return nil, false, err
			}
			images += len(legacy)
			for _, record := range legacy {
				if err := add(record); err != nil {
					if errors.Is(err, driver.ErrUnsupported) {
						return nil, false, nil
					}
					return nil, false, err
				}
			}
		}
	}
	return graph, true, nil
}

// validateHierarchyParents checks guards whose target is the parent's current
// status. The same target-status set includes create/transition child guards.
// An ordinary child added under an open parent must not enumerate siblings
// merely because that workflow has a guard on a different (closed) status.
func (o *Ontology) childGuardFactsNeeded(doc pipeline.Document) bool {
	if o == nil {
		return true // callers without ontology retain complete child facts
	}
	effective := o.effective(doc.Project).Document()
	typ, ok := effective.Types[doc.Type]
	if !ok {
		return false // the ordinary ontology validator reports the missing type
	}
	for _, transition := range effective.Workflows[typ.Workflow].Transitions {
		if transition.To != state(doc).Status {
			continue
		}
		for _, guard := range transition.Guards {
			if guard.Kind == ontology.NoOpenChildren {
				return true
			}
		}
	}
	return false
}
