package pipeline

import (
	"bytes"
	"context"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// hierarchySources is the ordered canonical-parent prefix, not a complete
// link/scheduling graph. Deleted sources remain in this prefix for later moves;
// only the final image removes them. The overlay contains actual hierarchy
// edits, never the read-only ancestor entries in prepared.writes.
type hierarchySources struct {
	snapshot *snapshotReader
	records  map[string]driver.Record
	stored   map[string]Document
	current  map[string]Document
	parents  map[string]string
	created  map[string]bool
	overlay  map[string]bool
	deleted  map[string]bool
	final    bool
}

func hierarchyRefs(ids map[string]bool) []model.Ref {
	refs := make([]model.Ref, 0, len(ids))
	for id := range ids {
		refs = append(refs, model.Ref{ID: id})
	}
	sort.Slice(refs, func(i, j int) bool { return recordLockKey(refs[i]) < recordLockKey(refs[j]) })
	return refs
}

func (s *hierarchySources) retain(record driver.Record) error {
	if driver.ValidateHierarchyRef(record.Ref) != nil || record.Kind != "bead" {
		return failure("ontology_guard_failed", "invalid hierarchy source image")
	}
	doc, err := DecodeDocument(record.Body)
	if err != nil || doc.ID != record.Ref.ID || driver.ValidateHierarchyRecord(record, doc.Parent) != nil {
		return failure("ontology_guard_failed", "invalid stored hierarchy document")
	}
	if prior, exists := s.records[doc.ID]; exists {
		if prior.Revision.String() != record.Revision.String() || !bytes.Equal(prior.Body, record.Body) {
			return failure("internal_error", "hierarchy source changed within the pinned snapshot")
		}
		return nil
	}
	s.records[doc.ID], s.stored[doc.ID] = record, doc
	if _, exists := s.parents[doc.ID]; !exists {
		s.parents[doc.ID] = parentID(doc.Parent)
	}
	return nil
}

func (s *hierarchySources) load(ctx context.Context, id string) error {
	if _, exists := s.records[id]; exists {
		return nil
	}
	ref := model.Ref{ID: id}
	if driver.ValidateHierarchyRef(ref) != nil {
		return failure("ontology_guard_failed", "invalid hierarchy source reference")
	}
	records, err := s.snapshot.Get(ctx, []model.Ref{ref})
	if err != nil {
		return err
	}
	if len(records) != 1 || recordLockKey(records[0].Ref) != recordLockKey(ref) {
		return failure("ontology_guard_failed", "parent is missing from hierarchy")
	}
	return s.retain(records[0])
}

// depths reads either the ordered prefix or the original source graph. Both
// paths retain exact source observations; original chains are needed even when
// an earlier prefix edit has moved an ancestor elsewhere.
func (s *hierarchySources) depths(ctx context.Context, id string, original bool) (map[string]uint64, error) {
	depths := map[string]uint64{}
	for id != "" {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, cycle := depths[id]; cycle {
			return nil, failure("ontology_guard_failed", "parent would form a cycle")
		}
		// A source created in this ordered batch has no pre-state image. Stop
		// the original chain before adding it; its final image is represented by
		// the after graph and its absent before closure is validated below.
		if original && s.created[id] {
			break
		}
		if _, exists := s.parents[id]; original || !exists {
			if err := s.load(ctx, id); err != nil {
				return nil, err
			}
		}
		depths[id] = uint64(len(depths))
		if len(depths) > maxClosureRows {
			return nil, failure("closure_too_large", "ancestor closure exceeds 5000 affected rows")
		}
		if original {
			id = parentID(s.stored[id].Parent)
		} else {
			id = s.parents[id]
		}
	}
	return depths, nil
}

// children excludes all earlier prefix images before the storage limit, then
// merges known matching members exactly once. More describes eligible base
// membership after those exclusions. There is no Query or over-fetch fallback.
func (s *hierarchySources) children(ctx context.Context, parents []model.Ref, allowance int) ([]string, bool, error) {
	selected := map[string]bool{}
	for _, parent := range parents {
		selected[parent.ID] = true
	}
	ids := make([]string, 0)
	for id := range s.overlay {
		if s.final && s.deleted[id] {
			continue
		}
		if selected[s.parents[id]] {
			ids = append(ids, id)
		}
	}
	// Even an overflowing prepared prefix needs the indexed coverage and
	// graph-membership witness. A local count alone cannot certify absence of
	// an unsupported or stale source index.
	if len(ids) > allowance {
		_, _, err := s.snapshot.HierarchyChildrenExcluding(ctx, parents, hierarchyRefs(s.overlay), 0)
		if err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}
	limit := allowance - len(ids)
	var records []driver.Record
	var more bool
	var err error
	if len(s.overlay) == 0 {
		records, more, err = s.snapshot.HierarchyChildren(ctx, parents, limit)
	} else {
		records, more, err = s.snapshot.HierarchyChildrenExcluding(ctx, parents, hierarchyRefs(s.overlay), limit)
	}
	if err != nil {
		return nil, false, err
	}
	for _, record := range records {
		if s.overlay[record.Ref.ID] {
			return nil, false, failure("internal_error", "driver returned an excluded hierarchy image")
		}
		if err := s.retain(record); err != nil {
			return nil, false, err
		}
		if !selected[s.parents[record.Ref.ID]] {
			return nil, false, failure("ontology_guard_failed", "hierarchy index differs from canonical Parent")
		}
		ids = append(ids, record.Ref.ID)
	}
	sort.Slice(ids, func(i, j int) bool {
		return recordLockKey(model.Ref{ID: ids[i]}) < recordLockKey(model.Ref{ID: ids[j]})
	})
	return ids, more, nil
}

func (s *hierarchySources) subtree(ctx context.Context, id string, cap int) ([]string, error) {
	ids, seen := []string{id}, map[string]bool{id: true}
	frontier := []model.Ref{{ID: id}}
	for len(frontier) != 0 {
		next := map[string]bool{}
		for start := 0; start < len(frontier); start += driver.HierarchyMaxParents {
			end := start + driver.HierarchyMaxParents
			if end > len(frontier) {
				end = len(frontier)
			}
			children, more, err := s.children(ctx, frontier[start:end], cap-len(ids))
			if err != nil {
				return nil, err
			}
			for _, child := range children {
				if seen[child] {
					return nil, failure("ontology_guard_failed", "duplicate or cyclic hierarchy membership")
				}
				seen[child], next[child] = true, true
				ids = append(ids, child)
			}
			if more {
				return nil, failure("closure_too_large", "ordered batch affects more than 5000 closure rows")
			}
		}
		frontier = hierarchyRefs(next)
	}
	return ids, nil
}

func (p *prepared) hierarchyIndexed(ctx context.Context) error {
	s := &hierarchySources{snapshot: p.snapshot, records: map[string]driver.Record{}, stored: map[string]Document{}, current: map[string]Document{}, parents: map[string]string{}, created: map[string]bool{}, overlay: map[string]bool{}, deleted: map[string]bool{}}
	for _, edit := range p.hierarchyEdits {
		if pending, exists := p.writes[edit.id]; exists && !pending.deleted {
			s.current[edit.id] = pending.doc
		}
	}
	affected, count := map[string]bool{}, 0
	for _, edit := range p.hierarchyEdits {
		if edit.create {
			ref := model.Ref{ID: edit.id}
			if driver.ValidateHierarchyRef(ref) != nil {
				return failure("ontology_guard_failed", "invalid created hierarchy source")
			}
			records, err := p.snapshot.Get(ctx, []model.Ref{ref})
			if err != nil {
				return err
			}
			if _, exists := s.parents[edit.id]; exists || len(records) != 0 {
				return failure("ontology_guard_failed", "duplicate created hierarchy source")
			}
			s.parents[edit.id] = edit.parent
			s.created[edit.id] = true
		}
		before, err := s.depths(ctx, edit.id, false)
		if err != nil {
			return err
		}
		ids := []string{edit.id}
		charge := len(before)
		if !edit.create && !edit.deleted {
			after := map[string]uint64{edit.id: 0}
			external, err := s.depths(ctx, edit.parent, false)
			if err != nil {
				return err
			}
			if _, cycle := external[edit.id]; cycle {
				return failure("ontology_guard_failed", "parent would form a cycle")
			}
			for ancestor, depth := range external {
				after[ancestor] = depth + 1
			}
			charge = closureDifference(before, after)
			if charge > 0 {
				// Divide before discovering children or reading any closure. Each
				// unchanged subtree member has this same external-pair difference,
				// including shared-ancestor depth replacements counted once.
				cap := (maxClosureRows - count) / charge
				if cap == 0 {
					return failure("closure_too_large", "ordered batch affects more than 5000 closure rows")
				}
				ids, err = s.subtree(ctx, edit.id, cap)
				if err != nil {
					return err
				}
			}
			s.parents[edit.id] = edit.parent
		}
		if charge > maxClosureRows-count {
			return failure("closure_too_large", "ordered batch affects more than 5000 closure rows")
		}
		count += charge * len(ids) // subtree already bounded by remaining/charge
		for _, id := range ids {
			affected[id] = true
			if _, err := s.depths(ctx, id, false); err != nil {
				return err
			}
		}
		s.overlay[edit.id] = true
		if edit.deleted {
			s.deleted[edit.id] = true
		}
	}
	// Prepared documents contain the final image of each edited source. During
	// ordered discovery an earlier move can legitimately differ from that final
	// parent, so validate agreement only after all ordered edits have applied.
	for id, doc := range s.current {
		if s.deleted[id] {
			continue
		}
		if parent, exists := s.parents[id]; !exists || parentID(doc.Parent) != parent {
			return failure("ontology_guard_failed", "prepared hierarchy image differs from its final Parent")
		}
	}
	// A deleted source remains available throughout ordered accounting. Only
	// now require every final canonical child to have detached or been deleted.
	s.final = true
	for id := range s.deleted {
		_, more, err := s.children(ctx, []model.Ref{{ID: id}}, 0)
		if err != nil {
			return err
		}
		if more {
			return failure("ontology_guard_failed", "parent must exist in the final graph; detach children before deletion")
		}
	}
	// Complete original ancestor chains before validating closure groups. An
	// edited ancestor's original Parent may differ from its ordered Parent.
	for {
		n := len(s.stored)
		ids := make([]string, 0, n)
		for id := range s.stored {
			ids = append(ids, id)
		}
		for _, id := range ids {
			if _, err := s.depths(ctx, id, true); err != nil {
				return err
			}
		}
		if len(s.stored) == n {
			break
		}
	}
	before, err := parentGraph(s.stored)
	if err != nil {
		return err
	}
	after := make(map[string]string, len(s.parents))
	for id, parent := range s.parents {
		if !s.deleted[id] {
			after[id] = parent
		}
	}
	if err := checkParentGraph(before); err != nil {
		return err
	}
	if err := checkParentGraph(after); err != nil {
		return err
	}
	p.hierarchySources = s
	return p.finishHierarchy(ctx, before, after, affected)
}
