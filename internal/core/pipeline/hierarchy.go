package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const maxClosureRows = 5000

type hierarchyEdit struct {
	id, parent      string
	create, deleted bool
}

type hierarchyDelta struct {
	remove, add []driver.DerivedRow
	locks       []driver.LockKey
	sentinels   []driver.Sentinel
}

func parentID(parent *model.Ref) string {
	if parent == nil {
		return ""
	}
	return parent.ID
}

// HierarchyParent resolves Parent and a single legacy parent-child link without
// rewriting stored bytes. Both forms may be present when they name the same
// validated canonical parent. Explicit external edges never enter hierarchy.
func HierarchyParent(doc Document) (*model.Ref, error) {
	parent := doc.Parent
	if err := validateParent(parent); err != nil {
		return nil, failure("invalid_params", "invalid hierarchy parent")
	}
	legacyParentSeen := false
	for _, link := range doc.Links {
		if link.Kind != "parent-child" {
			continue
		}
		targetKind, err := link.TargetKind.Normalized()
		if err != nil {
			return nil, failure("invalid_params", err.Error())
		}
		if targetKind == model.LinkTargetExternal {
			continue
		}
		if err := validateParent(&link.To); err != nil {
			return nil, failure("invalid_params", "invalid legacy hierarchy parent")
		}
		if legacyParentSeen {
			return nil, failure("invalid_params", "multiple legacy hierarchy parents")
		}
		legacyParentSeen = true
		if parent != nil && parent.ID != link.To.ID {
			return nil, failure("invalid_params", "conflicting parent representations")
		}
		if parent == nil {
			value := link.To
			parent = &value
		}
	}
	return parent, nil
}

// Resolve through the same ordered pending graph used by all other operations.
// In particular, an alias cannot grant existence to a later create, and a
// transient descendant cycle is not rescued by a later operation in the batch.
func (p *prepared) resolveParent(ctx context.Context, parent *model.Ref, aliases map[string]model.Ref, child string) (*model.Ref, error) {
	if parent == nil {
		// A detach is an explicit edit, not legacy-only stored input. Require
		// unlinking a retained legacy relation before removing its parent.
		if child != "" {
			node, err := p.load(ctx, model.Ref{ID: child})
			if err != nil {
				return nil, err
			}
			for _, link := range node.doc.Links {
				if link.Kind == "parent-child" && (link.TargetKind == "" || link.TargetKind == model.LinkTargetLocal) {
					return nil, failure("invalid_params", "detach conflicts with retained legacy parent")
				}
			}
		}
		return nil, nil
	}
	ref, err := p.resolve(*parent, aliases)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for id := ref.ID; id != ""; {
		if id == child || seen[id] {
			return nil, failure("ontology_guard_failed", "parent would form a cycle")
		}
		if p.maxHierarchyDepth > 0 && strings.Count(id, ".") >= p.maxHierarchyDepth {
			return nil, failure("invalid_params", fmt.Sprintf("maximum hierarchy depth (%d) exceeded for parent %s", p.maxHierarchyDepth, id))
		}
		seen[id] = true
		node, err := p.load(ctx, model.Ref{ID: id})
		if err != nil {
			return nil, err
		}
		parent, err := HierarchyParent(node.doc)
		if err != nil {
			return nil, err
		}
		id = parentID(parent)
	}
	return &ref, nil
}

func (p *prepared) documents(ctx context.Context) (map[string]Document, error) {
	docs := map[string]Document{}
	var cursor *driver.Cursor
	for {
		page, err := p.snapshot.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		for _, record := range page.Items {
			doc, err := DecodeDocument(record.Body)
			if err != nil || doc.ID != record.Ref.ID || !record.Ref.IsID() || !record.Ref.Branch.IsZero() {
				return nil, failure("ontology_guard_failed", "invalid stored hierarchy document")
			}
			if _, exists := docs[doc.ID]; exists {
				return nil, failure("internal_error", "duplicate bead in graph snapshot")
			}
			docs[doc.ID] = doc
		}
		if page.NextCursor == nil {
			return docs, nil
		}
		if len(page.Items) == 0 {
			return nil, failure("internal_error", "non-advancing graph snapshot")
		}
		cursor = page.NextCursor
	}
}

func parentGraph(docs map[string]Document) (map[string]string, error) {
	parents := make(map[string]string, len(docs))
	for id, doc := range docs {
		parent, err := HierarchyParent(doc)
		if err != nil {
			return nil, err
		}
		parents[id] = parentID(parent)
	}
	return parents, nil
}

func checkParentGraph(parents map[string]string) error {
	done := map[string]bool{}
	for id := range parents {
		path := map[string]bool{}
		for current := id; current != "" && !done[current]; current = parents[current] {
			if _, exists := parents[current]; !exists {
				return failure("ontology_guard_failed", "parent must exist in the final graph; detach children before deletion")
			}
			if path[current] {
				return failure("ontology_guard_failed", "parent would form a cycle")
			}
			path[current] = true
		}
		for current := range path {
			done[current] = true
		}
	}
	return nil
}

func ancestorDepths(id string, parents map[string]string) (map[string]uint64, error) {
	depths := map[string]uint64{}
	if _, exists := parents[id]; !exists {
		return depths, nil
	}
	for current := id; current != ""; current = parents[current] {
		if _, exists := parents[current]; !exists {
			return nil, failure("ontology_guard_failed", "parent is missing from hierarchy")
		}
		if _, cycle := depths[current]; cycle {
			return nil, failure("ontology_guard_failed", "parent would form a cycle")
		}
		depths[current] = uint64(len(depths))
		if len(depths) > maxClosureRows {
			return nil, failure("closure_too_large", "ancestor closure exceeds 5000 affected rows")
		}
	}
	return depths, nil
}

func closureDifference(before, after map[string]uint64) int {
	n := 0
	for id, depth := range before {
		if next, exists := after[id]; !exists || next != depth {
			n++
		}
	}
	for id := range after {
		if _, exists := before[id]; !exists {
			n++
		}
	}
	return n
}

func closureRows(id string, depths map[string]uint64) (map[string]driver.DerivedRow, error) {
	rows := make(map[string]driver.DerivedRow, len(depths))
	for ancestor, depth := range depths {
		row, err := AncestorRow(id, ancestor, depth)
		if err != nil {
			return nil, err
		}
		rows[row.Key] = row
	}
	return rows, nil
}

// Count the full ordered batch, including intermediate rewrites which cancel
// out in the final image. A depth replacement is one affected pair. Only the
// final net delta is staged; breaking a large rewrite into ops cannot evade
// the bound. Deleted sources remain available to subsequent subtree moves;
// the final graph must remove all dangling children explicitly.
func (p *prepared) hierarchyAffected(parents map[string]string) (map[string]bool, error) {
	children := map[string]map[string]bool{}
	for id, parent := range parents {
		if children[parent] == nil {
			children[parent] = map[string]bool{}
		}
		children[parent][id] = true
	}
	affected, count := map[string]bool{}, 0
	for _, edit := range p.hierarchyEdits {
		if edit.create {
			parents[edit.id] = edit.parent
			if children[edit.parent] == nil {
				children[edit.parent] = map[string]bool{}
			}
			children[edit.parent][edit.id] = true
		}
		ids := []string{edit.id}
		if !edit.create && !edit.deleted && parents[edit.id] != edit.parent {
			for i := 0; i < len(ids); i++ {
				for child := range children[ids[i]] {
					ids = append(ids, child)
				}
			}
		}
		before := make(map[string]map[string]uint64, len(ids))
		for _, id := range ids {
			depths, err := ancestorDepths(id, parents)
			if err != nil {
				return nil, err
			}
			before[id] = depths
			affected[id] = true
		}
		if !edit.create && !edit.deleted {
			delete(children[parents[edit.id]], edit.id)
			parents[edit.id] = edit.parent
			if children[edit.parent] == nil {
				children[edit.parent] = map[string]bool{}
			}
			children[edit.parent][edit.id] = true
		}
		for _, id := range ids {
			depths, err := ancestorDepths(id, parents)
			if err != nil {
				return nil, err
			}
			switch {
			case edit.create, edit.deleted:
				count += len(depths)
			default:
				count += closureDifference(before[id], depths)
			}
			if count > maxClosureRows {
				return nil, failure("closure_too_large", "ordered batch affects more than 5000 closure rows")
			}
		}
	}
	return affected, nil
}

// hierarchy prepares source/derived observations and the complete net delta
// before the single lock acquisition. Root-only creation needs no whole-store
// graph scan. Existing stores are never backfilled: a consulted live ancestor
// with missing/extra/wrong-depth rows fails closed.
func (p *prepared) hierarchy(ctx context.Context) (map[string]Document, error) {
	if len(p.hierarchyEdits) == 0 {
		// Field, metadata, comment and lease updates leave the parent graph
		// unchanged. They already observe their target record in prepare, so
		// avoid enumerating every bead merely to revalidate an untouched
		// hierarchy.
		return nil, nil
	}
	if _, ok := p.snapshot.reader.(driver.HierarchyChildrenExcludingReader); ok {
		// This planner produces selected source facts, never a complete graph.
		// Prepared images need the exclusion capability before storage's single
		// limit+1; without it the existing complete planner remains authoritative.
		// The caller retains the separate complete link/scheduling checks.
		if err := p.hierarchyIndexed(ctx); !errors.Is(err, driver.ErrUnsupported) {
			return nil, err
		}
		// A published interface is not proof that this snapshot has complete
		// index coverage. Preserve the existing full correctness path for a
		// populated or imported unbuilt store; never infer empty membership.
		p.hierarchySources = nil
	}
	complex := false
	for _, edit := range p.hierarchyEdits {
		complex = complex || !edit.create || edit.parent != ""
	}
	for _, pending := range p.writes {
		// Ordinary field/lease updates preserve an existing parent and do not
		// need to rescan the complete graph. Create/Reparent mark the parent
		// path in the hierarchy edits. A mixed batch can still contain a
		// parent-preserving update alongside a root create, so retain the full
		// check whenever that batch already requires a graph.
		complex = complex || (len(pending.before) != 0 && pending.doc.Parent != nil)
	}
	stored := map[string]Document{}
	if complex {
		var err error
		stored, err = p.documents(ctx)
		if err != nil {
			return nil, err
		}
	}
	before, err := parentGraph(stored)
	if err != nil {
		return nil, err
	}
	if err := checkParentGraph(before); err != nil {
		return nil, err
	}
	staged, err := parentGraph(stored)
	if err != nil {
		return nil, err
	}
	affected, err := p.hierarchyAffected(staged)
	if err != nil {
		return nil, err
	}
	final := make(map[string]Document, len(stored)+len(p.writes))
	for id, doc := range stored {
		final[id] = doc
	}
	for id, pending := range p.writes {
		if pending.deleted {
			delete(final, id)
		} else {
			final[id] = pending.doc
		}
	}
	after, err := parentGraph(final)
	if err != nil {
		return nil, err
	}
	if err := checkParentGraph(after); err != nil {
		return nil, err
	}
	if err := p.finishHierarchy(ctx, before, after, affected); err != nil {
		return nil, err
	}
	if !complex {
		return nil, nil
	}
	return final, nil
}

// Both source planners use the same exact closure validation, read set and
// final delta. Source discovery and ordered work accounting finish first.
func (p *prepared) finishHierarchy(ctx context.Context, before, after map[string]string, affected map[string]bool) error {
	oldRows, newRows := map[string]driver.DerivedRow{}, map[string]driver.DerivedRow{}
	consumed := map[string]bool{}
	for id := range affected {
		consumed[id] = true
		for _, source := range []struct {
			parents map[string]string
			rows    map[string]driver.DerivedRow
		}{{before, oldRows}, {after, newRows}} {
			depths, err := ancestorDepths(id, source.parents)
			if err != nil {
				return err
			}
			rows, err := closureRows(id, depths)
			if err != nil {
				return err
			}
			for ancestor := range depths {
				consumed[ancestor] = true
			}
			for key, row := range rows {
				source.rows[key] = row
			}
		}
	}
	keys := map[string]driver.LockKey{}
	for id := range consumed {
		ref := model.Ref{ID: id}
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return err
		}
		keys[encoded] = key
		s, err := driver.NewClosureSentinel(id)
		if err != nil {
			return err
		}
		key = driver.LockKey{Kind: driver.LockSentinel, Sentinel: s}
		encoded, _ = driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	if _, err := p.snapshot.observe(ctx, sortedKeys(keys)); err != nil {
		return err
	}
	expected := map[string]driver.DerivedRow{}
	for id := range consumed {
		depths, err := ancestorDepths(id, before)
		if err != nil {
			return err
		}
		rows, err := closureRows(id, depths)
		if err != nil {
			return err
		}
		for key, row := range rows {
			expected[key] = row
		}
	}
	if err := p.checkClosures(ctx, consumed, expected); err != nil {
		return err
	}
	delta := &hierarchyDelta{}
	changed := map[string]driver.LockKey{}
	for key, row := range oldRows {
		if next, exists := newRows[key]; !exists || !bytes.Equal(row.Value, next.Value) {
			delta.remove = append(delta.remove, row)
			changed[key] = ancestorLock(key)
		}
	}
	for key, row := range newRows {
		if old, exists := oldRows[key]; !exists || !bytes.Equal(row.Value, old.Value) {
			delta.add = append(delta.add, row)
			changed[key] = ancestorLock(key)
		}
	}
	scopes := map[string]bool{}
	for _, key := range sortedKeys(changed) {
		delta.locks = append(delta.locks, key)
		row := newRows[key.Derived.Key]
		if row.Key == "" {
			row = oldRows[key.Derived.Key]
		}
		descendant, _, _, err := DecodeAncestorRow(row)
		if err != nil {
			return err
		}
		scopes[descendant] = true
	}
	// Include exact misses for newly introduced pairs, not just consumed rows.
	if _, err := p.snapshot.observe(ctx, delta.locks); err != nil {
		return err
	}
	for id := range scopes {
		s, _ := driver.NewClosureSentinel(id)
		delta.sentinels = append(delta.sentinels, s)
	}
	sort.Slice(delta.remove, func(i, j int) bool { return delta.remove[i].Key < delta.remove[j].Key })
	sort.Slice(delta.add, func(i, j int) bool { return delta.add[i].Key < delta.add[j].Key })
	p.hierarchyDelta = delta
	return nil
}

func ancestorLock(key string) driver.LockKey {
	return driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: key}}
}

// Enumerate only the consumed descendant prefixes when the driver supports
// their union. The common-range fallback preserves existing driver behavior.
// Every selected descendant retains its observed source and closure sentinel.
func (p *prepared) checkClosures(ctx context.Context, consumed map[string]bool, expected map[string]driver.DerivedRow) error {
	if _, ok := p.snapshot.reader.(driver.HierarchyChildrenExcludingReader); ok {
		refs := hierarchyRefs(consumed)
		rows, err := p.snapshot.HierarchyClosureRows(ctx, refs)
		if err != nil {
			return err
		}
		for _, row := range rows {
			want, exists := expected[row.Key]
			if !exists || !bytes.Equal(row.Value, want.Value) {
				return failure("ontology_guard_failed", "malformed ancestor closure")
			}
		}
		if len(rows) != len(expected) {
			return failure("ontology_guard_failed", "malformed ancestor closure; populated-store backfill is not supported")
		}
		return nil
	}
	prefix := ""
	prefixes := make([]string, 0, len(consumed))
	first := true
	for id := range consumed {
		next, err := AncestorPrefix(id)
		if err != nil {
			return err
		}
		prefixes = append(prefixes, next)
		if first {
			prefix = next
			first = false
			continue
		}
		for !strings.HasPrefix(next, prefix) {
			prefix = prefix[:len(prefix)-1]
		}
	}
	var all []driver.DerivedRow
	var err error
	if reader, ok := p.snapshot.reader.(driver.DerivedPrefixReader); ok {
		sort.Strings(prefixes)
		all, err = reader.DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, prefixes)
	} else {
		all, err = p.snapshot.reader.DerivedRows(ctx, driver.DerivedAncestorClosure, prefix)
	}
	if err != nil {
		return err
	}
	rows := make([]driver.DerivedRow, 0, len(expected))
	keys := make([]driver.LockKey, 0, len(expected))
	previous := ""
	for _, row := range all {
		descendant, _, _, err := DecodeAncestorRow(row)
		if err != nil || (previous != "" && row.Key <= previous) {
			return failure("ontology_guard_failed", "malformed ancestor closure")
		}
		previous = row.Key
		if !consumed[descendant] {
			continue
		}
		want, exists := expected[row.Key]
		if !exists || !bytes.Equal(row.Value, want.Value) {
			return failure("ontology_guard_failed", "malformed ancestor closure")
		}
		rows = append(rows, row)
		keys = append(keys, ancestorLock(row.Key))
	}
	if len(rows) != len(expected) {
		return failure("ontology_guard_failed", "malformed ancestor closure; populated-store backfill is not supported")
	}
	states, err := p.snapshot.observe(ctx, keys)
	if err != nil {
		return err
	}
	for i, state := range states {
		if !state.Present || state.Derived == nil || state.Derived.Key != rows[i].Key || !bytes.Equal(state.Derived.Value, rows[i].Value) {
			return failure("internal_error", "driver closure enumeration differs from its snapshot observations")
		}
	}
	return nil
}
