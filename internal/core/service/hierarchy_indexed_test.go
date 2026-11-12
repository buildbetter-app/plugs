package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestIndexedGuardGraphOpenParentDoesNotEnumerateSiblings(t *testing.T) {
	parent := guardRecord(t, "parent", "")
	child := guardRecord(t, "new-child", "parent")
	r := &indexedGuardReader{records: map[string]driver.Record{"parent": parent}}
	for i := 0; i < 2500; i++ {
		id := fmt.Sprintf("sibling-%04d", i)
		r.records[id] = guardRecord(t, id, "parent")
	}
	proposal := port.ProposedChange{Changes: []port.Change{guardChange(t, driver.Record{}, child)}}
	graph, complete, err := indexedGuardGraph(context.Background(), proposal, r, NewOntology(nil))
	if err != nil || !complete || len(graph) != 2 || r.reads != 0 || r.gets != 1 || r.queries != 0 {
		t.Fatalf("open parent graph=%d complete=%t selectors=%d gets=%d queries=%d err=%v", len(graph), complete, r.reads, r.gets, r.queries, err)
	}
}

func TestIndexedGuardGraphRetainsOldParentCurrentStatusGuard(t *testing.T) {
	oldParent := guardRecord(t, "old-parent", "")
	var doc pipeline.Document
	if err := json.Unmarshal(oldParent.Body, &doc); err != nil {
		t.Fatal(err)
	}
	doc.Fields["status"] = "done"
	oldParent.Body, _ = json.Marshal(doc)
	newParent := guardRecord(t, "new-parent", "")
	before, after := guardRecord(t, "moving", "old-parent"), guardRecord(t, "moving", "new-parent")
	sibling := guardRecord(t, "remaining", "old-parent")
	r := &indexedGuardReader{records: map[string]driver.Record{"old-parent": oldParent, "new-parent": newParent, "moving": before, "remaining": sibling}}
	proposal := port.ProposedChange{Changes: []port.Change{guardChange(t, before, after)}}
	o := NewOntology(nil)
	graph, complete, err := indexedGuardGraph(context.Background(), proposal, r, o)
	if err != nil || !complete || len(graph) != 4 || r.reads == 0 {
		t.Fatalf("old guarded parent omitted: complete=%t graph=%d err=%v", complete, len(graph), err)
	}
	if err := o.validateHierarchyParents(proposal, graph); err == nil {
		t.Fatal("old parent's unsatisfied current-status child guard was bypassed")
	}
}

type indexedGuardReader struct {
	port.SnapshotReader
	records map[string]driver.Record
	queries int
	reads   int
	gets    int
	more    bool
}

func guardRecord(t *testing.T, id, parent string, links ...pipeline.Link) driver.Record {
	t.Helper()
	doc := pipeline.Document{ID: id, Type: "issue", Fields: map[string]any{"status": "todo"}, Links: links}
	if parent != "" {
		doc.Parent = &model.Ref{ID: parent}
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return driver.Record{Ref: model.Ref{ID: id}, Kind: "bead", Revision: model.Natural64(1), Body: body}
}

func (r *indexedGuardReader) Get(_ context.Context, refs []model.Ref) ([]driver.Record, error) {
	r.gets += len(refs)
	result := make([]driver.Record, 0, len(refs))
	for _, ref := range refs {
		if record, exists := r.records[ref.ID]; exists {
			result = append(result, record)
		}
	}
	return result, nil
}

func (r *indexedGuardReader) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	r.queries++
	return driver.Page[driver.Record]{}, errors.New("complete scan was used")
}

func (r *indexedGuardReader) selectRecords(limit int, excluded []model.Ref, match func(pipeline.Document) bool) ([]driver.Record, bool, error) {
	r.reads++
	if r.more {
		return nil, true, nil
	}
	omitted := map[string]bool{}
	for _, ref := range excluded {
		omitted[ref.ID] = true
	}
	ids := []string{}
	for id, record := range r.records {
		if omitted[id] {
			continue
		}
		doc, err := pipeline.DecodeDocument(record.Body)
		if err != nil {
			return nil, false, err
		}
		if match(doc) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	more := len(ids) > limit
	if more {
		ids = ids[:limit]
	}
	result := make([]driver.Record, len(ids))
	for i, id := range ids {
		result[i] = r.records[id]
	}
	return result, more, nil
}

func (r *indexedGuardReader) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	return r.HierarchyChildrenExcluding(ctx, parents, nil, limit)
}

func (r *indexedGuardReader) HierarchyChildrenExcluding(_ context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	selected := map[string]bool{}
	for _, parent := range parents {
		selected[parent.ID] = true
	}
	return r.selectRecords(limit, excluded, func(doc pipeline.Document) bool { return doc.Parent != nil && selected[doc.Parent.ID] })
}

func (r *indexedGuardReader) InverseSourceLinks(_ context.Context, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	selected, allowed := map[string]bool{}, map[string]bool{}
	for _, target := range targets {
		selected[target.ID] = true
	}
	for _, kind := range kinds {
		allowed[string(kind)] = true
	}
	return r.selectRecords(limit, excluded, func(doc pipeline.Document) bool {
		for _, link := range doc.Links {
			if selected[link.To.ID] && allowed[link.Kind] {
				return true
			}
		}
		return false
	})
}

func (r *indexedGuardReader) HierarchyClosureRows(context.Context, []model.Ref) ([]driver.DerivedRow, error) {
	return nil, nil
}

func guardChange(t *testing.T, before, after driver.Record) port.Change {
	t.Helper()
	return port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: after.Ref}, Before: before.Body, After: after.Body, Op: model.Op{Kind: model.OpReparent}}
}

func TestIndexedGuardGraphIncludesCanonicalAndLegacyChildren(t *testing.T) {
	parent := guardRecord(t, "parent", "")
	canonical := guardRecord(t, "canonical", "parent")
	legacy := guardRecord(t, "legacy", "", pipeline.Link{Kind: "parent-child", To: parent.Ref})
	r := &indexedGuardReader{records: map[string]driver.Record{"parent": parent, "canonical": canonical, "legacy": legacy}}
	proposal := port.ProposedChange{Changes: []port.Change{guardChange(t, parent, parent)}}
	graph, complete, err := indexedGuardGraph(context.Background(), proposal, r)
	if err != nil || !complete || r.queries != 0 || len(graph) != 3 || r.reads == 0 {
		t.Fatalf("complete=%t graph=%d reads=%d queries=%d err=%v", complete, len(graph), r.reads, r.queries, err)
	}
	if NewOntology(nil).noOpenChildren(graph["parent"].doc, graph) {
		t.Fatal("legacy and canonical open children were omitted")
	}
}

func TestIndexedGuardGraphExcludesPreparedMoveBeforeLimit(t *testing.T) {
	parent := guardRecord(t, "parent", "")
	other := guardRecord(t, "other", "")
	oldChild := guardRecord(t, "child", "parent")
	moved := guardRecord(t, "child", "other")
	r := &indexedGuardReader{records: map[string]driver.Record{"parent": parent, "other": other, "child": oldChild}}
	proposal := port.ProposedChange{Changes: []port.Change{guardChange(t, oldChild, moved)}}
	graph, complete, err := indexedGuardGraph(context.Background(), proposal, r)
	if err != nil || !complete || r.queries != 0 || graph["child"].doc.Parent.ID != "other" {
		t.Fatalf("complete=%t queries=%d graph=%v err=%v", complete, r.queries, graph, err)
	}
	if !NewOntology(nil).noOpenChildren(graph["parent"].doc, graph) {
		t.Fatal("prepared moved-away child remained under old parent")
	}
}

func TestIndexedGuardGraphIncompleteAndDeleteUseCompleteFallback(t *testing.T) {
	parent := guardRecord(t, "parent", "")
	r := &indexedGuardReader{records: map[string]driver.Record{"parent": parent}, more: true}
	proposal := port.ProposedChange{Changes: []port.Change{guardChange(t, parent, parent)}}
	_, complete, err := indexedGuardGraph(context.Background(), proposal, r)
	if err != nil || complete || r.reads == 0 {
		t.Fatalf("incomplete membership used as absence: complete=%t reads=%d err=%v", complete, r.reads, err)
	}
	proposal.Changes[0].Op.Kind = model.OpDelete
	proposal.Changes[0].After = nil
	r.reads = 0
	_, complete, err = indexedGuardGraph(context.Background(), proposal, r)
	if err != nil || complete || r.reads != 0 {
		t.Fatalf("delete used closed hierarchy link domain: complete=%t reads=%d err=%v", complete, r.reads, err)
	}
}

func TestIndexedGuardGraphBoundsAncestorReadsAndIgnoresCustomLinks(t *testing.T) {
	root := guardRecord(t, "root", "")
	custom := guardRecord(t, "custom", "", pipeline.Link{Kind: "relates-to", To: root.Ref})
	r := &indexedGuardReader{records: map[string]driver.Record{"root": root, "custom": custom}}
	proposal := port.ProposedChange{Changes: []port.Change{guardChange(t, custom, custom)}}
	graph, complete, err := indexedGuardGraph(context.Background(), proposal, r)
	if err != nil || !complete || r.gets != 0 || len(graph) != 1 {
		t.Fatalf("custom link expanded guard graph: complete=%t graph=%d gets=%d err=%v", complete, len(graph), r.gets, err)
	}
	chain := map[string]driver.Record{}
	for i := 0; i <= indexedHierarchyLimit; i++ {
		id := fmt.Sprintf("node-%05d", i)
		parent := ""
		if i < indexedHierarchyLimit {
			parent = fmt.Sprintf("node-%05d", i+1)
		}
		chain[id] = guardRecord(t, id, parent)
	}
	leaf := guardRecord(t, "leaf", "node-00000")
	r = &indexedGuardReader{records: chain}
	proposal = port.ProposedChange{Changes: []port.Change{guardChange(t, leaf, leaf)}}
	_, complete, err = indexedGuardGraph(context.Background(), proposal, r)
	if err != nil || complete || r.gets > indexedHierarchyLimit || r.queries != 0 {
		t.Fatalf("ancestor chain escaped bound: complete=%t gets=%d queries=%d err=%v", complete, r.gets, r.queries, err)
	}
}

func TestIndexedGuardGraphStopsAtOutgoingFanoutBound(t *testing.T) {
	links := make([]pipeline.Link, indexedHierarchyLimit+1)
	for i := range links {
		links[i] = pipeline.Link{Kind: "waits-for", To: model.Ref{ID: fmt.Sprintf("target-%05d", i)}}
	}
	root := guardRecord(t, "fanout", "", links...)
	r := &indexedGuardReader{records: map[string]driver.Record{"fanout": root}}
	proposal := port.ProposedChange{Changes: []port.Change{guardChange(t, root, root)}}
	_, complete, err := indexedGuardGraph(context.Background(), proposal, r)
	if err != nil || complete || r.gets != 0 || r.reads != 0 || r.queries != 0 {
		t.Fatalf("fanout escaped bound: complete=%t gets=%d reads=%d queries=%d err=%v", complete, r.gets, r.reads, r.queries, err)
	}
}
