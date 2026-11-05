package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type pausedClaimOntology struct {
	*Ontology
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (o *pausedClaimOntology) Validate(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) error {
	if p.Meta.Method != "bead.claim" && p.Meta.Method != "bead.claimNext" {
		return o.Ontology.Validate(ctx, p, r)
	}
	call := o.calls.Add(1)
	if err := o.Ontology.Validate(ctx, p, r); err != nil {
		return err
	}
	if call == 1 {
		close(o.entered)
		select {
		case <-o.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Use the real ontology, Writer, memory store and service. The hook pauses
// only after actual guard observations, so the intervening publication tests
// whether those exact observations force a restart or preserve a safe claim.
func TestIndependentRootClaimRetainsConfiguredChildGuardRaces(t *testing.T) {
	for _, method := range []string{"bead.claim", "bead.claimNext"} {
		for _, childGuard := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/child-guard=%t", method, childGuard), func(t *testing.T) {
				ctx := context.Background()
				f := setup(t)
				id, _ := create(t, f, "root to claim")
				var other model.Ref
				if !childGuard {
					other, _ = create(t, f, "independent root")
				}
				document := ontology.DefaultDocument()
				if childGuard {
					document.Workflows["issue"].Transitions[0].Guards = append(document.Workflows["issue"].Transitions[0].Guards, ontology.Guard{Kind: ontology.NoOpenChildren})
				}
				snapshot, problems := ontology.Compile(document, nil, nil)
				if len(problems) != 0 {
					t.Fatal(problems)
				}
				o := NewOntology(snapshot.Effective)
				f.svc = New(f.store, f.lease, pipeline.New(f.store, f.lease, pipeline.WithOntology(o)), WithOntology(o))
				paused := &pausedClaimOntology{Ontology: o, entered: make(chan struct{}), release: make(chan struct{})}
				var once sync.Once
				resume := func() { once.Do(func() { close(paused.release) }) }
				t.Cleanup(resume)
				first := New(f.store, f.lease, pipeline.New(f.store, f.lease, pipeline.WithOntology(paused)), WithOntology(o))
				key := meta(t)
				claim := func() (port.WriteOutcome, error) {
					// Pin the selected candidate's actual service lowering for both
					// verbs. Public queue traversal is exercised by the A4 witness.
					return first.claimWithMethod(ctx, anonymous, ClaimRequest{ID: id}, key, true, method)
				}
				type result struct {
					out port.WriteOutcome
					err error
				}
				done := make(chan result, 1)
				go func() { out, err := claim(); done <- result{out, err} }()
				select {
				case <-paused.entered:
				case <-time.After(time.Second):
					t.Fatal("claim did not reach its observed guard")
				}
				if childGuard {
					_, err := f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Parent: &id, Fields: map[string]any{"title": "concurrent open child"}}, meta(t))
					if err != nil {
						t.Fatal(err)
					}
				} else if _, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: other}, meta(t)); err != nil {
					t.Fatal(err)
				}
				resume()
				var got result
				select {
				case got = <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("claim did not finish")
				}
				if childGuard {
					wantCode(t, got.err, "ontology_guard_failed")
					if paused.calls.Load() != 2 {
						t.Fatalf("child membership phantom was not revalidated: %d", paused.calls.Load())
					}
				} else if got.err != nil || got.out.Replayed || got.out.Commit.Revision.String() != "4" || paused.calls.Load() != 1 {
					t.Fatalf("unrelated claim invalidated root guard: %+v calls=%d", got, paused.calls.Load())
				}
				stored, err := f.store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				r, err := f.store.Read(ctx, driver.View{Incarnation: stored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				versions, err := r.Versions(ctx, id, driver.PageReq{})
				if err != nil {
					t.Fatal(err)
				}
				principal, _ := driver.PrincipalKey(anonymous)
				receipt, err := r.IdempotencyByKey(ctx, principal, key.AgentLabel, method, key.IdempotencyKey)
				if err != nil {
					t.Fatal(err)
				}
				if childGuard {
					if stored.Head.String() != "2" || len(versions.Items) != 1 || receipt != nil {
						t.Fatalf("refused claim published: head=%s versions=%d receipt=%+v", stored.Head.String(), len(versions.Items), receipt)
					}
					return
				}
				if stored.Head.String() != "4" || len(versions.Items) != 2 || receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, got.out.Commit) {
					t.Fatalf("winner history/receipt mismatch: head=%s versions=%d receipt=%+v", stored.Head.String(), len(versions.Items), receipt)
				}
				again, err := claim()
				after, metaErr := f.store.Meta(ctx)
				if err != nil || metaErr != nil || !again.Replayed || !reflect.DeepEqual(again.Commit, got.out.Commit) || after.Head.String() != "4" || paused.calls.Load() != 1 {
					t.Fatalf("winner replay changed state: %+v errors=%v/%v calls=%d", again, err, metaErr, paused.calls.Load())
				}
			})
		}
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
