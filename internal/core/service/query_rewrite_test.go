package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type rewriteTracingStore struct {
	driver.Store
	queries []driver.Query
	gets    [][]driver.Ref
}

func (s *rewriteTracingStore) Read(ctx context.Context, v driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, v)
	if err != nil {
		return nil, err
	}
	return &rewriteTracingReader{Reader: r, store: s}, nil
}

type rewriteTracingReader struct {
	driver.Reader
	store *rewriteTracingStore
}

func (r *rewriteTracingReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.store.queries = append(r.store.queries, q)
	return r.Reader.Query(ctx, q)
}

func (r *rewriteTracingReader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	r.store.gets = append(r.store.gets, append([]driver.Ref(nil), refs...))
	return r.Reader.Get(ctx, refs)
}

func TestReadyQueryRewritePreservesDependencyClosure(t *testing.T) {
	for _, tc := range []struct {
		name, edge, blockerStatus string
		ancestor, waitsForChild   bool
		wantReady                 bool
	}{
		{name: "direct_open_blocker", edge: "blocks", blockerStatus: "todo"},
		{name: "direct_closed_blocker", edge: "blocks", blockerStatus: "done", wantReady: true},
		{name: "conditional_failed_blocker", edge: "conditional-blocks", blockerStatus: "failed", wantReady: true},
		{name: "conditional_success_blocker", edge: "conditional-blocks", blockerStatus: "done"},
		{name: "parent_child_open_target", edge: "parent-child", blockerStatus: "todo"},
		{name: "parent_child_closed_target", edge: "parent-child", blockerStatus: "done", wantReady: true},
		{name: "ancestor_open_blocker", edge: "blocks", blockerStatus: "todo", ancestor: true},
		{name: "ancestor_closed_blocker", edge: "blocks", blockerStatus: "done", ancestor: true, wantReady: true},
		{name: "waits_for_open_child", edge: "waits-for", blockerStatus: "todo", waitsForChild: true},
		{name: "waits_for_terminal_child", edge: "waits-for", blockerStatus: "todo", waitsForChild: true, wantReady: true},
		{name: "ancestor_waits_for_open_child", edge: "waits-for", blockerStatus: "todo", ancestor: true, waitsForChild: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setup(t)
			candidate := model.RefByID("rewrite-candidate", model.Natural64(0))
			parent := model.RefByID("rewrite-parent", model.Natural64(0))
			blocker := model.RefByID("rewrite-blocker", model.Natural64(0))
			child := model.RefByID("rewrite-child", model.Natural64(0))
			doc := func(ref model.Ref, status string) pipeline.Document {
				return pipeline.Document{ID: ref.ID, Type: "issue", Project: "local", Fields: map[string]any{"status": status}}
			}
			c, p, b := doc(candidate, "todo"), doc(parent, "todo"), doc(blocker, tc.blockerStatus)
			if tc.ancestor {
				c.Parent = &parent
				p.Links = []pipeline.Link{{Kind: tc.edge, To: blocker}}
			} else {
				c.Links = []pipeline.Link{{Kind: tc.edge, To: blocker}}
			}
			docs := []pipeline.Document{c, p, b}
			if tc.waitsForChild {
				status := "todo"
				if tc.wantReady {
					status = "done"
				}
				ch := doc(child, status)
				ch.Parent = &blocker
				docs = append(docs, ch)
			}
			var changes []conformance.FixtureChange
			for _, d := range docs {
				body, err := json.Marshal(d)
				if err != nil {
					t.Fatal(err)
				}
				changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(d.ID, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body})
			}
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
				t.Fatal(err)
			}
			trace := &rewriteTracingStore{Store: f.store}
			f.svc.store = trace
			page, err := f.svc.ReadyQuery(context.Background(), anonymous, ReadyRequest{Project: "local", QueryRewrite: &driver.QueryRewrite{IDs: []string{candidate.ID}}})
			if err != nil || (len(page.Items) == 1) != tc.wantReady || len(page.Items) > 1 || (len(page.Items) == 1 && page.Items[0].ID != candidate.ID) {
				t.Fatalf("rewritten readiness changed dependency facts: page=%+v wantReady=%t err=%v", page, tc.wantReady, err)
			}
			if len(trace.queries) == 0 || trace.queries[0].Rewrite == nil || len(trace.queries[0].Rewrite.IDs) != 1 || trace.queries[0].Rewrite.IDs[0] != candidate.ID {
				t.Fatalf("public ReadyQuery did not reach the driver rewrite: %+v", trace.queries)
			}
			if tc.waitsForChild {
				if len(trace.queries) < 2 || trace.queries[len(trace.queries)-1].Rewrite != nil {
					t.Fatal("waits-for omitted the complete reverse-child graph")
				}
			} else if len(trace.gets) == 0 {
				t.Fatal("narrowed graph did not load its off-candidate dependencies")
			}
		})
	}
}

func TestReadyQueryEmptyRewriteReturnsNoCandidates(t *testing.T) {
	f := setup(t)
	create(t, f, "visible only without the rewrite")
	trace := &rewriteTracingStore{Store: f.store}
	f.svc.store = trace
	page, err := f.svc.ReadyQuery(context.Background(), anonymous, ReadyRequest{Project: "local", QueryRewrite: &driver.QueryRewrite{IDs: []string{}}})
	if err != nil || len(page.Items) != 0 || len(trace.queries) != 1 || len(trace.gets) != 0 || trace.queries[0].Rewrite.IDs == nil {
		t.Fatalf("empty rewrite broadened or scanned dependencies: page=%+v queries=%+v gets=%v err=%v", page, trace.queries, trace.gets, err)
	}
}
