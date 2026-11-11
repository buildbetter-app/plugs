package service

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

func TestBlockedReadDerivesReasonsAndIgnoresStaleFlags(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	lease, err := store.Lease(ctx, "blocked-read", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(ctx); err != nil {
			t.Error(err)
		}
	})
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	link := func(id string) []pipeline.Link { return []pipeline.Link{{To: ref(id), Kind: "blocks"}} }
	documents := []pipeline.Document{
		{ID: "root", Fields: map[string]any{"status": "todo"}},
		{ID: "other", Fields: map[string]any{"status": "todo"}},
		{ID: "parent", Links: link("root"), Fields: map[string]any{"status": "todo", "blocked": false}},
		{ID: "child", Parent: ptrBlockedRef(ref("parent")), Fields: map[string]any{"status": "todo"}},
		{ID: "direct", Parent: ptrBlockedRef(ref("parent")), Links: link("other"), Fields: map[string]any{"status": "todo"}},
		{ID: "frozen", Links: link("root"), Fields: map[string]any{"status": "backlog"}},
		{ID: "closed", Links: link("root"), Fields: map[string]any{"status": "done"}},
		{ID: "pinned", Links: link("root"), Fields: map[string]any{"status": "pinned"}},
		{ID: "stale-true", Fields: map[string]any{"status": "todo", "blocked": true}},
		{ID: "left", Links: link("root"), Fields: map[string]any{"status": "todo"}},
		{ID: "right", Links: link("root"), Fields: map[string]any{"status": "todo"}},
		{ID: "join", Links: append(link("left"), link("right")...), Fields: map[string]any{"status": "todo"}},
	}
	changes := make([]conformance.FixtureChange, 0, len(documents))
	stamp := time.Date(2026, 11, 11, 17, 0, 0, 0, time.UTC)
	for _, doc := range documents {
		doc.Type, doc.Project, doc.CreatedAt = "issue", "p", &stamp
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: ref(doc.ID), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{Time: stamp}, changes); err != nil {
		t.Fatal(err)
	}
	w := pipeline.New(store, lease)
	s := New(store, lease, w)
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	before, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"parent": {"root"}, "child": {"parent"}, "direct": {"other"}, "frozen": {"root"}, "left": {"root"}, "right": {"root"}, "join": {"left", "right"}}
	for attempt := 0; attempt < 2; attempt++ {
		page, err := s.Blocked(ctx, by, ReadyRequest{Project: "p", Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		got := map[string][]string{}
		for _, item := range page.Items {
			if !item.Bead.Blocked || item.HiddenBlockers != 0 || item.BlockedByCount != len(item.BlockedBy) {
				t.Fatalf("incomplete blocked projection: %+v", item)
			}
			for _, blocker := range item.BlockedBy {
				got[item.Bead.ID] = append(got[item.Bead.ID], blocker.ID)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%v want=%v", got, want)
		}
	}
	allowed := func(_ context.Context, _ driver.Reader, bead Bead) (bool, error) {
		return bead.ID != "root" && bead.ID != "parent" && bead.ID != "direct", nil
	}
	request := ReadyRequest{Project: "p", Limit: 2}
	ids := []string{}
	for {
		page, err := s.BlockedWithFilter(ctx, by, request, allowed)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			ids = append(ids, item.Bead.ID)
			if item.Bead.ID == "child" && (item.HiddenBlockers != 1 || item.BlockedByCount != 1 || len(item.BlockedBy) != 0 || item.Bead.Parent != "" || !item.Bead.ParentHidden) {
				t.Fatalf("hidden fallback identity escaped: %+v", item)
			}
			if item.Bead.ID == "frozen" && (item.HiddenBlockers != 1 || len(item.BlockedBy) != 0 || len(item.Bead.Links) != 0) {
				t.Fatalf("hidden direct identity escaped: %+v", item)
			}
		}
		if page.Next == nil {
			break
		}
		request.Position = page.Next
	}
	if !reflect.DeepEqual(ids, []string{"child", "frozen", "join", "left", "right"}) {
		t.Fatalf("visibility before deterministic paging: %v", ids)
	}
	after, err := store.Meta(ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("read published metadata: %+v %+v %v", before, after, err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(ctx, []model.Ref{ref("parent"), ref("stale-true")})
	if err != nil || len(rows) != 2 {
		t.Fatalf("readback: %v", err)
	}
	for _, row := range rows {
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil || doc.Fields["blocked"] != (row.Ref.ID == "stale-true") {
			t.Fatalf("read repaired stored flag: %s %v", row.Body, err)
		}
	}
}

func ptrBlockedRef(ref model.Ref) *model.Ref { return &ref }

func TestBlockedReadDeepParentPropagationIsIterative(t *testing.T) {
	// The graph walk itself is also exercised at a depth larger than the
	// normal closure page, without recursive calls or storage side effects.
	s := &Service{ontology: NewOntology(nil)}
	graph := map[string]storedBead{}
	for i := 0; i < 20000; i++ {
		id := fmt.Sprintf("chain-%05d", i)
		doc := pipeline.Document{ID: id, Type: "issue", Project: "p", Fields: map[string]any{"status": "todo"}}
		if i == 0 {
			doc.Links = []pipeline.Link{{Kind: "blocks", To: model.RefByID("missing", model.Natural64(0))}}
		} else {
			doc.Parent = ptrBlockedRef(model.RefByID(fmt.Sprintf("chain-%05d", i-1), model.Natural64(0)))
		}
		graph[id] = storedBead{doc: doc}
	}
	reasons, err := s.blockedReasons(context.Background(), graph, nil)
	if err != nil || len(reasons) != len(graph) || len(reasons["chain-19999"]) != 1 || reasons["chain-19999"][0].Ref.ID != "chain-19998" {
		t.Fatalf("iterative chain propagation: count=%d err=%v", len(reasons), err)
	}
}
