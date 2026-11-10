package service

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type queueProjectionReaderStub struct {
	driver.Reader
	projection driver.QueueProjection
}

// Keep the real typed queue projection while refusing a full-source fallback.
type sourceProviderQueueReader struct {
	driver.Reader
	queue               driver.QueueProjectionReader
	projections, images int
	queries, gets       int
}

func (r *sourceProviderQueueReader) QueueProjection(ctx context.Context) (driver.QueueProjection, error) {
	r.projections++
	return r.queue.QueueProjection(ctx)
}

func (r *sourceProviderQueueReader) QueueProjectionImages(ctx context.Context, view driver.View, refs []model.Ref) ([]driver.Record, error) {
	r.images++
	return r.queue.QueueProjectionImages(ctx, view, refs)
}

func (r *sourceProviderQueueReader) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	r.queries++
	return driver.Page[driver.Record]{}, errors.New("typed source queue scanned records")
}

func (r *sourceProviderQueueReader) Get(context.Context, []model.Ref) ([]driver.Record, error) {
	r.gets++
	return nil, errors.New("typed source queue fetched records")
}

func (r queueProjectionReaderStub) QueueProjection(context.Context) (driver.QueueProjection, error) {
	return r.projection, nil
}

func (r queueProjectionReaderStub) QueueProjectionImages(context.Context, driver.View, []model.Ref) ([]driver.Record, error) {
	return nil, nil
}

func TestQueueReadyItemsByUsesTypedBlockedAndHybridOrder(t *testing.T) {
	created := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	project := "local"
	beads := []driver.QueueBead{
		{Bead: driver.SelectionBead{Ref: ref("old"), Type: "issue", Project: &project, Status: "todo", Priority: 0}, Source: driver.SelectionSource{Revision: model.Natural64(1)}, Facts: driver.QueueFacts{Version: driver.QueueFactsVersion, CreatedAt: created.Add(-49 * time.Hour)}},
		{Bead: driver.SelectionBead{Ref: ref("blocked"), Type: "issue", Project: &project, Status: "todo", Priority: 0}, Source: driver.SelectionSource{Revision: model.Natural64(1)}, Facts: driver.QueueFacts{Version: driver.QueueFactsVersion, CreatedAt: created.Add(-1 * time.Hour)}},
		{Bead: driver.SelectionBead{Ref: ref("recent"), Type: "issue", Project: &project, Status: "todo", Priority: 1}, Source: driver.SelectionSource{Revision: model.Natural64(1)}, Facts: driver.QueueFacts{Version: driver.QueueFactsVersion, CreatedAt: created.Add(-2 * time.Hour)}},
	}
	s := New(nil, nil, nil, WithClock(func() time.Time { return created }))
	var gotBy model.PrincipalRef
	var gotIDs []string
	blocked := func(_ context.Context, by model.PrincipalRef, ids []string, graph map[string]storedBead) (map[string]bool, error) {
		gotBy, gotIDs = by, append([]string(nil), ids...)
		if len(graph) != len(beads) || graph["blocked"].doc.ID != "blocked" || !graph["old"].doc.CreatedAt.Equal(created.Add(-49*time.Hour)) {
			t.Fatal("workflow callback lost the complete pinned typed graph")
		}
		return map[string]bool{"blocked": true}, nil
	}
	by := model.PrincipalRef{Kind: "user", ID: "queue-owner"}
	items, _, err := s.queueReadyItemsBy(context.Background(), by, ReadyRequest{Project: project}, queueProjectionReaderStub{projection: driver.QueueProjection{Beads: beads}}, blocked)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(items))
	for i := range items {
		ids[i] = items[i].ID
	}
	if !reflect.DeepEqual(gotIDs, []string{"blocked", "old", "recent"}) || gotBy.Kind != by.Kind || gotBy.ID != by.ID {
		t.Fatalf("ordinary blocked callback binding: ids=%v by=%+v", gotIDs, gotBy)
	}
	if !reflect.DeepEqual(ids, []string{"recent", "old"}) {
		t.Fatalf("hybrid order or blocked filtering: got=%v", ids)
	}
}

func TestQueueReadyItemsRefusesLegacyCreationInput(t *testing.T) {
	project := "local"
	bead := driver.QueueBead{Bead: driver.SelectionBead{Ref: model.RefByID("legacy", model.Natural64(0)), Type: "issue", Project: &project, Status: "todo"}, Source: driver.SelectionSource{Revision: model.Natural64(1)}, Facts: driver.QueueFacts{}}
	s := New(nil, nil, nil)
	items, projection, err := s.queueReadyItems(context.Background(), ReadyRequest{Project: project}, queueProjectionReaderStub{projection: driver.QueueProjection{Beads: []driver.QueueBead{bead}}})
	if err != nil || projection != nil || items != nil {
		t.Fatalf("legacy queue metadata accelerated: items=%v projection=%v err=%v", items, projection, err)
	}
}

// The adapter must use the pinned persisted rank policy even when a bead
// crosses the 48-hour boundary during the permitted one-minute rank lag.
// The injected thin reader refuses both full scans and point/body fallbacks.
type queueOrderReader struct {
	queueProjectionReaderStub
	rows                         []driver.DerivedRow
	ranks, queries, gets, images int
}

func (r *queueOrderReader) DerivedRows(_ context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	r.ranks++
	if table != driver.DerivedSortKey || prefix != "ready/" {
		return nil, errors.New("unexpected derived range")
	}
	return r.rows, nil
}

func (r *queueOrderReader) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	r.queries++
	return driver.Page[driver.Record]{}, errors.New("queue adapter scanned source bodies")
}

func (r *queueOrderReader) Get(context.Context, []model.Ref) ([]driver.Record, error) {
	r.gets++
	return nil, errors.New("queue adapter fetched source bodies")
}

func (r *queueOrderReader) QueueProjectionImages(context.Context, driver.View, []model.Ref) ([]driver.Record, error) {
	r.images++
	return nil, errors.New("queue adapter hydrated unselected source bodies")
}

func TestQueueReadyCandidatesPreservesPinnedRankFreshness(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	project := "local"
	older := now.Add(-48*time.Hour - 10*time.Second)
	recent := now.Add(-time.Hour)
	beads := []driver.QueueBead{
		{Bead: driver.SelectionBead{Ref: model.RefByID("older", model.Natural64(0)), Type: "issue", Project: &project, Status: "todo", Priority: 0}, Source: driver.SelectionSource{Revision: model.Natural64(1)}, Facts: driver.QueueFacts{Version: driver.QueueFactsVersion, CreatedAt: older}},
		{Bead: driver.SelectionBead{Ref: model.RefByID("recent", model.Natural64(0)), Type: "issue", Project: &project, Status: "todo", Priority: 1}, Source: driver.SelectionSource{Revision: model.Natural64(1)}, Facts: driver.QueueFacts{Version: driver.QueueFactsVersion, CreatedAt: recent}},
	}
	for _, tc := range []struct {
		name     string
		age      time.Duration
		revision model.Revision
		rank     int
		want     []string
		bad      bool
	}{
		{name: "fresh_boundary_crossing", age: 30 * time.Second, revision: model.Natural64(1), want: []string{"older", "recent"}},
		{name: "expired_rank", age: 61 * time.Second, revision: model.Natural64(1), want: []string{"recent", "older"}},
		{name: "source_revision_changed", age: 30 * time.Second, revision: model.Natural64(2), want: []string{"recent", "older"}},
		{name: "fresh_wrong_rank", age: 30 * time.Second, revision: model.Natural64(1), rank: 1, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(pipeline.ReadySortKey{Bucket: 0, Rank: tc.rank, CreatedAt: older, RankedAt: now.Add(-tc.age), Revision: tc.revision})
			if err != nil {
				t.Fatal(err)
			}
			reader := &queueOrderReader{
				queueProjectionReaderStub: queueProjectionReaderStub{projection: driver.QueueProjection{Beads: beads}},
				rows:                      []driver.DerivedRow{{Key: "ready/older", Value: body}},
			}
			s := New(nil, nil, nil, WithClock(func() time.Time { return now }))
			items, err := s.ReadyCandidates(context.Background(), anonymous, ReadyRequest{Project: project}, reader)
			if tc.bad {
				wantCode(t, err, "internal_error")
				if items != nil {
					t.Fatalf("invalid rank returned candidates: %+v", items)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				ids := make([]string, len(items))
				for i, item := range items {
					ids[i] = item.ID
				}
				if !reflect.DeepEqual(ids, tc.want) {
					t.Fatalf("candidate order=%v want=%v", ids, tc.want)
				}
			}
			if reader.ranks != 1 || reader.queries != 0 || reader.gets != 0 || reader.images != 0 {
				t.Fatalf("queue reader work ranks=%d query=%d get=%d images=%d", reader.ranks, reader.queries, reader.gets, reader.images)
			}
		})
	}
}

func TestQueueSpreadPreservesHybridCreationBoundaries(t *testing.T) {
	created := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	items := []Bead{
		{ID: "first", Fields: map[string]any{"priority": 1}},
		{ID: "second", Fields: map[string]any{"priority": 1}},
		{ID: "third", Fields: map[string]any{"priority": 1}},
	}
	keys := map[string]readyOrderKey{}
	for i, item := range items {
		keys[item.ID] = readyOrderKey{Bucket: 0, Rank: 1, CreatedAt: created.Add(time.Duration(i) * time.Second)}
	}
	for _, seed := range []string{"one", "two", "three"} {
		got, err := spreadClaimNextItemsObserved(context.Background(), items, seed, keys)
		if err != nil || !reflect.DeepEqual(got, items) {
			t.Fatalf("crossed creation boundary: got=%v err=%v", got, err)
		}
	}
}

func TestQueueReadyCandidatesUsesWorkflowBlocking(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "queue-workflow.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "queue-workflow", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	provider, err := workflowplugin.New(pluginhost.NewDurableState(store.(driver.CheckpointStore), lease, workflowplugin.Alias, "default"))
	if err != nil {
		t.Fatal(err)
	}
	provider.WithClock(func() time.Time { return now })
	by := model.PrincipalRef{Kind: "user", ID: "queue-owner"}
	invokeMeta := meta(t)
	invokeMeta.Method = "x.workflow.invoke"
	run, err := provider.Invoke(ctx, by, workflowplugin.InvokeRequest{Template: "queue-blocker", Project: "local", Mode: "ephemeral", Checklist: []workflowplugin.ChecklistItem{{Label: "blocking", Title: "Blocking task"}}}, invokeMeta)
	if err != nil || len(run.Items) != 1 {
		t.Fatalf("invoke: %+v %v", run, err)
	}
	blocker := model.RefByID(run.Items[0].ID, model.Natural64(0))
	linkMeta := meta(t)
	linkMeta.Method = "bead.link"
	if _, err := provider.Link(ctx, by, workflowplugin.LinkRequest{From: model.RefByID("blocked", model.Natural64(0)), To: blocker, Kind: "blocks"}, linkMeta); err != nil {
		t.Fatal(err)
	}
	project := "local"
	beads := []driver.QueueBead{}
	for _, id := range []string{"blocked", "free"} {
		beads = append(beads, driver.QueueBead{Bead: driver.SelectionBead{Ref: model.RefByID(id, model.Natural64(0)), Type: "issue", Project: &project, Status: "todo", Priority: 1}, Source: driver.SelectionSource{Revision: model.Natural64(1)}, Facts: driver.QueueFacts{Version: driver.QueueFactsVersion, CreatedAt: now.Add(-time.Hour)}})
	}
	s := New(store, lease, nil, WithClock(func() time.Time { return now }), WithWorkflow(provider))
	read := func(want []string) {
		t.Helper()
		r := &queueOrderReader{queueProjectionReaderStub: queueProjectionReaderStub{projection: driver.QueueProjection{Beads: beads}}}
		items, err := s.ReadyCandidates(ctx, by, ReadyRequest{Project: project, Agent: invokeMeta.Agent}, r)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, len(items))
		for i, item := range items {
			ids[i] = item.ID
		}
		if !reflect.DeepEqual(ids, want) {
			t.Fatalf("workflow blocking: got=%v want=%v", ids, want)
		}
		if r.ranks != 1 || r.queries != 0 || r.gets != 0 || r.images != 0 {
			t.Fatalf("queue work ranks=%d query=%d get=%d images=%d", r.ranks, r.queries, r.gets, r.images)
		}
	}
	read([]string{"free"})
	closeMeta := meta(t)
	closeMeta.Method = "bead.close"
	if _, err := provider.Close(ctx, by, blocker, "complete", closeMeta); err != nil {
		t.Fatal(err)
	}
	read([]string{"blocked", "free"})
}

func TestQueueReadyUsesSelectedSourceProviderForTypedMembership(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "queue-source.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "queue-source", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	checkpoint := store.(driver.CheckpointStore)
	newProvider := func(instance string) *workflowplugin.Provider {
		t.Helper()
		provider, err := workflowplugin.New(pluginhost.NewDurableState(checkpoint, lease, workflowplugin.Alias, instance))
		if err != nil {
			t.Fatal(err)
		}
		return provider.WithClock(func() time.Time { return now })
	}
	native, source := newProvider("default"), newProvider("source-selection-test")
	ontology := NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithClock(func() time.Time { return now }))
	s := New(store, lease, writer, WithOntology(ontology), WithClock(func() time.Time { return now }), WithWorkflow(native))
	by := model.PrincipalRef{Kind: "user", ID: "queue-owner"}
	create := func(title string) model.Ref {
		t.Helper()
		commit, err := s.Create(ctx, by, CreateRequest{Type: "issue", Fields: map[string]any{"title": title}}, meta(t))
		if err != nil || len(commit.Results) != 1 {
			t.Fatalf("create %s: commit=%+v err=%v", title, commit, err)
		}
		return commit.Results[0].Ref
	}
	sourceChild, nativeChild := create("source child"), create("native child")
	for _, test := range []struct {
		name     string
		provider *workflowplugin.Provider
		child    model.Ref
	}{
		{name: "source", provider: source, child: sourceChild},
		{name: "native", provider: native, child: nativeChild},
	} {
		invokeMeta := meta(t)
		invokeMeta.Method = "x.workflow.invoke"
		result, err := test.provider.Invoke(ctx, by, workflowplugin.InvokeRequest{
			Template: test.name + "-blocker", Project: "local", Mode: "ephemeral",
			Checklist: []workflowplugin.ChecklistItem{{Label: "blocking", Title: test.name + " blocker", Blocked: true}},
		}, invokeMeta)
		if err != nil || len(result.Items) != 1 {
			t.Fatalf("%s invoke: result=%+v err=%v", test.name, result, err)
		}
		linkMeta := meta(t)
		linkMeta.Method = "bead.link"
		if _, err := test.provider.Link(ctx, by, workflowplugin.LinkRequest{
			From: test.child, To: model.RefByID(result.Items[0].ID, model.Natural64(0)), Kind: "blocks",
		}, linkMeta); err != nil {
			t.Fatalf("%s link: %v", test.name, err)
		}
	}
	// The native and selected source checkpoints deliberately disagree: ordinary
	// Ready sees only nativeChild. The typed queue must use that same source view.
	s.source.Store(&sourceWorkflow{provider: source})
	includeEphemeral := false
	req := ReadyRequest{Project: "local", Agent: "test-agent", Limit: 10, Filter: &Filter{
		Resources: []string{sourceChild.ID, nativeChild.ID}, IncludeEphemeral: &includeEphemeral,
	}}
	ids := func(items []Bead) []string {
		result := make([]string, len(items))
		for i, item := range items {
			result[i] = item.ID
		}
		return result
	}
	ordinary, err := s.Ready(ctx, by, req)
	if err != nil || ordinary.Next != nil || !reflect.DeepEqual(ids(ordinary.Items), []string{nativeChild.ID}) {
		t.Fatalf("source-aware ordinary membership: page=%+v err=%v", ordinary, err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	queue, ok := reader.(driver.QueueProjectionReader)
	if !ok {
		t.Fatal("real SQL reader lost its typed queue projection")
	}
	bound := &sourceProviderQueueReader{Reader: reader, queue: queue}
	candidates, err := s.ReadyCandidates(ctx, by, req, bound)
	if err != nil || !reflect.DeepEqual(ids(candidates), ids(ordinary.Items)) {
		t.Fatalf("typed source queue differs from ordinary Ready: got=%v want=%v err=%v", ids(candidates), ids(ordinary.Items), err)
	}
	if bound.projections != 1 || bound.images != 0 || bound.queries != 0 || bound.gets != 0 {
		t.Fatalf("typed queue work projections=%d images=%d query=%d get=%d", bound.projections, bound.images, bound.queries, bound.gets)
	}
	includeEphemeral = true
	page, err := s.queueReadyPage(ctx, by, req, false)
	if err != nil || page.Next != nil || !reflect.DeepEqual(ids(page.Items), ids(ordinary.Items)) {
		t.Fatalf("queue page differs from selected source Ready: page=%+v err=%v", page, err)
	}
}
