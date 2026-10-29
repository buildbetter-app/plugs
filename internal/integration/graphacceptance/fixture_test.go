package graphacceptance_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const graphProject = "graph-acceptance"

var graphAnonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

// graphFixture composes the production service, ontology, Writer and one real
// storage engine. Link and lifecycle helpers intentionally keep their direct
// Writer boundary visible because bead.link/unlink/fail are unbound methods at
// this baseline.
type graphFixture struct {
	ctx   context.Context
	name  string
	path  string
	clock atomic.Int64
	seq   atomic.Uint64
	idsMu sync.Mutex
	ids   uint64

	store    driver.Store
	lease    driver.ActiveLease
	ontology *service.Ontology
	writer   *pipeline.Writer
	domain   *service.Service
}

func newGraphFixture(t *testing.T, name string) *graphFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	f := &graphFixture{
		ctx:  ctx,
		name: name,
	}
	f.clock.Store(time.Date(2026, 10, 29, 15, 0, 0, 0, time.UTC).UnixNano())
	if name == "sqlite" {
		f.path = t.TempDir() + "/graph.db"
	}
	f.open(t)
	t.Cleanup(func() { f.close(t) })
	return f
}

func (f *graphFixture) now() time.Time {
	return time.Unix(0, f.clock.Load()).UTC()
}

func (f *graphFixture) nextID() (string, error) {
	f.idsMu.Lock()
	f.ids++
	n := f.ids
	f.idsMu.Unlock()
	return fmt.Sprintf("pl-graph-%s-%03d", f.name, n), nil
}

func (f *graphFixture) open(t *testing.T) {
	t.Helper()
	var err error
	switch f.name {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{
			Clock: f.now, StoreID: driver.StoreID("graph-memory"), Incarnation: "graph-memory-incarnation",
		})
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.path})
	default:
		t.Fatalf("unknown graph engine %q", f.name)
	}
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "graph-"+f.name, driver.LeaseTTL)
	if err != nil {
		_ = f.store.Close()
		t.Fatal(err)
	}
	f.ontology = service.NewOntology(nil)
	f.writer = f.newWriter()
	f.domain = service.New(f.store, f.lease, f.writer, service.WithOntology(f.ontology), service.WithClock(f.now))
}

func (f *graphFixture) newWriter(options ...pipeline.Option) *pipeline.Writer {
	base := []pipeline.Option{
		pipeline.WithOntology(f.ontology),
		pipeline.WithClock(f.now),
		pipeline.WithIDGenerator(f.nextID),
	}
	return pipeline.New(f.store, f.lease, append(base, options...)...)
}

func (f *graphFixture) close(t *testing.T) {
	t.Helper()
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
		f.store = nil
	}
}

func (f *graphFixture) reopen(t *testing.T) {
	t.Helper()
	if f.name != "sqlite" {
		t.Fatalf("reopen is only a real SQLite boundary")
	}
	before, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.lease = nil
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store = nil
	f.open(t)
	after, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.StoreID != after.StoreID || before.Incarnation != after.Incarnation || before.Head.String() != after.Head.String() {
		t.Fatalf("SQLite reopen changed graph identity/head: before=%+v after=%+v", before, after)
	}
	t.Logf("real SQLite reopen preserved store=%s incarnation=%s head=%s", after.StoreID, after.Incarnation, after.Head)
}

func (f *graphFixture) meta(method string) model.WriteMeta {
	seq := f.seq.Add(1)
	return model.WriteMeta{
		Method: method, Project: graphProject, Agent: "graph-agent", AgentLabel: "graph-agent",
		IdempotencyKey: fmt.Sprintf("%064x", seq),
	}
}

func (f *graphFixture) create(t *testing.T, title string) model.Ref {
	t.Helper()
	commit, err := f.domain.Create(f.ctx, graphAnonymous, service.CreateRequest{
		Type: "issue", Fields: map[string]any{"title": title},
	}, f.meta("bead.create"))
	if err != nil || len(commit.Results) != 1 || !commit.Results[0].Ref.IsID() {
		t.Fatalf("real service create %s: commit=%+v err=%v", title, commit, err)
	}
	return commit.Results[0].Ref
}

func (f *graphFixture) apply(t *testing.T, method string, cs model.ChangeSet) model.Commit {
	t.Helper()
	meta := f.meta(method)
	commit, err := f.writer.Apply(f.ctx, graphAnonymous, cs, meta)
	if err != nil {
		t.Fatalf("direct Writer %s: %v", method, err)
	}
	return commit
}

func (f *graphFixture) applyErr(t *testing.T, method string, cs model.ChangeSet) (model.WriteMeta, error) {
	t.Helper()
	meta := f.meta(method)
	_, err := f.writer.Apply(f.ctx, graphAnonymous, cs, meta)
	return meta, err
}

func (f *graphFixture) closeBead(t *testing.T, ref model.Ref) model.Commit {
	t.Helper()
	commit, err := f.domain.Close(f.ctx, graphAnonymous, service.CloseRequest{ID: ref}, f.meta("bead.close"))
	if err != nil {
		t.Fatalf("real service close %s: %v", ref.ID, err)
	}
	return commit
}

func (f *graphFixture) lifecycle(t *testing.T, method string, ref model.Ref) model.Commit {
	t.Helper()
	commit, err := f.writer.Apply(f.ctx, graphAnonymous, model.ChangeSet{Ops: []model.Op{patchOp(ref, nil)}}, f.meta(method))
	if err != nil {
		t.Fatalf("direct Writer lifecycle %s %s: %v", method, ref.ID, err)
	}
	return commit
}

func (f *graphFixture) readyIDs(t *testing.T) []string {
	t.Helper()
	page, err := f.domain.Ready(f.ctx, graphAnonymous, service.ReadyRequest{Project: graphProject})
	if err != nil {
		t.Fatal(err)
	}
	if page.Next != nil || page.NextCursor != "" {
		t.Fatal("ready projection was unexpectedly truncated")
	}
	ids := make([]string, 0, len(page.Items))
	for _, item := range page.Items {
		ids = append(ids, item.ID)
	}
	sort.Strings(ids)
	return ids
}

func requireReady(t *testing.T, f *graphFixture, refs ...model.Ref) {
	t.Helper()
	want := make([]string, 0, len(refs))
	for _, ref := range refs {
		want = append(want, ref.ID)
	}
	sort.Strings(want)
	got := f.readyIDs(t)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ready graph=%v want=%v", got, want)
	}
}

func (f *graphFixture) document(t *testing.T, ref model.Ref, at *model.Revision) pipeline.Document {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("read document %s at %v: rows=%d err=%v", ref.ID, at, len(rows), err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func (f *graphFixture) historyRevision(t *testing.T, ref model.Ref, at model.Revision) service.Bead {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.domain.History(f.ctx, graphAnonymous, service.HistoryRequest{
		Project: graphProject,
		AsOf:    service.AsOf{Revision: at, Incarnation: meta.Incarnation},
		Filter:  &service.Filter{Resources: []string{ref.ID}},
	})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("service history %s at %s: page=%+v err=%v", ref.ID, at, page, err)
	}
	return page.Items[0]
}

type graphState struct {
	Meta      driver.Meta
	Records   map[string]driver.Record
	Versions  map[string][]driver.Version
	Log       []model.Changeset
	GraphLock driver.LockState
	Ready     []string
}

func (f *graphFixture) state(t *testing.T) graphState {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("complete graph query failed: page=%+v err=%v", page, err)
	}
	state := graphState{Meta: meta, Records: map[string]driver.Record{}, Versions: map[string][]driver.Version{}}
	for _, record := range page.Items {
		record.Body = append([]byte(nil), record.Body...)
		state.Records[record.Ref.ID] = record
		versions, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("complete version query for %s failed: page=%+v err=%v", record.Ref.ID, versions, err)
		}
		state.Versions[record.Ref.ID] = versions.Items
	}
	state.Log, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	locks, err := r.Observe(f.ctx, []driver.LockKey{driver.GraphBlockingSentinelLock})
	if err != nil || len(locks) != 1 {
		t.Fatalf("graph sentinel observation failed: locks=%+v err=%v", locks, err)
	}
	state.GraphLock = locks[0]
	state.Ready = f.readyIDs(t)
	return state
}

// linkPreparationBarrier only schedules callbacks. The real ontology,
// Writer graph evaluator and driver locks still make every graph decision.
// Neither first attempt can leave pre-validation until both pinned snapshots
// exist; the losing write must then restart against the winner's commit.
type linkPreparationBarrier struct {
	mu      sync.Mutex
	release chan struct{}
	bases   map[string][]string
}

func (b *linkPreparationBarrier) Validate(ctx context.Context, p port.ProposedChange, _ port.SnapshotReader) (port.Verdict, error) {
	b.mu.Lock()
	first := len(b.bases[p.RequestID]) == 0
	b.bases[p.RequestID] = append(b.bases[p.RequestID], p.BaseRevision.String())
	if first && len(b.bases) == 2 {
		close(b.release)
	}
	b.mu.Unlock()
	select {
	case <-b.release:
		return port.Verdict{Allow: true}, nil
	case <-ctx.Done():
		return port.Verdict{}, ctx.Err()
	}
}

func requireSameState(t *testing.T, before, after graphState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("failed graph write changed durable state:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func (f *graphFixture) requireNoReceipt(t *testing.T, meta model.WriteMeta) {
	t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(graphAnonymous)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(f.ctx, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if receipt != nil {
		t.Fatalf("refused graph write published receipt: %+v", receipt)
	}
}

func patchOp(ref model.Ref, fields map[string]any) model.Op {
	if fields == nil {
		fields = map[string]any{}
	}
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: fields}}
}

func linkOp(from, to model.Ref, kind string) model.Op {
	return model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: kind}}
}

func unlinkOp(from, to model.Ref, kind string) model.Op {
	return model.Op{Kind: model.OpUnlink, Link: &model.LinkOp{From: from, To: to, LinkKind: kind}}
}

func createOp(key, title string) model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: key, Type: "issue", Project: graphProject, Fields: map[string]any{"title": title}}}
}

func wantErrorCode(t *testing.T, err error, want model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != want {
		t.Fatalf("got %T %v, want %s", err, err, want)
	}
}
