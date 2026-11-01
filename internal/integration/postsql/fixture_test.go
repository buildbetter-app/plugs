package postsql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

var writerPrincipal = model.PrincipalRef{Kind: "user", ID: "post-sql-writer"}

type fixture struct {
	ctx    context.Context
	engine engine
	store  driver.Store
	lease  driver.ActiveLease
	ids    atomic.Uint64
}

var sourceRef = model.Ref{ID: "ontology/post-sql"}

func newFixture(t *testing.T, e engine) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, engine: e}
	f.open(t)
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error("close SQL store:", err)
		}
	})
	// Only the protected ontology source uses the conformance fixture seam.
	// All beads, versions, event plans and receipts below come from the Writer.
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(ctx, f.store, f.lease,
		driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "post-sql-source-fixture"}},
		[]conformance.FixtureChange{{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	if e.name == "sqlite_wal" {
		db, err := sql.Open(e.sqlDriver, e.dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var mode, version string
		if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
			t.Fatalf("SQLite must be file-backed WAL: mode=%q err=%v", mode, err)
		}
		if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
			t.Fatal(err)
		}
		t.Logf("real SQLite %s, file-backed WAL", version)
	}
	return f
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	f.store, err = f.engine.driver.Open(f.ctx, driver.Config{DSN: f.engine.dsn})
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "post-sql", driver.LeaseTTL)
	if err != nil {
		_ = f.store.Close()
		t.Fatal(err)
	}
	if _, ok := f.store.(driver.ResponseFinalizer); !ok {
		t.Fatal("real SQL response finalizer unavailable")
	}
	if _, ok := f.store.(driver.CheckpointStore); !ok {
		t.Fatal("real SQL checkpoint store unavailable")
	}
}

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	before, epoch := f.meta(t), f.lease.Epoch()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.open(t)
	same(t, before, f.meta(t))
	if f.lease.Epoch() <= epoch {
		t.Fatal("reopened SQL store reused the old lease epoch")
	}
}

func (f *fixture) writer(host *pluginhost.Host, store driver.Store) *pipeline.Writer {
	if store == nil {
		store = f.store
	}
	opts := []pipeline.Option{
		pipeline.WithPostFailureStore(f.store.(driver.CheckpointStore)),
		pipeline.WithOntology(service.NewOntology(nil)),
		pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(sourceRef))),
		pipeline.WithIDGenerator(func() (string, error) { return fmt.Sprintf("pl-post-sql-%d", f.ids.Add(1)), nil }),
	}
	if host != nil {
		opts = append(opts, pipeline.WithPostSubscribers(host.Posts()...))
	}
	return pipeline.New(store, f.lease, opts...)
}

type post struct {
	alias string
	after func(context.Context, pluginapi.CommittedChange, pluginapi.SnapshotReader) error
}

func nativeHost(t *testing.T, posts ...post) *pluginhost.Host {
	t.Helper()
	config := pluginhost.Config{}
	for _, p := range posts {
		config.Natives = append(config.Natives, sdk.Native{
			Instance: "default",
			Manifest: pluginapi.PluginManifest{
				ID: "app.example." + p.alias, Alias: p.alias, Version: "1.0.0",
				PluginAPI: pluginapi.APIVersion, Runtime: "native",
				Provides: []pluginapi.Provision{{Kind: "subscriber", Name: "observe", Phases: []pluginapi.Phase{pluginapi.PhasePost}, Reads: []string{"fields.title"}, MaxPointReads: 4}},
			},
			New: func(pluginapi.Host) (sdk.Registration, error) {
				return sdk.Registration{Posts: map[string]pluginapi.PostSubscriber{"observe": sdk.AfterFunc(p.after)}}, nil
			},
		})
		config.Bindings = append(config.Bindings, pluginhost.Binding{ID: "post-" + p.alias, Alias: p.alias, Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost, Reads: []string{"fields.title"}})
	}
	host, err := pluginhost.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func meta(n int) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", Project: "post-sql", IdempotencyKey: fmt.Sprintf("%032x", n)}
}

func create(titles ...string) model.ChangeSet {
	cs := model.ChangeSet{}
	for i, title := range titles {
		cs.Ops = append(cs.Ops, model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: fmt.Sprintf("item%d", i), Type: "issue", Fields: map[string]any{"title": title}}})
	}
	return cs
}

func patch(ref model.Ref, title string) model.ChangeSet {
	return model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": title}}}}}
}

func apply(t *testing.T, f *fixture, w *pipeline.Writer, cs model.ChangeSet, m model.WriteMeta) port.WriteOutcome {
	t.Helper()
	result, err := w.ApplyWithOutcome(f.ctx, writerPrincipal, cs, m)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (f *fixture) meta(t *testing.T) driver.Meta {
	t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (f *fixture) reader(t *testing.T) driver.Reader {
	t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: f.meta(t).Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (f *fixture) receipt(t *testing.T, m model.WriteMeta) *driver.IdempotencyRecord {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	key, err := driver.PrincipalKey(writerPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := r.IdempotencyByKey(f.ctx, key, "", m.Method, m.IdempotencyKey)
	if err != nil || entry == nil {
		t.Fatalf("missing real SQL receipt: entry=%+v err=%v", entry, err)
	}
	return entry
}

type graphState struct {
	Meta     driver.Meta
	Records  []driver.Record
	Versions [][]driver.Version
	Log      []model.Changeset
}

func (f *fixture) graph(t *testing.T, refs ...model.Ref) graphState {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	s := graphState{Meta: f.meta(t)}
	var err error
	s.Records, err = r.Get(f.ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		page, err := r.Versions(f.ctx, ref, driver.PageReq{Limit: 100})
		if err != nil || page.NextCursor != nil {
			t.Fatalf("incomplete version assertions: page=%+v err=%v", page, err)
		}
		s.Versions = append(s.Versions, page.Items)
	}
	s.Log, err = r.Changesets(f.ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func encoded(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func same(t *testing.T, want, got any) {
	t.Helper()
	a, b := encoded(t, want), encoded(t, got)
	if string(a) != string(b) {
		t.Fatalf("values differ:\nwant %s\n got %s", a, b)
	}
}

// Every callback effect goes to a disposable file, rather than treating an
// in-memory count as SQL durability or external delivery evidence.
func appendEffect(path string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(append(b, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func effects(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func code(err error) model.ErrorCode {
	var e *model.Error
	if errors.As(err, &e) {
		return e.Data.Code
	}
	return ""
}

func await[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal("timed out waiting for a real acceptance boundary")
		var zero T
		return zero
	}
}
