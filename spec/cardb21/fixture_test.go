// Package cardb20 reuses the admitted native build and disposable PostgreSQL
// support filegroups. B21 owns this independent HTTP acceptance fixture.
package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflow "github.com/buildbetter-app/plugs/plugins/workflow"
)

const b21Project = "b21-public"
const b21Agent = "b21-acceptance"

var b21Source = model.RefByID("ontology/b21-public", model.Natural64(0))

type b21Posts struct {
	mu    sync.Mutex
	calls []port.CommittedChange
}

func (p *b21Posts) After(_ context.Context, c port.CommittedChange, _ port.SnapshotReader) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, c)
	return nil
}
func (p *b21Posts) count() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.calls) }

type b21Fixture struct {
	t                 *testing.T
	ctx               context.Context
	engine, path, dsn string
	now               time.Time
	store             driver.Store
	lease             driver.ActiveLease
	api               *httptest.Server
	posts             *b21Posts
	ids, keys         int
	frames            []json.RawMessage
}
type b21Reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func newB21(t *testing.T, engine string) *b21Fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	f := &b21Fixture{t: t, ctx: ctx, engine: engine, now: time.Date(2026, 11, 11, 16, 42, 0, 0, time.UTC), posts: &b21Posts{}}
	if engine == "sqlite" {
		f.path = filepath.Join(t.TempDir(), "b21.sqlite")
	}
	if engine == "postgres" {
		f.dsn = startPostgres(t)
	}
	f.open(true)
	t.Cleanup(func() { f.close(); b21Retain(t, "public-wire-"+engine, f.frames) })
	return f
}
func (f *b21Fixture) open(seed bool) {
	t := f.t
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: func() time.Time { return f.now }, StoreID: "b21-memory", Incarnation: "b21-memory-incarnation"})
	case "sqlite":
		f.store, err = (sqlite.Driver{Clock: func() time.Time { return f.now }}).Open(f.ctx, driver.Config{DSN: f.path})
	case "postgres":
		f.store, err = postgres.Open(f.ctx, driver.Config{DSN: f.dsn})
	default:
		t.Fatalf("unknown engine %s", f.engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "b21-public-worker", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if seed {
		body, e := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: b21Source, Kind: "ontology", Op: model.OpCreate, Body: body}}); e != nil {
			t.Fatal(e)
		}
	}
	rules := service.NewOntology(nil)
	w := pipeline.New(f.store, f.lease, pipeline.WithOntology(rules), pipeline.WithClock(func() time.Time { return f.now }), pipeline.WithIDGenerator(func() (string, error) { f.ids++; return fmt.Sprintf("b21-%s-%04d", f.engine, f.ids), nil }), pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(b21Source))), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b21-replay-observer", Subscriber: f.posts}))
	meta, e := f.store.Meta(f.ctx)
	if e != nil {
		t.Fatal(e)
	}
	checkpoints, e := driver.RequireWorkflowCheckpointStore(f.store, f.lease)
	if e != nil {
		t.Fatal(e)
	}
	state, e := pluginhost.NewWorkflowRuntimeState(checkpoints, f.lease)
	if e != nil {
		t.Fatal(e)
	}
	// Adopted PR467/17:14 contract: memory has process checkpoints and never
	// advertises restart persistence. SQL uses the actual restart capability.
	marker, ok := state.(interface{ Persistent() bool })
	if !ok {
		t.Fatal("actual workflow state has no persistence marker")
	}
	wantLifetime := driver.CheckpointLifetimeRestart
	if f.engine == "memory" {
		wantLifetime = driver.CheckpointLifetimeProcess
	}
	if checkpoints.WorkflowCheckpointLifetime() != wantLifetime || marker.Persistent() != (f.engine != "memory") {
		t.Fatalf("real checkpoint capability lifetime=%s persistent=%v", checkpoints.WorkflowCheckpointLifetime(), marker.Persistent())
	}
	provider, e := workflow.New(state)
	if e != nil {
		t.Fatal(e)
	}
	provider.WithClock(func() time.Time { return f.now }).WithIdentity("workflow", "default", meta.Incarnation)
	domain := service.New(f.store, f.lease, w, service.WithOntology(rules), service.WithClock(func() time.Time { return f.now }), service.WithWorkflow(provider))
	registry, e := rpc.NewRegistry()
	if e != nil {
		t.Fatal(e)
	}
	if e = rpc.BindService(registry, domain); e != nil {
		t.Fatal(e)
	}
	server, e := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: b21Project, OntologyRevision: model.Natural64(1)})
	if e != nil {
		t.Fatal(e)
	}
	admission, e := authn.New(authn.Config{})
	if e != nil {
		t.Fatal(e)
	}
	handler, e := httpjson.New(server, admission, httpjson.Config{})
	if e != nil {
		t.Fatal(e)
	}
	f.api = httptest.NewServer(handler)
}
func (f *b21Fixture) close() {
	if f.api != nil {
		f.api.Close()
		f.api = nil
	}
	if f.lease != nil {
		if e := f.lease.Release(context.Background()); e != nil {
			var fenced driver.ErrFenced
			if !errors.As(e, &fenced) {
				f.t.Error(e)
			}
		}
		f.lease = nil
	}
	if f.store != nil {
		if e := f.store.Close(); e != nil {
			f.t.Error(e)
		}
		f.store = nil
	}
}
func (f *b21Fixture) reopen() {
	f.t.Helper()
	if f.engine == "memory" {
		f.t.Fatal("memory cannot claim persistence reopen")
	}
	before := f.state()
	f.close()
	f.open(false)
	b21Equal(f.t, before, f.state(), "SQL reopen complete durable state")
}
func (f *b21Fixture) key() string { f.keys++; return fmt.Sprintf("%032x", f.keys) }
func (f *b21Fixture) call(method, key string, params map[string]any) b21Reply {
	f.t.Helper()
	if key != "" {
		params["_meta"] = map[string]any{"project": b21Project, "agent": b21Agent, "idempotencyKey": key}
	}
	body, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "b21", "method": method, "params": params})
	if e != nil {
		f.t.Fatal(e)
	}
	req, e := http.NewRequestWithContext(f.ctx, http.MethodPost, f.api.URL+httpjson.Path, bytes.NewReader(body))
	if e != nil {
		f.t.Fatal(e)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	res, e := f.api.Client().Do(req)
	if e != nil {
		f.t.Fatal(e)
	}
	defer res.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if e != nil {
		f.t.Fatal(e)
	}
	f.frames = append(f.frames, json.RawMessage(body), json.RawMessage(append([]byte(nil), raw...)))
	var out b21Reply
	if e = json.Unmarshal(raw, &out); e != nil || res.StatusCode != http.StatusOK {
		f.t.Fatalf("%s HTTP=%d response=%s err=%v", method, res.StatusCode, raw, e)
	}
	return out
}
func (f *b21Fixture) apply(key string, cs model.ChangeSet) b21Reply {
	f.t.Helper()
	before, _ := json.Marshal(cs)
	r := f.call("tx.apply", key, map[string]any{"changeSet": cs})
	after, _ := json.Marshal(cs)
	b21Equal(f.t, string(before), string(after), "caller ChangeSet values")
	return r
}
func b21Commit(t *testing.T, r b21Reply) model.Commit {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("public refusal: %+v", r.Error.Data)
	}
	var c model.Commit
	if e := json.Unmarshal(r.Result, &c); e != nil {
		t.Fatal(e)
	}
	return c
}
func b21Code(t *testing.T, r b21Reply, code model.ErrorCode) {
	t.Helper()
	if r.Error == nil || r.Error.Data.Code != code || r.Error.Data.Retryable || r.Error.Data.TraceID == "" || len(r.Result) != 0 {
		t.Fatalf("want typed %s without result, got %+v result=%s", code, r.Error, r.Result)
	}
}
func b21Equal(t *testing.T, want, got any, label string) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("%s\nwant=%+v\ngot =%+v", label, want, got)
	}
}
func b21Create(key, title string) model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: key, Type: "issue", Project: b21Project, Fields: map[string]any{"title": title}}}
}
func b21Patch(ref model.Ref, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: fields}}
}
func b21Close(ref model.Ref) model.Op {
	return model.Op{Kind: model.OpClose, Close: &model.CloseOp{Target: ref}}
}
func b21Link(from, to model.Ref, gate string) model.Op {
	return model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: "waits-for", Metadata: &model.LinkMetadata{Gate: gate}}}
}

type b21State struct {
	StoreID     driver.StoreID
	Incarnation string
	Head        model.Revision
	Records     map[string]driver.Record
	Versions    map[string][]driver.Version
	Log         []model.Changeset
}

func (f *b21Fixture) state() b21State {
	f.t.Helper()
	m, e := f.store.Meta(f.ctx)
	if e != nil {
		f.t.Fatal(e)
	}
	r, e := f.store.Read(f.ctx, driver.View{Incarnation: m.Incarnation})
	if e != nil {
		f.t.Fatal(e)
	}
	defer r.Close()
	s := b21State{StoreID: m.StoreID, Incarnation: m.Incarnation, Head: m.Head, Records: map[string]driver.Record{}, Versions: map[string][]driver.Version{}}
	page, e := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if e != nil || page.NextCursor != nil {
		f.t.Fatalf("complete records %v %v", page, e)
	}
	for _, row := range page.Items {
		s.Records[row.Ref.ID] = row
		v, e := r.Versions(f.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if e != nil || v.NextCursor != nil {
			f.t.Fatal("complete versions", e)
		}
		s.Versions[row.Ref.ID] = v.Items
	}
	s.Log, e = r.Changesets(f.ctx, nil, 1000)
	if e != nil {
		f.t.Fatal(e)
	}
	return s
}
func (f *b21Fixture) receipt(key string) *driver.IdempotencyRecord {
	f.t.Helper()
	m, e := f.store.Meta(f.ctx)
	if e != nil {
		f.t.Fatal(e)
	}
	r, e := f.store.Read(f.ctx, driver.View{Incarnation: m.Incarnation})
	if e != nil {
		f.t.Fatal(e)
	}
	defer r.Close()
	principal, e := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if e != nil {
		f.t.Fatal(e)
	}
	v, e := r.IdempotencyByKey(f.ctx, principal, b21Agent, "tx.apply", key)
	if e != nil {
		f.t.Fatal(e)
	}
	return v
}
func (f *b21Fixture) doc(ref model.Ref) pipeline.Document {
	f.t.Helper()
	row, ok := f.state().Records[ref.ID]
	if !ok {
		f.t.Fatalf("missing committed %s", ref.ID)
	}
	d, e := pipeline.DecodeDocument(row.Body)
	if e != nil {
		f.t.Fatal(e)
	}
	return d
}
func (f *b21Fixture) publicRow(ref model.Ref) map[string]any {
	f.t.Helper()
	r := f.call("bead.get", "", map[string]any{"id": ref, "project": b21Project})
	if r.Error != nil {
		f.t.Fatalf("bead.get: %+v", r.Error.Data)
	}
	var row map[string]any
	d := json.NewDecoder(bytes.NewReader(r.Result))
	d.UseNumber()
	if e := d.Decode(&row); e != nil {
		f.t.Fatal(e)
	}
	bead, ok := row["bead"].(map[string]any)
	if !ok {
		f.t.Fatalf("missing public bead envelope %s", r.Result)
	}
	return bead
}
func b21Retain(t *testing.T, name string, value any) {
	t.Helper()
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if root == "" {
		return
	}
	name = strings.NewReplacer("/", "_", " ", "_").Replace(t.Name() + "-" + name)
	raw, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		t.Error(e)
		return
	}
	if e = os.WriteFile(filepath.Join(root, name+".json"), append(raw, '\n'), 0600); e != nil {
		t.Error(e)
	}
}

func b21Decode(t *testing.T, raw []byte, out any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if e := d.Decode(out); e != nil {
		t.Fatal(e)
	}
}

func (f *b21Fixture) publicFields(ref model.Ref, want map[string]any) {
	f.t.Helper()
	row := f.publicRow(ref)
	if row["id"] != ref.ID {
		f.t.Fatalf("exact public id %+v", row)
	}
	fields, ok := row["fields"].(map[string]any)
	if !ok {
		f.t.Fatalf("missing public fields %+v", row)
	}
	for key, value := range want {
		b21Equal(f.t, value, fields[key], "exact public field "+key)
	}
}
