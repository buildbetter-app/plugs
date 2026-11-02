package s1b21b24

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const (
	project = "s1-b21-b24"
	actor   = "s1-recorder"
)

var anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

var fixedCursorKey = []byte("s1-b21-b24-cursor-key-0123456789")

type frame struct {
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
}

type rpcError struct {
	Data struct {
		Code      string         `json:"code"`
		Retryable bool           `json:"retryable"`
		Details   map[string]any `json:"details"`
	} `json:"data"`
}

type reply struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
	Meta   struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

type fixture struct {
	t       *testing.T
	ctx     context.Context
	engine  string
	dsn     string
	path    string
	store   driver.Store
	lease   driver.ActiveLease
	writer  *pipeline.Writer
	service *service.Service
	server  *rpc.Server
	clock   *testclock.Clock
	ids     []string
	idIndex int
	frames  []frame
	mu      sync.Mutex
}

func newFixture(t *testing.T, engine string) *fixture {
	t.Helper()
	f := &fixture{
		t:      t,
		ctx:    context.Background(),
		engine: engine,
		clock:  testclock.New(t, time.Date(2026, 11, 1, 23, 0, 0, 123456000, time.UTC)),
		ids: []string{
			"s1-b21-anchor", "s1-b21-created", "s1-b22-anchor",
			"s1-b23-alpha", "s1-b23-beta", "s1-b23-closed", "s1-b24-item",
			"s1-i1-memory",
		},
	}
	if engine == "sqlite" {
		f.path = filepath.Join(t.TempDir(), "s1.sqlite")
	}
	if engine == "postgres" {
		f.dsn = f.clock.BindPostgres(t, testclock.PostgresDSN(t))
	}
	f.open(t)
	t.Cleanup(func() {
		f.close(t)
		f.writeTrace()
	})
	return f
}

func (f *fixture) nextID() (string, error) {
	if f.idIndex >= len(f.ids) {
		return "", fmt.Errorf("controlled ID sequence exhausted at %d", f.idIndex)
	}
	id := f.ids[f.idIndex]
	f.idIndex++
	return id, nil
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{
			Clock:       f.clock.Now,
			StoreID:     driver.StoreID("s1-memory-store"),
			Incarnation: "s1-memory-incarnation",
		})
	case "sqlite":
		f.store, err = (sqlite.Driver{Clock: f.clock.Now}).Open(f.ctx, driver.Config{DSN: f.path})
	case "postgres":
		f.store, err = postgres.Open(f.ctx, driver.Config{DSN: f.dsn})
	default:
		t.Fatalf("unknown engine %q", f.engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	f.bind(t)
}

func (f *fixture) bind(t *testing.T) {
	t.Helper()
	var err error
	f.lease, err = f.store.Lease(f.ctx, "s1-"+f.engine, driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(ontology),
		pipeline.WithClock(f.clock.Now),
		pipeline.WithIDGenerator(f.nextID),
	)
	f.service = service.New(f.store, f.lease, f.writer,
		service.WithOntology(ontology),
		service.WithClock(f.clock.Now),
	)
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.service); err != nil {
		t.Fatal(err)
	}
	view, err := viewops.New(f.store, f.lease, viewops.Config{OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := viewrpc.BindWithConfig(registry, view, viewrpc.Config{CursorKey: fixedCursorKey}); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{
		DefaultProject:   project,
		Incarnation:      meta.Incarnation,
		CursorKey:        fixedCursorKey,
		OntologyRevision: model.Natural64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) close(t *testing.T) {
	t.Helper()
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			var fenced driver.ErrFenced
			if !errors.As(err, &fenced) {
				t.Error(err)
			}
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

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	if f.engine != "sqlite" {
		return
	}
	if f.lease == nil || f.store == nil {
		t.Fatal("SQLite fixture is already closed")
	}
	if err := f.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.lease = nil
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store = nil
	var err error
	f.store, err = (sqlite.Driver{Clock: f.clock.Now}).Open(f.ctx, driver.Config{DSN: f.path})
	if err != nil {
		t.Fatal(err)
	}
	f.bind(t)
}

func key(label string) string {
	sum := sha256.Sum256([]byte("s1-b21-b24/" + label))
	return hex.EncodeToString(sum[:])
}

func (f *fixture) request(method, label string, params map[string]any) []byte {
	f.t.Helper()
	body := make(map[string]any, len(params)+1)
	for name, value := range params {
		body[name] = value
	}
	body["_meta"] = map[string]any{
		"project":        project,
		"agent":          actor,
		"idempotencyKey": key(label),
	}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": label, "method": method, "params": body})
	if err != nil {
		f.t.Fatal(err)
	}
	return raw
}

func (f *fixture) dispatch(raw []byte) reply {
	f.t.Helper()
	request := append([]byte(nil), raw...)
	response := f.server.Dispatch(f.ctx, request)
	if !bytes.Equal(raw, request) {
		f.t.Fatal("RPC dispatcher mutated caller request bytes")
	}
	f.mu.Lock()
	f.frames = append(f.frames, frame{Request: append([]byte(nil), raw...), Response: append([]byte(nil), response...)})
	f.mu.Unlock()
	var out reply
	decoder := json.NewDecoder(bytes.NewReader(response))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		f.t.Fatalf("invalid RPC response %s: %v", response, err)
	}
	return out
}

func (f *fixture) call(method, label string, params map[string]any) reply {
	return f.dispatch(f.request(method, label, params))
}

func commit(t *testing.T, got reply) model.Commit {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("unexpected RPC refusal: %s", got.Error.Data.Code)
	}
	var value model.Commit
	if err := json.Unmarshal(got.Result, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func (f *fixture) create(t *testing.T, label, title string) (model.Ref, model.Commit) {
	t.Helper()
	got := f.call("bead.create", label, map[string]any{
		"type": "issue", "fields": map[string]any{"title": title},
	})
	c := commit(t, got)
	if len(c.Results) != 1 {
		t.Fatalf("create result: %+v", c)
	}
	return c.Results[0].Ref, c
}

type storedState struct {
	Meta       driver.Meta
	Records    []driver.Record
	Versions   map[string][]driver.Version
	Changesets []model.Changeset
}

func (f *fixture) state(t *testing.T) storedState {
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
	page, err := r.Query(f.ctx, driver.Query{Limit: 10000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("record census: %+v %v", page, err)
	}
	state := storedState{Meta: meta, Versions: map[string][]driver.Version{}}
	state.Records = append([]driver.Record(nil), page.Items...)
	for i := range state.Records {
		state.Records[i].Body = append([]byte(nil), state.Records[i].Body...)
		versions, err := r.Versions(f.ctx, state.Records[i].Ref, driver.PageReq{Limit: 10000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("versions %s: %+v %v", state.Records[i].Ref.ID, versions, err)
		}
		state.Versions[state.Records[i].Ref.ID] = append([]driver.Version(nil), versions.Items...)
	}
	state.Changesets, err = r.Changesets(f.ctx, nil, 10000)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func sameState(t *testing.T, before, after storedState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("durable state changed: before=%+v after=%+v", before, after)
	}
}

func (f *fixture) writeTrace() {
	dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if dir == "" {
		return
	}
	f.mu.Lock()
	frames := append([]frame(nil), f.frames...)
	f.mu.Unlock()
	data, err := json.MarshalIndent(frames, "", "  ")
	if err != nil {
		f.t.Errorf("marshal trace: %v", err)
		return
	}
	name := strings.ReplaceAll(f.t.Name(), "/", "_") + "-" + f.engine + ".rpc.json"
	if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0600); err != nil {
		f.t.Errorf("write trace: %v", err)
	}
}

func (f *fixture) advance(t *testing.T, delta time.Duration) {
	t.Helper()
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(t, delta)
	f.bind(t)
}
