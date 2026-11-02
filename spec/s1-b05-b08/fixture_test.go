package s1b05b08

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/claimrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const (
	project = "s1-b05-b08"
	actor   = "s1-agent"
)

var anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type fixture struct {
	t       *testing.T
	ctx     context.Context
	engine  string
	dsn     string
	store   driver.Store
	storage driver.Driver
	lease   driver.ActiveLease
	writer  *pipeline.Writer
	service *service.Service
	server  *rpc.Server
	now     time.Time
	ids     []string
	index   int
}

func newFixture(t *testing.T, engine string) *fixture {
	t.Helper()
	f := &fixture{t: t, ctx: context.Background(), engine: engine, now: time.Date(2026, 11, 1, 23, 0, 0, 0, time.UTC), ids: []string{"s1-b05", "s1-b06", "s1-b07", "s1-b08", "s1-child", "s1-target", "s1-custom-source", "s1-custom-target"}}
	if engine == "memory" {
		f.store = memory.New()
	} else {
		f.storage = sqlite.Driver{}
		f.dsn = filepath.Join(t.TempDir(), "s1-b05-b08.db")
		if engine == "postgres" {
			f.storage, f.dsn = postgresFixture(t)
		} else if engine != "sqlite" {
			t.Fatalf("unknown fixture engine %q", engine)
		}
		var err error
		f.store, err = f.storage.Open(f.ctx, driver.Config{DSN: f.dsn})
		if err != nil {
			t.Fatal(err)
		}
	}
	f.open(t)
	t.Cleanup(func() { f.close() })
	return f
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	f.lease, err = f.store.Lease(f.ctx, "s1-b05-b08-host", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	nextID := func() (string, error) {
		if f.index >= len(f.ids) {
			return "", fmt.Errorf("declared fixture IDs exhausted")
		}
		id := f.ids[f.index]
		f.index++
		return id, nil
	}
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithClock(func() time.Time { return f.now }), pipeline.WithIDGenerator(nextID))
	f.service = service.New(f.store, f.lease, f.writer, service.WithOntology(ontology), service.WithClock(func() time.Time { return f.now }))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.service); err != nil {
		t.Fatal(err)
	}
	if err := claimrpc.Bind(registry, f.service); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: []byte("s1-b05-b08-fixed-cursor-key-32-bytes")})
	if err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) close() {
	if f.lease != nil {
		_ = f.lease.Release(context.Background())
		f.lease = nil
	}
	if f.store != nil {
		_ = f.store.Close()
		f.store = nil
	}
}

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	if f.storage == nil {
		return
	}
	f.close()
	store, err := f.storage.Open(f.ctx, driver.Config{DSN: f.dsn})
	if err != nil {
		t.Fatal(err)
	}
	f.store = store
	f.open(t)
}

func key(label string) string {
	digest := sha256.Sum256([]byte("s1-b05-b08/" + label))
	return hex.EncodeToString(digest[:])
}

type envelope struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	} `json:"error,omitempty"`
	Meta map[string]any `json:"_meta,omitempty"`
}

func (f *fixture) call(t *testing.T, ctx context.Context, method string, params map[string]any, idempotency, agent string) envelope {
	t.Helper()
	copyParams := make(map[string]any, len(params)+1)
	for k, v := range params {
		copyParams[k] = v
	}
	meta := map[string]any{"project": project}
	if idempotency != "" {
		meta["idempotencyKey"] = idempotency
	}
	if agent != "" {
		meta["agent"] = agent
	}
	copyParams["_meta"] = meta
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": copyParams})
	if err != nil {
		t.Fatal(err)
	}
	response := f.server.Dispatch(ctx, raw)
	var got envelope
	if err := json.Unmarshal(response, &got); err != nil {
		t.Fatalf("%s response %s: %v", method, response, err)
	}
	return got
}

func (f *fixture) create(t *testing.T, title string) model.Ref {
	t.Helper()
	got := f.call(t, f.ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}}, key("create-"+title), actor)
	if got.Error != nil {
		t.Fatalf("create refused: %s", got.Error.Data.Code)
	}
	var result model.Commit
	if err := json.Unmarshal(got.Result, &result); err != nil || len(result.Results) != 1 {
		t.Fatalf("create result=%s err=%v", got.Result, err)
	}
	return result.Results[0].Ref
}

type durableState struct {
	Head          string
	Record        []byte
	Versions      int
	Changes       int
	GraphSentinel string
}

func (f *fixture) state(t *testing.T, ref model.Ref) durableState {
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
	rows, err := r.Get(f.ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("state row=%v err=%v", rows, err)
	}
	versions, err := r.Versions(f.ctx, ref, driver.PageReq{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	states, err := r.Observe(f.ctx, []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: driver.SentinelGraphBlocking}})
	if err != nil || len(states) != 1 {
		t.Fatalf("graph sentinel: states=%+v err=%v", states, err)
	}
	return durableState{Head: meta.Head.String(), Record: append([]byte(nil), rows[0].Body...), Versions: len(versions.Items), Changes: len(changes), GraphSentinel: states[0].Revision.String()}
}

func (f *fixture) document(t *testing.T, ref model.Ref) pipeline.Document {
	t.Helper()
	doc, err := pipeline.DecodeDocument(f.state(t, ref).Record)
	if err != nil {
		t.Fatalf("decode %s: %v", ref.ID, err)
	}
	return doc
}
