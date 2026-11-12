package b13memory_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const project = "b13-public"
const agent = "b13-public-author-fixture"

var anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type scope struct{ Method, Key string }
type frame struct {
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
}
type reply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *wire.Error     `json:"error"`
	Meta    *struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

type fixture struct {
	t                *testing.T
	ctx              context.Context
	engine, dsn      string
	store, oracle    driver.Store
	db               *sql.DB
	lease            driver.ActiveLease
	stopRenewal      context.CancelFunc
	renewed          chan error
	server           *rpc.Server
	nextID           int
	request          int
	scopes           map[scope]bool
	frames           []frame
	policyError      bool
	fullcardWorkflow bool
}

func newFixture(t *testing.T, engine string) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	f := &fixture{t: t, ctx: ctx, engine: engine, scopes: map[scope]bool{}}
	if engine == "sqlite" {
		f.dsn = filepath.Join(t.TempDir(), "b13.db")
	} else if engine == "postgres" {
		f.dsn = ownedPostgres(t)
	}
	t.Cleanup(func() { f.close(); f.writeTrace() })
	f.open()
	return f
}

func (f *fixture) open() {
	f.t.Helper()
	var err error
	switch f.engine {
	case "memory":
		f.store = memory.New()
		f.oracle = f.store
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = postgres.Open(f.ctx, driver.Config{DSN: f.dsn})
	default:
		f.t.Fatalf("unknown engine %q", f.engine)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	if f.engine != "memory" {
		// A separate real handle and SQL connection observe the same file/DB.
		if f.engine == "sqlite" {
			f.oracle, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.dsn})
		} else {
			f.oracle, err = postgres.Open(f.ctx, driver.Config{DSN: f.dsn})
		}
		if err != nil {
			f.t.Fatal(err)
		}
		sqlDriver := "sqlite"
		if f.engine == "postgres" {
			sqlDriver = "pgx"
		}
		f.db, err = sql.Open(sqlDriver, f.dsn)
		if err != nil {
			f.t.Fatal(err)
		}
	}
	f.lease, err = f.store.Lease(f.ctx, agent, driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	renewCtx, stop := context.WithCancel(f.ctx)
	f.stopRenewal, f.renewed = stop, make(chan error, 1)
	lease, renewed := f.lease, f.renewed
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				renewed <- nil
				return
			case <-lease.Lost():
				renewed <- errors.New("B13 fixture lost its active lease")
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil {
					if renewCtx.Err() != nil {
						err = nil
					}
					renewed <- err
					return
				}
			}
		}
	}()
	f.bind()
}

func (f *fixture) bind() {
	f.t.Helper()
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
		f.nextID++
		if f.nextID > 16 {
			return "", errors.New("B13 ID inventory exhausted")
		}
		return fmt.Sprintf("b13-public-%02d", f.nextID), nil
	}))
	domain := service.New(f.store, f.lease, writer, service.WithOntology(ontology),
		service.WithMemoryReadAuthorizer(service.MemoryReadAuthorizerFunc(func(_ context.Context, _ model.PrincipalRef, m service.Memory) (bool, error) {
			if f.policyError {
				return false, errors.New("fixture policy unavailable")
			}
			return m.Key != "hidden", nil
		})))
	if f.fullcardWorkflow {
		service.WithWorkflow(f.fullcardProvider())(domain)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err = rpc.BindService(registry, domain); err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, CursorKey: []byte("b13-public-fixed-cursor-key-00001")})
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) close() {
	if f.stopRenewal != nil {
		f.stopRenewal()
		if err := <-f.renewed; err != nil {
			f.t.Error(err)
		}
		f.stopRenewal = nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if f.lease != nil {
		if err := f.lease.Release(ctx); err != nil {
			f.t.Error(err)
		}
		f.lease = nil
	}
	if f.db != nil {
		if err := f.db.Close(); err != nil {
			f.t.Error(err)
		}
		f.db = nil
	}
	if f.oracle != nil && f.oracle != f.store {
		if err := f.oracle.Close(); err != nil {
			f.t.Error(err)
		}
	}
	f.oracle = nil
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) reopen() {
	f.t.Helper()
	before := f.state()
	if f.engine == "memory" {
		// Memory only proves a fresh binding over the same live store.
		f.bind()
	} else {
		f.close()
		f.open() // exact same DSN; every earlier pool/connection was closed
	}
	requireSame(f.t, before, f.state())
}

func encode(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func decode[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var value T
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return value
}

func (f *fixture) call(method string, params map[string]any, key string) reply {
	f.t.Helper()
	f.request++
	p := make(map[string]any, len(params)+1)
	for k, v := range params {
		p[k] = v
	}
	meta := map[string]any{"agent": agent, "project": project}
	if key != "" {
		meta["idempotencyKey"] = key
		f.scopes[scope{method, key}] = true
	}
	p["_meta"] = meta
	request := encode(f.t, map[string]any{"jsonrpc": "2.0", "id": f.request, "method": method, "params": p})
	raw, err := inprocess.New(f.server).RoundTrip(f.ctx, request)
	if err != nil {
		f.t.Fatal(err)
	}
	f.frames = append(f.frames, frame{request, raw})
	r := decode[reply](f.t, raw)
	if r.JSONRPC != "2.0" || r.ID != f.request || (r.Error == nil) == (r.Result == nil) {
		f.t.Fatalf("invalid public response: %s", raw)
	}
	return r
}

func result(t *testing.T, r reply) json.RawMessage {
	t.Helper()
	if r.Error != nil || r.Meta != nil || len(r.Result) == 0 {
		t.Fatalf("expected fresh public result: %+v", r)
	}
	return r.Result
}

func (f *fixture) writeTrace() {
	dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if dir == "" {
		return
	}
	var buf bytes.Buffer
	for _, frame := range f.frames {
		buf.Write(encode(f.t, frame))
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "b13-"+f.engine+"-public.jsonl"), buf.Bytes(), 0600); err != nil {
		f.t.Error(err)
	}
}
