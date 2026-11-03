package b16sourcehistory_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const project = "demo"
const agent = "b16-source-history-client"

type fixture struct {
	t           *testing.T
	engine, dsn string
	store       driver.Store
	lease       driver.ActiveLease
	server      *rpc.Server
	sequence    int
	attempts    map[receiptIdentity]bool
	stopRenew   context.CancelFunc
	renewed     chan error
}

type reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
}

func eachEngine(t *testing.T, run func(*testing.T, *fixture)) {
	t.Helper()
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := &fixture{t: t, engine: engine, attempts: map[receiptIdentity]bool{}}
			if engine == "sqlite" {
				f.dsn = filepath.Join(t.TempDir(), "b16-source-history.db")
			} else if engine == "postgres" {
				f.dsn = freshDatabase(t)
			}
			t.Cleanup(f.close)
			f.open()
			run(t, f)
		})
	}
}

func (f *fixture) open() {
	f.t.Helper()
	var err error
	switch f.engine {
	case "memory":
		f.store = memory.New()
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = (postgres.Driver{}).Open(context.Background(), driver.Config{DSN: f.dsn})
	default:
		f.t.Fatalf("unknown real engine %q", f.engine)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease, err = f.store.Lease(context.Background(), "b16-source-history-host", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.stopRenew, f.renewed = cancel, make(chan error, 1)
	lease, renewed := f.lease, f.renewed
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				renewed <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil {
					if ctx.Err() != nil {
						err = nil
					}
					renewed <- err
					return
				}
			}
		}
	}()
	f.host()
}

// Rebuild the public host after import publishes a new incarnation.
func (f *fixture) host() {
	f.t.Helper()
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology))
	domain := service.New(f.store, f.lease, writer, service.WithOntology(ontology))
	f.bind(domain, viewops.Config{})
}

func (f *fixture) bind(domain rpc.Service, cfg viewops.Config) {
	f.t.Helper()
	cfg.OntologyRevision = model.Natural64(1)
	views, err := viewops.New(f.store, f.lease, cfg)
	if err != nil {
		f.t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		f.t.Fatal(err)
	}
	key := []byte("b16-source-history-cursor-key-001")
	if err := viewrpc.BindWithConfig(registry, views, viewrpc.Config{CursorKey: key}); err != nil {
		f.t.Fatal(err)
	}
	meta := f.meta()
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: key})
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) close() {
	f.t.Helper()
	if f.stopRenew != nil {
		f.stopRenew()
		if err := <-f.renewed; err != nil {
			f.t.Error(err)
		}
		f.stopRenew = nil
	}
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			f.t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) meta() driver.Meta {
	f.t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	return meta
}

func (f *fixture) reopen() {
	f.t.Helper()
	if f.engine == "memory" {
		f.t.Fatal("memory has no physical persistence reopen")
	}
	before := f.state()
	f.close()
	f.open()
	requireState(f.t, before, f.state())
	f.t.Logf("physical %s pool/file close + reopen preserved complete durable state", f.engine)
}

func (f *fixture) nextKey() string {
	f.sequence++
	return fmt.Sprintf("%032x", f.sequence)
}

func (f *fixture) request(method string, params map[string]any, key, code string) reply {
	f.t.Helper()
	copyParams := make(map[string]any, len(params)+1)
	for k, v := range params {
		copyParams[k] = v
	}
	meta := map[string]any{"project": project, "agent": agent}
	if supplied, ok := copyParams["_meta"].(map[string]any); ok {
		for k, v := range supplied {
			meta[k] = v
		}
	}
	if key != "" {
		meta["idempotencyKey"] = key
		f.track(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, agent, method, key)
	}
	copyParams["_meta"] = meta
	raw := encoded(f.t, map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": copyParams})
	raw, err := inprocess.New(f.server).RoundTrip(context.Background(), raw)
	if err != nil {
		f.t.Fatal(err)
	}
	var response reply
	if err := json.Unmarshal(raw, &response); err != nil {
		f.t.Fatal(err)
	}
	if code != "" {
		if response.Error == nil || string(response.Error.Data.Code) != code || len(response.Result) != 0 {
			f.t.Fatalf("%s want typed %s without result: %s", method, code, raw)
		}
	} else if response.Error != nil || len(response.Result) == 0 {
		f.t.Fatalf("%s failed: %s", method, raw)
	}
	return response
}

func encoded(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func commit(t *testing.T, response reply) model.Commit {
	t.Helper()
	var result model.Commit
	if err := json.Unmarshal(response.Result, &result); err != nil || result.Revision.IsZero() {
		t.Fatalf("expected real commit: %s: %v", response.Result, err)
	}
	return result
}

func (f *fixture) get(id string) json.RawMessage {
	f.t.Helper()
	return f.request("bead.get", map[string]any{"id": ref(id)}, "", "").Result
}

// Observers never write. All records enter through the real admitted importer
// or ordinary public tx.apply; no source image is repaired by the fixture.
type receiptIdentity struct {
	Principal, Agent, Method, Key string
}

func (f *fixture) track(principal model.PrincipalRef, agentLabel, method, key string) receiptIdentity {
	f.t.Helper()
	encoded, err := driver.PrincipalKey(principal)
	if err != nil {
		f.t.Fatal(err)
	}
	identity := receiptIdentity{encoded, agentLabel, method, key}
	f.attempts[identity] = true
	return identity
}

type durableState struct {
	Meta     driver.Meta
	Records  []driver.Record
	Versions map[string][]driver.Version
	Log      []model.Changeset
	Derived  map[driver.DerivedTable][]driver.DerivedRow
	Receipts map[receiptIdentity]*driver.IdempotencyRecord
}

func (f *fixture) state() durableState {
	f.t.Helper()
	meta := f.meta()
	at := meta.Head
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(context.Background(), driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		f.t.Fatalf("complete state records: %+v %v", page, err)
	}
	state := durableState{Meta: meta, Records: page.Items, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[receiptIdentity]*driver.IdempotencyRecord{}}
	for _, record := range page.Items {
		versions, err := r.Versions(context.Background(), record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			f.t.Fatalf("complete state versions: %+v %v", versions, err)
		}
		state.Versions[record.Ref.ID] = versions.Items
	}
	state.Log, err = r.Changesets(context.Background(), nil, 1000)
	if err != nil || len(state.Log) == 1000 {
		f.t.Fatalf("complete state changesets: %v", err)
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := r.DerivedRows(context.Background(), table, "")
		if err != nil {
			f.t.Fatal(err)
		}
		state.Derived[table] = rows
	}
	for identity := range f.attempts {
		receipt, err := r.IdempotencyByKey(context.Background(), identity.Principal, identity.Agent, identity.Method, identity.Key)
		if err != nil {
			f.t.Fatal(err)
		}
		if receipt != nil {
			state.Receipts[identity] = receipt
		}
	}
	return state
}

func requireState(t *testing.T, before, after durableState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("durable state changed: before=%+v after=%+v", before, after)
	}
}
