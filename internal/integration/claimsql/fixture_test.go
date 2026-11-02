package claimsql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/claimrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const claimProject = "claim-sql-project"

var (
	anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	core      = model.PrincipalRef{Kind: "system", ID: "core"}
)

type fixture struct {
	ctx     context.Context
	engine  engine
	clock   *testclock.Clock
	t       *testing.T
	store   driver.Store
	lease   driver.ActiveLease
	inc     string
	writer  *pipeline.Writer
	service *service.Service
	rpc     *rpc.Server
}

func newFixture(t *testing.T, e engine) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, engine: e, t: t, clock: testclock.New(t, time.Date(2026, 10, 29, 9, 0, 0, 123456000, time.UTC))}
	if e.name == "sqlite_wal" {
		f.engine.storage = sqlite.Driver{Clock: f.now}
	}
	if e.name == "postgres" {
		f.engine.dsn = f.clock.BindPostgres(t, e.dsn)
	}
	f.open(t)
	t.Cleanup(func() { f.close(t) })
	if e.name == "sqlite_wal" {
		db := e.db(t)
		var mode, version string
		if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
			t.Fatalf("real SQLite journal mode=%q err=%v", mode, err)
		}
		if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
			t.Fatal(err)
		}
		t.Logf("real SQLite %s, file-backed WAL", version)
	}
	return f
}

func (f *fixture) now() time.Time {
	return f.clock.Now()
}

func (f *fixture) advance(delta time.Duration) {
	f.t.Helper()
	if err := f.lease.Release(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	f.clock.Advance(f.t, delta)
	lease, err := f.store.Lease(f.ctx, "claim-sql-clock-advance", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease = lease
	f.compose(f.t)
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	store, err := f.engine.storage.Open(f.ctx, driver.Config{DSN: f.engine.dsn})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(f.ctx, "claim-sql-host", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	f.store, f.lease = store, lease
	meta := f.meta(t)
	if f.inc == "" {
		f.inc = meta.Incarnation
	} else if f.inc != meta.Incarnation {
		t.Fatalf("store incarnation changed across reopen: %s -> %s", f.inc, meta.Incarnation)
	}
	f.compose(t)
}

func (f *fixture) compose(t *testing.T) {
	t.Helper()
	f.writer, f.service, f.rpc = compose(t, f.store, f.lease, f.now, f.inc)
}

func compose(t *testing.T, store driver.Store, lease driver.ActiveLease, now func() time.Time, incarnation string) (*pipeline.Writer, *service.Service, *rpc.Server) {
	t.Helper()
	otype := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(otype), pipeline.WithClock(now))
	domain := service.New(store, lease, writer, service.WithOntology(otype), service.WithClock(now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	if err := claimrpc.Bind(registry, domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: incarnation, OntologyRevision: model.Natural64(1), DefaultProject: claimProject})
	if err != nil {
		t.Fatal(err)
	}
	return writer, domain, server
}

func (f *fixture) close(t *testing.T) {
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

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	before := f.meta(t)
	epoch := f.lease.Epoch()
	f.close(t)
	f.open(t)
	after := f.meta(t)
	if !reflect.DeepEqual(before, after) || f.lease.Epoch() <= epoch {
		t.Fatalf("close/reopen changed durable identity/head or reused host epoch: before=%+v after=%+v epoch=%d->%d", before, after, epoch, f.lease.Epoch())
	}
	t.Logf("%s close/reopen preserved store %s, incarnation %s, head %s; lease epochs %d -> %d", f.engine.name, before.StoreID, before.Incarnation, before.Head, epoch, f.lease.Epoch())
}

func (f *fixture) meta(t *testing.T) driver.Meta {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func (f *fixture) reader(t *testing.T) driver.Reader {
	t.Helper()
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.inc})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

type durableState struct {
	Meta     driver.Meta
	Records  []driver.Record
	Versions map[string][]driver.Version
	Log      []model.Changeset
	Receipt  *driver.IdempotencyRecord
	Ready    []string
}

func (f *fixture) state(t *testing.T, key string) durableState {
	t.Helper()
	s := durableState{Meta: f.meta(t), Versions: map[string][]driver.Version{}}
	r := f.reader(t)
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("bead query=%+v err=%v", page, err)
	}
	s.Records = page.Items
	for _, record := range s.Records {
		versions, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("versions for %s=%+v err=%v", record.Ref.ID, versions, err)
		}
		s.Versions[record.Ref.ID] = versions.Items
	}
	s.Log, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		principal, err := driver.PrincipalKey(core)
		if err != nil {
			t.Fatal(err)
		}
		s.Receipt, err = r.IdempotencyByKey(f.ctx, principal, "", "bead.release", key)
		if err != nil {
			t.Fatal(err)
		}
	}
	s.Ready = f.readyIDs(t)
	return s
}

func (f *fixture) record(t *testing.T, ref model.Ref) driver.Record {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("record %s=%+v err=%v", ref.ID, rows, err)
	}
	return rows[0]
}

func (f *fixture) refFromResult(t *testing.T, result map[string]any) model.Ref {
	t.Helper()
	rows, ok := result["results"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("missing single result: %+v", result)
	}
	row, ok := rows[0].(map[string]any)
	if !ok {
		t.Fatalf("invalid result row: %+v", rows[0])
	}
	raw, err := json.Marshal(row["ref"])
	if err != nil {
		t.Fatal(err)
	}
	var ref model.Ref
	if err := json.Unmarshal(raw, &ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

func writeKey(n int) string { return fmt.Sprintf("%064x", n) }

func params(payload map[string]any, key, agent string) map[string]any {
	meta := map[string]any{"project": claimProject}
	if key != "" {
		meta["idempotencyKey"] = key
	}
	if agent != "" {
		meta["agent"] = agent
	}
	payload["_meta"] = meta
	return payload
}

type rpcReply struct {
	Envelope map[string]any
	Raw      []byte
	Code     string
}

func dispatch(server *rpc.Server, ctx context.Context, by model.PrincipalRef, method string, payload map[string]any) rpcReply {
	request := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": payload}
	raw, err := json.Marshal(request)
	if err != nil {
		panic(err)
	}
	response := server.Dispatch(rpc.WithPrincipal(ctx, by), raw)
	var envelope map[string]any
	decoder := json.NewDecoder(bytes.NewReader(response))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		panic(fmt.Sprintf("invalid RPC response %s: %v", response, err))
	}
	reply := rpcReply{Envelope: envelope, Raw: response}
	if value, ok := envelope["error"].(map[string]any); ok {
		if data, ok := value["data"].(map[string]any); ok {
			reply.Code, _ = data["code"].(string)
		}
	}
	return reply
}

func mustResult(t *testing.T, reply rpcReply) map[string]any {
	t.Helper()
	if reply.Code != "" {
		t.Fatalf("RPC error %s: %s", reply.Code, reply.Raw)
	}
	result, ok := reply.Envelope["result"].(map[string]any)
	if !ok {
		t.Fatalf("missing RPC result: %s", reply.Raw)
	}
	return result
}

func mustCode(t *testing.T, reply rpcReply, want string) {
	t.Helper()
	if reply.Code != want {
		t.Fatalf("want RPC error %q, got %q: %s", want, reply.Code, reply.Raw)
	}
}

func mustError(t *testing.T, reply rpcReply) string {
	t.Helper()
	if reply.Code == "" {
		t.Fatalf("expected RPC error, got %s", reply.Raw)
	}
	return reply.Code
}

func requireSameState(t *testing.T, before, after durableState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("durable SQL state changed: before=%+v after=%+v", before, after)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (f *fixture) readyIDs(t *testing.T) []string {
	t.Helper()
	reply := dispatch(f.rpc, f.ctx, anonymous, "bead.ready", params(map[string]any{"limit": 100}, "", ""))
	items := mustResult(t, reply)["items"].([]any)
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.(map[string]any)["id"].(string))
	}
	return ids
}

func (f *fixture) createAndClaim(t *testing.T, n int, agent string) model.Ref {
	t.Helper()
	created := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.create", params(map[string]any{
		"type": "issue", "fields": map[string]any{"title": fmt.Sprintf("SQL claim %d", n)},
	}, writeKey(n*10+1), agent)))
	ref := f.refFromResult(t, created)
	claimed := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.claim", params(map[string]any{"id": ref}, writeKey(n*10+2), agent)))
	if claimed["leaseTtlMs"].(json.Number).String() != "300000" || claimed["leaseExpiresAt"] != f.now().Add(5*time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("claim did not use the controlled five-minute lease: %+v", claimed)
	}
	return ref
}

func (f *fixture) assertBead(t *testing.T, ref model.Ref, wantStatus string, wantLease bool) {
	t.Helper()
	reply := dispatch(f.rpc, f.ctx, anonymous, "bead.get", params(map[string]any{"id": ref}, "", ""))
	got := mustResult(t, reply)
	bead := got["bead"].(map[string]any)
	if bead["status"] != wantStatus {
		t.Fatalf("bead %s status=%v want %s", ref.ID, bead["status"], wantStatus)
	}
	_, hasLease := got["lease"]
	if hasLease != wantLease {
		t.Fatalf("bead %s lease presence=%t want %t: %+v", ref.ID, hasLease, got)
	}
}

func (f *fixture) tick() time.Duration {
	if f.engine.name == "postgres" {
		return time.Microsecond
	}
	return time.Nanosecond
}
