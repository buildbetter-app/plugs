package collab_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.t = c.t.Add(d) }

type fixture struct {
	store   *memory.Store
	lease   driver.ActiveLease
	service *service.Service
	manager *collab.Manager
	server  *rpc.Server
	clock   *testClock
	config  collab.Config
	key     int
}

func newFixture(t *testing.T, configure func(*collab.Config)) *fixture {
	t.Helper()
	f := &fixture{store: memory.New(), clock: &testClock{t: time.Date(2026, 10, 29, 13, 0, 0, 0, time.UTC)}}
	t.Cleanup(func() { _ = f.store.Close() })
	var err error
	f.lease, err = f.store.Lease(context.Background(), "collab-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.lease.Release(context.Background()) })
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithClock(f.clock.now))
	f.service = service.New(f.store, f.lease, writer, service.WithOntology(ontology), service.WithClock(f.clock.now))
	f.config = collab.Config{Service: f.service, Lease: f.lease, Clock: f.clock.now, AuthorizationInstalled: func() bool { return false }}
	if configure != nil {
		configure(&f.config)
	}
	f.manager, err = collab.New(context.Background(), f.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.manager.Close() })
	f.server = f.bind(t, f.manager)
	return f
}

func (f *fixture) bind(t *testing.T, manager *collab.Manager) *rpc.Server {
	t.Helper()
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.service); err != nil {
		t.Fatal(err)
	}
	if err := collab.Bind(registry, manager); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "project-a"})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func rpcCall(t *testing.T, server *rpc.Server, ctx context.Context, method string, params any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := inprocess.New(server).RoundTrip(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	v, err := wire.DecodeJSON(reply)
	if err != nil {
		t.Fatal(err)
	}
	return v.(map[string]any)
}

func result(t *testing.T, response map[string]any) map[string]any {
	t.Helper()
	v, ok := response["result"].(map[string]any)
	if !ok {
		t.Fatalf("RPC failed: %v", response)
	}
	return v
}

func code(t *testing.T, response map[string]any, expected string) {
	t.Helper()
	e, _ := response["error"].(map[string]any)
	d, _ := e["data"].(map[string]any)
	if d["code"] != expected {
		t.Fatalf("want %s: %v", expected, response)
	}
}

func (f *fixture) call(t *testing.T, by model.PrincipalRef, method string, p map[string]any) map[string]any {
	t.Helper()
	ctx := rpc.WithPrincipal(context.Background(), by)
	return rpcCall(t, f.server, ctx, method, p)
}

var alice = model.PrincipalRef{Kind: "user", ID: "user:alice"}
var bob = model.PrincipalRef{Kind: "user", ID: "user:bob"}

func (f *fixture) write(t *testing.T, by model.PrincipalRef, method string, p map[string]any) map[string]any {
	t.Helper()
	f.key++
	if p["_meta"] == nil {
		p["_meta"] = map[string]any{}
	}
	p["_meta"].(map[string]any)["idempotencyKey"] = fmt.Sprintf("%032x", f.key)
	return result(t, f.call(t, by, method, p))
}

func (f *fixture) create(t *testing.T, project string) string {
	t.Helper()
	v := f.write(t, alice, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "Presence boundary"}, "_meta": map[string]any{"project": project}})
	return v["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
}

func (f *fixture) open(t *testing.T, by model.PrincipalRef, p map[string]any) string {
	t.Helper()
	return result(t, f.call(t, by, "session.open", p))["sessionId"].(string)
}

func (f *fixture) rows(t *testing.T, by model.PrincipalRef, project string) []any {
	t.Helper()
	return result(t, f.call(t, by, "presence.list", map[string]any{"project": project}))["presence"].([]any)
}

func durableState(t *testing.T, f *fixture) []byte {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	log, err := r.Changesets(ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal([]any{meta, rows, log})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRealRPCPresenceLifecycleUsesServiceClaimsWithoutDurableWrites(t *testing.T) {
	f := newFixture(t, nil)
	id := f.create(t, "project-a")
	f.write(t, alice, "bead.claim", map[string]any{"id": map[string]any{"id": id}})
	before := durableState(t, f)
	session := f.open(t, alice, map[string]any{})
	row := result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": session, "focus": "bead/" + id, "workingOn": []string{"bead/" + id, "bead/" + id}}))
	if row["focus"] != "bead/"+id || !reflect.DeepEqual(row["workingOn"], []any{"bead/" + id}) {
		t.Fatal(row)
	}
	rows := f.rows(t, bob, "project-a")
	if len(rows) != 1 || !reflect.DeepEqual(rows[0], row) {
		t.Fatal(rows)
	}
	if len(f.rows(t, bob, "project-b")) != 0 {
		t.Fatal("cross-project presence leaked")
	}
	closed := result(t, f.call(t, alice, "session.close", map[string]any{"sessionId": session}))
	if !reflect.DeepEqual(closed, map[string]any{"closed": true}) {
		t.Fatal(closed)
	}
	if len(f.rows(t, bob, "project-a")) != 0 {
		t.Fatal("closed presence retained")
	}
	code(t, f.call(t, alice, "session.close", map[string]any{"sessionId": session}), "not_found")
	if after := durableState(t, f); string(before) != string(after) {
		t.Fatal("presence altered graph, head or changeset bytes")
	}
}

func TestRealRPCOnlyBoundMethodsDiscoverAndStrictParamsRejectSubstitution(t *testing.T) {
	f := newFixture(t, nil)
	methods := result(t, f.call(t, alice, "rpc.methods", map[string]any{}))["methods"].([]any)
	for _, name := range []string{"session.open", "session.close", "presence.set", "presence.list"} {
		found := false
		for _, method := range methods {
			if method == name {
				found = true
			}
		}
		if !found {
			t.Fatalf("actual bound method missing: %s", name)
		}
	}
	for _, method := range methods {
		if method == "presence.watch" {
			t.Fatal("unsupported watch advertised")
		}
	}
	code(t, f.call(t, alice, "presence.watch", map[string]any{"sessionId": "none"}), "method_not_found")
	for _, p := range []map[string]any{{"principal": bob}, {"sponsor": bob}, {"actor": "other"}, {"_meta": map[string]any{"principal": bob}}, {"onBehalfOf": nil}} {
		code(t, f.call(t, alice, "session.open", p), "invalid_params")
	}
	code(t, f.call(t, alice, "session.open", map[string]any{"onBehalfOf": bob.ID}), "forbidden")
	if rows := f.rows(t, alice, "project-a"); len(rows) != 0 {
		t.Fatal("failed calls opened sessions", rows)
	}
	id := f.open(t, alice, map[string]any{})
	code(t, f.call(t, bob, "presence.set", map[string]any{"sessionId": id}), "not_found")
	code(t, f.call(t, bob, "session.close", map[string]any{"sessionId": id}), "not_found")
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "onBehalfOf": bob.ID}), "forbidden")
	if !strings.Contains(fmt.Sprint(f.rows(t, alice, "project-a")), alice.ID) {
		t.Fatal("owner was replaced")
	}
}
