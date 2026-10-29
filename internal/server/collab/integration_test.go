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
	writer  *pipeline.Writer
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
	f.writer = writer
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

func TestRealRPCHalfOpenExpiryOnlySuccessfulSetRenewsAndReconnectReopens(t *testing.T) {
	f := newFixture(t, nil)
	id := f.open(t, alice, map[string]any{})
	lifetime, err := f.manager.SessionContext(context.Background(), collab.Caller{Principal: alice}, id)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.advance(4 * time.Minute)
	if len(f.rows(t, alice, "project-a")) != 1 {
		t.Fatal("expired early")
	}
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "focus": "bead/missing"}), "not_found")
	f.clock.advance(time.Minute)
	if len(f.rows(t, alice, "project-a")) != 0 {
		t.Fatal("list/failed set renewed expiry at exact boundary")
	}
	if lifetime.Err() == nil {
		t.Fatal("expiry did not cancel session context")
	}
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id}), "not_found")
	code(t, f.call(t, alice, "session.close", map[string]any{"sessionId": id}), "not_found")
	reopened := f.open(t, alice, map[string]any{})
	if reopened == id {
		t.Fatal("reconnect reused expired ID")
	}
	f.clock.advance(4 * time.Minute)
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": reopened, "focus": "", "workingOn": []string{}}))
	f.clock.advance(4 * time.Minute)
	if len(f.rows(t, alice, "project-a")) != 1 {
		t.Fatal("successful set did not renew")
	}
	f.clock.advance(time.Minute)
	if len(f.rows(t, alice, "project-a")) != 0 {
		t.Fatal("renewed session exceeded five minutes")
	}
}

func TestRealRPCQuotasAreReclaimedByCloseAndExpiry(t *testing.T) {
	f := newFixture(t, func(c *collab.Config) { c.Limits = collab.Limits{Sessions: 3, PerPrincipal: 2, PerSponsor: 2} })
	first := f.open(t, alice, map[string]any{})
	f.open(t, alice, map[string]any{})
	code(t, f.call(t, alice, "session.open", map[string]any{}), "too_many_sessions")
	f.open(t, bob, map[string]any{})
	charlie := model.PrincipalRef{Kind: "user", ID: "user:charlie"}
	code(t, f.call(t, charlie, "session.open", map[string]any{}), "too_many_sessions")
	result(t, f.call(t, alice, "session.close", map[string]any{"sessionId": first}))
	f.open(t, charlie, map[string]any{})
	f.clock.advance(collab.SessionTTL)
	if len(f.rows(t, alice, "project-a")) != 0 {
		t.Fatal("quota retained expired rows")
	}
	f.open(t, alice, map[string]any{})
}

func TestRealRPCFortySessionsCompactWithoutTransferringOwnership(t *testing.T) {
	f := newFixture(t, nil)
	ids := map[string]bool{}
	for range 40 {
		id := f.open(t, alice, map[string]any{"onBehalfOf": alice.ID})
		if len(id) != 64 || ids[id] {
			t.Fatal("session IDs are not distinct 256-bit tokens")
		}
		ids[id] = true
	}
	rows := f.rows(t, bob, "project-a")
	if len(rows) != 1 {
		t.Fatalf("forty sessions produced %d rows", len(rows))
	}
	row := rows[0].(map[string]any)
	if row["onBehalfOf"] != alice.ID || !ids[row["sessionId"].(string)] || len(row) != 3 {
		t.Fatal(row)
	}
	code(t, f.call(t, bob, "session.close", map[string]any{"sessionId": row["sessionId"]}), "not_found")
	result(t, f.call(t, alice, "session.close", map[string]any{"sessionId": row["sessionId"]}))
	if len(f.rows(t, bob, "project-a")) != 1 {
		t.Fatal("closing a representative dropped other live members")
	}
	code(t, f.call(t, bob, "session.open", map[string]any{"onBehalfOf": alice.ID}), "forbidden")
	f.open(t, bob, map[string]any{})
	if len(f.rows(t, bob, "project-a")) != 2 {
		t.Fatal("unrelated session was grouped")
	}
}

func TestRealRPCWorkingOnRequiresLiveCallerClaimsAndExpiredReclaimRefuses(t *testing.T) {
	f := newFixture(t, nil)
	bead := f.create(t, "project-a")
	resource := "bead/" + bead
	aliceSession := f.open(t, alice, map[string]any{})
	bobSession := f.open(t, bob, map[string]any{})
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": aliceSession, "workingOn": []string{resource}}), "forbidden")
	f.write(t, alice, "bead.claim", map[string]any{"id": map[string]any{"id": bead}})
	code(t, f.call(t, bob, "presence.set", map[string]any{"sessionId": bobSession, "workingOn": []string{resource}}), "forbidden")
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": aliceSession, "focus": resource, "workingOn": []string{resource}}))
	f.clock.advance(4 * time.Minute)
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": aliceSession}))
	f.clock.advance(time.Minute)
	rows := f.rows(t, bob, "project-a")
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	row := rows[0].(map[string]any)
	if row["focus"] != resource || row["workingOn"] != nil {
		t.Fatal("expired claim left a marker", row)
	}
	// B05 recovery has no implemented service/RPC producer at this base.
	// Expiry alone does not reopen a lifecycle slot. Actual expired foreign
	// takeover and durable reclaim behavior remain unqualified.
	before := durableState(t, f)
	code(t, f.call(t, bob, "bead.claim", map[string]any{"id": map[string]any{"id": bead}, "_meta": map[string]any{"idempotencyKey": strings.Repeat("c", 32)}}), "invalid_params")
	if string(before) != string(durableState(t, f)) {
		t.Fatal("unsupported expired takeover changed durable state")
	}
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": aliceSession}), "forbidden")
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": aliceSession, "workingOn": []string{}}))
	if f.rows(t, bob, "project-a")[0].(map[string]any)["workingOn"] != nil {
		t.Fatal("expired claim leaked as current work")
	}
}

func TestRealRPCClosedClaimIsRemovedFromCurrentWorkingOn(t *testing.T) {
	f := newFixture(t, nil)
	bead := f.create(t, "project-a")
	resource := "bead/" + bead
	f.write(t, alice, "bead.claim", map[string]any{"id": map[string]any{"id": bead}})
	id := f.open(t, alice, map[string]any{})
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "focus": resource, "workingOn": []string{resource}}))
	// Use the delivered bound lifecycle operation, while the claim is live.
	// This is a real current-state change, independent of B05 recovery.
	f.write(t, alice, "bead.close", map[string]any{"id": map[string]any{"id": bead}})
	before := durableState(t, f)
	row := f.rows(t, bob, "project-a")[0].(map[string]any)
	if row["focus"] != resource || row["workingOn"] != nil {
		t.Fatal("closed claim remained working-on or removed readable focus", row)
	}
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id}), "forbidden")
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "workingOn": []string{}}))
	if string(before) != string(durableState(t, f)) {
		t.Fatal("current-claim presence checks changed durable state")
	}
}

func TestRealRPCDeletedFocusHasNoMarkerAndCrossProjectFocusRefuses(t *testing.T) {
	f := newFixture(t, nil)
	bead := f.create(t, "project-a")
	other := f.create(t, "project-b")
	id := f.open(t, alice, map[string]any{})
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "focus": "bead/" + other}), "not_found")
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "focus": "bead/" + bead}))
	_, err := f.writer.Apply(context.Background(), alice, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: model.RefByID(bead, model.Natural64(0))}}}}, model.WriteMeta{Method: "bead.delete", Project: "project-a", IdempotencyKey: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})
	if err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t, bob, "project-a")
	row := rows[0].(map[string]any)
	if len(row) != 2 || row["focus"] != nil || strings.Contains(fmt.Sprint(rows), bead) {
		t.Fatal("deleted focus left a resource marker", rows)
	}
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "focus": ""}))
}

func TestRealRPCInputBoundsAndAnonymousClaimLabels(t *testing.T) {
	f := newFixture(t, nil)
	id := f.open(t, alice, map[string]any{})
	for _, focus := range []string{"view/not-supported", "bead/", "bead/" + strings.Repeat("a", collab.MaxResourceBytes)} {
		code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "focus": focus}), "invalid_params")
	}
	resources := make([]string, collab.MaxWorkingOn+1)
	code(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "workingOn": resources}), "invalid_params")
	code(t, f.call(t, alice, "presence.list", map[string]any{"project": ""}), "project_required")
	anonymous := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	bead := f.create(t, "project-a")
	f.write(t, anonymous, "bead.claim", map[string]any{"id": map[string]any{"id": bead}, "_meta": map[string]any{"agent": "client-a"}})
	session := f.open(t, anonymous, map[string]any{"_meta": map[string]any{"agent": "client-a"}})
	code(t, f.call(t, anonymous, "presence.set", map[string]any{"sessionId": session, "_meta": map[string]any{"agent": "client-b"}}), "not_found")
	result(t, f.call(t, anonymous, "presence.set", map[string]any{"sessionId": session, "workingOn": []string{"bead/" + bead}, "_meta": map[string]any{"agent": "client-a"}}))
	code(t, f.call(t, alice, "session.close", map[string]any{"sessionId": session, "_meta": map[string]any{"agent": "client-a"}}), "not_found")
}
