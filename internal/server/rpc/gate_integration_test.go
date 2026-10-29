package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type gateRuntime struct {
	store    driver.Store
	lease    driver.ActiveLease
	server   *rpc.Server
	dsn      string
	ontology *service.Ontology
	clock    time.Time
}

func gateOntology(t *testing.T) *service.Ontology {
	t.Helper()
	doc := ontology.DefaultDocument()
	doc.GateKinds = map[string]ontology.GateKind{
		"human_approval": {
			Resolver: "builtin.manual",
			Params:   map[string]any{"min_approvals": json.Number("2")},
		},
		"external": {Resolver: "plugin.external"},
	}
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("gate ontology did not compile: %+v", problems)
	}
	return service.NewOntology(func(project string) ontology.Effective { return snapshot.Effective(project) })
}

func newGateRuntime(t *testing.T, engine, dsn string, o *service.Ontology, clock time.Time) *gateRuntime {
	t.Helper()
	ctx := context.Background()
	var store driver.Store
	var err error
	switch engine {
	case "memory":
		store = memory.New()
	case "sqlite":
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: dsn})
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
	default:
		t.Fatalf("unknown gate engine %q", engine)
	}
	lease, err := store.Lease(ctx, "gate-rpc", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatalf("lease %s: %v", engine, err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatalf("meta %s: %v", engine, err)
	}
	writer := pipeline.New(store, lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return clock }))
	domain := service.New(store, lease, writer, service.WithOntology(o), service.WithClock(func() time.Time { return clock }))
	registry, err := rpc.NewRegistry()
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatalf("bind gate service: %v", err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "local", OntologyRevision: model.Natural64(1), CursorKey: bytes.Repeat([]byte{0x51}, 32)})
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatalf("new RPC server: %v", err)
	}
	runtime := &gateRuntime{store: store, lease: lease, server: server, dsn: dsn, ontology: o, clock: clock}
	t.Cleanup(func() { runtime.close() })
	return runtime
}

func (r *gateRuntime) close() {
	ctx := context.Background()
	if r.lease != nil {
		_ = r.lease.Release(ctx)
		r.lease = nil
	}
	if r.store != nil {
		_ = r.store.Close()
		r.store = nil
	}
	r.server = nil
}

func (r *gateRuntime) restart(t *testing.T, engine string) {
	t.Helper()
	if engine != "sqlite" {
		t.Fatalf("restart is only meaningful for SQLite")
	}
	r.close()
	opened, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: r.dsn})
	if err != nil {
		t.Fatalf("reopen sqlite: %v", err)
	}
	lease, err := opened.Lease(context.Background(), "gate-rpc-restarted", driver.LeaseTTL)
	if err != nil {
		_ = opened.Close()
		t.Fatalf("reacquire sqlite lease: %v", err)
	}
	meta, err := opened.Meta(context.Background())
	if err != nil {
		_ = lease.Release(context.Background())
		_ = opened.Close()
		t.Fatal(err)
	}
	writer := pipeline.New(opened, lease, pipeline.WithOntology(r.ontology), pipeline.WithClock(func() time.Time { return r.clock }))
	domain := service.New(opened, lease, writer, service.WithOntology(r.ontology), service.WithClock(func() time.Time { return r.clock }))
	registry, err := rpc.NewRegistry()
	if err != nil {
		_ = lease.Release(context.Background())
		_ = opened.Close()
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		_ = lease.Release(context.Background())
		_ = opened.Close()
		t.Fatalf("bind restarted gate service: %v", err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "local", OntologyRevision: model.Natural64(1), CursorKey: bytes.Repeat([]byte{0x51}, 32)})
	if err != nil {
		_ = lease.Release(context.Background())
		_ = opened.Close()
		t.Fatal(err)
	}
	r.store, r.lease, r.server = opened, lease, server
}

func gateAgent(id, sponsor string) model.PrincipalRef {
	human := model.PrincipalRef{Kind: "user", ID: sponsor}
	return model.PrincipalRef{Kind: "agent", ID: id, Sponsor: &human}
}

func gateCall(t *testing.T, server *rpc.Server, ctx context.Context, method string, params map[string]any) map[string]any {
	t.Helper()
	raw, err := inprocess.New(server).RoundTrip(ctx, request(method, paramsJSON(t, params)))
	if err != nil {
		t.Fatalf("%s round trip: %v", method, err)
	}
	value, err := wire.DecodeJSON(raw)
	if err != nil {
		t.Fatalf("%s response %s: %v", method, raw, err)
	}
	return value.(map[string]any)
}

func gateRoundTrip(t *testing.T, server *rpc.Server, ctx context.Context, method string, params map[string]any) (map[string]any, []byte) {
	t.Helper()
	raw, err := inprocess.New(server).RoundTrip(ctx, request(method, paramsJSON(t, params)))
	if err != nil {
		t.Fatalf("%s round trip: %v", method, err)
	}
	value, err := wire.DecodeJSON(raw)
	if err != nil {
		t.Fatalf("%s response %s: %v", method, raw, err)
	}
	return value.(map[string]any), raw
}

func gateCommit(t *testing.T, response map[string]any) map[string]any {
	t.Helper()
	return result(t, response)["commit"].(map[string]any)
}

func gateState(t *testing.T, response map[string]any) (string, string) {
	t.Helper()
	value := result(t, response)
	return value["state"].(string), value["approvals"].(json.Number).String()
}

func gateChangesets(t *testing.T, store driver.Store) []model.Changeset {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	changes, err := reader.Changesets(ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return changes
}

func gateEventTypes(t *testing.T, changes model.Changeset) []model.DurableEventType {
	t.Helper()
	out := make([]model.DurableEventType, len(changes.Events))
	for i, event := range changes.Events {
		out[i] = event.Type
	}
	return out
}

func TestGateRPCMemoryAndSQLiteAtomicPersistenceReplayAndHistory(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			dsn := ""
			if engine == "sqlite" {
				dsn = filepath.Join(t.TempDir(), "gate.db")
			}
			o := gateOntology(t)
			runtime := newGateRuntime(t, engine, dsn, o, time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
			anonymousCtx := context.Background()
			aliceCtx := rpc.WithPrincipal(context.Background(), gateAgent("agent:alice-1", "user:alice"))
			aliceOtherAgentCtx := rpc.WithPrincipal(context.Background(), gateAgent("agent:alice-2", "user:alice"))
			bobCtx := rpc.WithPrincipal(context.Background(), gateAgent("agent:bob-1", "user:bob"))

			createParams := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "approval gate", "gate_kind": "human_approval"}})
			created := result(t, gateCall(t, runtime.server, anonymousCtx, "bead.create", createParams))
			ref := createdRef(t, created)
			createdRevision := created["revision"]

			firstParams := writeParams(t, map[string]any{"id": ref})
			firstResponse, firstRaw := gateRoundTrip(t, runtime.server, aliceCtx, "gate.approve", firstParams)
			if state, approvals := gateState(t, firstResponse); state != "pending" || approvals != "1" {
				t.Fatalf("first sponsor approval: state=%s approvals=%s", state, approvals)
			}
			firstResult := firstRawResult(t, firstRaw)

			if engine == "sqlite" {
				runtime.restart(t, engine)
			}
			replayResponse, replayRaw := gateRoundTrip(t, runtime.server, aliceCtx, "gate.approve", firstParams)
			if replayMeta, ok := replayResponse["_meta"].(map[string]any); !ok || replayMeta["replayed"] != true {
				t.Fatalf("keyed gate replay was not marked: %v", replayResponse)
			}
			if !bytes.Equal(firstResult, firstRawResult(t, replayRaw)) {
				t.Fatalf("keyed gate replay changed result bytes: first=%s replay=%s", firstResult, firstRawResult(t, replayRaw))
			}

			duplicateParams := writeParams(t, map[string]any{"id": ref})
			duplicate := gateCall(t, runtime.server, aliceOtherAgentCtx, "gate.approve", duplicateParams)
			if state, approvals := gateState(t, duplicate); state != "pending" || approvals != "1" {
				t.Fatalf("duplicate sponsor approval: state=%s approvals=%s", state, approvals)
			}

			staleParams := writeParams(t, map[string]any{
				"id":     ref,
				"expect": []any{map[string]any{"ref": ref, "revision": createdRevision}},
			})
			stale := gateCall(t, runtime.server, bobCtx, "gate.approve", staleParams)
			requireCode(t, stale, "precondition_failed")
			beforeStale := len(gateChangesets(t, runtime.store))

			secondParams := writeParams(t, map[string]any{"id": ref})
			second := gateCall(t, runtime.server, bobCtx, "gate.approve", secondParams)
			if state, approvals := gateState(t, second); state != "resolved" || approvals != "2" {
				t.Fatalf("distinct sponsor approvals: state=%s approvals=%s", state, approvals)
			}
			secondCommit := gateCommit(t, second)
			if secondCommit["revision"].(json.Number).String() == createdRevision.(json.Number).String() {
				t.Fatal("resolved approval did not advance the bead revision")
			}
			rows := secondCommit["results"].([]any)
			if len(rows) != 1 || rows[0].(map[string]any)["record"] == nil {
				t.Fatalf("gate result omitted full final bead image: %v", secondCommit)
			}
			record := rows[0].(map[string]any)["record"].(map[string]any)
			fields := record["fields"].(map[string]any)
			if fields["status"] != "done" || fields["gate_state"] != "resolved" || fields["gate_approvals"].([]any) == nil {
				t.Fatalf("resolved gate image missing durable state: %v", fields)
			}

			changes := gateChangesets(t, runtime.store)
			if len(changes) != beforeStale+1 || len(changes) != 3 {
				t.Fatalf("stale/duplicate/replay changed durable history: got %d before stale %d", len(changes), beforeStale)
			}
			if got := gateEventTypes(t, changes[1]); len(got) != 1 || got[0] != model.EventGateApproved {
				t.Fatalf("pending approval events=%v", got)
			}
			if got := gateEventTypes(t, changes[2]); len(got) != 2 || got[0] != model.EventBeadClosed || got[1] != model.EventGateApproved {
				t.Fatalf("resolved approval events=%v", got)
			}

			historyParams := map[string]any{"asOf": map[string]any{"revision": secondCommit["revision"]}, "filter": map[string]any{"resources": []string{ref["id"].(string)}}}
			history := result(t, gateCall(t, runtime.server, anonymousCtx, "history.query", historyParams))
			items := history["items"].([]any)
			if len(items) != 1 || items[0].(map[string]any)["status"] != "done" {
				t.Fatalf("history did not expose resolved gate: %v", history)
			}

			// A resolver failure uses the same transaction and event plan as a
			// manual close, but lowers to the failure slot and retains details.
			externalParams := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "external gate", "gate_kind": "external"}})
			externalCreated := result(t, gateCall(t, runtime.server, anonymousCtx, "bead.create", externalParams))
			externalRef := createdRef(t, externalCreated)
			reportParams := writeParams(t, map[string]any{"id": externalRef, "state": "failure", "details": map[string]any{"provider": "fixture", "attempt": 1}})
			reported := gateCall(t, runtime.server, anonymousCtx, "gate.report", reportParams)
			if state, approvals := gateState(t, reported); state != "failed" || approvals != "0" {
				t.Fatalf("resolver failure: state=%s approvals=%s", state, approvals)
			}
			reportedCommit := gateCommit(t, reported)
			reportedRecord := reportedCommit["results"].([]any)[0].(map[string]any)["record"].(map[string]any)
			reportedFields := reportedRecord["fields"].(map[string]any)
			if reportedFields["status"] != "failed" || reportedFields["gate_details"].(map[string]any)["provider"] != "fixture" {
				t.Fatalf("resolver failure was not atomic or details were lost: %v", reportedRecord)
			}
			changes = gateChangesets(t, runtime.store)
			last := changes[len(changes)-1]
			if got := gateEventTypes(t, last); len(got) != 2 || got[0] != model.EventBeadFailed || got[1] != model.EventGateReported {
				t.Fatalf("failure report events=%v", got)
			}
		})
	}
}

func firstRawResult(t *testing.T, raw []byte) []byte {
	t.Helper()
	var response map[string]json.RawMessage
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), response["result"]...)
}

func TestGateRPCRefusesAnonymousMultiApprovalBeforePersistence(t *testing.T) {
	runtime := newGateRuntime(t, "memory", "", gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
	created := result(t, gateCall(t, runtime.server, context.Background(), "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "anonymous gate", "gate_kind": "human_approval"}})))
	ref := createdRef(t, created)
	before, err := runtime.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	response := gateCall(t, runtime.server, context.Background(), "gate.approve", writeParams(t, map[string]any{"id": ref}))
	requireCode(t, response, "forbidden")
	manualReport := gateCall(t, runtime.server, context.Background(), "gate.report", writeParams(t, map[string]any{"id": ref, "state": "success"}))
	requireCode(t, manualReport, "forbidden")
	after, err := runtime.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if before.Head.String() != after.Head.String() {
		t.Fatalf("anonymous refusal advanced storage head: before=%s after=%s", before.Head, after.Head)
	}
	if got := len(gateChangesets(t, runtime.store)); got != 1 {
		t.Fatalf("anonymous refusal published a changeset: %d", got)
	}
}
