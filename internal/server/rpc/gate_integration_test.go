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
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type gateRuntime struct {
	store    driver.Store
	lease    driver.ActiveLease
	writer   *pipeline.Writer
	server   *rpc.Server
	dsn      string
	ontology *service.Ontology
	clock    time.Time
	options  []pipeline.Option
}

func gateOntology(t *testing.T) *service.Ontology {
	t.Helper()
	doc := ontology.DefaultDocument()
	doc.GateKinds = map[string]ontology.GateKind{
		"human_approval": {
			Resolver: "builtin.manual",
			Params:   map[string]any{"min_approvals": json.Number("2")},
		},
		"external":        {Resolver: "plugin.external"},
		"single_approval": {Resolver: "builtin.manual"},
	}
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("gate ontology did not compile: %+v", problems)
	}
	return service.NewOntology(func(project string) ontology.Effective { return snapshot.Effective(project) })
}

func newGateRuntime(t *testing.T, engine, dsn string, o *service.Ontology, clock time.Time, options ...pipeline.Option) *gateRuntime {
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
	writer := pipeline.New(store, lease, append([]pipeline.Option{pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return clock })}, options...)...)
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
	runtime := &gateRuntime{store: store, lease: lease, writer: writer, server: server, dsn: dsn, ontology: o, clock: clock, options: options}
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
	r.writer = nil
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
	writer := pipeline.New(opened, lease, append([]pipeline.Option{pipeline.WithOntology(r.ontology), pipeline.WithClock(func() time.Time { return r.clock })}, r.options...)...)
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
	r.store, r.lease, r.writer, r.server = opened, lease, writer, server
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

func gateRecords(t *testing.T, store driver.Store, ref model.Ref) []driver.Record {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	records, err := reader.Get(context.Background(), []model.Ref{ref})
	if err != nil {
		t.Fatal(err)
	}
	return records
}

type gateReplayAuthorizer struct {
	deny   bool
	stages []port.AuthorizationStage
}

func (a *gateReplayAuthorizer) Authorize(_ context.Context, stage port.AuthorizationStage, _ port.ProposedChange, _ port.SnapshotReader) error {
	a.stages = append(a.stages, stage)
	if a.deny {
		return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
	}
	return nil
}

func TestGateRPCReplaysReceiptWithoutCurrentBead(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		for _, method := range []string{"gate.approve", "gate.report"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				authorizer := &gateReplayAuthorizer{}
				trusted := port.EventContext{Cause: model.EventCause{Root: "gate-replay-root", Depth: model.Natural64(0), Chain: []string{}}, Chain: []string{}}
				runtime := newGateRuntime(t, engine, filepath.Join(t.TempDir(), "replay.db"), gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC),
					pipeline.WithAuthorizer(authorizer), pipeline.WithEventContext(gateTrustedContext{value: trusted}))
				principal := gateAgent("agent:alice", "user:alice")
				ctx := rpc.WithPrincipal(context.Background(), principal)
				kind := "single_approval"
				if method == "gate.report" {
					kind = "external"
				}
				created := result(t, gateCall(t, runtime.server, ctx, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "durable gate", "gate_kind": kind}})))
				ref := createdRef(t, created)
				params := map[string]any{"id": ref}
				if method == "gate.approve" {
					// Explicit caller expectations are part of request identity, but
					// must not be rechecked against a later or absent live image.
					params["expect"] = []any{map[string]any{"ref": ref, "revision": created["revision"]}}
				} else {
					params["state"] = "success"
					params["details"] = map[string]any{"provider": "fixture"}
				}
				params = writeParams(t, params)
				first, firstRaw := gateRoundTrip(t, runtime.server, ctx, method, params)
				gateCommit(t, first)
				target := model.Ref{ID: ref["id"].(string)}
				_, err := runtime.writer.ApplyWithOutcome(ctx, principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: target}}}}, model.WriteMeta{
					Method: "tx.apply", Project: "local", IdempotencyKey: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Return: "full",
				})
				if err != nil {
					t.Fatalf("delete resolved gate through shared Writer: %v", err)
				}
				if engine == "sqlite" {
					runtime.restart(t, engine)
				}
				if len(gateRecords(t, runtime.store, target)) != 0 {
					t.Fatal("gate image survived deletion/restart")
				}
				before, err := runtime.store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				history := len(gateChangesets(t, runtime.store))
				authorizer.stages = nil
				replay, replayRaw := gateRoundTrip(t, runtime.server, ctx, method, params)
				result(t, replay)
				if !bytes.Equal(firstRawResult(t, firstRaw), firstRawResult(t, replayRaw)) || replay["_meta"].(map[string]any)["replayed"] != true {
					t.Fatalf("absent-image replay lost its original result: %s", replayRaw)
				}
				if len(authorizer.stages) != 1 || authorizer.stages[0] != port.AuthorizeReplay {
					t.Fatalf("receipt was not reauthorized: %v", authorizer.stages)
				}
				authorizer.deny, authorizer.stages = true, nil
				requireCode(t, gateCall(t, runtime.server, ctx, method, params), "forbidden")
				if len(authorizer.stages) != 1 || authorizer.stages[0] != port.AuthorizeReplay {
					t.Fatalf("revocation bypassed replay authorization: %v", authorizer.stages)
				}
				authorizer.deny = false
				changed := map[string]any{}
				for key, value := range params {
					changed[key] = value
				}
				changed["expect"] = []any{map[string]any{"ref": ref, "revision": gateCommit(t, first)["revision"]}}
				requireCode(t, gateCall(t, runtime.server, ctx, method, changed), "idempotency_key_reused")
				fresh := map[string]any{"id": ref}
				if method == "gate.report" {
					fresh["state"] = "success"
				}
				requireCode(t, gateCall(t, runtime.server, ctx, method, writeParams(t, fresh)), "not_found")
				after, err := runtime.store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if before.Head.String() != after.Head.String() || len(gateChangesets(t, runtime.store)) != history || len(gateRecords(t, runtime.store, target)) != 0 {
					t.Fatal("replay/refusals changed storage head, history or deleted image")
				}
			})
		}
	}
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
			// A direct human and a different service/agent sponsor chain are
			// still the same human. They cannot create observations or events.
			aliceHuman := model.PrincipalRef{Kind: "user", ID: "user:alice"}
			aliceService := model.PrincipalRef{Kind: "service", ID: "service:alice", Sponsor: &aliceHuman}
			for _, identity := range []model.PrincipalRef{aliceHuman, {Kind: "agent", ID: "agent:alice-nested", Sponsor: &aliceService}} {
				response := gateCall(t, runtime.server, rpc.WithPrincipal(context.Background(), identity), "gate.approve", writeParams(t, map[string]any{"id": ref}))
				if state, approvals := gateState(t, response); state != "pending" || approvals != "1" {
					t.Fatalf("sponsor-resolved human was counted twice: state=%s approvals=%s", state, approvals)
				}
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
			reported, reportedRaw := gateRoundTrip(t, runtime.server, anonymousCtx, "gate.report", reportParams)
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
			if engine == "sqlite" {
				runtime.restart(t, engine)
			}
			reportReplay, reportReplayRaw := gateRoundTrip(t, runtime.server, anonymousCtx, "gate.report", reportParams)
			if !bytes.Equal(firstRawResult(t, reportedRaw), firstRawResult(t, reportReplayRaw)) || reportReplay["_meta"].(map[string]any)["replayed"] != true {
				t.Fatalf("terminal report replay changed result: %s", reportReplayRaw)
			}
			for _, changed := range []map[string]any{
				{"id": externalRef, "state": "success", "details": reportParams["details"], "_meta": reportParams["_meta"]},
				{"id": externalRef, "state": "failure", "details": map[string]any{"provider": "changed"}, "_meta": reportParams["_meta"]},
			} {
				requireCode(t, gateCall(t, runtime.server, anonymousCtx, "gate.report", changed), "idempotency_key_reused")
			}
			if len(gateChangesets(t, runtime.store)) != len(changes) {
				t.Fatal("report replay or key reuse changed durable history")
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

func TestGateRPCRecoveredObservationsUseEvaluatorCountAcrossRestart(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			runtime := newGateRuntime(t, engine, filepath.Join(t.TempDir(), "recovered.db"), gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
			ref := model.Ref{ID: "pl-recovered-gate"}
			body, err := json.Marshal(pipeline.Document{ID: ref.ID, Type: "issue", Project: "local", Fields: map[string]any{
				"title": "recovered gate", "status": "todo", "gate_kind": "human_approval", "gate_state": "pending",
				"gate_approvals": []ontology.GateApproval{
					{Principal: model.PrincipalRef{Kind: "anonymous"}},
					{Principal: gateAgent("agent:alice-1", "user:alice")},
					{Principal: gateAgent("agent:alice-2", "user:alice")},
				},
			}})
			if err != nil {
				t.Fatal(err)
			}
			// A restore/older projection can retain observations that do not all
			// count. Production gate writes still use the single Writer.
			if _, err := conformance.Seed(context.Background(), runtime.store, runtime.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body}}); err != nil {
				t.Fatal(err)
			}
			alice := rpc.WithPrincipal(context.Background(), gateAgent("agent:alice-3", "user:alice"))
			params := writeParams(t, map[string]any{"id": ref})
			first, raw := gateRoundTrip(t, runtime.server, alice, "gate.approve", params)
			if state, approvals := gateState(t, first); state != "pending" || approvals != "1" {
				t.Fatalf("recovered observation count: state=%s approvals=%s", state, approvals)
			}
			changes := gateChangesets(t, runtime.store)
			if events := gateEventTypes(t, changes[len(changes)-1]); len(events) != 1 || events[0] != model.EventBeadUpdated {
				t.Fatalf("duplicate approval emitted an approval event: %v", events)
			}
			if engine == "sqlite" {
				runtime.restart(t, engine)
			}
			replay, replayRaw := gateRoundTrip(t, runtime.server, alice, "gate.approve", params)
			if !bytes.Equal(firstRawResult(t, raw), firstRawResult(t, replayRaw)) || replay["_meta"].(map[string]any)["replayed"] != true {
				t.Fatalf("recovered gate replay changed result: %s", replayRaw)
			}
			duplicate := gateCall(t, runtime.server, alice, "gate.approve", writeParams(t, map[string]any{"id": ref}))
			if state, approvals := gateState(t, duplicate); state != "pending" || approvals != "1" || len(gateChangesets(t, runtime.store)) != len(changes) {
				t.Fatalf("duplicate recovered approval changed count/history: %v", duplicate)
			}
			bob := rpc.WithPrincipal(context.Background(), gateAgent("agent:bob", "user:bob"))
			resolved := gateCall(t, runtime.server, bob, "gate.approve", writeParams(t, map[string]any{"id": ref}))
			if state, approvals := gateState(t, resolved); state != "resolved" || approvals != "2" {
				t.Fatalf("second human did not resolve with the evaluator count: %v", resolved)
			}
			rows := gateCommit(t, resolved)["results"].([]any)
			fields := rows[0].(map[string]any)["record"].(map[string]any)["fields"].(map[string]any)
			if len(fields["gate_approvals"].([]any)) != 4 || fields["status"] != "done" {
				t.Fatalf("resolution discarded observations or missed the slot: %v", fields)
			}
		})
	}
}

func TestGateRPCReservesApprovalStateForTheWriter(t *testing.T) {
	runtime := newGateRuntime(t, "memory", "", gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
	ctx := context.Background()
	for _, field := range []string{"gate_approvals", "gate_state", "gate_approval_count", "gate_details"} {
		fields := map[string]any{"title": "forged gate", "gate_kind": "human_approval", field: "forged"}
		response := gateCall(t, runtime.server, ctx, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": fields}))
		requireCode(t, response, "ontology_use_slot_verb")
	}
	created := result(t, gateCall(t, runtime.server, ctx, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "real gate", "gate_kind": "human_approval"}})))
	ref := createdRef(t, created)
	for _, fields := range []map[string]any{{"gate_kind": nil}, {"gate_state": "resolved"}, {"gate_approvals": []any{}}, {"gate_approval_count": 2}} {
		response := gateCall(t, runtime.server, ctx, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": fields}))
		requireCode(t, response, "ontology_use_slot_verb")
	}
	if got := len(gateChangesets(t, runtime.store)); got != 1 {
		t.Fatalf("reserved field refusals wrote history: %d", got)
	}
}

func TestGateRPCResolutionSettlesTheClaimAtomically(t *testing.T) {
	runtime := newGateRuntime(t, "memory", "", gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
	ctx := rpc.WithPrincipal(context.Background(), gateAgent("agent:alice", "user:alice"))
	created := result(t, gateCall(t, runtime.server, ctx, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "claimed gate", "gate_kind": "single_approval"}})))
	ref := createdRef(t, created)
	result(t, gateCall(t, runtime.server, ctx, "bead.claim", writeParams(t, map[string]any{"id": ref})))
	approved := gateCall(t, runtime.server, ctx, "gate.approve", writeParams(t, map[string]any{"id": ref}))
	record := gateCommit(t, approved)["results"].([]any)[0].(map[string]any)["record"].(map[string]any)
	if record["lease"] != nil || record["fields"].(map[string]any)["status"] != "done" {
		t.Fatalf("gate resolution retained the live claim: %v", record)
	}
}

func TestGateRPCForeignResolutionSettlesTheClaimAtomically(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		for _, scenario := range []struct {
			name, method, kind, reportState, gateState, status string
			slotEvent                                          model.DurableEventType
		}{
			{"manual", "gate.approve", "single_approval", "", "resolved", "done", model.EventBeadClosed},
			{"external-success", "gate.report", "external", "success", "resolved", "done", model.EventBeadClosed},
			{"external-failure", "gate.report", "external", "failure", "failed", "failed", model.EventBeadFailed},
		} {
			t.Run(engine+"/"+scenario.name, func(t *testing.T) {
				runtime := newGateRuntime(t, engine, filepath.Join(t.TempDir(), "foreign.db"), gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
				alice := rpc.WithPrincipal(context.Background(), gateAgent("agent:alice", "user:alice"))
				actor := gateAgent("agent:bob", "user:bob")
				if scenario.method == "gate.report" {
					actor = model.PrincipalRef{Kind: "service", ID: "service:resolver"}
				}
				foreign := rpc.WithPrincipal(context.Background(), actor)
				created := result(t, gateCall(t, runtime.server, alice, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "claimed gate", "gate_kind": scenario.kind}})))
				ref := createdRef(t, created)
				result(t, gateCall(t, runtime.server, alice, "bead.claim", writeParams(t, map[string]any{"id": ref})))
				if engine == "sqlite" {
					runtime.restart(t, engine)
				}
				target := model.Ref{ID: ref["id"].(string)}
				claimed := gateRecords(t, runtime.store, target)[0]
				before := len(gateChangesets(t, runtime.store))
				requireCode(t, gateCall(t, runtime.server, foreign, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "foreign update"}})), "lease_not_held")
				params := map[string]any{"id": ref, "expect": []any{map[string]any{"ref": ref, "revision": created["revision"]}}}
				if scenario.method == "gate.report" {
					params["state"] = scenario.reportState
				}
				requireCode(t, gateCall(t, runtime.server, foreign, scenario.method, writeParams(t, params)), "precondition_failed")
				afterRefusal := gateRecords(t, runtime.store, target)[0]
				if before != len(gateChangesets(t, runtime.store)) || claimed.Revision.String() != afterRefusal.Revision.String() || !bytes.Equal(claimed.Body, afterRefusal.Body) {
					t.Fatal("foreign/stale refusal changed claim, image or history")
				}
				params["expect"] = []any{map[string]any{"ref": ref, "revision": claimed.Revision}}
				resolved := gateCall(t, runtime.server, foreign, scenario.method, writeParams(t, params))
				if state, _ := gateState(t, resolved); state != scenario.gateState {
					t.Fatalf("foreign resolution state=%s", state)
				}
				commit := gateCommit(t, resolved)
				record := commit["results"].([]any)[0].(map[string]any)["record"].(map[string]any)
				if record["lease"] != nil || record["fields"].(map[string]any)["status"] != scenario.status {
					t.Fatalf("foreign gate resolution was not atomic: %v", record)
				}
				changes := gateChangesets(t, runtime.store)
				last := changes[len(changes)-1]
				if len(changes) != before+1 || len(last.Changes) != 1 || last.Changes[0].Ref.ID != target.ID || last.Principal.ID != actor.ID || last.Revision.String() != commit["revision"].(json.Number).String() {
					t.Fatalf("resolution did not record one atomic actor/image: %+v", last)
				}
				gateEvent := model.EventGateApproved
				if scenario.method == "gate.report" {
					gateEvent = model.EventGateReported
				}
				if events := gateEventTypes(t, last); len(events) != 2 || events[0] != scenario.slotEvent || events[1] != gateEvent {
					t.Fatalf("resolution event plan=%v", events)
				}
				persisted, err := pipeline.DecodeDocument(gateRecords(t, runtime.store, target)[0].Body)
				if err != nil || persisted.Lease != nil || persisted.Fields["status"] != scenario.status || persisted.Fields["gate_state"] != scenario.gateState {
					t.Fatalf("settled claim/image was not persisted: %+v, %v", persisted, err)
				}
			})
		}
	}
}

func TestGateRPCPendingForeignApprovalPreservesClaim(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			runtime := newGateRuntime(t, engine, filepath.Join(t.TempDir(), "pending.db"), gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
			alice := rpc.WithPrincipal(context.Background(), gateAgent("agent:alice", "user:alice"))
			bob := rpc.WithPrincipal(context.Background(), gateAgent("agent:bob", "user:bob"))
			carol := rpc.WithPrincipal(context.Background(), gateAgent("agent:carol", "user:carol"))
			created := result(t, gateCall(t, runtime.server, alice, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "claimed multi-human gate", "gate_kind": "human_approval"}})))
			ref := createdRef(t, created)
			claimed := result(t, gateCall(t, runtime.server, alice, "bead.claim", writeParams(t, map[string]any{"id": ref})))
			requireCode(t, gateCall(t, runtime.server, bob, "gate.report", writeParams(t, map[string]any{"id": ref, "state": "success"})), "forbidden")
			pending := gateCall(t, runtime.server, bob, "gate.approve", writeParams(t, map[string]any{"id": ref}))
			if state, approvals := gateState(t, pending); state != "pending" || approvals != "1" {
				t.Fatalf("foreign first approval bypassed threshold: %v", pending)
			}
			pendingRecord := gateCommit(t, pending)["results"].([]any)[0].(map[string]any)["record"].(map[string]any)
			claimedRecord := claimed["results"].([]any)[0].(map[string]any)["record"].(map[string]any)
			pendingLease, _ := json.Marshal(pendingRecord["lease"])
			claimedLease, _ := json.Marshal(claimedRecord["lease"])
			if pendingRecord["lease"] == nil || !bytes.Equal(pendingLease, claimedLease) || pendingRecord["fields"].(map[string]any)["status"] != "in_progress" {
				t.Fatalf("pending foreign approval changed claim/workflow: %v", pendingRecord)
			}
			if engine == "sqlite" {
				runtime.restart(t, engine)
			}
			before := len(gateChangesets(t, runtime.store))
			duplicate := gateCall(t, runtime.server, bob, "gate.approve", writeParams(t, map[string]any{"id": ref}))
			if state, approvals := gateState(t, duplicate); state != "pending" || approvals != "1" || len(gateChangesets(t, runtime.store)) != before {
				t.Fatalf("duplicate foreign approval changed count/history: %v", duplicate)
			}
			resolved := gateCall(t, runtime.server, carol, "gate.approve", writeParams(t, map[string]any{"id": ref}))
			if state, approvals := gateState(t, resolved); state != "resolved" || approvals != "2" {
				t.Fatalf("second distinct human did not resolve: %v", resolved)
			}
			record := gateCommit(t, resolved)["results"].([]any)[0].(map[string]any)["record"].(map[string]any)
			if record["lease"] != nil || record["fields"].(map[string]any)["status"] != "done" || len(gateChangesets(t, runtime.store)) != before+1 {
				t.Fatalf("second foreign approval did not atomically settle claim: %v", record)
			}
		})
	}
}

type gateTrustedContext struct{ value port.EventContext }

func (s gateTrustedContext) EventContext(context.Context, port.ProposedChange, port.SnapshotReader) (port.EventContext, error) {
	return s.value, nil
}

type gateAllowAuthorizer struct{}

func (gateAllowAuthorizer) Authorize(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error {
	return nil
}

func TestGateRPCUsesTrustedContextWithoutConditionalAudit(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			trusted := port.EventContext{
				Cause: model.EventCause{Root: "trusted-gate-root", Depth: model.Natural64(1), Chain: []string{"trusted-trigger"}},
				Chain: []string{"authz/gate"},
			}
			runtime := newGateRuntime(t, engine, filepath.Join(t.TempDir(), "context.db"), gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC),
				pipeline.WithAuthorizer(gateAllowAuthorizer{}), pipeline.WithEventContext(gateTrustedContext{value: trusted}))
			created := result(t, gateCall(t, runtime.server, context.Background(), "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "trusted gate", "gate_kind": "single_approval"}})))
			result(t, gateCall(t, runtime.server, context.Background(), "gate.approve", writeParams(t, map[string]any{"id": createdRef(t, created)})))
			changes := gateChangesets(t, runtime.store)
			entry := changes[len(changes)-1]
			if len(entry.Events) != 2 || len(entry.Chain) != 1 || entry.Chain[0] != "authz/gate" {
				t.Fatalf("gate event plan lost authorization provenance: %+v", entry)
			}
			for _, event := range entry.Events {
				p := event.Provenance
				if p.Cause.Root != trusted.Cause.Root || p.Cause.Depth.String() != "1" || len(p.Cause.Chain) != 1 || p.Cause.Chain[0] != "trusted-trigger" || len(p.Chain) != 1 || p.Chain[0] != "authz/gate" {
					t.Fatalf("gate event lost trusted context: %+v", p)
				}
			}
		})
	}
}

func TestGateRPCRequiresTrustedContextWithAuthorizer(t *testing.T) {
	runtime := newGateRuntime(t, "memory", "", gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC), pipeline.WithAuthorizer(gateAllowAuthorizer{}))
	created := result(t, gateCall(t, runtime.server, context.Background(), "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "gate", "gate_kind": "single_approval"}})))
	requireCode(t, gateCall(t, runtime.server, context.Background(), "gate.approve", writeParams(t, map[string]any{"id": createdRef(t, created)})), "invalid_params")
	if len(gateChangesets(t, runtime.store)) != 1 {
		t.Fatal("missing trusted context persisted the approval")
	}
}

func TestGateRPCGuardRefusalDoesNotPersistPartialResolution(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		for _, method := range []string{"gate.approve", "gate.report"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				runtime := newGateRuntime(t, engine, filepath.Join(t.TempDir(), "guard.db"), gateOntology(t), time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
				kind := "single_approval"
				if method == "gate.report" {
					kind = "external"
				}
				created := result(t, gateCall(t, runtime.server, context.Background(), "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "guarded gate", "gate_kind": kind, "template": true}})))
				ref := createdRef(t, created)
				params := map[string]any{"id": ref}
				if method == "gate.report" {
					params["state"] = "success"
				}
				params = writeParams(t, params)
				requireCode(t, gateCall(t, runtime.server, context.Background(), method, params), "ontology_guard_failed")
				if len(gateChangesets(t, runtime.store)) != 1 {
					t.Fatal("guard refusal persisted part of the gate resolution")
				}
				result(t, gateCall(t, runtime.server, context.Background(), "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"template": false}})))
				resolved := gateCall(t, runtime.server, context.Background(), method, params)
				state, _ := gateState(t, resolved)
				fields := gateCommit(t, resolved)["results"].([]any)[0].(map[string]any)["record"].(map[string]any)["fields"].(map[string]any)
				if state != "resolved" || fields["status"] != "done" || len(gateChangesets(t, runtime.store)) != 3 {
					t.Fatalf("retry did not publish one complete resolution: %v", resolved)
				}
			})
		}
	}
}
