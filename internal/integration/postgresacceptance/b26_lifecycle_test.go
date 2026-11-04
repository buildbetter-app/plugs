package postgresacceptance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

type b26HTTP struct {
	store  driver.Store
	lease  driver.ActiveLease
	server *httptest.Server
	token  string
	serial atomic.Int64
}

func b26Host(t *testing.T, store driver.Store, writerOptions ...pipeline.Option) *b26HTTP {
	t.Helper()
	ctx := context.Background()
	lease, err := store.Lease(ctx, "b26-public", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx); _ = store.Close() })
	metadata, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	options := []service.Option{}
	if persistent, ok := store.(driver.PersistentCheckpointStore); ok && persistent.Persistent() {
		provider, err := workflowplugin.New(pluginhost.NewDurableState(persistent, lease, "workflow", "default"))
		if err != nil {
			t.Fatal(err)
		}
		options = append(options, service.WithWorkflow(provider.WithIdentity("workflow", "default", metadata.Incarnation)))
	}
	ontology := service.NewOntology(nil)
	options = append(options, service.WithOntology(ontology))
	writer := pipeline.New(store, lease, append([]pipeline.Option{pipeline.WithOntology(ontology)}, writerOptions...)...)
	domain := service.New(store, lease, writer, options...)
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: metadata.Incarnation, DefaultProject: project, AuthMode: "authenticated", CursorKey: bytes.Repeat([]byte{42}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	keys := authnapikey.New(authnapikey.Config{})
	issued, err := keys.Issue(model.PrincipalRef{Kind: "user", ID: "user:b26-public"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{Provider: keys})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, authenticator, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)
	return &b26HTTP{store: store, lease: lease, server: httpServer, token: issued.Token}
}
func (h *b26HTTP) call(t *testing.T, method string, params map[string]any, label string) reply {
	t.Helper()
	serial := h.serial.Add(1)
	values := map[string]any{}
	for name, value := range params {
		values[name] = value
	}
	meta := map[string]any{"project": project, "agent": "b26-public"}
	if label != "" {
		meta["idempotencyKey"] = key(label)
	}
	values["_meta"] = meta
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": serial, "method": method, "params": values})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, h.server.URL+httpjson.Path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Authorization", "Bearer "+h.token)
	response, err := h.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result reply
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}
func b26OK(t *testing.T, r reply) json.RawMessage {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("public call refused: %+v; data=%+v", r.Error, r.Error.Data)
	}
	return r.Result
}
func (h *b26HTTP) get(t *testing.T, id string) map[string]any {
	t.Helper()
	return decode[map[string]any](t, b26OK(t, h.call(t, "bead.get", map[string]any{"id": ref(id)}, "")))
}
func (h *b26HTTP) ready(t *testing.T, include bool) map[string]bool {
	t.Helper()
	page := decode[struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}](t, b26OK(t, h.call(t, "bead.ready", map[string]any{"limit": 100, "filter": map[string]any{"includeEphemeral": include}}, "")))
	ids := map[string]bool{}
	for _, item := range page.Items {
		ids[item.ID] = true
	}
	return ids
}
func (h *b26HTTP) create(t *testing.T, title string) string {
	t.Helper()
	commit := decode[model.Commit](t, b26OK(t, h.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}}, title)))
	if len(commit.Results) != 1 {
		t.Fatal("create did not return one item")
	}
	return commit.Results[0].Ref.ID
}
func b26NoVersion(t *testing.T, store driver.Store, id string) {
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
	records, err := reader.Get(ctx, []model.Ref{ref(id)})
	if err != nil || len(records) != 0 {
		t.Fatalf("ephemeral core record: %+v %v", records, err)
	}
	versions, err := reader.Versions(ctx, ref(id), driver.PageReq{Limit: 100})
	if err != nil || len(versions.Items) != 0 {
		t.Fatalf("ephemeral durable versions: %+v %v", versions, err)
	}
}

func TestB26PublicLifecycleMemoryCapabilityRefusal(t *testing.T) {
	store := memory.New()
	h := b26Host(t, store)
	before, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	denied := h.call(t, "x.workflow.invoke", map[string]any{"template": "b26", "project": project, "mode": "ephemeral", "checklist": []map[string]any{{"label": "one", "title": "one", "type": "issue"}}}, "memory-ephemeral")
	if denied.Error == nil || denied.Error.Data.Code != "plugin_durable_state_unavailable" {
		t.Fatalf("memory refusal=%+v", denied.Error)
	}
	after, err := store.Meta(context.Background())
	if err != nil || after != before {
		t.Fatalf("memory refusal mutated source: %+v %v", after, err)
	}
	checkpoint, err := store.LoadCheckpoint(context.Background(), "plugin/workflow/default/durable")
	if err != nil || checkpoint.Generation != 0 {
		t.Fatalf("memory invented a checkpoint: %+v %v", checkpoint, err)
	}
	t.Log("memory proof is typed refusal, never positive ephemeral execution")
}
func TestB26PublicLifecycleSQLiteAtomicMoves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b26-public.db")
	store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b26Lifecycle(t, b26Host(t, store), db, "sqlite")
}
func TestB26PublicLifecyclePostgreSQLAtomicMoves(t *testing.T) {
	dsn := freshDatabase(t)
	store := openStore(t, dsn)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b26Lifecycle(t, b26Host(t, store), db, "postgres")
}
func b26Lifecycle(t *testing.T, h *b26HTTP, db *sql.DB, engine string) {
	t.Run("CheckpointNoopReplayAndCASRollback", func(t *testing.T) { b26CheckpointNoop(t, h) })
	t.Run("MixedBlockerDirectWritesAndReplay", func(t *testing.T) { b26DirectGuards(t, h) })
	t.Run("OntologyDemotionFreshAndCompleted", func(t *testing.T) { b26DemotionStatuses(t, h) })
	t.Run("WaitsForDurableChildGates", func(t *testing.T) { b26WaitsForDurable(t, h) })
	t.Run("WaitsForWorkflowChildGates", func(t *testing.T) { b26WaitsForWorkflow(t, h) })
	t.Run("PromotionDoesNotPersistDerivedBlockers", func(t *testing.T) { b26PromotionBlockers(t, h) })
	t.Run("ExplicitBlockerFlagMoves", func(t *testing.T) { b26ExplicitBlocker(t, h) })
	ctx := context.Background()
	target := h.create(t, "b26 target")
	blocker := h.create(t, "b26 durable blocker")
	dependent := h.create(t, "b26 durable dependent")
	invoked := decode[workflowplugin.InvokeResult](t, b26OK(t, h.call(t, "x.workflow.invoke", map[string]any{"template": "b26", "project": project, "mode": "ephemeral", "checklist": []map[string]any{{"label": "moving", "title": "ephemeral body", "type": "issue", "fields": map[string]any{"description": "preserve me"}, "labels": []string{"alpha"}, "metadata": map[string]any{"user.note": "value"}}, {"label": "keeper", "title": "keeper", "type": "issue"}}}, "b26-invoke")))
	item, keeper := invoked.Items[0].ID, invoked.Items[1].ID
	b26NoVersion(t, h.store, item)
	b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(item), "fields": map[string]any{"labels": []string{"alpha", "", "alpha"}}}, "ephemeral-labels"))
	labels := h.get(t, item)["bead"].(map[string]any)["labels"]
	if !reflect.DeepEqual(labels, []any{"alpha"}) {
		t.Fatalf("public ephemeral label replacement was not normalized: %v", labels)
	}
	if h.ready(t, false)[item] || !h.ready(t, true)[item] {
		t.Fatal("explicit ephemeral inclusion did not control ready membership")
	}
	empty := b26OK(t, h.call(t, "bead.claimNext", map[string]any{"filter": map[string]any{"includeEphemeral": false, "resources": []string{item}}}, "excluded-terminal"))
	if string(empty) != "null" {
		t.Fatalf("terminal fallback claimed an excluded workflow item: %s", empty)
	}
	if _, ok := h.get(t, item)["lease"]; ok {
		t.Fatal("excluded claimNext created an operational claim")
	}
	b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(item), "to": ref(blocker), "kind": "blocks"}, "ephemeral-durable-block"))
	if h.ready(t, true)[item] {
		t.Fatal("open durable blocker did not block workflow item")
	}
	b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(blocker), "reason": "done"}, "close-durable-blocker"))
	if !h.ready(t, true)[item] || h.get(t, item)["bead"].(map[string]any)["blocked"] == true {
		t.Fatal("closed durable blocker did not release workflow item")
	}
	b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(dependent), "to": ref(item), "kind": "blocks"}, "durable-ephemeral-block"))
	if h.ready(t, true)[dependent] {
		t.Fatal("active workflow blocker did not block durable dependent")
	}
	b26OK(t, h.call(t, "bead.comment", map[string]any{"id": ref(item), "body": "preserve original comment"}, "ephemeral-comment"))
	beforeItem := h.get(t, item)
	checkpoint := workflowCheckpoint(t, h.store.(driver.CheckpointStore)).Checkpoint
	head, err := h.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	missing := h.call(t, "x.workflow.run.promote", map[string]any{"item": ref(item), "target": ref("missing-durable")}, "missing-target")
	if missing.Error == nil || missing.Error.Data.Code != "not_found" {
		t.Fatalf("missing target=%+v", missing.Error)
	}
	if after := workflowCheckpoint(t, h.store.(driver.CheckpointStore)).Checkpoint; !reflect.DeepEqual(after, checkpoint) {
		t.Fatal("invalid target partially moved provider state")
	}
	// Fail an actual version-family SQL insertion after the checkpoint has been
	// staged for publication, exercising rollback inside the real driver commit.
	var install, remove string
	if engine == "sqlite" {
		install = `CREATE TRIGGER b26_aux_failure BEFORE INSERT ON versions BEGIN SELECT RAISE(ABORT,'b26 injected auxiliary failure'); END`
		remove = `DROP TRIGGER b26_aux_failure`
	} else {
		install = `CREATE FUNCTION plug_postgres.b26_aux_failure() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'b26 injected auxiliary failure'; END$$; CREATE TRIGGER b26_aux_failure BEFORE INSERT ON plug_postgres.versions FOR EACH ROW EXECUTE FUNCTION plug_postgres.b26_aux_failure()`
		remove = `DROP TRIGGER b26_aux_failure ON plug_postgres.versions; DROP FUNCTION plug_postgres.b26_aux_failure()`
	}
	if _, err := db.ExecContext(ctx, install); err != nil {
		t.Fatal(err)
	}
	failed := h.call(t, "x.workflow.run.promote", map[string]any{"item": ref(item), "target": ref(target)}, "promote-after-fault")
	if failed.Error == nil {
		t.Fatal("SQL auxiliary fault succeeded")
	}
	if _, err := db.ExecContext(ctx, remove); err != nil {
		t.Fatal(err)
	}
	if after := workflowCheckpoint(t, h.store.(driver.CheckpointStore)).Checkpoint; !reflect.DeepEqual(after, checkpoint) {
		t.Fatal("SQL fault published workflow checkpoint")
	}
	afterHead, err := h.store.Meta(ctx)
	if err != nil || afterHead.Head != head.Head {
		t.Fatal("SQL fault published ordinary changes")
	}
	if !reflect.DeepEqual(beforeItem, h.get(t, item)) {
		t.Fatal("failed move changed live source")
	}
	promoted := decode[workflowplugin.PromoteResult](t, b26OK(t, h.call(t, "x.workflow.run.promote", map[string]any{"item": ref(item), "target": ref(target)}, "promote-after-fault")))
	if promoted.Source.ID != item || promoted.Target.ID != target {
		t.Fatalf("promotion identity=%+v", promoted)
	}
	durable := h.get(t, target)
	bead := durable["bead"].(map[string]any)
	fields := bead["fields"].(map[string]any)
	if fields["title"] != "ephemeral body" || fields["description"] != "preserve me" || len(bead["labels"].([]any)) != 1 || !reflect.DeepEqual(durable["comments"], beforeItem["comments"]) {
		t.Fatalf("promotion lost public auxiliary fields: %v", durable)
	}
	if !h.ready(t, true)[target] || bead["blocked"] == true {
		t.Fatal("promotion persisted a satisfied dependency as an explicit blocker")
	}
	b26NoVersion(t, h.store, item)
	demoted := decode[workflowplugin.DemoteResult](t, b26OK(t, h.call(t, "x.workflow.run.demote", map[string]any{"source": ref(target), "template": "demoted", "label": "moved"}, "demote")))
	retry := decode[workflowplugin.DemoteResult](t, b26OK(t, h.call(t, "x.workflow.run.demote", map[string]any{"source": ref(target), "template": "demoted", "label": "moved"}, "demote")))
	if !reflect.DeepEqual(retry, demoted) {
		t.Fatal("demotion retry changed identity/body")
	}
	gone := h.call(t, "bead.get", map[string]any{"id": ref(target)}, "")
	if gone.Error == nil || gone.Error.Data.Code != "not_found" {
		t.Fatalf("demotion did not remove ordinary source: %+v", gone.Error)
	}
	moved := h.get(t, demoted.Item.ID)
	if !reflect.DeepEqual(moved["comments"], beforeItem["comments"]) || demoted.Item.Metadata["user.note"] != "value" || len(demoted.Item.Links) < 2 {
		t.Fatalf("demotion lost comments/metadata/inbound/outbound relations: %+v %v", demoted, moved)
	}
	b26NoVersion(t, h.store, demoted.Item.ID)
	b26OK(t, h.call(t, "bead.claim", map[string]any{"id": ref(keeper)}, "claim-keeper"))
	if _, ok := h.get(t, keeper)["lease"]; !ok {
		t.Fatal("standard get lost operational claim visibility")
	}
	b26NoVersion(t, h.store, keeper)
	for i, id := range []string{keeper, demoted.Item.ID} {
		b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(id), "reason": "done"}, fmt.Sprintf("finish-%d", i)))
	}
	completed := workflowCheckpoint(t, h.store.(driver.CheckpointStore))
	for _, payload := range []string{"ephemeral body", "preserve me", "preserve original comment", "user.note"} {
		if bytes.Contains(completed.Checkpoint.Payload, []byte(payload)) {
			t.Fatalf("completed lifecycle retained payload %q in the raw checkpoint", payload)
		}
	}
	terminal := decode[workflowplugin.DemoteResult](t, b26OK(t, h.call(t, "x.workflow.run.demote", map[string]any{"source": ref(target), "template": "demoted", "label": "moved"}, "demote")))
	if terminal.Run.State != "completed" || terminal.Item.ID != demoted.Item.ID || terminal.Item.Title != "" || terminal.Item.Fields != nil || terminal.Item.Metadata != nil || terminal.Item.Comments != nil || terminal.Item.Claim != nil {
		t.Fatalf("completed demotion replay retained a payload or recreated the run: %+v", terminal)
	}
	if after := workflowCheckpoint(t, h.store.(driver.CheckpointStore)); !reflect.DeepEqual(after, completed) {
		t.Fatal("terminal demotion replay published a new checkpoint")
	}
	if !h.ready(t, true)[dependent] {
		t.Fatal("completion did not delete mixed-plane blocker")
	}
	t.Log("real authenticated HTTP, explicit inclusion, both blocker directions, comments/labels, no core shadow versions/leases, atomic SQL auxiliary rollback, promotion/demotion and replay")
}

// This is a real native host registration and callback boundary. It is not
// claimed as a protected CLI producer capture or native admission.
func TestB26NativeLifecycleRegistrationAndTrustedMetadataSQLite(t *testing.T) {
	ctx := context.Background()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "b26-native.db")})
	if err != nil {
		t.Fatal(err)
	}
	h := b26Host(t, store)
	metadata, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "user", ID: "user:b26-public"}
	type trustedKey struct{}
	type trusted struct{ meta model.WriteMeta }
	metaFrom := func(ctx context.Context) model.WriteMeta { v, _ := ctx.Value(trustedKey{}).(trusted); return v.meta }
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, h.lease, pipeline.WithOntology(ontology))
	domain := func(provider *workflowplugin.Provider) *service.Service {
		return service.New(store, h.lease, writer, service.WithOntology(ontology), service.WithWorkflow(provider.WithIdentity("workflow", "default", metadata.Incarnation)))
	}
	native := workflowplugin.NativeWithLifecycle(workflowplugin.NativeLifecycleAdapter{
		Principal: func(context.Context) model.PrincipalRef { return by }, WriteMeta: metaFrom,
		Invoke: func(ctx context.Context, p *workflowplugin.Provider, r workflowplugin.InvokeRequest) (workflowplugin.InvokeResult, error) {
			return domain(p).WorkflowInvoke(ctx, by, r, metaFrom(ctx))
		},
		Promote: func(ctx context.Context, p *workflowplugin.Provider, who model.PrincipalRef, r workflowplugin.PromoteRequest, m model.WriteMeta) (workflowplugin.PromoteResult, error) {
			if who.ID != by.ID || m.Project != project || m.IdempotencyKey == "" {
				t.Fatal("native promotion lost trusted scope")
			}
			return domain(p).WorkflowPromote(ctx, who, r, m)
		},
		Demote: func(ctx context.Context, p *workflowplugin.Provider, who model.PrincipalRef, r workflowplugin.DemoteRequest, m model.WriteMeta) (workflowplugin.DemoteResult, error) {
			if who.ID != by.ID || m.Project != project || m.IdempotencyKey == "" {
				t.Fatal("native demotion lost trusted scope")
			}
			return domain(p).WorkflowDemote(ctx, who, r, m)
		},
	})
	host, err := pluginhost.New(pluginhost.Config{Natives: []sdk.Native{native}, CheckpointStore: store.(driver.CheckpointStore), CheckpointLease: h.lease})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	invoke := func(method, label string, params any) json.RawMessage {
		raw, err := json.Marshal(params)
		if err != nil {
			t.Fatal(err)
		}
		callCtx := context.WithValue(ctx, trustedKey{}, trusted{model.WriteMeta{Project: project, IdempotencyKey: key(label), Method: method}})
		result, err := host.Invoke(callCtx, method, raw)
		if err != nil {
			t.Fatalf("native %s: %v", method, err)
		}
		return result
	}
	run := decode[workflowplugin.InvokeResult](t, invoke("x.workflow.invoke", "native-invoke", workflowplugin.InvokeRequest{Template: "native", Project: project, Mode: "ephemeral", Checklist: []workflowplugin.ChecklistItem{{Label: "item", Title: "native body", Type: "issue"}}}))
	target := h.create(t, "native-target")
	invoke("x.workflow.run.promote", "native-promote", workflowplugin.PromoteRequest{Item: ref(run.Items[0].ID), Target: ref(target)})
	if checkpoint := workflowCheckpoint(t, store.(driver.CheckpointStore)); bytes.Contains(checkpoint.Checkpoint.Payload, []byte("native body")) {
		t.Fatal("final native promotion retained the removed source payload in its checkpoint")
	}
	terminal := decode[workflowplugin.InvokeResult](t, invoke("x.workflow.invoke", "native-invoke", workflowplugin.InvokeRequest{Template: "native", Project: project, Mode: "ephemeral", Checklist: []workflowplugin.ChecklistItem{{Label: "item", Title: "native body", Type: "issue"}}}))
	if terminal.Run == nil || terminal.Run.State != "completed" || len(terminal.Items) != 0 {
		t.Fatalf("final promotion replay resurrected an ephemeral item: %+v", terminal)
	}
	moved := decode[workflowplugin.DemoteResult](t, invoke("x.workflow.run.demote", "native-demote", workflowplugin.DemoteRequest{Source: ref(target), Template: "native", Label: "back"}))
	if moved.Item.Title != "native body" {
		t.Fatalf("native lifecycle body=%+v", moved)
	}
	b26NoVersion(t, store, moved.Item.ID)
	t.Log("native host registered all three declared resources and invoked trusted metadata callbacks; no CLI capture admission")
}

// Exercise the receipt-only branch directly on each real SQL driver. This
// catches an identical-target move that otherwise loses its staged checkpoint.
func b26CheckpointNoop(t *testing.T, h *b26HTTP) {
	t.Helper()
	ctx := context.Background()
	before, err := h.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const checkpointKey = "b26/noop-participant"
	publish := func(idem string, expected uint64, payload string) (model.Commit, error) {
		tx, err := h.store.Write(ctx, h.lease)
		if err != nil {
			return model.Commit{}, err
		}
		defer tx.Close()
		if _, err = tx.Lock(ctx, nil); err != nil {
			return model.Commit{}, err
		}
		if _, err = tx.Head(ctx); err != nil {
			return model.Commit{}, err
		}
		if err = tx.(driver.CheckpointWriteTx).StageCheckpoint(ctx, checkpointKey, expected, []byte(payload)); err != nil {
			return model.Commit{}, err
		}
		return tx.(driver.NoopCommitter).CommitNoop(ctx, driver.ChangesetHeader{Incarnation: before.Incarnation, Kind: model.ChangesetWrite, Time: time.Now().UTC(), Principal: model.PrincipalRef{Kind: "user", ID: "user:b26-public"}, Method: "x.workflow.run.promote", IdempotencyKey: key(idem), RequestHash: "b26/noop-digest", Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}}})
	}
	first, err := publish("noop-initial", 0, "first")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := publish("noop-initial", 0, "must-not-publish")
	if err != nil || !reflect.DeepEqual(first, replay) {
		t.Fatalf("stale CAS replay: %+v %v", replay, err)
	}
	if _, err := publish("noop-fresh-conflict", 0, "must-roll-back"); !errors.Is(err, driver.ErrCheckpointConflict) {
		t.Fatalf("fresh stale CAS=%v", err)
	}
	checkpoint, err := h.store.(driver.CheckpointStore).LoadCheckpoint(ctx, checkpointKey)
	if err != nil || checkpoint.Generation != 1 || string(checkpoint.Payload) != "first" {
		t.Fatalf("noop namespace=%+v %v", checkpoint, err)
	}
	after, err := h.store.Meta(ctx)
	if err != nil || before.Head != after.Head {
		t.Fatalf("receipt-only publication consumed Head: %+v %v", after, err)
	}
}

func b26Invoke(t *testing.T, h *b26HTTP, label string, count int) []workflowplugin.ItemView {
	t.Helper()
	checklist := make([]map[string]any, count)
	for i := range checklist {
		checklist[i] = map[string]any{"label": fmt.Sprintf("item-%d", i), "title": label, "type": "issue"}
	}
	return decode[workflowplugin.InvokeResult](t, b26OK(t, h.call(t, "x.workflow.invoke", map[string]any{"template": label, "project": project, "mode": "ephemeral", "checklist": checklist}, label))).Items
}

func b26AssertRefusalUnchanged(t *testing.T, h *b26HTTP, method string, params map[string]any, label, code string) {
	t.Helper()
	before := workflowCheckpoint(t, h.store.(driver.CheckpointStore))
	meta, err := h.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := h.call(t, method, params, label)
	if result.Error == nil || string(result.Error.Data.Code) != code {
		t.Fatalf("%s expected %s, got %+v", method, code, result.Error)
	}
	after, err := h.store.Meta(context.Background())
	if err != nil || after != meta || !reflect.DeepEqual(before, workflowCheckpoint(t, h.store.(driver.CheckpointStore))) {
		t.Fatalf("%s refusal published a graph/checkpoint change: %v", method, err)
	}
}

func b26DirectGuards(t *testing.T, h *b26HTTP) {
	claimTarget, closeTarget, batchTarget := h.create(t, "guard-claim"), h.create(t, "guard-close"), h.create(t, "guard-batch")
	blocker := b26Invoke(t, h, "guard-blocker", 1)[0].ID
	for i, id := range []string{claimTarget, closeTarget, batchTarget} {
		b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(id), "to": ref(blocker), "kind": "blocks"}, fmt.Sprintf("guard-link-%d", i)))
		if h.ready(t, true)[id] || h.get(t, id)["bead"].(map[string]any)["blocked"] != true {
			t.Fatal("mixed blocker disagreed between ordinary get and ready")
		}
	}
	batch := map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpClose, Close: &model.CloseOp{Target: ref(batchTarget)}}}}}
	b26AssertRefusalUnchanged(t, h, "bead.claim", map[string]any{"id": ref(claimTarget)}, "guard-claim-write", "ontology_guard_failed")
	b26AssertRefusalUnchanged(t, h, "bead.close", map[string]any{"id": ref(closeTarget), "reason": "guarded close"}, "guard-close-write", "ontology_guard_failed")
	b26AssertRefusalUnchanged(t, h, "tx.apply", batch, "guard-batch-write", "ontology_guard_failed")
	b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(blocker)}, "guard-complete"))
	claim := b26OK(t, h.call(t, "bead.claim", map[string]any{"id": ref(claimTarget)}, "guard-claim-write"))
	closeResult := b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(closeTarget), "reason": "guarded close"}, "guard-close-write"))
	batchResult := b26OK(t, h.call(t, "tx.apply", batch, "guard-batch-write"))
	newBlocker := b26Invoke(t, h, "guard-new-blocker", 1)[0].ID
	for i, id := range []string{claimTarget, closeTarget, batchTarget} {
		b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(id), "to": ref(newBlocker), "kind": "blocks"}, fmt.Sprintf("guard-new-link-%d", i)))
	}
	before := workflowCheckpoint(t, h.store.(driver.CheckpointStore))
	meta, err := h.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []struct {
		method, label string
		params        map[string]any
		result        json.RawMessage
	}{
		{"bead.claim", "guard-claim-write", map[string]any{"id": ref(claimTarget)}, claim},
		{"bead.close", "guard-close-write", map[string]any{"id": ref(closeTarget), "reason": "guarded close"}, closeResult},
		{"tx.apply", "guard-batch-write", batch, batchResult},
	} {
		var want, got map[string]any
		if err := json.Unmarshal(call.result, &want); err != nil {
			t.Fatal(err)
		}
		raw := b26OK(t, h.call(t, call.method, call.params, call.label))
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		delete(want, "_meta")
		delete(got, "_meta")
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s replay consulted a later blocker or changed receipt: %s", call.method, raw)
		}
	}
	after, err := h.store.Meta(context.Background())
	if err != nil || after != meta || !reflect.DeepEqual(before, workflowCheckpoint(t, h.store.(driver.CheckpointStore))) {
		t.Fatal("ordinary replay republished graph or checkpoint")
	}
	b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(newBlocker)}, "guard-new-complete"))
}

func b26DemotionStatuses(t *testing.T, h *b26HTTP) {
	fresh := h.create(t, "status-fresh-source")
	if h.get(t, fresh)["bead"].(map[string]any)["status"] != "todo" {
		t.Fatal("fixture lacks canonical initial status")
	}
	moved := decode[workflowplugin.DemoteResult](t, b26OK(t, h.call(t, "x.workflow.run.demote", map[string]any{"source": ref(fresh), "template": "status", "label": "fresh"}, "status-demote-fresh")))
	if moved.Item.Status != "open" || !h.ready(t, true)[moved.Item.ID] {
		t.Fatalf("fresh canonical task became unclaimable: %+v", moved)
	}
	b26OK(t, h.call(t, "bead.claim", map[string]any{"id": ref(moved.Item.ID)}, "status-claim-fresh"))
	b26NoVersion(t, h.store, moved.Item.ID)
	b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(moved.Item.ID)}, "status-complete-fresh"))
	completed := h.create(t, "status-completed-payload-marker")
	b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(completed)}, "status-close-source"))
	result := decode[workflowplugin.DemoteResult](t, b26OK(t, h.call(t, "x.workflow.run.demote", map[string]any{"source": ref(completed), "template": "status", "label": "completed"}, "status-demote-completed")))
	if result.Run.State != "completed" || result.Item.Status != "closed" || result.Item.Title != "" || h.ready(t, true)[result.Item.ID] {
		t.Fatalf("completed source became a live payload: %+v", result)
	}
	checkpoint := workflowCheckpoint(t, h.store.(driver.CheckpointStore))
	if bytes.Contains(checkpoint.Checkpoint.Payload, []byte("status-completed-payload-marker")) {
		t.Fatal("completed demotion retained payload")
	}
	replay := decode[workflowplugin.DemoteResult](t, b26OK(t, h.call(t, "x.workflow.run.demote", map[string]any{"source": ref(completed), "template": "status", "label": "completed"}, "status-demote-completed")))
	if !reflect.DeepEqual(result, replay) || !reflect.DeepEqual(checkpoint, workflowCheckpoint(t, h.store.(driver.CheckpointStore))) {
		t.Fatal("completed demotion replay recreated or changed state")
	}
	for _, id := range []string{completed, result.Item.ID} {
		gone := h.call(t, "bead.get", map[string]any{"id": ref(id)}, "")
		if gone.Error == nil || gone.Error.Data.Code != "not_found" {
			t.Fatalf("completed demotion left %s visible: %+v", id, gone.Error)
		}
	}
	b26NoVersion(t, h.store, result.Item.ID)
}

func b26WaitsForDurable(t *testing.T, h *b26HTTP) {
	for _, gate := range []string{"nil", "default", "all-children", "any-children"} {
		t.Run(gate, func(t *testing.T) {
			prefix := "wait-durable-" + gate
			spawner := h.create(t, prefix+"-spawner")
			children := []string{}
			for i := 0; i < 2; i++ {
				c := decode[model.Commit](t, b26OK(t, h.call(t, "bead.create", map[string]any{"type": "issue", "parent": ref(spawner), "fields": map[string]any{"title": prefix}}, fmt.Sprintf("%s-child-%d", prefix, i))))
				children = append(children, c.Results[0].Ref.ID)
			}
			dependent := b26Invoke(t, h, prefix+"-dependent", 1)[0].ID
			params := map[string]any{"from": ref(dependent), "to": ref(spawner), "kind": "waits-for"}
			if gate != "nil" {
				m := map[string]any{}
				if gate != "default" {
					m["gate"] = gate
				}
				params["metadata"] = m
			}
			b26OK(t, h.call(t, "bead.link", params, prefix+"-link"))
			if h.ready(t, true)[dependent] {
				t.Fatal("open durable child did not block mixed waits-for")
			}
			b26AssertRefusalUnchanged(t, h, "bead.claim", map[string]any{"id": ref(dependent)}, prefix+"-claim-refusal", "busy")
			b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(children[0])}, prefix+"-close-first"))
			if h.ready(t, true)[dependent] != (gate == "any-children") {
				t.Fatal("mixed gate did not distinguish all/any child completion")
			}
			b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(children[1])}, prefix+"-close-second"))
			if !h.ready(t, true)[dependent] {
				t.Fatal("all terminal durable children did not satisfy mixed gate")
			}
			b26OK(t, h.call(t, "bead.claim", map[string]any{"id": ref(dependent)}, prefix+"-claim"))
			b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(dependent)}, prefix+"-complete"))
		})
	}
}

func b26WaitsForWorkflow(t *testing.T, h *b26HTTP) {
	for _, gate := range []string{"nil", "default", "all-children", "any-children"} {
		t.Run(gate, func(t *testing.T) {
			prefix := "wait-workflow-" + gate
			items := b26Invoke(t, h, prefix, 3)
			spawner := items[0].ID
			for i, item := range items[1:] {
				b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(item.ID), "fields": map[string]any{"parent": ref(spawner)}}, fmt.Sprintf("%s-parent-%d", prefix, i)))
			}
			dependent := h.create(t, prefix+"-dependent")
			params := map[string]any{"from": ref(dependent), "to": ref(spawner), "kind": "waits-for"}
			if gate != "nil" {
				m := map[string]any{}
				if gate != "default" {
					m["gate"] = gate
				}
				params["metadata"] = m
			}
			b26OK(t, h.call(t, "bead.link", params, prefix+"-link"))
			if h.ready(t, true)[dependent] {
				t.Fatal("open workflow child did not block ordinary dependent")
			}
			b26AssertRefusalUnchanged(t, h, "bead.claim", map[string]any{"id": ref(dependent)}, prefix+"-claim-refusal", "ontology_guard_failed")
			b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(items[1].ID)}, prefix+"-close-first"))
			if h.ready(t, true)[dependent] != (gate == "any-children") {
				t.Fatal("workflow gate did not distinguish all/any child completion")
			}
			b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(spawner)}, prefix+"-close-spawner"))
			if h.ready(t, true)[dependent] != (gate == "nil" || gate == "any-children") {
				t.Fatal("terminal-spawner fallback was applied to explicit all-children gate")
			}
			if gate == "all-children" || gate == "default" {
				b26AssertRefusalUnchanged(t, h, "bead.close", map[string]any{"id": ref(dependent)}, prefix+"-close-refusal", "ontology_guard_failed")
			}
			b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(items[2].ID)}, prefix+"-close-last"))
			if !h.ready(t, true)[dependent] {
				t.Fatal("completed workflow run left an ordinary blocker")
			}
		})
	}
}

func b26PromotionBlockers(t *testing.T, h *b26HTTP) {
	items := b26Invoke(t, h, "promote-derived-blockers", 2)
	target := h.create(t, "promote-derived-target")
	b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(items[0].ID), "to": ref(items[1].ID), "kind": "blocks"}, "promote-derived-link"))
	b26OK(t, h.call(t, "x.workflow.run.promote", map[string]any{"item": ref(items[0].ID), "target": ref(target)}, "promote-derived-move"))
	if h.ready(t, true)[target] || h.get(t, target)["bead"].(map[string]any)["blocked"] != true {
		t.Fatal("promotion lost live workflow blocker")
	}
	b26AssertRefusalUnchanged(t, h, "bead.claim", map[string]any{"id": ref(target)}, "promote-derived-claim", "ontology_guard_failed")
	b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(items[1].ID)}, "promote-derived-complete"))
	if !h.ready(t, true)[target] || h.get(t, target)["bead"].(map[string]any)["blocked"] == true {
		t.Fatal("derived blocker became sticky after workflow completion")
	}
}

type b26GuardMutator func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error)

func (f b26GuardMutator) Mutate(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	return f(ctx, p, r)
}

func b26GuardRace(t *testing.T, store driver.Store) {
	var h *b26HTTP
	var dependent, blocker string
	var once sync.Once
	mutator := b26GuardMutator(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
		if p.Meta.Method == "bead.claim" && p.Meta.IdempotencyKey == key("generation-race-claim") {
			once.Do(func() {
				b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(dependent), "to": ref(blocker), "kind": "blocks"}, "generation-race-edge"))
			})
		}
		return nil, nil
	})
	h = b26Host(t, store, pipeline.WithMutators(pipeline.MutatorBinding{ID: "b26-race", Mutator: mutator}))
	dependent = h.create(t, "generation-race-dependent")
	blocker = b26Invoke(t, h, "generation-race-blocker", 1)[0].ID
	before, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := h.call(t, "bead.claim", map[string]any{"id": ref(dependent)}, "generation-race-claim")
	if result.Error == nil || result.Error.Data.Code != "ontology_guard_failed" {
		t.Fatalf("checkpoint-racing direct claim published: %+v", result.Error)
	}
	after, err := store.Meta(context.Background())
	if err != nil || before.Head != after.Head {
		t.Fatalf("racing claim consumed a durable revision: %v", err)
	}
	if _, ok := h.get(t, dependent)["lease"]; ok {
		t.Fatal("racing claim persisted a lease")
	}
	if h.ready(t, true)[dependent] {
		t.Fatal("racing mixed edge was lost")
	}
}
func TestB26MixedBlockerGenerationFenceSQLite(t *testing.T) {
	store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "guard-race.db")})
	if err != nil {
		t.Fatal(err)
	}
	b26GuardRace(t, store)
}
func TestB26MixedBlockerGenerationFencePostgreSQL(t *testing.T) {
	b26GuardRace(t, openStore(t, freshDatabase(t)))
}

// b26HeadRaceStore intercepts only the final host checkpoint call. All reads,
// ordinary writes, fences and failed/successful CAS operations use the real SQL
// driver. No provider or resolver result is fabricated by the race fixture.
type b26HeadRaceStore struct {
	driver.Store
	driver.PersistentCheckpointStore
	guard    driver.HeadGuardedCheckpointStore
	mu       sync.Mutex
	before   func(driver.CoreSnapshotExpectation)
	rejected int
}

func (s *b26HeadRaceStore) SaveCheckpointAtHead(ctx context.Context, lease driver.ActiveLease, name string, expected uint64, payload []byte, head driver.CoreSnapshotExpectation) (driver.Checkpoint, error) {
	s.mu.Lock()
	before := s.before
	s.before = nil
	s.mu.Unlock()
	if before != nil {
		before(head)
	}
	result, err := s.guard.SaveCheckpointAtHead(ctx, lease, name, expected, payload, head)
	if errors.Is(err, driver.ErrCheckpointConflict) {
		s.mu.Lock()
		s.rejected++
		s.mu.Unlock()
	}
	return result, err
}

func b26ReverseGuardRace(t *testing.T, open func(*testing.T) driver.Store) {
	for _, method := range []string{"bead.claim", "bead.claimNext", "bead.claimNext.empty", "bead.close"} {
		for _, relation := range []string{"blocks", "waits-for/nil", "waits-for/default", "waits-for/all-children", "waits-for/any-children"} {
			t.Run(method+"/"+relation, func(t *testing.T) {
				empty := method == "bead.claimNext.empty"
				rpcMethod := method
				if empty {
					rpcMethod = "bead.claimNext"
				}
				base := open(t)
				store := &b26HeadRaceStore{Store: base, PersistentCheckpointStore: base.(driver.PersistentCheckpointStore), guard: base.(driver.HeadGuardedCheckpointStore)}
				h := b26Host(t, store)
				item := b26Invoke(t, h, "reverse-race-item", 1)[0].ID
				blocker := h.create(t, "reverse-race-blocker")
				reopen := blocker
				link := map[string]any{"from": ref(item), "to": ref(blocker), "kind": "blocks"}
				if relation != "blocks" {
					child := decode[model.Commit](t, b26OK(t, h.call(t, "bead.create", map[string]any{"type": "issue", "parent": ref(blocker), "fields": map[string]any{"title": "reverse-race-child"}}, "reverse-race-child")))
					reopen = child.Results[0].Ref.ID
					link["kind"] = "waits-for"
					if relation != "waits-for/nil" {
						metadata := map[string]any{}
						if relation != "waits-for/default" {
							metadata["gate"] = relation[len("waits-for/"):]
						}
						link["metadata"] = metadata
					}
				}
				if !empty {
					b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(reopen)}, "reverse-race-initial-close"))
				}
				b26OK(t, h.call(t, "bead.link", link, "reverse-race-link"))
				if h.ready(t, true)[item] == empty {
					t.Fatal("fixture's ready state does not match its dependency")
				}
				before := workflowCheckpoint(t, store)
				originalMeta, err := store.Meta(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				var reopenedMeta driver.Meta
				store.mu.Lock()
				store.before = func(head driver.CoreSnapshotExpectation) {
					if head.StoreID != originalMeta.StoreID || head.Incarnation != originalMeta.Incarnation || head.Head != originalMeta.Head {
						t.Errorf("publication did not bind the observed complete graph: %+v", head)
					}
					// Reopen after every durable resolver read, immediately before
					// the real driver's atomic core-head/checkpoint CAS boundary.
					if empty {
						b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(reopen), "fields": map[string]any{"description": "after empty-queue blocker snapshot"}}, "reverse-race-update"))
					} else {
						b26OK(t, h.call(t, "bead.reopen", map[string]any{"id": ref(reopen), "reason": "deterministic reverse race"}, "reverse-race-reopen"))
					}
					reopenedMeta, err = store.Meta(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(before, workflowCheckpoint(t, store)) {
						t.Fatal("ordinary reopen changed workflow namespace")
					}
				}
				store.mu.Unlock()
				params := map[string]any{"id": ref(item)}
				if rpcMethod == "bead.claimNext" {
					params = map[string]any{"filter": map[string]any{"includeEphemeral": true, "resources": []string{item}}}
				}
				result := h.call(t, rpcMethod, params, "reverse-race-write")
				store.mu.Lock()
				rejected, pending := store.rejected, store.before != nil
				store.mu.Unlock()
				if rejected != 1 || pending {
					t.Fatalf("race did not reach/reject the atomic publication: rejected=%d", rejected)
				}
				if result.Error == nil || result.Error.Data.Code != "write_conflict" {
					t.Fatalf("racing mutation was not refused: %+v", result.Error)
				}
				if !reflect.DeepEqual(before, workflowCheckpoint(t, store)) {
					t.Fatal("failed head fence published a receipt, claim or close")
				}
				afterMeta, err := store.Meta(context.Background())
				if err != nil || afterMeta != reopenedMeta {
					t.Fatalf("workflow mutation wrote core history: %v", err)
				}
				if h.ready(t, true)[item] || h.get(t, item)["lease"] != nil {
					t.Fatal("stale blocker observation became a live workflow claim or close")
				}
				b26NoVersion(t, store, item)
				if empty {
					// A fresh decision may publish the terminal null only after
					// the new core snapshot passes the real SQL fence.
					if string(b26OK(t, h.call(t, rpcMethod, params, "reverse-race-empty-terminal"))) != "null" {
						t.Fatal("fresh blocked queue did not return terminal null")
					}
					terminal := workflowCheckpoint(t, store)
					if !reflect.DeepEqual(before.Items, terminal.Items) || !reflect.DeepEqual(before.Runs, terminal.Runs) || terminal.Checkpoint.Generation <= before.Checkpoint.Generation {
						t.Fatal("fresh terminal receipt changed live workflow state or was not persisted")
					}
					b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(reopen), "fields": map[string]any{"description": "after fenced empty receipt"}}, "reverse-race-after-empty"))
					if string(b26OK(t, h.call(t, rpcMethod, params, "reverse-race-empty-terminal"))) != "null" || !reflect.DeepEqual(terminal, workflowCheckpoint(t, store)) {
						t.Fatal("terminal null replay changed checkpoint after a later core write")
					}
					b26NoVersion(t, store, item)
				}
				b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(reopen)}, "reverse-race-satisfy-again"))
				b26OK(t, h.call(t, rpcMethod, params, "reverse-race-success"))
				// Receipt replay precedes a later durable blocker observation.
				b26OK(t, h.call(t, "bead.reopen", map[string]any{"id": ref(reopen), "reason": "after successful workflow write"}, "reverse-race-reopen-again"))
				replayState := workflowCheckpoint(t, store)
				b26OK(t, h.call(t, rpcMethod, params, "reverse-race-success"))
				if !reflect.DeepEqual(replayState, workflowCheckpoint(t, store)) {
					t.Fatal("workflow receipt replay republished checkpoint")
				}
			})
		}
	}
}

func TestB26ReverseDurableBlockerFenceSQLite(t *testing.T) {
	b26ReverseGuardRace(t, func(t *testing.T) driver.Store {
		store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "reverse-race.db")})
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}
func TestB26ReverseDurableBlockerFencePostgreSQL(t *testing.T) {
	b26ReverseGuardRace(t, func(t *testing.T) driver.Store { return openStore(t, freshDatabase(t)) })
}

func b26ExplicitBlocker(t *testing.T, h *b26HTTP) {
	for _, method := range []string{"bead.claim", "bead.close", "tx.apply"} {
		t.Run(method, func(t *testing.T) {
			prefix := "explicit-blocker-" + method
			item := b26Invoke(t, h, prefix, 1)[0].ID
			b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(item), "fields": map[string]any{"blocked": true}}, prefix+"-set"))
			if h.ready(t, true)[item] || h.get(t, item)["bead"].(map[string]any)["blocked"] != true {
				t.Fatal("explicit workflow blocker was ignored")
			}
			b26AssertRefusalUnchanged(t, h, "bead.close", map[string]any{"id": ref(item)}, prefix+"-workflow-close", "ontology_guard_failed")
			target := h.create(t, prefix+"-target")
			b26OK(t, h.call(t, "x.workflow.run.promote", map[string]any{"item": ref(item), "target": ref(target)}, prefix+"-promote"))
			if h.ready(t, true)[target] || h.get(t, target)["bead"].(map[string]any)["blocked"] != true {
				t.Fatal("promotion dropped explicit blocker")
			}
			params := map[string]any{"id": ref(target)}
			if method == "tx.apply" {
				params = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpClose, Close: &model.CloseOp{Target: ref(target)}}}}}
			}
			b26AssertRefusalUnchanged(t, h, method, params, prefix+"-write", "ontology_guard_failed")
			b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(target), "fields": map[string]any{"blocked": false}}, prefix+"-clear"))
			if !h.ready(t, true)[target] {
				t.Fatal("explicit blocker clear did not release promoted work")
			}
			result := b26OK(t, h.call(t, method, params, prefix+"-write"))
			b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(target), "fields": map[string]any{"blocked": true}}, prefix+"-set-again"))
			checkpoint := workflowCheckpoint(t, h.store.(driver.CheckpointStore))
			meta, err := h.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			replay := b26OK(t, h.call(t, method, params, prefix+"-write"))
			var want, got map[string]any
			if err := json.Unmarshal(result, &want); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(replay, &got); err != nil {
				t.Fatal(err)
			}
			delete(want, "_meta")
			delete(got, "_meta")
			if !reflect.DeepEqual(want, got) {
				t.Fatal("explicit blocker changed an existing successful receipt")
			}
			after, err := h.store.Meta(context.Background())
			if err != nil || after != meta || !reflect.DeepEqual(checkpoint, workflowCheckpoint(t, h.store.(driver.CheckpointStore))) {
				t.Fatal("blocked replay republished graph or checkpoint")
			}
		})
	}
}
