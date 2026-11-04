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
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
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
	serial int
}

func b26Host(t *testing.T, store driver.Store) *b26HTTP {
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
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
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
	h.serial++
	values := map[string]any{}
	for name, value := range params {
		values[name] = value
	}
	meta := map[string]any{"project": project, "agent": "b26-public"}
	if label != "" {
		meta["idempotencyKey"] = key(label)
	}
	values["_meta"] = meta
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": h.serial, "method": method, "params": values})
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
