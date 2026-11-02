package rpc_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func openWorkflowSQLiteServer(t *testing.T, path string) (*rpc.Server, *service.Service, driver.Store, driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "workflow-rpc", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	checkpoints, ok := store.(driver.CheckpointStore)
	if !ok {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal("SQLite store did not expose CheckpointStore")
	}
	state := pluginhost.NewDurableStateWithPersistence(checkpoints, lease, workflowplugin.Alias, "default", true)
	workflow, err := workflowplugin.New(state)
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	workflow.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
	domain := service.New(store, lease, writer, service.WithOntology(ontology), service.WithWorkflow(workflow))
	registry, err := rpc.NewRegistry()
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	config := pagingConfig()
	config.Incarnation = meta.Incarnation
	server, err := rpc.New(registry, config)
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	return server, domain, store, lease
}

func workflowMeta(key, agent string) map[string]any {
	return map[string]any{"idempotencyKey": key, "agent": agent, "project": "workflow-project"}
}

func TestEphemeralWorkflowUsesStandardToolsAcrossRestartAndLeavesNoHistory(t *testing.T) {
	ctx := rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	path := filepath.Join(t.TempDir(), "workflow.db")
	server, domain, store, lease := openWorkflowSQLiteServer(t, path)
	closeStore := func() {
		t.Helper()
		if lease != nil {
			if err := lease.Release(context.Background()); err != nil {
				t.Error(err)
			}
			lease = nil
		}
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
			store = nil
		}
	}
	t.Cleanup(closeStore)

	invokeKey := strings.Repeat("1", 64)
	invokeParams := map[string]any{
		"template":  "review-template",
		"project":   "workflow-project",
		"mode":      "ephemeral",
		"checklist": []map[string]any{{"label": "step-a", "title": "private workflow title", "type": "task"}},
		"_meta":     workflowMeta(invokeKey, "owner-agent"),
	}
	invoked := result(t, call(t, server, ctx, "x.workflow.invoke", paramsJSON(t, invokeParams)))
	if invoked["plane"] != "ephemeral" {
		t.Fatalf("workflow invocation plane=%v", invoked["plane"])
	}
	items := invoked["items"].([]any)
	item := items[0].(map[string]any)
	itemID := item["id"].(string)
	if item["plane"] != "ephemeral" || item["kind"] != "workflow.item" {
		t.Fatalf("workflow item did not expose its plane/kind: %v", item)
	}

	ready := result(t, call(t, server, ctx, "bead.ready", paramsJSON(t, map[string]any{
		"limit": 10, "_meta": workflowMeta(strings.Repeat("2", 64), "owner-agent"),
	})))
	readyItems := ready["items"].([]any)
	if len(readyItems) != 1 || readyItems[0].(map[string]any)["id"] != itemID {
		t.Fatalf("standard bead.ready did not expose the workflow item: %v", ready)
	}
	requireCode(t, call(t, server, ctx, "bead.get", paramsJSON(t, map[string]any{
		"id": map[string]any{"id": itemID}, "_meta": workflowMeta(strings.Repeat("3", 64), "other-agent"),
	})), "forbidden")

	commentResponse := call(t, server, ctx, "bead.comment", paramsJSON(t, map[string]any{
		"id": map[string]any{"id": itemID}, "body": "private workflow comment", "_meta": workflowMeta(strings.Repeat("5", 64), "owner-agent"),
	}))
	if _, ok := commentResponse["error"]; ok {
		_, directErr := domain.CommentWithOutcome(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, service.CommentRequest{ID: model.RefByID(itemID, model.Natural64(0)), Body: "private workflow comment"}, model.WriteMeta{Method: "bead.comment", Project: "workflow-project", Agent: "owner-agent", AgentLabel: "owner-agent", IdempotencyKey: strings.Repeat("c", 64)})
		t.Fatalf("standard bead.comment response=%v; direct service error=%v", commentResponse, directErr)
	}
	commented := result(t, commentResponse)
	if len(commented["results"].([]any)) != 1 {
		t.Fatalf("standard bead.comment result=%v", commented)
	}

	// Reopen the same SQLite store before closing the checklist. The new
	// provider instance must see the live item and its comment from the
	// plugin-owned checkpoint.
	closeStore()
	server, domain, store, lease = openWorkflowSQLiteServer(t, path)
	reopened := result(t, call(t, server, ctx, "bead.get", paramsJSON(t, map[string]any{
		"id": map[string]any{"id": itemID}, "_meta": workflowMeta(strings.Repeat("6", 64), "owner-agent"),
	})))
	if reopened["bead"].(map[string]any)["id"] != itemID {
		t.Fatalf("reopened standard bead.get lost item: %v", reopened)
	}
	comments := reopened["comments"].([]any)
	if len(comments) != 1 || comments[0].(map[string]any)["body"] != "private workflow comment" {
		t.Fatalf("reopened workflow comment was not durable: %v", reopened)
	}

	claimed := result(t, call(t, server, ctx, "bead.claimNext", paramsJSON(t, map[string]any{
		"_meta": workflowMeta(strings.Repeat("7", 64), "owner-agent"),
	})))
	if len(claimed["results"].([]any)) != 1 {
		t.Fatalf("standard bead.claimNext did not claim workflow item: %v", claimed)
	}
	closed := result(t, call(t, server, ctx, "bead.close", paramsJSON(t, map[string]any{
		"id": map[string]any{"id": itemID}, "reason": "done", "_meta": workflowMeta(strings.Repeat("8", 64), "owner-agent"),
	})))
	if len(closed["results"].([]any)) != 1 {
		t.Fatalf("standard bead.close result=%v", closed)
	}
	requireCode(t, call(t, server, ctx, "bead.get", paramsJSON(t, map[string]any{
		"id": map[string]any{"id": itemID}, "_meta": workflowMeta(strings.Repeat("9", 64), "owner-agent"),
	})), "not_found")
	readyAfter := result(t, call(t, server, ctx, "bead.ready", paramsJSON(t, map[string]any{
		"limit": 10, "_meta": workflowMeta(strings.Repeat("a", 64), "owner-agent"),
	})))
	if len(readyAfter["items"].([]any)) != 0 {
		t.Fatalf("completed workflow item remained ready: %v", readyAfter)
	}

	// The original invocation key replays a terminal identity-safe receipt and
	// cannot recreate the deleted item or disclose its title/comment.
	replayed := result(t, call(t, server, ctx, "x.workflow.invoke", paramsJSON(t, invokeParams)))
	if replayed["plane"] != "ephemeral" || len(replayed["items"].([]any)) != 0 {
		t.Fatalf("terminal workflow replay resurrected payload: %v", replayed)
	}
	raw := paramsJSON(t, replayed)
	if strings.Contains(raw, "private workflow title") || strings.Contains(raw, "private workflow comment") {
		t.Fatalf("terminal replay disclosed deleted payload: %s", raw)
	}

	// Core history is a separate permanent plane and must remain empty for the
	// plugin-owned item.
	asOf := ready["asOf"].(map[string]any)
	history := result(t, call(t, server, ctx, "history.query", paramsJSON(t, map[string]any{
		"asOf":  asOf,
		"_meta": workflowMeta(strings.Repeat("b", 64), "owner-agent"),
	})))
	if len(history["items"].([]any)) != 0 {
		t.Fatalf("ephemeral item entered permanent history: %v", history)
	}
}
