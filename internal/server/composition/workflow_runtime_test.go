package composition

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestMemoryRuntimeKeepsOrdinaryWorkflowToolsAndRefusesEphemeral(t *testing.T) {
	f := startRuntime(t, Config{Driver: "memory", Project: "workflow-memory", Address: DefaultAddress})
	ctx := context.Background()
	params := map[string]any{
		"template": "template", "project": "workflow-memory", "mode": "permanent",
		"checklist": []map[string]any{{"label": "one", "title": "ordinary durable item", "type": "issue"}},
	}
	created, err := f.client.Call(ctx, "x.workflow.invoke", params, "")
	if err != nil {
		t.Fatal(err)
	}
	invoked := resultObject(t, created)
	items := invoked["items"].([]any)
	if invoked["plane"] != "permanent" || len(items) != 1 {
		t.Fatalf("memory permanent invocation=%v", invoked)
	}
	id := items[0].(map[string]any)["id"].(string)
	ready, err := f.client.Call(ctx, "bead.ready", map[string]any{"limit": 10}, "")
	if err != nil {
		t.Fatal(err)
	}
	readyItems := resultObject(t, ready)["items"].([]any)
	if len(readyItems) != 1 || readyItems[0].(map[string]any)["id"] != id {
		t.Fatalf("ordinary memory ready=%v", readyItems)
	}
	params["mode"] = "ephemeral"
	_, err = f.client.Call(ctx, "x.workflow.invoke", params, "")
	var refused *wire.Error
	if !errors.As(err, &refused) || refused.Data.Code != "method_not_found" {
		t.Fatalf("memory runtime accepted ephemeral invocation: %v", err)
	}
	if _, err := f.client.Call(ctx, "bead.get", map[string]any{"id": map[string]any{"id": id}}, ""); err != nil {
		t.Fatalf("ephemeral refusal broke ordinary memory reads: %v", err)
	}
}

func TestAuthenticatedWorkflowHTTPRestartCompletionAndHistory(t *testing.T) {
	ctx := context.Background()
	keys := authnapikey.New(authnapikey.Config{})
	owner := model.PrincipalRef{Kind: "user", ID: "user:workflow-owner"}
	issue := func(principal model.PrincipalRef) authnapikey.IssuedKey {
		t.Helper()
		key, err := keys.Issue(principal, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	ownerKey := issue(owner)
	otherKey := issue(model.PrincipalRef{Kind: "user", ID: "user:workflow-other"})
	config := Config{
		Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "workflow-runtime.db"),
		Project: "workflow-http", Address: DefaultAddress, AuthMode: "authenticated",
		Auth: authn.Config{Provider: keys},
	}
	current := startRuntime(t, config)
	initialMeta, err := current.r.Store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertIncarnation := func() {
		t.Helper()
		meta, err := current.r.Store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if meta.Incarnation != initialMeta.Incarnation {
			t.Fatalf("SQLite reopen changed incarnation: first=%q reopened=%q", initialMeta.Incarnation, meta.Incarnation)
		}
	}
	newClient := func(key authnapikey.IssuedKey, project string) *client.Client {
		t.Helper()
		transport, err := client.NewHTTP(client.HTTPConfig{Server: "http://" + current.server.Addr().String(), Token: key.Token})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(transport.Close)
		c, err := client.New(transport, project, "workflow-runtime-agent")
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	ownerClient := newClient(ownerKey, config.Project)
	call := func(c *client.Client, method string, params map[string]any, key string) client.Result {
		t.Helper()
		value, err := c.Call(ctx, method, params, key)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		return value
	}
	ref := func(id string) map[string]any { return map[string]any{"id": map[string]any{"id": id}} }
	params := map[string]any{
		"template": "review", "project": config.Project, "mode": "ephemeral",
		"checklist": []map[string]any{{"label": "one", "title": "ephemeral confidential title", "type": "task"}},
	}
	invocation := resultObject(t, call(ownerClient, "x.workflow.invoke", params, strings.Repeat("1", 64)))
	itemID := invocation["items"].([]any)[0].(map[string]any)["id"].(string)
	itemRef := ref(itemID)
	ready := resultObject(t, call(ownerClient, "bead.ready", map[string]any{"limit": 10}, ""))
	if rows := ready["items"].([]any); len(rows) != 1 || rows[0].(map[string]any)["id"] != itemID {
		t.Fatalf("authenticated standard ready lost workflow item: %v", ready)
	}
	commentParams := ref(itemID)
	commentParams["body"] = "ephemeral confidential comment"
	call(ownerClient, "bead.comment", commentParams, strings.Repeat("2", 64))
	_, err = newClient(otherKey, config.Project).Call(ctx, "bead.get", itemRef, "")
	var denied *wire.Error
	if !errors.As(err, &denied) || denied.Data.Code != "forbidden" {
		t.Fatalf("other authenticated principal read workflow item: %v", err)
	}
	_, err = newClient(ownerKey, "other-project").Call(ctx, "bead.get", itemRef, "")
	if !errors.As(err, &denied) || denied.Data.Code != "forbidden" {
		t.Fatalf("cross-project point read was not denied: %v", err)
	}
	assertNoHistory := func() {
		t.Helper()
		currentReady := resultObject(t, call(ownerClient, "bead.ready", map[string]any{"limit": 10}, ""))
		result := resultObject(t, call(ownerClient, "history.query", map[string]any{"asOf": currentReady["asOf"]}, ""))
		if len(result["items"].([]any)) != 0 {
			t.Fatalf("ephemeral item entered permanent history: %v", result)
		}
		archive, manifest, err := restore.Dump(ctx, current.r.Store, "full")
		if err != nil {
			t.Fatal(err)
		}
		if manifest.RecordCount != 0 || manifest.VersionCount != 0 || manifest.ChangesetCount != 0 {
			t.Fatalf("ephemeral operation entered native dump history: records=%d versions=%d changesets=%d", manifest.RecordCount, manifest.VersionCount, manifest.ChangesetCount)
		}
		for _, value := range []string{itemID, "ephemeral confidential title", "ephemeral confidential comment"} {
			if bytes.Contains(archive, []byte(value)) {
				t.Fatalf("permanent native dump retained ephemeral value %q", value)
			}
		}
	}
	assertNoHistory()
	current.stop(t)
	current = startRuntime(t, config)
	assertIncarnation()
	ownerClient = newClient(ownerKey, config.Project)
	reopened := resultObject(t, call(ownerClient, "bead.get", itemRef, ""))
	if comments := reopened["comments"].([]any); len(comments) != 1 || comments[0].(map[string]any)["body"] != commentParams["body"] {
		t.Fatalf("restart lost durable workflow comment: %v", reopened)
	}
	claim := call(ownerClient, "bead.claimNext", map[string]any{}, strings.Repeat("3", 64))
	var liveClaim model.Commit
	if err := json.Unmarshal(claim.JSON, &liveClaim); err != nil {
		t.Fatal(err)
	}
	if len(liveClaim.Results) != 1 || liveClaim.Results[0].Ref.ID != itemID ||
		liveClaim.Revision.String() != "0" || liveClaim.LeaseExpiresAt == nil ||
		liveClaim.LeaseTtlMs == nil || liveClaim.LeaseTtlMs.String() != "1800000" {
		t.Fatalf("live standard claim lost its identity or lease metadata: %s", claim.JSON)
	}
	replayedClaim := call(ownerClient, "bead.claimNext", map[string]any{}, strings.Repeat("3", 64))
	if !bytes.Equal(claim.JSON, replayedClaim.JSON) {
		t.Fatalf("claimNext response loss changed committed identity: first=%s replay=%s", claim.JSON, replayedClaim.JSON)
	}
	if !keys.Revoke(ownerKey.ID) {
		t.Fatal("owner credential was not revoked")
	}
	_, err = ownerClient.Call(ctx, "bead.get", itemRef, "")
	if !errors.As(err, &denied) || denied.Data.Code != "unauthenticated" {
		t.Fatalf("revoked credential retained workflow access: %v", err)
	}
	ownerClient = newClient(issue(owner), config.Project)
	closeParams := ref(itemID)
	closeParams["reason"] = "done"
	closed := call(ownerClient, "bead.close", closeParams, strings.Repeat("4", 64))
	// A process restart preserves the store incarnation. Model a lost final
	// response by reopening before retrying its identity-only completion.
	current.stop(t)
	current = startRuntime(t, config)
	assertIncarnation()
	ownerClient = newClient(issue(owner), config.Project)
	replayedClose := call(ownerClient, "bead.close", closeParams, strings.Repeat("4", 64))
	if !bytes.Equal(closed.JSON, replayedClose.JSON) {
		t.Fatalf("terminal restart replay changed completion receipt: first=%s replay=%s", closed.JSON, replayedClose.JSON)
	}
	// The schema admits an identity-only receipt shape. Establish its lifecycle
	// provenance at the real producer boundary: only replay after completion
	// and reopen may lose the original live claim's lease fields.
	terminalClaim := call(ownerClient, "bead.claimNext", map[string]any{}, strings.Repeat("3", 64))
	var erasedClaim model.Commit
	if err := json.Unmarshal(terminalClaim.JSON, &erasedClaim); err != nil {
		t.Fatal(err)
	}
	if len(erasedClaim.Results) != 1 || erasedClaim.Revision.String() != "0" ||
		erasedClaim.Results[0].Ref != liveClaim.Results[0].Ref ||
		erasedClaim.Results[0].Revision != liveClaim.Results[0].Revision ||
		erasedClaim.LeaseExpiresAt != nil || erasedClaim.LeaseTtlMs != nil ||
		len(erasedClaim.Results[0].Record) != 0 || len(erasedClaim.Warnings) != 0 ||
		bytes.Contains(terminalClaim.JSON, []byte("ephemeral confidential")) {
		t.Fatalf("terminal standard claim replay changed identity or retained payload: live=%s terminal=%s", claim.JSON, terminalClaim.JSON)
	}
	terminal := call(ownerClient, "x.workflow.invoke", params, strings.Repeat("1", 64))
	if len(resultObject(t, terminal)["items"].([]any)) != 0 || bytes.Contains(terminal.JSON, []byte("ephemeral confidential")) {
		t.Fatalf("terminal restart replay disclosed or recreated payload: %s", terminal.JSON)
	}
	_, err = ownerClient.Call(ctx, "bead.get", itemRef, "")
	if !errors.As(err, &denied) || denied.Data.Code != "not_found" {
		t.Fatalf("completed workflow item remained readable after restart: %v", err)
	}
	if rows := resultObject(t, call(ownerClient, "bead.ready", map[string]any{"limit": 10}, ""))["items"].([]any); len(rows) != 0 {
		t.Fatalf("completed workflow remained ready: %v", rows)
	}
	checkpoint, err := current.r.Store.(driver.CheckpointStore).LoadCheckpoint(ctx, "plugin/workflow/default/durable")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(checkpoint.Payload, []byte("ephemeral confidential")) {
		t.Fatal("completion retained body or comment in durable operational state")
	}
	assertNoHistory()
	t.Log("authenticated HTTP invoke/ready/comment/get/claimNext/close; two runtime reopenings; credential/project denials; terminal replay; native history/export exclusion")
}

func TestWorkflowRuntimeCollectsExpiredTerminalReceiptsOnReady(t *testing.T) {
	ctx := context.Background()
	config := Config{Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "workflow-gc.db"), Project: "workflow-gc", Address: DefaultAddress}
	current := startRuntime(t, config)
	call := func(method string, params map[string]any, key string) client.Result {
		t.Helper()
		result, err := current.client.Call(ctx, method, params, key)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		return result
	}
	params := map[string]any{"template": "cleanup", "project": config.Project, "mode": "ephemeral",
		"checklist": []map[string]any{{"label": "one", "title": "workflow item"}}}
	ended := resultObject(t, call("x.workflow.invoke", params, strings.Repeat("1", 64)))
	endedID := ended["items"].([]any)[0].(map[string]any)["id"].(string)
	call("bead.close", map[string]any{"id": map[string]any{"id": endedID}}, strings.Repeat("2", 64))
	live := call("x.workflow.invoke", params, strings.Repeat("3", 64))
	liveObject := resultObject(t, live)
	liveRunID := liveObject["run"].(map[string]any)["id"].(string)
	liveID := liveObject["items"].([]any)[0].(map[string]any)["id"].(string)
	state := pluginhost.NewDurableState(current.r.Store.(driver.PersistentCheckpointStore), current.r.Lease, "workflow", "default")
	var document map[string]json.RawMessage
	found, revision, err := state.Read(ctx, "workflow/state", &document)
	if err != nil || !found {
		t.Fatalf("read actual checkpoint: found=%v err=%v", found, err)
	}
	var receipts map[string]map[string]json.RawMessage
	if err := json.Unmarshal(document["receipts"], &receipts); err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 3 {
		t.Fatalf("pre-cleanup receipt count=%d, want completed invoke/close and live invoke", len(receipts))
	}
	// Seed eligibility through the real scoped capability. This proves runtime
	// activation after expiry, not elapsed wall-clock time or a changed clock.
	expiredAt, err := json.Marshal(time.Now().UTC().Add(-driver.IdempotencyWindow))
	if err != nil {
		t.Fatal(err)
	}
	liveReceiptKey := ""
	for key, receipt := range receipts {
		var runID string
		if err := json.Unmarshal(receipt["runId"], &runID); err != nil {
			t.Fatal(err)
		}
		if runID == liveRunID {
			liveReceiptKey = key
		}
		receipt["expiresAt"] = expiredAt
	}
	if liveReceiptKey == "" {
		t.Fatal("unfinished run receipt was not present before cleanup")
	}
	document["receipts"], err = json.Marshal(receipts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, "workflow/state", document, revision); err != nil {
		t.Fatal(err)
	}
	current.stop(t)
	current = startRuntime(t, config)
	ready := resultObject(t, call("bead.ready", map[string]any{"limit": 10}, ""))["items"].([]any)
	if len(ready) != 1 || ready[0].(map[string]any)["id"] != liveID {
		t.Fatalf("ordinary ready removed the old unfinished run: %v", ready)
	}
	state = pluginhost.NewDurableState(current.r.Store.(driver.PersistentCheckpointStore), current.r.Lease, "workflow", "default")
	if found, _, err := state.Read(ctx, "workflow/state", &document); err != nil || !found {
		t.Fatalf("read cleanup checkpoint: found=%v err=%v", found, err)
	}
	receipts = nil
	if err := json.Unmarshal(document["receipts"], &receipts); err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 1 || receipts[liveReceiptKey] == nil {
		t.Fatalf("normal ready did not physically collect only ended receipts: count=%d", len(receipts))
	}
	var audit []struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal(document["audit"], &audit); err != nil {
		t.Fatal(err)
	}
	gcCount := 0
	for _, entry := range audit {
		if entry.Event == "workflow.receipt.gc" {
			gcCount++
		}
	}
	if gcCount != 2 {
		t.Fatalf("durable receipt GC audit count=%d, want 2", gcCount)
	}
	replay := call("x.workflow.invoke", params, strings.Repeat("3", 64))
	if !bytes.Equal(live.JSON, replay.JSON) {
		t.Fatalf("old unfinished invocation did not replay after restart/GC: first=%s replay=%s", live.JSON, replay.JSON)
	}
	call("bead.comment", map[string]any{"id": map[string]any{"id": liveID}, "body": "after cleanup"}, strings.Repeat("4", 64))
	item := resultObject(t, call("bead.get", map[string]any{"id": map[string]any{"id": liveID}}, ""))
	if comments := item["comments"].([]any); len(comments) != 1 || comments[0].(map[string]any)["body"] != "after cleanup" {
		t.Fatalf("ordinary post-cleanup mutation was not durable: %v", item)
	}
}
