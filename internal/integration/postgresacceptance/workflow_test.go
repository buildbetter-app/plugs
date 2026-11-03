package postgresacceptance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type workflowReceiptState struct {
	Principal string          `json:"principal"`
	Method    string          `json:"method"`
	Key       string          `json:"key"`
	RunID     string          `json:"runId"`
	Terminal  bool            `json:"terminal"`
	Result    json.RawMessage `json:"result"`
}

type workflowCheckpointState struct {
	Checkpoint driver.Checkpoint
	Runs       map[string]json.RawMessage
	Items      map[string]json.RawMessage
	Receipts   map[string]workflowReceiptState
	Audit      []struct {
		Event string `json:"event"`
	}
}

// Read the actual driver checkpoint through an independent handle. The plugin
// namespace is decoded only for observations; no provider or host state is
// trusted to report its own publication or cleanup.
func workflowCheckpoint(t *testing.T, store driver.CheckpointStore) workflowCheckpointState {
	t.Helper()
	checkpoint, err := store.LoadCheckpoint(context.Background(), "plugin/workflow/default/durable")
	if err != nil {
		t.Fatal(err)
	}
	out := workflowCheckpointState{Checkpoint: checkpoint}
	if checkpoint.Generation == 0 {
		if len(checkpoint.Payload) != 0 {
			t.Fatal("absent workflow checkpoint has a payload")
		}
		return out
	}
	var namespace struct {
		Values map[string]struct {
			Revision uint64          `json:"revision"`
			Value    json.RawMessage `json:"value"`
		} `json:"values"`
	}
	if err := json.Unmarshal(checkpoint.Payload, &namespace); err != nil {
		t.Fatal(err)
	}
	entry, ok := namespace.Values["workflow/state"]
	if !ok || entry.Revision == 0 || len(entry.Value) == 0 {
		t.Fatal("persisted workflow state is missing from the real checkpoint")
	}
	if err := json.Unmarshal(entry.Value, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestB26EphemeralWorkflowPostgresHTTPReopenReplayAndCleanup(t *testing.T) {
	ctx := context.Background()
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs("pl-workflow-permanent")
	keys := authnapikey.New(authnapikey.Config{})
	owner := model.PrincipalRef{Kind: "user", ID: "user:workflow-postgres-owner"}
	issued, err := keys.Issue(owner, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	principal, err := driver.PrincipalKey(owner)
	if err != nil {
		t.Fatal(err)
	}
	independent := openStore(t, dsn)
	h := startWorkflowHost(t, openStore(t, dsn), sequence, trace, keys, issued.Token)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before := graph(t, independent)
	ledger(t, before, 0, map[string]int{})
	if len(before.Records) != 0 || len(before.Versions) != 0 {
		t.Fatalf("fresh PostgreSQL graph is not empty: %+v", before)
	}
	sqlBefore := metadataSQLState(t, db)
	for _, table := range []string{"records", "versions", "states", "blobs", "changesets", "receipts", "allocator_states"} {
		if len(sqlBefore[table]) != 0 {
			t.Fatalf("fresh PostgreSQL %s family is not empty: %v", table, sqlBefore[table])
		}
	}
	if initial := workflowCheckpoint(t, independent); initial.Checkpoint.Generation != 0 {
		t.Fatalf("new PostgreSQL workflow already has a checkpoint: %+v", initial)
	}
	assertNoShadow := func(itemID string) {
		t.Helper()
		unchanged(t, before, graph(t, independent))
		sqlAfter := metadataSQLState(t, db)
		for _, table := range []string{"meta", "records", "versions", "states", "blobs", "changesets", "receipts", "allocator_states"} {
			if !reflect.DeepEqual(sqlBefore[table], sqlAfter[table]) {
				t.Fatalf("workflow operation published permanent %s rows: before=%v after=%v", table, sqlBefore[table], sqlAfter[table])
			}
		}
		if itemID != "" {
			r := reader(t, independent)
			defer r.Close()
			versions, err := r.Versions(ctx, model.RefByID(itemID, model.Natural64(0)), driver.PageReq{Limit: 100})
			if err != nil || len(versions.Items) != 0 || versions.NextCursor != nil {
				t.Fatalf("workflow item has permanent versions: %+v %v", versions, err)
			}
		}
	}
	// Authenticated principal receipts have an empty agent scope.
	assertNoOrdinaryReceipt := func(method, requestKey string) {
		t.Helper()
		if stored := receiptFor(t, independent, owner, method, requestKey, ""); stored != nil {
			t.Fatalf("workflow %s published an ordinary receipt: %+v", method, stored)
		}
	}
	invokeKey, commentKey, closeKey := key("b26-workflow-invoke"), key("b26-workflow-comment"), key("b26-workflow-close")
	params := map[string]any{
		"template":  "postgres-workflow",
		"project":   project,
		"mode":      "ephemeral",
		"checklist": []map[string]any{{"label": "step", "title": "private workflow item", "type": "task"}},
	}

	// The same public HTTP endpoint must reject a request without the key that
	// was actually issued by the installed API-key provider.
	requestBody, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "x.workflow.invoke", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	unauthenticated, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	unauthenticated.Header.Set("Content-Type", "application/json")
	unauthenticated.Header.Set("Plug-RPC", "1")
	response, err := h.client.Do(unauthenticated)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	admission := decode[reply](t, responseBody)
	if response.StatusCode != http.StatusUnauthorized || admission.Error == nil || admission.Error.Data.Code != "unauthenticated" {
		t.Fatalf("missing bearer reached PostgreSQL workflow provider: status=%d error=%v", response.StatusCode, admission.Error)
	}
	assertNoShadow("")
	if after := workflowCheckpoint(t, independent); after.Checkpoint.Generation != 0 {
		t.Fatalf("unauthenticated request published a private checkpoint: %+v", after)
	}

	invoked := h.call(t, "x.workflow.invoke", params, invokeKey)
	if invoked.Error != nil || invoked.Meta != nil {
		t.Fatalf("authenticated ephemeral invocation failed or invented replay metadata: %v", invoked.Error)
	}
	initial := decode[workflowplugin.InvokeResult](t, invoked.Result)
	if initial.Plane != "ephemeral" || initial.Run == nil || len(initial.Items) != 1 || initial.Items[0].Plane != "ephemeral" || initial.Run.Owner != principal {
		t.Fatalf("unexpected authenticated PostgreSQL workflow invocation: %+v", initial)
	}
	runID, itemID := initial.Run.ID, initial.Items[0].ID
	assertNoShadow(itemID)
	assertNoOrdinaryReceipt("x.workflow.invoke", invokeKey)
	active := workflowCheckpoint(t, independent)
	if active.Checkpoint.Generation == 0 || len(active.Runs) != 1 || len(active.Items) != 1 || len(active.Receipts) != 1 || len(active.Audit) != 1 || active.Audit[0].Event != "workflow.run.created" || len(active.Runs[runID]) == 0 || len(active.Items[itemID]) == 0 || !bytes.Contains(active.Checkpoint.Payload, []byte("private workflow item")) {
		t.Fatalf("real checkpoint omitted active run, item, receipt or audit: %+v", active)
	}
	for _, stored := range active.Receipts {
		if stored.Method != "x.workflow.invoke" || stored.Key != invokeKey || stored.Principal != principal || stored.RunID != runID || stored.Terminal || !bytes.Equal(stored.Result, invoked.Result) {
			t.Fatalf("invoke receipt is not bound to authenticated run/result: %+v", stored)
		}
	}

	comment := h.call(t, "bead.comment", map[string]any{"id": map[string]any{"id": itemID}, "body": "private workflow comment"}, commentKey)
	if comment.Error != nil {
		t.Fatalf("workflow comment failed: %v", comment.Error)
	}
	ready := h.call(t, "bead.ready", map[string]any{"limit": 10}, "")
	if ready.Error != nil || !bytes.Contains(ready.Result, []byte(itemID)) {
		t.Fatalf("authenticated workflow item was not ready: %+v", ready)
	}
	assertNoShadow(itemID)
	assertNoOrdinaryReceipt("bead.comment", commentKey)
	commented := workflowCheckpoint(t, independent)
	if len(commented.Runs) != 1 || len(commented.Items) != 1 || len(commented.Receipts) != 2 || len(commented.Audit) != 2 || commented.Audit[1].Event != "workflow.comment" || len(commented.Items[itemID]) == 0 || !bytes.Contains(commented.Checkpoint.Payload, []byte("private workflow comment")) {
		t.Fatalf("comment was not durably recorded in the private plane: %+v", commented)
	}

	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startWorkflowHost(t, openStore(t, dsn), sequence, trace, keys, issued.Token)
	if after := workflowCheckpoint(t, independent); !reflect.DeepEqual(after, commented) {
		t.Fatalf("PostgreSQL reopen changed active checkpoint: before=%+v after=%+v", commented, after)
	}
	assertNoShadow(itemID)
	reopened := h.call(t, "bead.get", map[string]any{"id": map[string]any{"id": itemID}}, "")
	if reopened.Error != nil || !bytes.Contains(reopened.Result, []byte("private workflow comment")) {
		t.Fatalf("PostgreSQL reopen lost authenticated workflow comment: %+v", reopened)
	}
	replayed := h.call(t, "x.workflow.invoke", params, invokeKey)
	if replayed.Error != nil || replayed.Meta != nil || !bytes.Equal(replayed.Result, invoked.Result) {
		t.Fatalf("workflow receipt replay changed immutable run/result after reopen: first=%s replay=%s err=%v", invoked.Result, replayed.Result, replayed.Error)
	}
	if after := workflowCheckpoint(t, independent); !reflect.DeepEqual(after, commented) {
		t.Fatalf("same-key replay allocated a shadow run, receipt or checkpoint: before=%+v after=%+v", commented, after)
	}
	assertNoShadow(itemID)
	changed := map[string]any{"template": "postgres-workflow", "project": project, "mode": "ephemeral",
		"checklist": []map[string]any{{"label": "step", "title": "different item", "type": "task"}}}
	refused(t, h.call(t, "x.workflow.invoke", changed, invokeKey), "idempotency_key_reused")
	if after := workflowCheckpoint(t, independent); !reflect.DeepEqual(after, commented) {
		t.Fatalf("changed replay request altered private state: before=%+v after=%+v", commented, after)
	}
	assertNoShadow(itemID)

	closed := h.call(t, "bead.close", map[string]any{"id": map[string]any{"id": itemID}, "reason": "done"}, closeKey)
	if closed.Error != nil {
		t.Fatalf("workflow close failed: %v", closed.Error)
	}
	assertNoShadow(itemID)
	assertNoOrdinaryReceipt("bead.close", closeKey)
	completed := workflowCheckpoint(t, independent)
	if len(completed.Runs) != 0 || len(completed.Items) != 0 || len(completed.Receipts) != 3 || len(completed.Audit) != 4 || completed.Audit[3].Event != "workflow.run.completed" ||
		bytes.Contains(completed.Checkpoint.Payload, []byte("private workflow item")) || bytes.Contains(completed.Checkpoint.Payload, []byte("private workflow comment")) {
		t.Fatalf("terminal checkpoint retained operational payload or lost identity audit: %+v", completed)
	}
	for _, stored := range completed.Receipts {
		if !stored.Terminal || stored.Principal != principal || stored.RunID != runID || bytes.Contains(stored.Result, []byte("private workflow")) {
			t.Fatalf("terminal receipt retained payload or lost authenticated identity: %+v", stored)
		}
	}
	refused(t, h.call(t, "bead.get", map[string]any{"id": map[string]any{"id": itemID}}, ""), "not_found")
	page(t, h.call(t, "bead.ready", map[string]any{"limit": 10}, ""), 0)
	page(t, h.call(t, "history.query", map[string]any{"asOf": map[string]any{"revision": model.Natural64(0), "incarnation": before.Meta.Incarnation}, "limit": 10}, ""), 0)

	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startWorkflowHost(t, openStore(t, dsn), sequence, trace, keys, issued.Token)
	if after := workflowCheckpoint(t, independent); !reflect.DeepEqual(after, completed) {
		t.Fatalf("PostgreSQL post-completion reopen restored erased payload: before=%+v after=%+v", completed, after)
	}
	assertNoShadow(itemID)
	refused(t, h.call(t, "bead.get", map[string]any{"id": map[string]any{"id": itemID}}, ""), "not_found")
	page(t, h.call(t, "bead.ready", map[string]any{"limit": 10}, ""), 0)
	terminal := h.call(t, "x.workflow.invoke", params, invokeKey)
	if terminal.Error != nil || terminal.Meta != nil {
		t.Fatalf("terminal workflow replay failed or invented replay metadata: %+v", terminal)
	}
	result := decode[workflowplugin.InvokeResult](t, terminal.Result)
	if result.Plane != "ephemeral" || result.Run == nil || result.Run.ID != runID || result.Run.State != "completed" || len(result.Items) != 0 ||
		bytes.Contains(terminal.Result, []byte("private workflow item")) || bytes.Contains(terminal.Result, []byte("private workflow comment")) {
		t.Fatalf("terminal receipt replay disclosed or recreated operational state: %+v", terminal)
	}
	if after := workflowCheckpoint(t, independent); !reflect.DeepEqual(after, completed) {
		t.Fatalf("terminal replay republished private state: before=%+v after=%+v", completed, after)
	}
	assertNoShadow(itemID)
	assertNoOrdinaryReceipt("x.workflow.invoke", invokeKey)
	assertNoOrdinaryReceipt("bead.comment", commentKey)
	assertNoOrdinaryReceipt("bead.close", closeKey)
	malformedKey := key("b26-workflow-malformed")
	refused(t, h.call(t, "x.workflow.invoke", map[string]any{"template": "postgres-workflow", "project": project, "mode": "ephemeral", "checklist": []any{}}, malformedKey), "invalid_params")
	if after := workflowCheckpoint(t, independent); !reflect.DeepEqual(after, completed) {
		t.Fatalf("malformed invocation altered private checkpoint: before=%+v after=%+v", completed, after)
	}
	assertNoShadow(itemID)
	assertNoOrdinaryReceipt("x.workflow.invoke", malformedKey)
	permanentKey := key("b26-workflow-permanent")
	permanent := h.call(t, "x.workflow.invoke", map[string]any{"template": "postgres-workflow", "project": project, "mode": "permanent",
		"checklist": []map[string]any{{"label": "ordinary", "title": "ordinary workflow item", "type": "issue"}}}, permanentKey)
	if permanent.Error != nil {
		t.Fatalf("permanent workflow invocation failed after ephemeral cleanup: %v", permanent.Error)
	}
	ordinary := decode[workflowplugin.InvokeResult](t, permanent.Result)
	if ordinary.Plane != "permanent" || len(ordinary.Items) != 1 || ordinary.Items[0].ID != "pl-workflow-permanent" {
		t.Fatalf("permanent invocation did not use the ordinary Writer: %+v", ordinary)
	}
	ledger(t, graph(t, independent, ordinary.Items[0].ID), 1, map[string]int{ordinary.Items[0].ID: 1})
	if stored := receiptFor(t, independent, owner, "x.workflow.invoke", permanentKey, ""); stored == nil || stored.ResponsePending {
		t.Fatalf("permanent workflow omitted its ordinary receipt: %+v", stored)
	}
	if after := workflowCheckpoint(t, independent); !reflect.DeepEqual(after, completed) {
		t.Fatalf("permanent invocation mutated completed private workflow state: before=%+v after=%+v", completed, after)
	}
	t.Log("B26 authenticated PostgreSQL HTTP: issued credential, private invoke/comment/ready/reopen/replay/close, independent graph/SQL/receipt/checkpoint no-shadow, post-completion payload erasure, and ordinary permanent control")
}
