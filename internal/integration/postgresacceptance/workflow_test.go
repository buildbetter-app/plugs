package postgresacceptance

import (
	"bytes"
	"testing"

	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func TestB26EphemeralWorkflowPostgresHTTPReopenReplayAndCleanup(t *testing.T) {
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs()
	h := startWorkflowHost(t, openStore(t, dsn), sequence, trace)
	invokeKey := key("b26-workflow-invoke")
	params := map[string]any{
		"template":  "postgres-workflow",
		"project":   project,
		"mode":      "ephemeral",
		"checklist": []map[string]any{{"label": "step", "title": "private workflow item", "type": "task"}},
	}
	invoked := h.call(t, "x.workflow.invoke", params, invokeKey)
	if invoked.Error != nil {
		t.Fatalf("ephemeral invocation failed: %v", invoked.Error)
	}
	initial := decode[workflowplugin.InvokeResult](t, invoked.Result)
	if initial.Plane != "ephemeral" || initial.Run == nil || len(initial.Items) != 1 || initial.Items[0].Plane != "ephemeral" {
		t.Fatalf("unexpected PostgreSQL workflow invocation: %+v", initial)
	}
	itemID := initial.Items[0].ID
	comment := h.call(t, "bead.comment", map[string]any{
		"id":   map[string]any{"id": itemID},
		"body": "private workflow comment",
	}, key("b26-workflow-comment"))
	if comment.Error != nil {
		t.Fatalf("workflow comment failed: %v", comment.Error)
	}
	ready := h.call(t, "bead.ready", map[string]any{"limit": 10}, key("b26-workflow-ready"))
	if ready.Error != nil {
		t.Fatalf("workflow ready failed: %v", ready.Error)
	}
	if !bytes.Contains(ready.Result, []byte(itemID)) {
		t.Fatalf("workflow item was not visible to ready: %s", ready.Result)
	}

	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startWorkflowHost(t, openStore(t, dsn), sequence, trace)
	reopened := h.call(t, "bead.get", map[string]any{"id": map[string]any{"id": itemID}}, key("b26-workflow-get"))
	if reopened.Error != nil || !bytes.Contains(reopened.Result, []byte("private workflow comment")) {
		t.Fatalf("PostgreSQL reopen lost workflow comment: %+v", reopened)
	}
	replayed := h.call(t, "x.workflow.invoke", params, invokeKey)
	if replayed.Error != nil || !replayed.replayed() || !bytes.Equal(replayed.Result, invoked.Result) {
		t.Fatalf("workflow invocation replay changed after reopen: first=%s replay=%s err=%v replayed=%v", invoked.Result, replayed.Result, replayed.Error, replayed.replayed())
	}
	closed := h.call(t, "bead.close", map[string]any{"id": map[string]any{"id": itemID}, "reason": "done"}, key("b26-workflow-close"))
	if closed.Error != nil {
		t.Fatalf("workflow close failed: %v", closed.Error)
	}
	notFound := h.call(t, "bead.get", map[string]any{"id": map[string]any{"id": itemID}}, key("b26-workflow-missing"))
	refused(t, notFound, "not_found")
	readyAfter := h.call(t, "bead.ready", map[string]any{"limit": 10}, key("b26-workflow-ready-after"))
	if readyAfter.Error != nil || bytes.Contains(readyAfter.Result, []byte(itemID)) {
		t.Fatalf("completed workflow remained ready: %+v", readyAfter)
	}
	terminal := h.call(t, "x.workflow.invoke", params, invokeKey)
	if terminal.Error != nil || !terminal.replayed() || bytes.Contains(terminal.Result, []byte("private workflow item")) || bytes.Contains(terminal.Result, []byte("private workflow comment")) {
		t.Fatalf("terminal replay retained workflow payload: %+v", terminal)
	}
	t.Log("B26: real PostgreSQL HTTP workflow invocation, standard-tool continuation, close/reopen replay, and terminal cleanup")
}
