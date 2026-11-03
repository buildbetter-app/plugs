package composition

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

func TestMemoryRuntimeKeepsOrdinaryWorkflowToolsAndRefusesEphemeral(t *testing.T) {
	f := startRuntime(t, Config{Driver: "memory", Project: "workflow-memory", Address: DefaultAddress})
	ctx := context.Background()
	params := map[string]any{
		"template": "template", "project": "workflow-memory", "mode": "permanent",
		"checklist": []map[string]any{{"label": "one", "title": "ordinary durable item", "type": "task"}},
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
