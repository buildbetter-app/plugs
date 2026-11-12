package cardb20

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	publicproof "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflow "github.com/buildbetter-app/plugs/plugins/workflow"
)

func TestB23ReadyCountMixedPlanesCurrentPublicHost(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			h := publicproof.NewHost(t, engine)
			durable := h.Create("b23-ready-durable")
			raw := h.Call("x.workflow.invoke", "b23-ready-invoke", map[string]any{"template": "b23-ready", "project": "b07positive", "mode": "ephemeral", "checklist": []any{map[string]any{"label": "next", "title": "b23 workflow ready"}}}, "")
			var invoked workflow.InvokeResult
			if err := json.Unmarshal(raw.Result, &invoked); err != nil || len(invoked.Items) != 1 {
				t.Fatal("actual workflow invocation missing", err)
			}
			item := invoked.Items[0]
			capability, err := driver.RequireWorkflowCheckpointStore(h.Runtime.Store, h.Runtime.Lease)
			if err != nil {
				t.Fatal(err)
			}
			if engine == "memory" {
				if capability.WorkflowCheckpointLifetime() != driver.CheckpointLifetimeProcess {
					t.Fatal("memory did not report process lifetime")
				}
				// The optional stronger marker promises restart durability. Its
				// absence does not negate the admitted process checkpoint.
				if p, ok := h.Runtime.Store.(driver.PersistentCheckpointStore); ok && p.Persistent() {
					t.Fatal("memory persistence marker is not truthfully false")
				}
			}
			count := func(label string, include bool, want int) {
				t.Helper()
				for _, limit := range []int{1, 1000} {
					before := h.Capture()
					meta, err := h.Runtime.Store.Meta(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					reply := h.Call("bead.ready", fmt.Sprintf("%s-%d", label, limit), map[string]any{"countOnly": true, "limit": limit, "filter": map[string]any{"resources": []string{durable.ID, item.ID}, "includeEphemeral": include}}, "")
					var body map[string]json.RawMessage
					if json.Unmarshal(reply.Result, &body) != nil {
						t.Fatal("ready result invalid")
					}
					var total int
					if json.Unmarshal(body["count"], &total) != nil || total != want || string(body["items"]) != "[]" {
						t.Fatalf("mixed count=%s want=%d", reply.Result, want)
					}
					var at viewops.AsOf
					if err := json.Unmarshal(body["asOf"], &at); err != nil || at.Revision != meta.Head || at.Incarnation != meta.Incarnation {
						t.Fatalf("mixed count snapshot=%s want=%+v: %v", reply.Result, meta, err)
					}
					if _, ok := body["nextCursor"]; ok {
						t.Fatal("mixed count emitted cursor")
					}
					proof.Same(t, before, h.Capture())
				}
			}
			count("both-planes", true, 2)
			count("durable-only", false, 1)
			// The same dependency lifecycle must feed listing and count. The core
			// target is blocked by the actual workflow item until public close.
			h.Call("bead.link", "b23-ready-mixed-link", map[string]any{"from": durable, "to": model.RefByID(item.ID, model.Natural64(0)), "kind": "blocks"}, "")
			count("workflow-blocker", true, 1)
			count("hidden-workflow-plane-blocker", false, 0)
			h.Call("bead.close", "b23-ready-close-workflow", map[string]any{"id": model.RefByID(item.ID, model.Natural64(0))}, "")
			count("workflow-blocker-closed", true, 1)
			original := h.Runtime.Store
			h.Reopen()
			if engine == "memory" && !reflect.DeepEqual(original, h.Runtime.Store) {
				t.Fatal("memory used a replacement store")
			}
			count("same-store-or-sql-reopen", true, 1)
			if _, err := h.Runtime.Store.Meta(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestB23ReadyCountTraversesPastInternalThousandPage(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := open(t, engine)
			for batch := 0; batch < 3; batch++ {
				n := 500
				if batch == 2 {
					n = 5
				}
				ops := make([]model.Op, 0, n)
				for i := 0; i < n; i++ {
					ops = append(ops, model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": fmt.Sprintf("ready-count-%d-%d", batch, i)}}})
				}
				var commit model.Commit
				if err := json.Unmarshal(f.call("tx.apply", fmt.Sprintf("ready-batch-%d", batch), map[string]any{"changeSet": model.ChangeSet{Ops: ops}}), &commit); err != nil || len(commit.Results) != n {
					t.Fatal("public ready corpus was not fully committed", err)
				}
			}
			readyCount(t, f, "complete-1005", map[string]any{}, 1005)
			f.reopen()
			readyCount(t, f, "reopen-complete-1005", map[string]any{}, 1005)
		})
	}
}
