package cardb20

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	public "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflow "github.com/buildbetter-app/plugs/plugins/workflow"
)

// This uses the ordinary composition root, including its actual default
// provider. The older gatecontract fixture deliberately omits that provider
// on memory and is not evidence for this process-lifetime positive route.
func b14MixedPublic(t *testing.T, engine string) {
	t.Helper()
	h := public.NewHost(t, engine)
	capability, err := driver.RequireWorkflowCheckpointStore(h.Runtime.Store, h.Runtime.Lease)
	if err != nil {
		t.Fatal(err)
	}
	// The selected memory Store deliberately has no restart-persistence
	// marker. Ask the production workflow-state adapter bound to that exact
	// capability/lease for its actual Persistent() result, without installing
	// another provider or writing a checkpoint.
	state, err := pluginhost.NewWorkflowRuntimeState(capability, h.Runtime.Lease)
	if err != nil {
		t.Fatal(err)
	}
	persistence, ok := state.(interface{ Persistent() bool })
	if !ok {
		t.Fatal("selected workflow checkpoint has no explicit persistence marker")
	}
	wantLifetime := driver.CheckpointLifetimeRestart
	if engine == "memory" {
		wantLifetime = driver.CheckpointLifetimeProcess
	}
	if capability.WorkflowCheckpointLifetime() != wantLifetime || persistence.Persistent() != (engine != "memory") {
		t.Fatal("selected provider misstates its actual checkpoint lifetime")
	}
	proof.Save(t, h.Root, "b14-mixed-lifetime.json", map[string]any{"engine": engine, "lifetime": capability.WorkflowCheckpointLifetime(), "persistent": persistence.Persistent(), "physicalReopen": engine != "memory"})
	seq := 0
	call := func(method string, params map[string]any, code string) public.Reply {
		seq++
		return h.Call(method, fmt.Sprintf("b14-mixed-%d", seq), params, code)
	}
	create := func(typ, title string, fields map[string]any, parent *model.Ref) model.Ref {
		values := map[string]any{"title": title}
		for key, value := range fields {
			values[key] = value
		}
		params := map[string]any{"type": typ, "fields": values}
		if parent != nil {
			params["parent"] = parent
		}
		commit := b14Result[model.Commit](t, call("bead.create", params, ""))
		if len(commit.Results) != 1 {
			t.Fatal("durable create did not commit exactly one image")
		}
		return commit.Results[0].Ref
	}
	get := func(ref model.Ref) map[string]any {
		return b14Result[map[string]any](t, call("bead.get", map[string]any{"id": ref}, ""))["bead"].(map[string]any)
	}
	invoke := func(label string, checklist []any) []model.Ref {
		invoked := b14Result[workflow.InvokeResult](t, call("x.workflow.invoke", map[string]any{"template": label, "project": h.Config.Project, "mode": "ephemeral", "checklist": checklist}, ""))
		if invoked.Plane != "ephemeral" || invoked.Run == nil || len(invoked.Items) != len(checklist) {
			t.Fatal("default invocation lost the actual workflow run/items")
		}
		refs := []model.Ref{}
		for _, item := range invoked.Items {
			if item.Kind != workflow.ItemKind || item.Plugin != workflow.Alias || item.Project != h.Config.Project || item.Plane != "ephemeral" || item.RunID != invoked.Run.ID {
				t.Fatal("invocation did not use the selected default workflow provider")
			}
			refs = append(refs, model.RefByID(item.ID, model.Natural64(0)))
		}
		return refs
	}
	durable := create("gate", "durable-gate", map[string]any{"priority": 3, "await_type": "human", "await_id": "opaque", "waiters": []string{"old", "", "old"}}, nil)
	template := create("gate", "template", map[string]any{"priority": 0, "template": true}, nil)
	spawner := create("issue", "durable-spawner", nil, nil)
	durableWaiters := []model.Ref{}
	for _, predicate := range b14Predicates {
		durableWaiters = append(durableWaiters, create("issue", "durable-"+predicate.Name, nil, nil))
	}
	checklist := []any{
		map[string]any{"label": "high", "title": "active-gate", "type": "gate", "fields": map[string]any{"priority": 1, "await_type": "gh:run", "await_id": "opaque", "waiters": []string{"old", "", "old"}}},
		map[string]any{"label": "closed", "title": "closed-gate", "type": "gate", "fields": map[string]any{"priority": 2}},
		map[string]any{"label": "low", "title": "low-gate", "type": "gate", "fields": map[string]any{"priority": 4}},
		map[string]any{"label": "spawner", "title": "workflow-spawner", "type": "issue"},
		map[string]any{"label": "ordinary", "title": "await-does-not-select-gate", "type": "issue", "fields": map[string]any{"await_type": "human", "priority": 0}},
	}
	for _, predicate := range b14Predicates {
		checklist = append(checklist, map[string]any{"label": predicate.Name, "title": "workflow-" + predicate.Name, "type": "issue"})
	}
	active := invoke("b14-mixed", checklist)
	workflowWaiters := active[5:]
	workflowSpawner := active[3]
	call("bead.close", map[string]any{"id": active[1]}, "")
	waiters := []string{"next", "next", ""}
	old := get(durable)
	change := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: durable, Fields: map[string]any{"waiters": waiters}}},
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: active[0], Fields: map[string]any{"waiters": waiters}}},
	}}
	for i, predicate := range b14Predicates {
		metadata := &model.LinkMetadata{Gate: predicate.Gate, AlsoBlocks: predicate.Also}
		change.Ops = append(change.Ops,
			model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: durableWaiters[i], To: workflowSpawner, LinkKind: "waits-for", Metadata: metadata}},
			model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: workflowWaiters[i], To: spawner, LinkKind: "waits-for", Metadata: metadata}},
		)
	}
	committed := b14Result[model.Commit](t, call("tx.apply", map[string]any{"changeSet": change}, ""))
	if len(committed.Results) != len(change.Ops) || committed.Results[0].Ref != durable || committed.Results[1].Ref != active[0] {
		t.Fatal("mixed commit lost operation order or exact durable/workflow identities")
	}
	for _, ref := range []model.Ref{durable, active[0]} {
		bead := get(ref)
		fields := bead["fields"].(map[string]any)
		if bead["type"] != "gate" || fields["await_id"] != "opaque" || !reflect.DeepEqual(fields["waiters"], []any{"next", "next", ""}) {
			t.Fatal("mixed committed readback lost exact gate members", bead)
		}
	}
	historical := b14Result[map[string]any](t, call("history.get", map[string]any{"ref": durable, "asOf": map[string]any{"revision": old["revision"]}}, ""))["body"]
	if !reflect.DeepEqual(historical.(map[string]any)["fields"].(map[string]any)["waiters"], []any{"old", "", "old"}) {
		t.Fatal("mixed waiter assignment rewrote the old durable history image")
	}
	listCheck := func() {
		t.Helper()
		before := h.Capture()
		for _, test := range []struct {
			Params map[string]any
			Want   []string
		}{
			{map[string]any{}, []string{template.ID, active[0].ID, durable.ID, active[2].ID}},
			{map[string]any{"all": true}, []string{template.ID, active[0].ID, active[1].ID, durable.ID, active[2].ID}},
			{map[string]any{"excludeTemplates": true, "limit": 1}, []string{active[0].ID}},
			{map[string]any{"excludeTemplates": true, "limit": 1, "awaitType": "human"}, []string{}},
		} {
			page := b14Result[struct{ Items []struct{ ID string } }](t, call("gate.list", test.Params, ""))
			ids := []string{}
			for _, item := range page.Items {
				ids = append(ids, item.ID)
			}
			if !reflect.DeepEqual(ids, test.Want) {
				t.Fatalf("mixed gate filter/order=%v want=%v", ids, test.Want)
			}
		}
		proof.Same(t, before, h.Capture())
	}
	listCheck()
	check := func(stage int) {
		t.Helper()
		page := b14Result[struct{ Items []struct{ ID string } }](t, call("bead.ready", map[string]any{"limit": 1000, "filter": map[string]any{"includeEphemeral": true}}, ""))
		ready := map[string]bool{}
		for _, item := range page.Items {
			ready[item.ID] = true
		}
		for _, plane := range [][]model.Ref{durableWaiters, workflowWaiters} {
			got := []bool{}
			for i, ref := range plane {
				got = append(got, ready[ref.ID])
				if !b14Stages[stage].Ready[i] {
					before := h.Capture()
					call("bead.close", map[string]any{"id": ref}, "ontology_guard_failed")
					proof.Same(t, before, h.Capture())
				}
			}
			if !reflect.DeepEqual(got, b14Stages[stage].Ready) {
				t.Fatalf("mixed %s readiness=%v want=%v", b14Stages[stage].Name, got, b14Stages[stage].Ready)
			}
		}
	}
	check(0)
	durableChildren := []model.Ref{create("issue", "child-one", nil, &spawner), create("issue", "child-two", nil, &spawner)}
	children := invoke("b14-mixed-children", []any{
		map[string]any{"label": "one", "title": "child-one", "type": "issue", "parent": workflowSpawner},
		map[string]any{"label": "two", "title": "child-two", "type": "issue", "parent": workflowSpawner},
		map[string]any{"label": "anchor", "title": "retain-closed-children", "type": "issue"},
	})
	check(1)
	call("bead.close", map[string]any{"id": durableChildren[0]}, "")
	call("bead.close", map[string]any{"id": children[0]}, "")
	check(2)
	beforeReopen := h.Capture()
	if engine != "memory" {
		h.Reopen()
		proof.Same(t, beforeReopen, h.Capture())
	}
	listCheck()
	check(2)
	if got := b14Result[map[string]any](t, call("history.get", map[string]any{"ref": durable, "asOf": map[string]any{"revision": old["revision"]}}, ""))["body"]; !reflect.DeepEqual(got, historical) {
		t.Fatal("SQL reopen changed historical mixed-batch waiter values")
	}
	for _, ref := range []model.Ref{durableChildren[1], children[1]} {
		call("bead.close", map[string]any{"id": ref}, "")
	}
	check(3)
	for _, ref := range []model.Ref{spawner, workflowSpawner} {
		call("bead.close", map[string]any{"id": ref}, "")
	}
	check(4)
	for _, plane := range []struct {
		Refs                        []model.Ref
		Status, Kind, Plane, Plugin string
	}{
		{Refs: durableWaiters, Status: "done"},
		{Refs: workflowWaiters, Status: "closed", Kind: workflow.ItemKind, Plane: "ephemeral", Plugin: workflow.Alias},
	} {
		for _, ref := range plane.Refs {
			call("bead.close", map[string]any{"id": ref}, "")
			bead := get(ref)
			kind, _ := bead["kind"].(string)
			actualPlane, _ := bead["plane"].(string)
			plugin, _ := bead["plugin"].(string)
			if bead["status"] != plane.Status || kind != plane.Kind || actualPlane != plane.Plane || plugin != plane.Plugin || bead["type"] != "issue" || bead["id"] != ref.ID {
				t.Fatal("admitted mixed waits-for close did not commit its exact plane status")
			}
		}
	}
	// Record the complete original checkpoint/SQL images as well as public
	// request/responses. Memory has only this process-lifetime terminal image.
	proof.Save(t, h.Root, "b14-mixed-terminal.json", h.Capture())
}
