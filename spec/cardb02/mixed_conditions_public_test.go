package cardb02_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	workflow "github.com/buildbetter-app/plugs/plugins/workflow"
)

func TestB02PublicFixtureReferenceIdentity(t *testing.T) {
	for _, test := range []struct {
		name, left, right string
		equal             bool
	}{
		{"omitted-and-explicit-zero", `{"id":"ordinary"}`, `{"id":"ordinary","branch":0}`, true},
		{"equivalent-integer-spelling", `{"id":"ordinary","branch":1e0}`, `{"id":"ordinary","branch":1}`, true},
		{"different-branch", `{"id":"ordinary","branch":1}`, `{"id":"ordinary","branch":2}`, false},
		{"empty-id-versus-empty-key", `{"id":""}`, `{"key":""}`, false},
		{"same-text-different-arm", `{"id":"ordinary"}`, `{"key":"ordinary"}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var left, right model.Ref
			if err := json.Unmarshal([]byte(test.left), &left); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(test.right), &right); err != nil {
				t.Fatal(err)
			}
			if sameRef(left, right) != test.equal || sameRef(right, left) != test.equal {
				t.Fatal("readback identity lost the selected arm or canonical branch")
			}
		})
	}
	invalid := model.RefByID("ordinary", model.Natural("invalid"))
	if sameRef(invalid, invalid) || sameRef(invalid, model.RefByID("ordinary", model.Natural("also-invalid"))) {
		t.Fatal("invalid branch spellings must not acquire a shared identity")
	}
}

// The ordinary and workflow routes have different receipt storage. Compare
// the returned commit and the complete state, including both receipt stores,
// on replay; neither route may publish a second graph or history effect.
func (f *blockedFixture) mixedWrite(method string, params map[string]any) {
	t := f.h.T
	t.Helper()
	before := f.h.Capture()
	commit := f.write(method, params)
	label := fmt.Sprintf("b02-%04d-%s", f.seq, method)
	after := f.h.Capture()
	changed := map[string]bool{}
	for _, result := range commit.Results {
		changed[result.Ref.ID] = true
	}
	for _, row := range before.Rows {
		if !changed[row.Ref.ID] && (!reflect.DeepEqual(row, after.Row(t, row.Ref.ID)) || !reflect.DeepEqual(before.Versions[row.Ref.ID], after.Versions[row.Ref.ID])) {
			t.Fatal("mixed mutation rewrote an unchanged durable dependent/body/timestamp/history", row.Ref.ID)
		}
	}
	for label, receipt := range before.Receipts {
		if !reflect.DeepEqual(receipt, after.Receipts[label]) {
			t.Fatal("mixed mutation changed an original core receipt", label)
		}
	}
	oldWorkflow, newWorkflow := before.Workflow(t), after.Workflow(t)
	var oldItems, newItems, oldReceipts, newReceipts map[string]json.RawMessage
	if json.Unmarshal(oldWorkflow["items"], &oldItems) != nil || json.Unmarshal(newWorkflow["items"], &newItems) != nil ||
		json.Unmarshal(oldWorkflow["receipts"], &oldReceipts) != nil || json.Unmarshal(newWorkflow["receipts"], &newReceipts) != nil ||
		oldItems == nil || newItems == nil || oldReceipts == nil || newReceipts == nil || len(oldItems) != len(newItems) {
		t.Fatal("mixed mutation lost the original workflow item/receipt inventory")
	}
	// The request, rather than its returned results or observed poststate,
	// independently identifies the only workflow endpoints allowed to change.
	fields := []string{"id"}
	if method == "bead.link" {
		fields = []string{"from", "to"}
	}
	workflowChanged := map[string]bool{}
	for _, field := range fields {
		ref, ok := params[field].(model.Ref)
		if !ok || !ref.IsID() {
			t.Fatal("mixed mutation requires an authored endpoint reference", field)
		}
		if _, ephemeral := oldItems[ref.ID]; ephemeral {
			workflowChanged[ref.ID] = true
		}
	}
	for id, item := range oldItems {
		next, exists := newItems[id]
		if !exists || (!workflowChanged[id] && !bytes.Equal(item, next)) {
			t.Fatal("mixed mutation rewrote an unaffected workflow item/body/generation", id)
		}
		if workflowChanged[id] {
			var oldItem, newItem struct{ Generation uint64 }
			if json.Unmarshal(item, &oldItem) != nil || json.Unmarshal(next, &newItem) != nil || newItem.Generation != oldItem.Generation+1 {
				t.Fatal("mixed mutation did not advance only its workflow endpoint once", id)
			}
		}
	}
	for key, receipt := range oldReceipts {
		if !bytes.Equal(receipt, newReceipts[key]) {
			t.Fatal("mixed mutation changed an original workflow receipt", key)
		}
	}
	if len(workflowChanged) == 0 {
		if !reflect.DeepEqual(before.Checkpoints[proof.Namespace], after.Checkpoints[proof.Namespace]) {
			t.Fatal("ordinary mutation changed the workflow checkpoint")
		}
	} else {
		if len(workflowChanged) != 1 || len(newReceipts) != len(oldReceipts)+1 {
			t.Fatal("mixed mutation must publish exactly its one workflow receipt")
		}
		for key, raw := range newReceipts {
			if _, retained := oldReceipts[key]; retained {
				continue
			}
			var receipt struct {
				Method, Key, ItemID string
				Result              model.Commit
			}
			if json.Unmarshal(raw, &receipt) != nil || receipt.Method != method || receipt.Key != f.h.Scopes[label].Key || !workflowChanged[receipt.ItemID] || !reflect.DeepEqual(receipt.Result, commit) {
				t.Fatal("new workflow receipt differs from the authored request and exact commit")
			}
		}
		proof.WorkflowNamespaceEffect(t, before, after)
	}
	proof.Save(t, f.h.Root, label+"-initial-workflow-conservation.json", map[string]any{"method": method, "request": params, "before": before.Checkpoints, "after": after.Checkpoints, "workflowEndpointsAllowedToChange": workflowChanged})
	check := func() {
		t.Helper()
		reply := f.h.Call(method, label, params, "")
		var replay model.Commit
		if json.Unmarshal(reply.Result, &replay) != nil || !reflect.DeepEqual(commit, replay) {
			t.Fatal("mixed mutation replay changed the exact original commit")
		}
		proof.Same(t, after, f.h.Capture())
	}
	check()
	if f.h.Config.Driver != "memory" {
		f.h.Reopen()
		proof.Same(t, after, f.h.Capture())
		check()
	}
}

func (f *blockedFixture) mixedLink(from, to, kind string, metadata *model.LinkMetadata) {
	params := map[string]any{"from": f.refs[from], "to": f.refs[to], "kind": kind}
	if metadata != nil {
		params["metadata"] = metadata
	}
	f.mixedWrite("bead.link", params)
}

func (f *blockedFixture) mixedReadback(statuses map[string]string) {
	t := f.h.T
	t.Helper()
	before := f.h.Capture()
	var items map[string]struct {
		ID, Status string
		Generation uint64
	}
	if json.Unmarshal(before.Workflow(t)["items"], &items) != nil || len(statuses) != len(f.refs) {
		t.Fatal("complete authored mixed endpoint inventory required")
	}
	for role, want := range statuses {
		ref, exists := f.refs[role]
		if !exists {
			t.Fatal("unbound expected mixed endpoint", role)
		}
		var got struct {
			Bead struct {
				ID, Project, Status, Kind, Plane string
				Revision                         model.Revision
				Fields                           map[string]any
			}
		}
		reply := f.call("bead.get", map[string]any{"id": ref})
		if json.Unmarshal(reply.Result, &got) != nil || got.Bead.ID != ref.ID || got.Bead.Project != "b07positive" || got.Bead.Status != want || got.Bead.Fields["title"] != "B02 "+role {
			t.Fatalf("%s exact public mixed status/title/project differs: %s", role, reply.Result)
		}
		revision, err := got.Bead.Revision.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		if item, ephemeral := items[ref.ID]; ephemeral {
			if item.ID != ref.ID || item.Status != want || revision != model.Natural64(item.Generation).String() || got.Bead.Kind != workflow.ItemKind || got.Bead.Plane != "ephemeral" {
				t.Fatal("public mixed endpoint differs from its actual workflow checkpoint", role)
			}
		} else {
			row := before.Row(t, ref.ID)
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil || doc.Fields["status"] != want || doc.Fields["title"] != "B02 "+role || revision != row.Revision.String() || got.Bead.Kind != "" || got.Bead.Plane != "" {
				t.Fatal("public mixed endpoint differs from its actual durable row", role, err)
			}
		}
	}
	proof.Same(t, before, f.h.Capture())
}

func (f *blockedFixture) observeMixed(phase string, blocked map[string][]string, statuses map[string]string) {
	f.h.T.Helper()
	f.mixedReadback(statuses)
	f.observe(phase, blocked)
	// observe physically replaces SQL hosts and checks the entire stored
	// image. Verify the same public endpoint values on that replacement too.
	f.mixedReadback(statuses)
	proof.Save(f.h.T, f.h.Root, "b02-statuses-"+phase+".json", statuses)
}

func TestB02PublicBlockedMixedAllAnyAlsoLifecycle(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newBlockedFixture(t, engine)
			for _, role := range []string{"spawner", "durable-child", "durable-all", "durable-any", "durable-also", "durable-any-also"} {
				f.create(role)
			}
			f.invoke("ephemeral-child", "ephemeral-all", "ephemeral-any", "ephemeral-also", "ephemeral-any-also")
			for _, role := range []string{"durable-child", "ephemeral-child"} {
				f.mixedLink(role, "spawner", "parent-child", nil)
			}
			for _, waiter := range []struct {
				role, gate string
				also       bool
			}{
				{"durable-all", "all-children", false}, {"ephemeral-all", "all-children", false},
				{"durable-any", "any-children", false}, {"ephemeral-any", "any-children", false},
				{"durable-also", "all-children", true}, {"ephemeral-also", "all-children", true},
				{"durable-any-also", "any-children", true}, {"ephemeral-any-also", "any-children", true},
			} {
				f.mixedLink(waiter.role, "spawner", "waits-for", &model.LinkMetadata{Gate: waiter.gate, AlsoBlocks: waiter.also})
			}
			statuses := map[string]string{"spawner": "todo", "durable-child": "todo", "durable-all": "todo", "durable-any": "todo", "durable-also": "todo", "durable-any-also": "todo", "ephemeral-child": "open", "ephemeral-all": "open", "ephemeral-any": "open", "ephemeral-also": "open", "ephemeral-any-also": "open"}
			bothOpen := map[string][]string{"durable-all": {"spawner"}, "ephemeral-all": {"spawner"}, "durable-any": {"spawner"}, "ephemeral-any": {"spawner"}, "durable-also": {"spawner"}, "ephemeral-also": {"spawner"}, "durable-any-also": {"spawner"}, "ephemeral-any-also": {"spawner"}}
			oneClosed := map[string][]string{"durable-all": {"spawner"}, "ephemeral-all": {"spawner"}, "durable-also": {"spawner"}, "ephemeral-also": {"spawner"}, "durable-any-also": {"spawner"}, "ephemeral-any-also": {"spawner"}}
			spawnerOpen := map[string][]string{"durable-also": {"spawner"}, "ephemeral-also": {"spawner"}, "durable-any-also": {"spawner"}, "ephemeral-any-also": {"spawner"}}
			f.observeMixed("mixed-gates-both-open", bothOpen, statuses)
			f.mixedWrite("bead.close", map[string]any{"id": f.refs["durable-child"]})
			statuses["durable-child"] = "done"
			f.observeMixed("mixed-gates-one-closed", oneClosed, statuses)
			f.mixedWrite("bead.reopen", map[string]any{"id": f.refs["durable-child"]})
			statuses["durable-child"] = "todo"
			f.observeMixed("mixed-gates-child-reopened", bothOpen, statuses)
			f.mixedWrite("bead.close", map[string]any{"id": f.refs["durable-child"]})
			statuses["durable-child"] = "done"
			f.mixedWrite("bead.close", map[string]any{"id": f.refs["ephemeral-child"]})
			statuses["ephemeral-child"] = "closed"
			f.observeMixed("mixed-gates-children-closed", spawnerOpen, statuses)
			f.mixedWrite("bead.close", map[string]any{"id": f.refs["spawner"]})
			statuses["spawner"] = "done"
			f.observeMixed("mixed-gates-spawner-closed", map[string][]string{}, statuses)
			f.mixedWrite("bead.reopen", map[string]any{"id": f.refs["spawner"]})
			statuses["spawner"] = "todo"
			f.observeMixed("mixed-gates-spawner-reopened", spawnerOpen, statuses)
			f.mixedWrite("bead.reopen", map[string]any{"id": f.refs["durable-child"]})
			statuses["durable-child"] = "todo"
			// The closed workflow child still satisfies any while the reopened
			// durable child makes all live again. Keep this run active throughout.
			f.observeMixed("mixed-gates-durable-child-live-again", oneClosed, statuses)
		})
	}
}

func TestB02PublicBlockedMixedConditionalOutcomes(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newBlockedFixture(t, engine)
			for _, role := range []string{"blocker", "durable-ordinary", "durable-conditional"} {
				f.create(role)
			}
			f.invoke("ephemeral-ordinary", "ephemeral-conditional", "ephemeral-blocker")
			f.mixedLink("ephemeral-ordinary", "blocker", "blocks", nil)
			f.mixedLink("ephemeral-conditional", "blocker", "conditional-blocks", nil)
			f.mixedLink("durable-ordinary", "ephemeral-blocker", "blocks", nil)
			f.mixedLink("durable-conditional", "ephemeral-blocker", "conditional-blocks", nil)
			statuses := map[string]string{"blocker": "todo", "durable-ordinary": "todo", "durable-conditional": "todo", "ephemeral-ordinary": "open", "ephemeral-conditional": "open", "ephemeral-blocker": "open"}
			live := map[string][]string{"ephemeral-ordinary": {"blocker"}, "ephemeral-conditional": {"blocker"}, "durable-ordinary": {"ephemeral-blocker"}, "durable-conditional": {"ephemeral-blocker"}}
			terminalSuccess := map[string][]string{"ephemeral-conditional": {"blocker"}, "durable-ordinary": {"ephemeral-blocker"}, "durable-conditional": {"ephemeral-blocker"}}
			terminalFailure := map[string][]string{"durable-ordinary": {"ephemeral-blocker"}, "durable-conditional": {"ephemeral-blocker"}}
			f.observeMixed("mixed-conditional-live", live, statuses)
			f.mixedWrite("bead.close", map[string]any{"id": f.refs["blocker"]})
			statuses["blocker"] = "done"
			f.observeMixed("mixed-conditional-success-unmet", terminalSuccess, statuses)
			f.mixedWrite("bead.reopen", map[string]any{"id": f.refs["blocker"]})
			statuses["blocker"] = "todo"
			f.observeMixed("mixed-conditional-success-reopened", live, statuses)
			f.mixedWrite("bead.fail", map[string]any{"id": f.refs["blocker"]})
			statuses["blocker"] = "failed"
			f.observeMixed("mixed-conditional-failure-satisfied", terminalFailure, statuses)
			f.mixedWrite("bead.reopen", map[string]any{"id": f.refs["blocker"]})
			statuses["blocker"] = "todo"
			f.observeMixed("mixed-conditional-failure-reopened", live, statuses)
			f.mixedWrite("bead.cancel", map[string]any{"id": f.refs["blocker"]})
			statuses["blocker"] = "canceled"
			f.observeMixed("mixed-conditional-canceled-unmet", terminalSuccess, statuses)
			f.mixedWrite("bead.reopen", map[string]any{"id": f.refs["blocker"]})
			statuses["blocker"] = "todo"
			f.mixedWrite("bead.defer", map[string]any{"id": f.refs["blocker"]})
			statuses["blocker"] = "backlog"
			f.observeMixed("mixed-conditional-backlog-live", live, statuses)
			f.mixedWrite("bead.reopen", map[string]any{"id": f.refs["blocker"]})
			statuses["blocker"] = "todo"
			f.mixedWrite("bead.close", map[string]any{"id": f.refs["ephemeral-blocker"]})
			statuses["ephemeral-blocker"] = "closed"
			// A closed success-only workflow blocker remains in this active run.
			// It satisfies blocks but cannot supply the adopted failure outcome.
			f.observeMixed("mixed-conditional-workflow-success", map[string][]string{"ephemeral-ordinary": {"blocker"}, "ephemeral-conditional": {"blocker"}, "durable-conditional": {"ephemeral-blocker"}}, statuses)
		})
	}
}
