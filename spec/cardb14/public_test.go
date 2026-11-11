package cardb20

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	public "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
)

func b14Result[T any](t *testing.T, reply public.Reply) T {
	t.Helper()
	var result T
	if reply.Error != nil || json.Unmarshal(reply.Result, &result) != nil {
		t.Fatalf("invalid public result: %s %+v", reply.Result, reply.Error)
	}
	return result
}

func b14Public(t *testing.T, engine string) b14Projection {
	t.Helper()
	h := public.NewHost(t, engine)
	sequence := 0
	call := func(method string, params map[string]any, code string) public.Reply {
		sequence++
		return h.Call(method, fmt.Sprintf("b14-%s-%d", method, sequence), params, code)
	}
	create := func(typ, title string, fields map[string]any, parent *model.Ref) model.Ref {
		t.Helper()
		values := map[string]any{"title": title}
		for k, v := range fields {
			values[k] = v
		}
		params := map[string]any{"type": typ, "fields": values}
		if parent != nil {
			params["parent"] = parent
		}
		commit := b14Result[model.Commit](t, call("bead.create", params, ""))
		if len(commit.Results) != 1 {
			t.Fatal("create did not return one committed image")
		}
		return commit.Results[0].Ref
	}
	get := func(ref model.Ref) map[string]any {
		return b14Result[map[string]any](t, call("bead.get", map[string]any{"id": ref}, ""))["bead"].(map[string]any)
	}
	transition := func(method string, ref model.Ref) {
		b14Result[model.Commit](t, call(method, map[string]any{"id": ref}, ""))
	}
	// No seed, catalog plan, source importer or alternate runtime installs a
	// gate here. The actual composition root selects its ordinary default.
	// Discovery is not bound in this ordinary host. Keep that actual result
	// distinct from successful default gate create/get/list behavior below.
	call("ontology.schema", map[string]any{"project": h.Config.Project, "type": "gate"}, "method_not_found")
	bare := create("gate", "bare", map[string]any{"priority": 3}, nil)
	template := create("gate", "template", map[string]any{"priority": 0, "template": true, "await_type": "human"}, nil)
	timer := create("gate", "timer", map[string]any{"priority": 1, "await_type": "timer", "timeout": 0}, nil)
	human := create("gate", "human", map[string]any{"priority": 2, "await_type": "human", "await_id": "opaque", "waiters": []string{"", "b", "a", "b"}}, nil)
	closed := create("gate", "closed", map[string]any{"priority": 0, "await_type": "human"}, nil)
	transition("bead.close", closed)
	create("issue", "ordinary-await", map[string]any{"priority": 0, "await_type": "human"}, nil)
	names := map[string]string{bare.ID: "bare", template.ID: "template", timer.ID: "timer", human.ID: "human", closed.ID: "closed"}
	list := func(params map[string]any) []string {
		t.Helper()
		result := b14Result[struct{ Items []struct{ ID string } }](t, call("gate.list", params, ""))
		out := []string{}
		for _, item := range result.Items {
			name, ok := names[item.ID]
			if !ok {
				t.Fatalf("gate listing admitted non-gate %s", item.ID)
			}
			out = append(out, name)
		}
		return out
	}
	sorted := func(values []string) []string { sort.Strings(values); return values }
	p := b14Projection{DefaultType: get(bare)["type"].(string), Readiness: map[string][]bool{}}
	p.BareAwait, _ = get(bare)["fields"].(map[string]any)["await_type"].(string)
	p.DefaultList = sorted(list(map[string]any{}))
	p.AllList = sorted(list(map[string]any{"all": true}))
	p.CheckLimit = list(map[string]any{"excludeTemplates": true, "limit": 1})
	p.AwaitLimit = list(map[string]any{"excludeTemplates": true, "limit": 1, "awaitType": "human"})
	if p.DefaultType != "gate" || p.BareAwait != "" || !reflect.DeepEqual(p.DefaultList, []string{"bare", "human", "template", "timer"}) || !reflect.DeepEqual(p.AllList, []string{"bare", "closed", "human", "template", "timer"}) || !reflect.DeepEqual(p.CheckLimit, []string{"timer"}) || len(p.AwaitLimit) != 0 {
		t.Fatalf("default gate selection: %+v", p)
	}
	before := get(human)
	oldRevision := before["revision"]
	commit := b14Result[model.Commit](t, call("bead.update", map[string]any{"id": human, "fields": map[string]any{"waiters": []string{"next", "next", ""}}}, ""))
	if len(commit.Results) != 1 || commit.Results[0].Ref.ID != human.ID {
		t.Fatal("whole-list update lost the committed gate")
	}
	old := b14Result[map[string]any](t, call("history.get", map[string]any{"ref": human, "asOf": map[string]any{"revision": oldRevision}}, ""))["body"].(map[string]any)
	if !reflect.DeepEqual(old["fields"].(map[string]any)["waiters"], []any{"", "b", "a", "b"}) {
		t.Fatal("waiter replacement rewrote the old public history image")
	}
	wantCurrent := get(human)
	fields := wantCurrent["fields"].(map[string]any)
	if !reflect.DeepEqual(fields["waiters"], []any{"next", "next", ""}) || fields["await_id"] != "opaque" || fields["await_type"] != "human" {
		t.Fatalf("gate member readback lost exact ordered values: %v", fields)
	}
	p.Waiters = []string{"next", "next", ""}
	// Independently verify actual version rows and their canonical stored body
	// against the public response; this is physical storage readback, not a seed.
	state := h.Capture()
	found := false
	for _, row := range state.Rows {
		if row.Ref.ID == human.ID {
			found = true
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil || doc.Type != "gate" || !reflect.DeepEqual(doc.Fields["waiters"], []any{"next", "next", ""}) {
				t.Fatalf("committed gate body differs: %s %v", row.Body, err)
			}
		}
	}
	if !found {
		t.Fatal("committed gate has no physical stored row")
	}
	spawner := create("issue", "spawner", nil, nil)
	waiters := []model.Ref{}
	for _, predicate := range b14Predicates {
		waiter := create("issue", predicate.Name, nil, nil)
		metadata := map[string]any{}
		if predicate.Gate != "" {
			metadata["gate"] = predicate.Gate
		}
		if predicate.Also {
			metadata["also_blocks"] = true
		}
		call("bead.link", map[string]any{"from": waiter, "to": spawner, "kind": "waits-for", "metadata": metadata}, "")
		waiters = append(waiters, waiter)
	}
	check := func(stage int) {
		t.Helper()
		result := b14Result[struct{ Items []struct{ ID string } }](t, call("bead.ready", map[string]any{"limit": 1000}, ""))
		present := map[string]bool{}
		for _, item := range result.Items {
			present[item.ID] = true
		}
		got := []bool{}
		for _, waiter := range waiters {
			got = append(got, present[waiter.ID])
		}
		if !reflect.DeepEqual(got, b14Stages[stage].Ready) {
			t.Fatalf("%s ready=%v want=%v", b14Stages[stage].Name, got, b14Stages[stage].Ready)
		}
		p.Readiness[b14Stages[stage].Name] = got
		for i, waiter := range waiters {
			if got[i] {
				transition("bead.close", waiter)
				if get(waiter)["status"] != "done" {
					t.Fatal("admitted close did not commit the installed close-slot status")
				}
				transition("bead.reopen", waiter)
			} else {
				before := h.Capture()
				call("bead.close", map[string]any{"id": waiter}, "ontology_guard_failed")
				after := h.Capture()
				if !reflect.DeepEqual(before.Meta, after.Meta) || !reflect.DeepEqual(before.Rows, after.Rows) || !reflect.DeepEqual(before.Versions, after.Versions) || !reflect.DeepEqual(before.Changes, after.Changes) {
					t.Fatal("refused waits-for close changed committed state/history")
				}
			}
		}
	}
	check(0)
	first := create("issue", "first-child", nil, &spawner)
	second := create("issue", "second-child", nil, &spawner)
	check(1)
	transition("bead.close", first)
	check(2)
	// Equal typed metadata retains both the stored version and its predicate.
	// A changed predicate must immediately change ready/close behavior.
	all := waiters[1]
	allBefore := get(all)
	allRevision := allBefore["revision"]
	beforeEqual := h.Capture()
	call("bead.link", map[string]any{"from": all, "to": spawner, "kind": "waits-for", "metadata": json.RawMessage(`{ "also_blocks":false, "gate":"all-children" }`)}, "")
	afterEqual := h.Capture()
	if !reflect.DeepEqual(beforeEqual.Meta, afterEqual.Meta) || !reflect.DeepEqual(beforeEqual.Rows, afterEqual.Rows) || !reflect.DeepEqual(beforeEqual.Versions, afterEqual.Versions) || !reflect.DeepEqual(beforeEqual.Changes, afterEqual.Changes) {
		t.Fatal("equal typed waits-for metadata wrote a new value/history")
	}
	call("bead.link", map[string]any{"from": all, "to": spawner, "kind": "waits-for", "metadata": map[string]any{"gate": "any-children"}}, "")
	transition("bead.close", all)
	transition("bead.reopen", all)
	oldAll := b14Result[map[string]any](t, call("history.get", map[string]any{"ref": all, "asOf": map[string]any{"revision": allRevision}}, ""))["body"].(map[string]any)
	links := oldAll["links"].([]any)
	if len(links) != 1 || links[0].(map[string]any)["metadata"].(map[string]any)["gate"] != "all-children" {
		t.Fatal("changed predicate rewrote the original all-children history")
	}
	call("bead.link", map[string]any{"from": all, "to": spawner, "kind": "waits-for", "metadata": map[string]any{"gate": "all-children"}}, "")
	call("bead.close", map[string]any{"id": all}, "ontology_guard_failed")
	// Reopen each real SQL store without reseeding catalog, gate, predicate,
	// waiter members or history. Memory retains its same actual store.
	h.Reopen()
	if !reflect.DeepEqual(wantCurrent, get(human)) {
		t.Fatal("physical reopen changed the exact public gate")
	}
	oldReopened := b14Result[map[string]any](t, call("history.get", map[string]any{"ref": human, "asOf": map[string]any{"revision": oldRevision}}, ""))["body"]
	if !reflect.DeepEqual(old, oldReopened) || !reflect.DeepEqual(p.DefaultList, sorted(list(map[string]any{}))) {
		t.Fatal("reopen changed historical waiter image or default gate listing")
	}
	if reopened := b14Result[map[string]any](t, call("history.get", map[string]any{"ref": all, "asOf": map[string]any{"revision": allRevision}}, ""))["body"]; !reflect.DeepEqual(oldAll, reopened) {
		t.Fatal("SQL reopen changed the old waits-for predicate image")
	}
	check(2)
	transition("bead.close", second)
	check(3)
	transition("bead.close", spawner)
	check(4)
	// Reopening children of the closed spawner demonstrates that removing
	// the also_blocks OR does not remove the independent child predicate.
	transition("bead.reopen", first)
	check(5)
	transition("bead.reopen", second)
	check(6)
	return p
}
