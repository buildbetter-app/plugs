package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These are diagnostic images, not accepted ordinary scheduling mutations.
// Membership is separate from topology: deleting z leaves a's durable outgoing
// fact and y's inbound fact intact, exactly as native dependency rows can outlive
// their source hydration in a damaged disposable database.
type b06BlockingRelation struct {
	From string    `json:"from"`
	To   model.Ref `json:"to"`
	Kind string    `json:"kind"`
}

func b06BlockingCheckpoint(f *b06PlugFixture, label string, change func(map[string]map[string]json.RawMessage)) {
	f.t.Helper()
	store := f.store.(driver.CheckpointStore)
	before, err := store.LoadCheckpoint(context.Background(), b06WorkflowNamespace)
	if err != nil {
		f.t.Fatal(err)
	}
	core := f.image(label + "-core-before")
	var namespace struct {
		Values map[string]struct {
			Revision uint64                     `json:"revision"`
			Value    map[string]json.RawMessage `json:"value"`
		} `json:"values"`
	}
	if err := json.Unmarshal(before.Payload, &namespace); err != nil || len(namespace.Values) != 1 {
		f.t.Fatal("complete actual workflow namespace required", err)
	}
	for key, value := range namespace.Values {
		var items map[string]map[string]json.RawMessage
		if err := json.Unmarshal(value.Value["items"], &items); err != nil || len(items) == 0 {
			f.t.Fatal("actual workflow membership required", err)
		}
		change(items)
		value.Value["items"], err = json.Marshal(items)
		if err != nil {
			f.t.Fatal(err)
		}
		value.Revision++
		namespace.Values[key] = value
	}
	payload, err := json.Marshal(namespace)
	if err != nil {
		f.t.Fatal(err)
	}
	after, err := store.SaveCheckpoint(context.Background(), f.lease, b06WorkflowNamespace, before.Generation, payload)
	if err != nil || after.Generation != before.Generation+1 || !bytes.Equal(after.Payload, payload) {
		f.t.Fatal("diagnostic checkpoint commit differs from exact proposed image", err)
	}
	readback, err := store.LoadCheckpoint(context.Background(), b06WorkflowNamespace)
	if err != nil || !reflect.DeepEqual(after, readback) || !bytes.Equal(core, f.image(label+"-core-after")) {
		f.t.Fatal("diagnostic checkpoint readback/core conservation changed", err)
	}
	b06Retain(f.t, label+"-checkpoint.json", map[string]any{"before": before, "committed": after, "readback": readback, "ordinary_admission": false})
}

func b06BlockingRelations(t *testing.T, item map[string]json.RawMessage, field string, edges ...b06Edge) {
	t.Helper()
	if item == nil {
		t.Fatal("diagnostic workflow item missing")
	}
	var relations []b06BlockingRelation
	if len(item[field]) != 0 {
		if err := json.Unmarshal(item[field], &relations); err != nil {
			t.Fatal(err)
		}
	}
	for _, edge := range edges {
		relations = append(relations, b06BlockingRelation{edge.From, model.RefByID(edge.To, model.Natural64(0)), edge.Kind})
	}
	var err error
	item[field], err = json.Marshal(relations)
	if err != nil {
		t.Fatal(err)
	}
}

func b06BlockingCheck(f *b06PlugFixture, expected [][]string, missing int) {
	f.t.Helper()
	for _, tracks := range []bool{false, true} {
		got := b06MixedReport(f, tracks, 100)
		if !reflect.DeepEqual(b06PlugPaths(got), expected) || got.CycleCount.String() != "2" || got.Truncated || len(got.Items) != 2 {
			f.t.Fatalf("mixed default blocking raw paths/count changed: %+v want%v", got, expected)
		}
		for i, cycle := range got.Items {
			wantMissing := 0
			if i == 0 {
				wantMissing = missing
			}
			if cycle.MemberCount.String() != fmt.Sprint(len(expected[i])) || cycle.UnreadableCount.String() != fmt.Sprint(wantMissing) || cycle.Partial != (wantMissing != 0) {
				f.t.Fatalf("mixed blocking hydration/count changed: %+v", cycle)
			}
			for j, member := range cycle.Path {
				unreadable := expected[i][j] == ""
				if member.Unreadable != unreadable || (member.Ref == nil) != unreadable {
					f.t.Fatalf("missing/hidden member lost position or disclosed identity: %+v", member)
				}
			}
		}
		limited := b06MixedReport(f, tracks, 1)
		if !reflect.DeepEqual(limited.Items, got.Items[:1]) || limited.CycleCount.String() != "2" || !limited.Truncated || !reflect.DeepEqual(limited.AsOf, got.AsOf) {
			f.t.Fatalf("mixed default blocking limit changed count/path: %+v", limited)
		}
	}
	// All cycles cross planes. A durable-only selector must still be empty.
	b06AssertReport(f.t, f.report(false), [][]string{})
}

func b06RunBlockingPlug(t *testing.T, engine string) [][][]string {
	f := b06OpenPlug(t, engine)
	f.by = model.PrincipalRef{Kind: "user", ID: "user:b06-blocking"}
	b06InstallMixed(f)
	f.create("b06-block-request-anchor")
	ids := b06InvokeMixed(f, "block-z", "block-y", "block-v")
	z, y, v := ids[0], ids[1], ids[2]
	f.seedDiagnostic("mixed-default-blocking", []string{"b06-block-c", "b06-block-b", "b06-block-a"}, []b06Edge{
		{"b06-block-b", "b06-block-a", "blocks"},
		{"b06-block-a", z, "blocks"},
		{"b06-block-c", v, "blocks"},
	})
	f.requireEdges("mixed-blocking-durable", []b06Edge{{"b06-block-b", "b06-block-a", "blocks"}, {"b06-block-a", z, "blocks"}, {"b06-block-c", v, "blocks"}})
	b06BlockingCheckpoint(f, "mixed-blocking-topology", func(items map[string]map[string]json.RawMessage) {
		b06BlockingRelations(t, items[z], "inbound", b06Edge{"b06-block-a", z, "blocks"})
		b06BlockingRelations(t, items[z], "links", b06Edge{z, y, "blocks"})
		b06BlockingRelations(t, items[y], "inbound", b06Edge{z, y, "blocks"})
		b06BlockingRelations(t, items[y], "links", b06Edge{y, "b06-block-b", "blocks"})
		b06BlockingRelations(t, items[v], "inbound", b06Edge{"b06-block-c", v, "blocks"})
		b06BlockingRelations(t, items[v], "links", b06Edge{v, "b06-block-c", "blocks"})
	})
	want := [][]string{{"b06-block-a", z, y, "b06-block-b"}, {"b06-block-c", v}}
	b06BlockingCheck(f, want, 0)
	// Two more real facts with the same endpoints must not multiply paths.
	b06MixedParallel(f, z, "b06-block-a")
	b06BlockingCheck(f, want, 0)
	before := b06MixedImage(f, "blocking-complete-before-reopen")
	f.reopen()
	b06InstallMixed(f)
	if !bytes.Equal(before, b06MixedImage(f, "blocking-complete-after-reopen")) {
		t.Fatal("complete mixed blocking SQL reopen lost exact committed state")
	}
	b06BlockingCheck(f, want, 0)
	b06BlockingCheckpoint(f, "mixed-blocking-missing-hydration", func(items map[string]map[string]json.RawMessage) {
		if _, exists := items[z]; !exists {
			t.Fatal("workflow hydration was already absent")
		}
		delete(items, z)
	})
	partial := [][]string{{"b06-block-a", "", y, "b06-block-b"}, {"b06-block-c", v}}
	b06BlockingCheck(f, partial, 1)
	before = b06MixedImage(f, "blocking-partial-before-reopen")
	f.reopen()
	b06InstallMixed(f)
	if !bytes.Equal(before, b06MixedImage(f, "blocking-partial-after-reopen")) {
		t.Fatal("partial mixed blocking SQL reopen lost exact committed state")
	}
	b06BlockingCheck(f, partial, 1)
	b06Retain(t, "mixed-blocking-public.json", map[string]any{"engine": engine, "workflow_ids_in_label_order": ids, "raw_complete_paths": want, "raw_partial_paths": partial, "absent_hydration": z, "retained_source_facts": "durable a outgoing and extant y inbound", "sql_reopen": engine != "memory", "native_history": "UNQUALIFIED"})
	return [][][]string{want, partial}
}
