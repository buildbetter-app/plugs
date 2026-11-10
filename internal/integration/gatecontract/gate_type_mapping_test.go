package gatecontract_test

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

func TestB14PublicGateTypeInstallMapTypePreservesLiteralMembers(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		initial := ontology.DefaultDocument()
		delete(initial.Types, ontology.GateType)
		initial.Types["legacy_gate"] = initial.Types["issue"]
		seedGateCatalog(t, f, initial)
		f.refuse("bead.create", map[string]any{"type": "gate", "fields": map[string]any{"title": "not installed"}}, "invalid_params")
		id := gateCreate(f, "legacy_gate", "gh:run", map[string]any{"await_id": "opaque", "timeout": 0, "waiters": []string{"", "b", "a", "b"}})
		before := f.currentBody(id)
		candidate := ontology.DefaultDocument()
		candidate.Types["legacy_gate"] = initial.Types["legacy_gate"]
		plan := gatePlan(f, candidate, []any{typeMapping("legacy_gate", "gate", id)}, 1)
		migration := plan["changes"].(map[string]any)["migration"].(map[string]any)
		if migration["affected"] != float64(1) || !reflect.DeepEqual(migration["directives"], decodedJSON(t, []any{typeMapping("legacy_gate", "gate", id)})) {
			t.Fatalf("retained map_type report: %v", migration)
		}
		gateApply(f, plan, 1)
		after := f.currentBody(id)
		if after["type"] != "gate" || after["id"] != id || after["project"] != before["project"] || after["status"] != before["status"] || !reflect.DeepEqual(after["fields"], before["fields"]) {
			t.Fatalf("type mapping lost literal record members: %v -> %v", before, after)
		}
		created := gateCreate(f, "gate", "timer", nil)
		check := func() {
			t.Helper()
			got := gateIDs(f.call("gate.list", map[string]any{}, false, ""))
			if !reflect.DeepEqual(got, []string{id, created}) {
				t.Fatalf("installed gate listing: %v", got)
			}
			schema := f.call("ontology.schema", map[string]any{"type": "gate"}, false, "")
			if schema["properties"] == nil {
				t.Fatalf("installed gate schema absent: %v", schema)
			}
			if f.bead(id)["status"] != before["status"] || !reflect.DeepEqual(f.currentBody(id)["fields"], before["fields"]) {
				t.Fatal("durable map_type readback drift")
			}
		}
		check()
		if f.engine != "memory" {
			f.close()
			f.open()
			bindGateHost(t, f, f.store, true, false)
			check()
		}
	})
}

func TestB14PublicGateTypeMappingsRefuseWithoutWrites(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		initial := ontology.DefaultDocument()
		initial.Types["legacy_gate"] = initial.Types["issue"]
		seedGateCatalog(t, f, initial)
		id := gateCreate(f, "legacy_gate", "opaque", map[string]any{"waiters": []string{"preserve"}})
		before, history, meta := f.currentBody(id), f.historyCount(), f.meta()
		for _, test := range []struct {
			name      string
			mappings  []any
			candidate ontology.Document
		}{
			{"identity", []any{typeMapping("gate", "gate", "")}, ontology.DefaultDocument()},
			{"unknown-target", []any{typeMapping("legacy_gate", "absent", id)}, initial},
			{"unknown-source", []any{typeMapping("absent", "gate", "")}, initial},
			{"unmatched-id", []any{typeMapping("legacy_gate", "gate", "missing")}, initial},
			{"conflicting", []any{typeMapping("legacy_gate", "gate", id), secondTypeMapping("legacy_gate", "issue", id)}, initial},
		} {
			t.Run(test.name, func(t *testing.T) {
				// Keep existing keys so the refusal belongs to the selected map.
				test.candidate.Types["legacy_gate"] = initial.Types["legacy_gate"]
				plan := gatePlan(f, test.candidate, test.mappings, 1)
				if plan["changes"].(map[string]any)["ready"] != false || len(plan["violations"].([]any)) == 0 {
					t.Fatalf("invalid mapping admitted: %v", plan)
				}
			})
		}
		archived := ontology.DefaultDocument()
		archived.Types["legacy_gate"] = initial.Types["legacy_gate"]
		gate := archived.Types["gate"]
		gate.Archived = true
		archived.Types["gate"] = gate
		plan := gatePlan(f, archived, []any{typeMapping("legacy_gate", "gate", id)}, 1)
		if plan["changes"].(map[string]any)["ready"] != false {
			t.Fatal("archived mapping target admitted")
		}
		for _, mapping := range []map[string]any{
			{"id": "invalid", "kind": "map_status", "target": map[string]any{"project": "local", "type": "legacy_gate"}, "from": "legacy_gate", "to": "gate"},
			{"id": "invalid", "kind": "map_type", "target": map[string]any{"type": "legacy_gate"}, "from": "legacy_gate", "to": "gate"},
			{"id": "invalid", "kind": "map_type", "target": map[string]any{"project": "local", "type": "legacy_gate"}, "to": "gate"},
			{"id": "invalid", "kind": "map_type", "target": map[string]any{"project": "local", "type": "legacy_gate"}, "from": "legacy_gate", "to": "gate", "value": true},
		} {
			f.call("ontology.plan", map[string]any{"project": "", "document": initial, "typeMappings": []any{mapping}}, false, "invalid_params")
		}
		f.call("ontology.plan", map[string]any{"project": "", "document": initial, "directives": []any{}}, false, "invalid_params")
		if !reflect.DeepEqual(meta, f.meta()) || history != f.historyCount() || !reflect.DeepEqual(before, f.currentBody(id)) {
			t.Fatal("refused planning changed data, source, receipts or history")
		}
	})
}

func secondTypeMapping(from, to, id string) map[string]any {
	mapping := typeMapping(from, to, id)
	mapping["id"] = "b14-map-type-conflict"
	return mapping
}
