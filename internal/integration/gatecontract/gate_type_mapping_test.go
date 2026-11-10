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
		beforeFields := before["fields"].(map[string]any)
		beforeStatus, ok := beforeFields["status"].(string)
		if !ok || beforeStatus == "" {
			t.Fatalf("created history body lacks a literal status: %v", before)
		}
		candidate := ontology.DefaultDocument()
		candidate.Types["legacy_gate"] = initial.Types["legacy_gate"]
		plan := gatePlan(f, candidate, []any{typeMapping("legacy_gate", "gate", id)}, 1)
		migration := plan["changes"].(map[string]any)["migration"].(map[string]any)
		if migration["affected"] != float64(1) || !reflect.DeepEqual(migration["directives"], decodedJSON(t, []any{typeMapping("legacy_gate", "gate", id)})) {
			t.Fatalf("retained map_type report: %v", migration)
		}
		gateApply(f, plan, 1)
		after := f.currentBody(id)
		if after["type"] != "gate" || after["id"] != id || after["project"] != before["project"] || !reflect.DeepEqual(after["fields"], beforeFields) {
			t.Fatalf("type mapping lost literal record members: %v -> %v", before, after)
		}
		created := gateCreate(f, "gate", "timer", nil)
		check := func() {
			t.Helper()
			got := gateIDs(f.call("gate.list", map[string]any{}, false, ""))
			if !reflect.DeepEqual(got, []string{created, id}) {
				t.Fatalf("installed gate listing: %v", got)
			}
			schema := f.call("ontology.schema", map[string]any{"project": "local", "type": "gate"}, false, "")
			if schema["properties"] == nil {
				t.Fatalf("installed gate schema absent: %v", schema)
			}
			bead, body := f.bead(id), f.currentBody(id)
			if bead["status"] != beforeStatus || !reflect.DeepEqual(body["fields"], beforeFields) {
				t.Fatalf("durable map_type readback drift: bead=%v body=%v want status=%q fields=%v", bead, body, beforeStatus, beforeFields)
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
		source := f.call("ontology.get", map[string]any{"project": ""}, false, "")
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
				previous := f.t
				f.t = t
				defer func() { f.t = previous }()
				// Keep existing keys so the refusal belongs to the selected map.
				test.candidate.Types["legacy_gate"] = initial.Types["legacy_gate"]
				response := f.call("ontology.plan", map[string]any{"project": "", "document": test.candidate, "typeMappings": test.mappings, "expect": expect(gateOntologyRef.ID, 1)}, false, "invalid_params")
				if test.name == "identity" && response["error"].(map[string]any)["data"].(map[string]any)["hint"] != "mapping must change its key" {
					t.Fatalf("identity mapping lost the actual planner refusal: %v", response)
				}
			})
		}
		archived := ontology.DefaultDocument()
		archived.Types["legacy_gate"] = initial.Types["legacy_gate"]
		gate := archived.Types["gate"]
		gate.Archived = true
		archived.Types["gate"] = gate
		f.call("ontology.plan", map[string]any{"project": "", "document": archived, "typeMappings": []any{typeMapping("legacy_gate", "gate", id)}, "expect": expect(gateOntologyRef.ID, 1)}, false, "invalid_params")
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
		if current := f.call("ontology.get", map[string]any{"project": ""}, false, ""); !reflect.DeepEqual(source, current) {
			t.Fatal("refused mappings changed the public source catalog")
		}
	})
}

func secondTypeMapping(from, to, id string) map[string]any {
	mapping := typeMapping(from, to, id)
	mapping["id"] = "b14-map-type-conflict"
	return mapping
}
