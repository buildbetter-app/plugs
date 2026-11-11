package reverseconverter

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

func TestNativeImportNamesEveryPopulatedFieldAndRowCount(t *testing.T) {
	dump := cloneDump(fixtureDump())
	values := map[string]any{
		"agent_state": "private-state", "await_id": "approval", "await_type": "human",
		"hook_bead": "demo-1", "role_bead": "demo-2", "role_type": "agent",
		"event_kind": "event", "is_template": json.Number("1"), "mol_type": "template",
		"payload": map[string]any{"n": json.Number("9007199254740993")},
		"target":  "demo-3", "timeout_ns": json.Number("9007199254740993"),
		"waiters": []any{"recipient", nil}, dumpformat.EphemeralTypeField: "run", "work_type": "workflow",
		"estimated_minutes": json.Number("5"),
	}
	for i := 0; i < 2; i++ {
		for field, value := range values {
			dump.Records["bead"][i].Source.Fields[field] = value
		}
	}
	before := digestDump(dump)
	result, err := ConvertWithOptions(dump, Options{All: true, NativeImport: true})
	var refused *PhysicalFieldRefusal
	if !errors.As(err, &refused) || len(result.JSONL) != 0 || result.Digest != "" || len(refused.Fields) != len(values) {
		t.Fatalf("native preflight: result=%+v error=%v", result.Report, err)
	}
	for i, count := range refused.Fields {
		if _, ok := values[count.Field]; !ok || count.Table != "issues" || count.Rows != 2 || i > 0 && refused.Fields[i-1].Field >= count.Field {
			t.Fatalf("incomplete or unordered field census: %+v", refused.Fields)
		}
	}
	if strings.Contains(err.Error(), "private-state") || strings.Contains(err.Error(), "demo-") || digestDump(dump) != before {
		t.Fatal("refusal leaked source data or changed input")
	}
	// Pure serialization keeps the exact existing opaque-metadata contract.
	serialized, err := ConvertWithOptions(dump, Options{All: true})
	if err != nil {
		t.Fatal(err)
	}
	var boundary *PhysicalFieldRefusal
	if err = ValidateNativeImportJSONL(serialized.JSONL); !errors.As(err, &boundary) || !reflect.DeepEqual(boundary.Fields, refused.Fields) {
		t.Fatalf("target preflight can be bypassed through serialization: %v", err)
	}
}

func TestNativeImportAllowsOnlyEmptyOrDefaultUnsupportedCells(t *testing.T) {
	for _, test := range []struct {
		name, field string
		value       any
		allow       bool
	}{
		{"null-state", "agent_state", nil, true},
		{"empty-state", "agent_state", "", true},
		{"populated-state", "agent_state", "0", false},
		{"zero-duration", "timeout_ns", json.Number("0"), true},
		{"text-duration", "timeout_ns", "0", false},
		{"wide-duration", "timeout_ns", json.Number("9007199254740993"), false},
		{"false-template", "is_template", false, true},
		{"zero-template", "is_template", "0", true},
		{"true-template", "is_template", true, false},
		{"default-text-flag", "pinned", "0", true},
		{"populated-text-flag", "pinned", "1", false},
		{"empty-payload", "payload", "", true},
		{"empty-object-presence", "payload", map[string]any{}, false},
		{"empty-waiters", "waiters", []any{}, true},
		{"null-element", "waiters", []any{nil}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			dump := cloneDump(fixtureDump())
			dump.Records["bead"][0].Source.Fields[test.field] = test.value
			result, err := ConvertWithOptions(dump, Options{All: true, NativeImport: true})
			if test.allow {
				if err != nil || len(result.JSONL) == 0 || ValidateNativeImportJSONL(result.JSONL) != nil {
					t.Fatalf("default cell refused: %v", err)
				}
			} else {
				var refused *PhysicalFieldRefusal
				if !errors.As(err, &refused) || len(result.JSONL) != 0 || !reflect.DeepEqual(refused.Fields, []PhysicalFieldCount{{Table: "issues", Field: test.field, Rows: 1}}) {
					t.Fatalf("populated cell exported: %v", err)
				}
			}
		})
	}
}

func TestReverseRejectsFloatingPointMetadataBeforeOutput(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
	}{
		{"scalar", float64(9007199254740992)},
		{"typed-map", map[string]float32{"n": 1}},
		{"nested-map", []any{map[string]any{"n": float64(1)}}},
		{"struct", struct{ N float64 }{float64(9007199254740993)}},
		{"pointer-struct", &struct{ N float64 }{float64(9007199254740993)}},
		{"nested-struct", &struct{ Nested []any }{[]any{struct{ N float32 }{1}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dump := cloneDump(fixtureDump())
			dump.Records["bead"][0].Body["metadata"].(map[string]any)["number"] = test.value
			result, err := Convert(dump)
			var refused *Refusal
			if !errors.As(err, &refused) || len(result.JSONL) != 0 || !strings.Contains(err.Error(), "exact JSON numbers") {
				t.Fatalf("inexact dynamic value certified: %v", err)
			}
		})
	}
	dump := cloneDump(fixtureDump())
	dump.Records["bead"][0].Body["metadata"].(map[string]any)["number"] = map[string]any{"wide": json.Number("9007199254740993"), "nested": []any{json.Number("-9007199254740993"), json.Number("1.0"), json.Number("1e3")}}
	dump.Records["bead"][0].Body["metadata"].(map[string]any)["exact_struct"] = struct{ N json.Number }{json.Number("9007199254740993")}
	result, err := Convert(dump)
	if err != nil || !bytes.Contains(result.JSONL, []byte(`"wide":9007199254740993`)) || !bytes.Contains(result.JSONL, []byte(`[-9007199254740993,1.0,1e3]`)) || !bytes.Contains(result.JSONL, []byte(`"exact_struct":{"N":9007199254740993}`)) {
		t.Fatalf("exact number spelling changed: %s %v", result.JSONL, err)
	}
}
