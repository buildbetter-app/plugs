package model

import (
	"encoding/json"
	"testing"
)

func TestTypedTargetStatusWireAndSchema(t *testing.T) {
	schemas := componentSchemas(t)
	for _, tc := range []struct{ schema, raw string }{
		{"CreateOp", `{"kind":"create","type":"native-work-item","fields":{},"targetStatus":"native-ready-alt"}`},
		{"CloseOp", `{"kind":"close","target":{"id":"pl-one"},"targetStatus":"native-done-alt","reason":"checked"}`},
	} {
		t.Run(tc.schema, func(t *testing.T) {
			var op Op
			if err := json.Unmarshal([]byte(tc.raw), &op); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(op)
			if err != nil {
				t.Fatal(err)
			}
			if err := schemaCheck(schemas, schemas[tc.schema].(map[string]any), parseJSON(t, encoded)); err != nil {
				t.Fatal(err)
			}
			roundtrip := parseJSON(t, encoded).(map[string]any)
			if roundtrip["targetStatus"] != parseJSON(t, []byte(tc.raw)).(map[string]any)["targetStatus"] {
				t.Fatal("exact status spelling lost")
			}
		})
	}
	for _, raw := range []string{
		`{"kind":"create","type":"native-work-item","fields":{},"targetStatus":""}`,
		`{"kind":"create","type":"native-work-item","fields":{},"targetStatus":null}`,
		`{"kind":"create","type":"native-work-item","fields":{"status":"native-open"},"targetStatus":"native-ready-alt"}`,
		`{"kind":"close","target":{"id":"pl-one"},"targetStatus":""}`,
		`{"kind":"close","target":{"id":"pl-one"},"targetStatus":null}`,
		`{"kind":"close","target":{"id":"pl-one"},"targetStatus":3}`,
		`{"kind":"patch","target":{"id":"pl-one"},"fields":{},"targetStatus":"native-ready-alt"}`,
	} {
		var op Op
		if err := json.Unmarshal([]byte(raw), &op); err == nil {
			t.Fatalf("accepted invalid selector: %s", raw)
		}
	}
}
