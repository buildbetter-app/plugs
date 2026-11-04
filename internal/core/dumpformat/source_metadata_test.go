package dumpformat

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSourceMetadataObjectPreservesSQLTextAndLosslessValues(t *testing.T) {
	const raw = " {\"large\":9007199254740993,\"null\":null,\"nested\":[\" 外部 \",null,{\"value\":1.25}]} \n"
	want := map[string]any{"large": json.Number("9007199254740993"), "null": nil, "nested": []any{" 外部 ", nil, map[string]any{"value": json.Number("1.25")}}}
	for _, value := range []any{raw, want} {
		got, err := SourceMetadataObject(value)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("metadata type/value changed: %#v %v", got, err)
		}
		got["nested"].([]any)[2].(map[string]any)["value"] = "mutated"
		if want["nested"].([]any)[2].(map[string]any)["value"] != json.Number("1.25") {
			t.Fatal("semantic projection mutated original source")
		}
	}
	none, err := SourceMetadataObject(nil)
	if err != nil || none != nil {
		t.Fatal("SQL NULL became explicit object")
	}
	empty, err := SourceMetadataObject("{}")
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatal("explicit empty object became absent")
	}
}

func TestSourceMetadataObjectRejectsAmbiguousOrMalformedValues(t *testing.T) {
	for _, raw := range []any{
		"", " ", "null", `[]`, `"{}"`, `1`, `true`, `{`,
		`{"gate":"all-children","gate":"any-children"}`,
		`{"nested":{"x":1,"x":2}}`, `{} {}`, `{} false`,
		`{"x":"\ud800"}`, "{\"x\":\"\xff\"}",
		[]any{}, json.Number("1"), false,
	} {
		if got, err := SourceMetadataObject(raw); err == nil {
			t.Fatalf("invalid source metadata admitted: %#v => %#v", raw, got)
		}
	}
}
