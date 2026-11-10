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

func TestSourceWaitsForMetadataDistinguishesAbsenceFromExplicitDefault(t *testing.T) {
	none, err := SourceWaitsForMetadata(nil)
	if err != nil || none != nil {
		t.Fatal("absent metadata lost legacy waits semantics")
	}
	for _, raw := range []any{`{}`, map[string]any{}, `{"gate":"all-children","also_blocks":false}`} {
		got, err := SourceWaitsForMetadata(raw)
		if err != nil || got == nil || got.Gate != "all-children" || got.AlsoBlocks {
			t.Fatalf("explicit default changed: %+v %v", got, err)
		}
	}
	got, err := SourceWaitsForMetadata(` {"gate":"any-children","also_blocks":true} `)
	if err != nil || got == nil || got.Gate != "any-children" || !got.AlsoBlocks {
		t.Fatalf("SQL-text waits predicate changed: %+v %v", got, err)
	}
	for _, raw := range []string{`{"gate":null}`, `{"also_blocks":null}`, `{"gate":""}`, `{"gate":"any"}`, `{"also_blocks":"true"}`, `{"also_blocks":1}`, `{"Gate":"any-children"}`, `{"other":true}`, `{"gate":"all-children","gate":"any-children"}`, `null`, `"{}"`, `[]`} {
		if value, err := SourceWaitsForMetadata(raw); err == nil {
			t.Fatalf("malformed waits predicate admitted: %s => %+v", raw, value)
		}
	}
}
