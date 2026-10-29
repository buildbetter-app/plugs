package dumpformat

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestDetachedValuesPreserveNestedNullAndNumber(t *testing.T) {
	a := &Archive{byKind: map[string][]Record{"bead": {{Body: Row{"nested": map[string]any{"null": nil, "number": json.Number("9007199254740993"), "array": []any{"original"}}}}}}}
	got := a.Records("bead")
	nested := got[0].Body["nested"].(map[string]any)
	if nested["null"] != nil || nested["number"] != json.Number("9007199254740993") {
		t.Fatalf("values=%#v", nested)
	}
	nested["number"] = json.Number("1")
	nested["array"].([]any)[0] = "changed"
	if reflect.DeepEqual(got, a.Records("bead")) {
		t.Fatal("accessor did not detach nested JSON")
	}
}

func TestTypedJSONRequiresExactPresentNonNullMembers(t *testing.T) {
	for _, data := range []string{`{}`, `{"SHA256":"x","size":1}`, `{"sha256":"x","size":null}`, `{"sha256":"x","size":1,"size":2}`, `{"sha256":"x","size":1} {}`, `{"sha256":"x","size":1,"extra":0}`} {
		var value File
		if err := decodeJSON([]byte(data), &value); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
	var value File
	if err := decodeJSON([]byte(`{"sha256":"x","size":1}`), &value); err != nil {
		t.Fatal(err)
	}
}
