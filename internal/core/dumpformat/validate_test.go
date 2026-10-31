package dumpformat

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestIssueAwaitIDIsOpaqueTextOnly(t *testing.T) {
	fixture := newMergeFixture(t, "pl")
	fixture.records["bead"][0].Source.Fields["await_id"] = "opaque-await/v1\x00"
	if _, err := Read(bytes.NewReader(fixture.bytes(t, nil))); err != nil {
		t.Fatalf("opaque await_id was refused: %v", err)
	}

	fixture.records["bead"][0].Source.Fields["await_id"] = json.Number("1")
	if _, err := Read(bytes.NewReader(fixture.bytes(t, nil))); err == nil || !strings.Contains(err.Error(), "await_id must be text") {
		t.Fatalf("non-text await_id was accepted: %v", err)
	}
}

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

func TestJSONRejectsInvalidUnicodeEscapesAndExcessiveNesting(t *testing.T) {
	for _, data := range []string{`"\ud800"`, `"\udc00"`, `"\ud800\u0041"`} {
		var value any
		if err := decodeJSON([]byte(data), &value); err == nil {
			t.Fatalf("accepted invalid Unicode escape %s", data)
		}
	}
	var value any
	if err := decodeJSON([]byte(`"\ud83d\ude42"`), &value); err != nil {
		t.Fatal(err)
	}
	if err := decodeJSON([]byte(strings.Repeat("[", 130)+"0"+strings.Repeat("]", 130)), &value); err == nil {
		t.Fatal("unbounded nesting accepted")
	}
}

func TestCredentialReferencesMatchProducerPredicate(t *testing.T) {
	for _, key := range []string{"github.api_key", "github.api-key", "github.apikey", "github.secret", "github.token", "github.password", "github.passwd", "github.credential", "github.private_key", "github.privatekey", "github.privkey", "github.pwd", "github.pat", "github.auth", "github.bearer", "github.cert", "github.key", "github.keys", "database.dsn", "cloud.access_key", "GITHUB.TOKEN"} {
		t.Run(key, func(t *testing.T) {
			if !secretKey(key) {
				t.Fatal("missed producer credential key")
			}
			if err := validateSecretReferences(map[string]any{"nested": []any{map[string]any{key: "raw-or-digest"}}}); err == nil {
				t.Fatal("raw nested credential accepted")
			}
			if err := validateSecretReferences(map[string]any{key: map[string]any{"$secret": "synthetic/reference"}}); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, key := range []string{"project_id", "commit.author", "issue.path", "export.pattern", "sort.keyword"} {
		if secretKey(key) {
			t.Fatalf("public near miss %s classified as credential", key)
		}
	}
	for _, ref := range []any{map[string]any{"$secret": ""}, map[string]any{"$secret": nil}, map[string]any{"$secret": "valid", "extra": true}} {
		if err := validateSecretReferences(ref); err == nil {
			t.Fatal("invalid reference accepted")
		}
	}
}
