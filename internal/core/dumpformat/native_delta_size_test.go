package dumpformat

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestNativeDeltaJSONSizeMatchesExactRelevantEncodings(t *testing.T) {
	cursor := uint64(66)
	profile := NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: NativeCommitSchemaScope, MainVersion: &cursor, Absences: []NativeCommitSchemaAbsence{{Family: "old", Reason: "absent"}}}
	for _, value := range []any{
		nil, true, false, "\x00\b\f\n\r\t<>&\u2028\u2029世界\xff", []byte{0, 1, 2, 255}, []byte(nil), []byte{},
		json.Number("9007199254740993"), json.Number("1e10000000"), float32(-0.25), math.Copysign(0, -1),
		model.Natural("1e1000000"), model.RefByID("sample", model.Natural64(0)), model.Ref{Key: "key"},
		json.RawMessage(" { \"a\" : [\"<>&\u2028\", -0, 2.00e+4] } "),
		map[string]any{"empty": []string{}, "nil": []string(nil), "nested": map[string]any{"z": json.Number("-0"), "a": "value"}},
		NativeDeltaHeader{Type: "header", Format: NativeDeltaFormat, StoreID: "public", Projects: []string{"public"}, Head: "head", Ignore: NativeTable{Columns: []string{}, Rows: []Row{}}, ConfigPolicy: map[string]string{}},
		profile, NativeHistoryImage{Sequence: 1, CommitID: "commit", SchemaProfile: &profile, Tables: map[string]NativeTable{"table": {Columns: []string{"id", "value"}, Rows: []Row{{"id": json.Number("7"), "value": "<large>"}}}}},
	} {
		var expected bytes.Buffer
		if err := json.NewEncoder(&expected).Encode(value); err != nil {
			t.Fatal(err)
		}
		got, err := NativeDeltaJSONSize(value)
		if err != nil || got != uint64(expected.Len()) {
			t.Fatalf("%T count=%d want=%d error=%v bytes=%q", value, got, expected.Len(), err, expected.Bytes())
		}
	}
}

type deltaUnallocatedMarshaler struct{ called *bool }

func (m deltaUnallocatedMarshaler) MarshalJSON() ([]byte, error) {
	*m.called = true
	return []byte(`"ignored"`), nil
}

func TestNativeDeltaSizeDoesNotInvokeUnknownAggregateMarshalers(t *testing.T) {
	called := false
	if _, err := NativeDeltaJSONSize(deltaUnallocatedMarshaler{&called}); err == nil || called {
		t.Fatalf("unsupported custom encoding executed: called=%t err=%v", called, err)
	}
}
