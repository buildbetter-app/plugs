package dumpformat

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNativeDeltaJSONSizeMatchesExactRelevantEncodings(t *testing.T) {
	cursor := uint64(66)
	profile := NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: NativeCommitSchemaScope, MainVersion: &cursor, Absences: []NativeCommitSchemaAbsence{{Family: "old", Reason: "absent"}}}
	for _, value := range []any{
		nil, true, false, time.Time{}, time.Date(2026, 11, 11, 3, 44, 0, 123, time.FixedZone("offset", 3600)), model.PrincipalRef{Kind: "user", ID: "actor", Caveats: map[string]any{"exact": json.Number("9007199254740993")}}, "\x00\b\f\n\r\t<>&\u2028\u2029世界\xff", []byte{0, 1, 2, 255}, []byte(nil), []byte{},
		json.Number("9007199254740993"), json.Number("1e10000000"), float32(-0.25), math.Copysign(0, -1),
		model.Natural("1e1000000"), model.RefByID("sample", model.Natural64(0)), model.Ref{Key: "key"},
		driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 72, Children: map[string]uint64{"demo-1": 8}},
		driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorHash, HashLength: 7},
		driver.NewHashAllocator("demo", 3, 7, 0), driver.NewHashAllocator("demo", 3, 7, 0.001),
		driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorRandom, RandomBytes: 16, SuffixEncoding: "lower-hex", TopLevelShape: "prefix-random", ChildShape: "parent-random"},
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

func TestNativeDeltaJSONSizeMatchesCommitAndReceiptWire(t *testing.T) {
	when := time.Date(2026, 11, 19, 9, 0, 0, 123, time.UTC)
	ttl, replaced := model.Natural64(1000), false
	result := model.OpResult{Ref: model.RefByID("receipt-record", model.Natural64(0)), Revision: model.Natural64(2), Record: json.RawMessage(` { "payload": "<>&\u2028", "large": 9007199254740993 } `), SetBy: map[string]string{"/payload": "trusted"}, Replaced: &replaced}
	commit := model.Commit{Revision: model.Natural64(2), Results: []model.OpResult{result}, Warnings: []model.Warning{{Code: "post_subscriber_failed", Message: "<warning>", Details: map[string]any{"nested": []any{json.Number("9007199254740993"), "<>&"}}}}, WouldCommit: true, Violations: []model.Violation{{Guard: "guard", Message: "message", Details: map[string]any{"valid": true}}}, LeaseExpiresAt: &when, LeaseTtlMs: &ttl}
	receipt := driver.IdempotencyRecord{AttemptID: "not-serialized", RequestHash: "request", Commit: commit, Changeset: model.Changeset{Revision: model.Natural64(2), Branch: model.Natural64(0), Incarnation: "target", Kind: model.ChangesetRestore, Time: when, Principal: model.PrincipalRef{Kind: "user", ID: "receiver"}, Chain: []string{}, IdempotencyKey: "original", Changes: []model.ChangeRecord{{Ref: result.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}}}}}
	for _, value := range []any{model.Commit{Revision: model.Natural64(0), Results: []model.OpResult{}, Warnings: []model.Warning{}}, result, commit, receipt, &receipt} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal("valid receipt wire fixture", err)
		}
		want := uint64(len(encoded) + 1)
		got, err := NativeDeltaJSONSize(value)
		if err != nil || got != want {
			t.Fatalf("%T counted=%d marshaled=%d error=%v", value, got, want, err)
		}
		budget := NewNativeDeltaBudget(want)
		owner := budget.Scope()
		if err := owner.ChargeJSON("receipt", value); err != nil || budget.Usage().CurrentCategories["receipt"] != want {
			t.Fatal("receipt charge is not its exact marshaled size", want, err, budget.Usage())
		}
		owner.Release()
		if budget.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("receipt owner cleanup", budget.Usage())
		}
		limited := NewNativeDeltaBudget(want - 1)
		candidate := limited.Scope()
		err = candidate.ChargeJSON("receipt", value)
		var refusal *NativeDeltaDecodedLimitError
		if !errors.As(err, &refusal) || refusal.MeasuredBytes != want || limited.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("receipt bypassed exact budget admission", err, want)
		}
		candidate.Release()
	}
	called := false
	commit.Warnings[0].Details = map[string]any{"unknown": deltaUnallocatedMarshaler{&called}}
	if _, err := NativeDeltaJSONSize(commit); err == nil || called {
		t.Fatal("receipt counting invoked an unknown aggregate marshaler", err, called)
	}
}
