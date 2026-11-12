package dumpformat

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestNativePreparedCaptureFileLedgerBindsOrderedRootsAndParts(t *testing.T) {
	archive, _ := preparedDeltaFixture(t)
	prepared, err := archive.PrepareNativeDelta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := prepared.NativeCaptureFileLedger(prepared.budget.parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger) != 3 || ledger[0].Name != "capture-set.json" || ledger[1].Name != "head.tar" || ledger[2].Name != "parts/00000000000000000001.part" {
		t.Fatalf("unexpected native capture ledger: %#v", ledger)
	}
	for _, file := range ledger {
		if !validDeltaSHA256(file.SHA256) {
			t.Fatalf("invalid native capture ledger entry: %#v", file)
		}
	}
	if ledger[0].SHA256 != digest([]byte("synthetic root")) {
		t.Fatal("capture root commitment changed")
	}
}

func TestNativePreparedCaptureFileLedgerChargesBeforeReturningCandidate(t *testing.T) {
	archive, _ := preparedDeltaFixture(t)
	prepared, err := archive.PrepareNativeDelta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	owner := prepared.budget.parent.Scope()
	defer owner.Release()
	need, err := nativeCaptureFileLedgerJSONSize(prepared.files)
	if err != nil {
		t.Fatal(err)
	}
	root := owner.root()
	originalLimit, held := root.limit, root.used
	defer func() { root.limit = originalLimit }()
	root.limit = held + need - 1
	ledger, err := prepared.NativeCaptureFileLedger(owner)
	var limit *NativeDeltaDecodedLimitError
	if ledger != nil || !errors.As(err, &limit) || limit.Phase != "native_capture_file_ledger" || limit.RejectedChargeBytes != need {
		t.Fatalf("ledger escaped pre-admission limit: %#v %v", ledger, err)
	}
	if got := owner.Usage().CurrentAccountedDecodedBytes; got != held {
		t.Fatalf("refused ledger retained decoded bytes: %d", got)
	}
	root.limit = held + need
	ledger, err = prepared.NativeCaptureFileLedger(owner)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(ledger)
	if err != nil || uint64(len(encoded)+1) != need || owner.ReservedBytes("native_capture_file_ledger") != need {
		t.Fatalf("ledger charge differs from encoded payload: need=%d bytes=%d error=%v", need, len(encoded)+1, err)
	}
	prepared.Close()
	if owner.ReservedBytes("native_capture_file_ledger") != need || len(ledger) != 3 {
		t.Fatal("source close released surviving ledger")
	}
	ledger = nil
	owner.Release()
	if owner.Usage().CurrentCategories["native_capture_file_ledger"] != 0 {
		t.Fatal("discarded ledger retained reservation")
	}
}

func TestNativeStoredCredentialRefusalIsTypedAndCountsOnly(t *testing.T) {
	table := NativeDeltaTableSchema{Name: "config", Columns: []NativeDeltaColumn{{Name: "key", SQLType: "text", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	const logicalKey = "api_key_REVIEW_MARKER"
	cells := []NativeDeltaCell{{Kind: "string", Value: logicalKey}, {Kind: "string", Value: "do-not-leak-this"}}
	key, err := NativeDeltaPrimaryKey(table, cells)
	if err != nil {
		t.Fatal(err)
	}
	rowBytes, err := json.Marshal(Row{"key": logicalKey, "value": "do-not-leak-this"})
	if err != nil {
		t.Fatal(err)
	}
	row := NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(append(rowBytes, '\n'))}
	_, _, err = validateNativeDeltaRow(table, row, nil)
	var refusal *NativeStoredCredentialError
	if !errors.As(err, &refusal) || refusal.Code != "native_stored_credential" || refusal.Family != "config" || refusal.Field != "value" || refusal.Count != 1 {
		t.Fatalf("missing typed credential refusal: %T %v", err, err)
	}
	encoded, marshalErr := json.Marshal(refusal)
	if marshalErr != nil || strings.Contains(string(encoded), logicalKey) || strings.Contains(string(encoded), "do-not-leak-this") || strings.Contains(err.Error(), "do-not-leak-this") || strings.Contains(err.Error(), logicalKey) {
		t.Fatal("credential value escaped typed refusal")
	}
	if err := validateSecretReferences(map[string]any{"api_key": "do-not-leak-this"}); err == nil || errors.As(err, &refusal) {
		t.Fatal("portable credential validator changed or became native typed error")
	}
	nestedMarker := "nested-token-REVIEW-MARKER"
	err = validateNativeDeltaSecretReferences("config", map[string]any{
		"key":   "ordinary-setting",
		"value": map[string]any{nestedMarker: "nested-secret"},
	})
	if !errors.As(err, &refusal) || refusal.Field != "value" || strings.Contains(err.Error(), nestedMarker) || strings.Contains(err.Error(), "nested-secret") {
		t.Fatalf("nested credential diagnostic leaked source path or value: %T %v", err, err)
	}
	encoded, marshalErr = json.Marshal(refusal)
	if marshalErr != nil || strings.Contains(string(encoded), nestedMarker) || strings.Contains(string(encoded), "nested-secret") {
		t.Fatal("nested credential diagnostic JSON leaked source path or value")
	}
}

func TestNativeBlankPeerCredentialStillPasses(t *testing.T) {
	for _, value := range []any{nil, "", map[string]any{"$bytes": ""}} {
		if err := validateNativeDeltaSecretReferences("federation_peers", map[string]any{"password_encrypted": value}); err != nil {
			t.Fatalf("blank peer credential refused: %#v: %v", value, err)
		}
	}
}
