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
	ledger, err := prepared.NativeCaptureFileLedger()
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

func TestNativeStoredCredentialRefusalIsTypedAndCountsOnly(t *testing.T) {
	table := NativeDeltaTableSchema{Name: "config", Columns: []NativeDeltaColumn{{Name: "key", SQLType: "text", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	cells := []NativeDeltaCell{{Kind: "string", Value: "api_key"}, {Kind: "string", Value: "do-not-leak-this"}}
	key, err := NativeDeltaPrimaryKey(table, cells)
	if err != nil {
		t.Fatal(err)
	}
	rowBytes, err := json.Marshal(Row{"key": "api_key", "value": "do-not-leak-this"})
	if err != nil {
		t.Fatal(err)
	}
	row := NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(append(rowBytes, '\n'))}
	_, _, err = validateNativeDeltaRow(table, row, nil)
	var refusal *NativeStoredCredentialError
	if !errors.As(err, &refusal) || refusal.Code != "native_stored_credential" || refusal.Family != "config" || refusal.Field != "api_key" || refusal.Count != 1 {
		t.Fatalf("missing typed credential refusal: %T %v", err, err)
	}
	if strings.Contains(err.Error(), "do-not-leak-this") {
		t.Fatal("credential value escaped typed refusal")
	}
	if err := validateSecretReferences(map[string]any{"api_key": "do-not-leak-this"}); err == nil || errors.As(err, &refusal) {
		t.Fatal("portable credential validator changed or became native typed error")
	}
}

func TestNativeBlankPeerCredentialStillPasses(t *testing.T) {
	for _, value := range []any{nil, "", map[string]any{"$bytes": ""}} {
		if err := validateNativeDeltaSecretReferences("federation_peers", map[string]any{"password_encrypted": value}); err != nil {
			t.Fatalf("blank peer credential refused: %#v: %v", value, err)
		}
	}
}
