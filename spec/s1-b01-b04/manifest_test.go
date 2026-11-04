package s1b01b04

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"testing"
)

// These immutable inputs are shared by the recorded-integrity, replay, and
// claim-acceptance targets while retaining independent Bazel source sets.
//
//go:embed manifest.json
var manifestBytes []byte

//go:embed run-1.json
var runOneBytes []byte

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func decodeObject(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		t.Fatal("accepted trailing JSON value")
	}
	return value
}
