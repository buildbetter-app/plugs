package s1b01b04

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"testing"
)

// The replay target has its own source set, so keep the small integrity
// helpers it consumes beside the replay fixture rather than relying on the
// separately built recorded-integrity target.
//
//go:embed manifest.json
var manifestBytes []byte

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
