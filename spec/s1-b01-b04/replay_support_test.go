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

func referenceIDs(t *testing.T, card, key string) []string {
	t.Helper()
	artifact := decodeObject(t, runOneBytes)
	vector := decodeObject(t, []byte(artifact["rawJSON"].(string)))
	cards := vector["projection"].(map[string]any)
	entry := cards[card].(map[string]any)
	values, ok := entry[key].([]any)
	if !ok {
		t.Fatalf("reference projection %s.%s is not an array", card, key)
	}
	ids := make([]string, 0, len(values))
	for _, value := range values {
		id, ok := value.(string)
		if !ok {
			t.Fatalf("reference projection %s.%s contains non-string value %#v", card, key, value)
		}
		ids = append(ids, id)
	}
	return ids
}
