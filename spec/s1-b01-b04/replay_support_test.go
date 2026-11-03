package s1b01b04

import (
	"testing"
)

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
