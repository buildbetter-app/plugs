package wire

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestDecidedSessionMethodsCannotDisappearTogether(t *testing.T) {
	for _, missing := range []string{"session.open", "session.close", "presence.set"} {
		t.Run(missing, func(t *testing.T) {
			value, err := DecodeJSON(catalogJSON)
			if err != nil {
				t.Fatal(err)
			}
			doc := value.(map[string]any)
			registry := doc["x-method-registry"].(map[string]any)
			without := func(items []any) []any {
				var kept []any
				for _, item := range items {
					if item.(map[string]any)["name"] != missing {
						kept = append(kept, item)
					}
				}
				return kept
			}
			doc["methods"] = without(doc["methods"].([]any))
			entries := without(registry["methods"].([]any))
			registry["methods"] = entries
			registry["methodCount"] = len(entries)
			raw, err := json.Marshal(entries)
			if err != nil {
				t.Fatal(err)
			}
			registry["descriptorSha256"] = digest(raw)
			var names bytes.Buffer
			for _, item := range entries {
				names.WriteString(item.(map[string]any)["name"].(string) + "\n")
			}
			registry["nameSha256"] = digest(names.Bytes())
			raw, err = json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = ParseCatalog(raw); err == nil {
				t.Fatal("mutually omitted decided method was accepted after recomputing both inventory digests")
			}
		})
	}
}
