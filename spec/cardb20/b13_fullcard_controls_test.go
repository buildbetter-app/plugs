package cardb20

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/service"
)

// These cases use literal expected values, never the implementation's slug or
// search helper. The native and Plug calls each have to satisfy that oracle.
var b13FullcardRefusals = []struct {
	name   string
	native []string
	plug   map[string]any
}{
	{"empty-content", []string{"--json", "remember", ""}, map[string]any{"content": ""}},
	{"punctuation-only", []string{"--json", "remember", "!!! ... ---"}, map[string]any{"content": "!!! ... ---"}},
	{"non-ascii-only", []string{"--json", "remember", "東京"}, map[string]any{"content": "東京"}},
	{"unicode-blank-key", []string{"--json", "remember", "valid", "--key", "\u2003\t"}, map[string]any{"key": "\u2003\t", "content": "valid"}},
}

type b13SearchCase struct {
	name, query string
	want        map[string]string
}

func b13SearchCases(phase string, all map[string]string) []b13SearchCase {
	keys, original, replacement := map[string]string{}, map[string]string{}, map[string]string{}
	switch phase {
	case "created", "remembered", "fidelity":
		keys = map[string]string{b13PublicKey: b13PublicBody, b13TrimmedPublicKey: b13TrimmedPublicBody, b13PublicKey + "neighbor": "exact neighbor"}
		original[b13PublicKey] = b13PublicBody
	case "replaced":
		keys = map[string]string{b13PublicKey: b13PublicReplacement, b13TrimmedPublicKey: b13TrimmedPublicBody, b13PublicKey + "neighbor": "exact neighbor"}
		replacement[b13PublicKey] = b13PublicReplacement
	case "forgotten":
		keys = map[string]string{b13TrimmedPublicKey: b13TrimmedPublicBody, b13PublicKey + "neighbor": "exact neighbor"}
	}
	return []b13SearchCase{
		{"empty-query", "", all},
		{"key-only-folds-query-and-row", "pUbLiC kEy/ω", keys},
		{"value-only-folds-query-and-row", "vErBaTiM nAtIvE", original},
		{"replacement-value", "rEpLaCeD ω", replacement},
		{"settings-never-memory", "sUrViVoR", map[string]string{}},
		{"miss-nonnull", "nO sUcH mEmOrY", map[string]string{}},
	}
}

func b13NativeSearchControls(n *b13Reference, phase string, all map[string]string) {
	n.r.t.Helper()
	// Each CLI call is a separate process. Physical reads bind committed
	// values and the independent durable/ephemeral decoys after every phase.
	rows := n.sql(phase+"-physical-memories", "SELECT `key`,value FROM config WHERE `key` LIKE 'kv.memory.%'")
	physical := map[string]string{}
	for _, row := range rows {
		key, keyOK := row["key"].(string)
		value, valueOK := row["value"].(string)
		if !keyOK || !valueOK || len(key) < len("kv.memory.") {
			n.r.t.Fatal("native physical memory row changed type")
		}
		physical[key[len("kv.memory."):]] = value
	}
	b13Same(n.r.t, physical, all)
	for _, plane := range []struct{ table, title string }{{"issues", "B13 durable plane-only survivor"}, {"wisps", "B13 ephemeral plane-only survivor"}} {
		b13Same(n.r.t, n.sql(phase+"-physical-"+plane.table, "SELECT title FROM "+plane.table+" WHERE title='"+plane.title+"'"), []map[string]any{{"title": plane.title}})
	}
	for _, tc := range b13SearchCases(phase, all) {
		label := phase + "-search-" + tc.name
		var wire map[string]any
		b13Decode(n.r.t, n.call(label, []string{"--json", "memories", tc.query}, 0), &wire)
		if wire == nil || wire["schema_version"] != json.Number("1") {
			n.r.t.Fatal("native search must return a nonnull object and schema_version=1")
		}
		got := map[string]string{}
		for key, value := range wire {
			if key == "schema_version" {
				continue
			}
			text, ok := value.(string)
			if !ok {
				n.r.t.Fatal("native search value is not a string")
			}
			got[key] = text
		}
		b13Same(n.r.t, got, tc.want)
		n.projections[label] = got
		n.r.writeJSON(filepath.Join(n.r.root, label+"-comparison.json"), map[string]any{"query": tc.query, "literal_expected": tc.want, "actual_native": got, "literal_wire": wire})
	}
}

func b13PlugSearchControls(t *testing.T, phase string, native map[string]map[string]string, call func(string, string, map[string]any, int, string) json.RawMessage) {
	t.Helper()
	for _, tc := range b13SearchCases(phase, native[phase]) {
		label := phase + "-search-" + tc.name
		var page struct {
			Items []service.Memory `json:"items"`
			Next  string           `json:"nextCursor"`
		}
		b13Decode(t, call(label, "memory.list", map[string]any{"query": tc.query, "limit": 100}, 0, ""), &page)
		if page.Items == nil || page.Next != "" {
			t.Fatal("search must return a complete nonnull array")
		}
		got := map[string]string{}
		for _, item := range page.Items {
			if _, exists := got[item.Key]; exists {
				t.Fatal("duplicate effective search key")
			}
			got[item.Key] = item.Content
		}
		b13Same(t, got, tc.want)
		b13Same(t, got, native[label])
	}
}
