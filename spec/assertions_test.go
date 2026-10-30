package spec

import (
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/acceptance"
)

// assertPaths makes every omitted field explicit and requires every selected
// path. It retains array order and never turns an absent value into null.
func assertPaths(t *testing.T, response []byte, want map[string]any) {
	t.Helper()
	paths := make([]string, 0, len(want))
	for path := range want {
		paths = append(paths, path)
	}
	got, err := acceptance.Project(response, paths...)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := acceptance.EqualJSON(expected, got); err != nil {
		t.Fatal(err)
	}
}

func valueAt[T any](t *testing.T, response []byte, path string) T {
	t.Helper()
	projection, err := acceptance.Project(response, path)
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(projection, &values); err != nil {
		t.Fatal(err)
	}
	var value T
	if err := json.Unmarshal(values[path], &value); err != nil {
		t.Fatal(err)
	}
	return value
}

// orderedIDs projects the entire page, including its length. Selecting just
// the first expected rows would hide an extra incorrectly admitted bead.
func orderedIDs(t *testing.T, response []byte) []string {
	t.Helper()
	items := valueAt[[]map[string]json.RawMessage](t, response, "/result/items")
	ids := make([]string, 0, len(items))
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		var id string
		if err := json.Unmarshal(item["id"], &id); err != nil || id == "" || seen[id] {
			t.Fatalf("page contains an invalid or repeated ID: %s", item["id"])
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids
}

func assertIDs(t *testing.T, response []byte, want ...string) {
	t.Helper()
	got := orderedIDs(t, response)
	expected := append([]string{}, want...)
	w, _ := json.Marshal(expected)
	g, _ := json.Marshal(got)
	if err := acceptance.EqualJSON(w, g); err != nil {
		t.Fatal(err)
	}
}
