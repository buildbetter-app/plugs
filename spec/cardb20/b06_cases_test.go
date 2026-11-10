package cardb20

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// B06 keeps native identifiers and edge kinds in the shared command corpus.
// Each runtime records its original requests and outputs before comparison.
type b06Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Kind string `json:"kind"`
}

type b06CycleCase struct {
	Name  string    `json:"name"`
	IDs   []string  `json:"ids"`
	Edges []b06Edge `json:"edges"`
}

func b06CycleCases() []b06CycleCase {
	return []b06CycleCase{
		{"pure-tracks-loop", []string{"b06-pure-c", "b06-pure-a", "b06-pure-b"}, []b06Edge{
			{"b06-pure-a", "b06-pure-b", "tracks"}, {"b06-pure-b", "b06-pure-c", "tracks"}, {"b06-pure-c", "b06-pure-a", "tracks"},
		}},
		{"mixed-tracks-loop", []string{"b06-mixed-c", "b06-mixed-b", "b06-mixed-a"}, []b06Edge{
			{"b06-mixed-a", "b06-mixed-b", "blocks"}, {"b06-mixed-b", "b06-mixed-c", "tracks"}, {"b06-mixed-c", "b06-mixed-a", "tracks"},
		}},
		{"rotated-lowest-id", []string{"b06-rotate-z", "b06-rotate-m", "b06-rotate-a"}, []b06Edge{
			{"b06-rotate-z", "b06-rotate-m", "blocks"}, {"b06-rotate-m", "b06-rotate-a", "tracks"}, {"b06-rotate-a", "b06-rotate-z", "tracks"},
		}},
	}
}

// These rows are diagnostic setup, never evidence of accepted ordinary writes.
// The first source has three actual parallel entries in the stored adjacency.
func b06ParallelEdges() []b06Edge {
	return []b06Edge{
		{"b06-parallel-a", "b06-parallel-b", "blocks"},
		{"b06-parallel-a", "b06-parallel-b", "blocks"},
		{"b06-parallel-a", "b06-parallel-b", "tracks"},
		{"b06-parallel-b", "b06-parallel-a", "blocks"},
	}
}

func b06ExpectedPaths(parallel bool) [][]string {
	paths := [][]string{{"b06-mixed-a", "b06-mixed-b", "b06-mixed-c"}}
	if parallel {
		paths = append(paths, []string{"b06-parallel-a", "b06-parallel-b"})
	}
	return append(paths, []string{"b06-rotate-a", "b06-rotate-z", "b06-rotate-m"})
}

func b06Retain(t *testing.T, name string, value any) {
	t.Helper()
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute declared evidence directory required")
	}
	dir := filepath.Join(root, strings.ReplaceAll(t.Name(), "/", "_"))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
