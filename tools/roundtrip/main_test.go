package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHarnessHistoryModesAndPrivateOutput(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "plug-roundtrip-public-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	for _, mode := range []string{"full", "partial", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			output := filepath.Join(root, mode)
			result := run([]string{"--input", "../../internal/integration/offlinehistory/testdata/dolt-source-partial.tar", "--output-dir", output, "--history-mode", mode})
			if mode == "full" {
				if result.Status != "BLOCK" || result.Stage != "history" || result.Reason != "history_non-contiguous" {
					t.Fatalf("strict incomplete history: %+v", result)
				}
				entries, err := os.ReadDir(output)
				if err != nil || len(entries) != 0 {
					t.Fatal("refused history produced output")
				}
				return
			}
			if result.Status != "PASS" || result.Writes != 1 || result.Claims != 1 || result.BaselineSHA256 == result.ReverseSHA256 || result.HistoryLossItems == 0 {
				t.Fatalf("public runtime workload: %+v", result)
			}
			for path, want := range map[string]os.FileMode{output: 0o700, filepath.Join(output, "reverse-baseline.jsonl"): 0o600, filepath.Join(output, "reverse-live.jsonl"): 0o600} {
				info, err := os.Stat(path)
				if err != nil || info.Mode().Perm() != want {
					t.Fatal("runtime output permissions changed")
				}
			}
		})
	}
}

func TestHarnessErrorsContainNoSourcePathsOrParserText(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "plug-roundtrip-public-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	input := filepath.Join(root, "private-source-marker")
	if err := os.WriteFile(input, []byte("private-source-marker: not an archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	result := run([]string{"--input", input, "--output-dir", filepath.Join(root, "output")})
	encoded, err := json.Marshal(result)
	if err != nil || result.Reason != "archive_contract_refused" || strings.Contains(string(encoded), root) || strings.Contains(string(encoded), "private-source-marker") {
		t.Fatal("runtime diagnostics disclosed source details")
	}
}
