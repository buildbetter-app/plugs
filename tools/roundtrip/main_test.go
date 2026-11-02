package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
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
			if result.Status != "WAIT" || result.Stage != "beads_reimport" || result.Reason != "pinned_beads_artifact_required" || result.BeadsReimport != "WAIT" || result.BeadsReadback != "WAIT" || result.Writes != 1 || result.Claims != 1 || result.BaselineSHA256 == result.ReverseSHA256 || result.HistoryLossItems == 0 {
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

func TestHarnessNativeProfileCannotDiscloseManifestKind(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "plug-roundtrip-public-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	const marker = "untrusted-source-kind-marker"
	data := nativeEnvelopeWithKind(t, marker)
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil || !archive.Native() {
		t.Fatalf("public regression fixture must be a valid native envelope: %v", err)
	}
	input := filepath.Join(root, "untrusted-source-path-marker")
	if err := os.WriteFile(input, data, 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "output")
	result := run([]string{"--input", input, "--output-dir", output})
	encoded, err := json.Marshal(result)
	if err != nil || result.Status != "BLOCK" || result.Reason != "source_image_refused" || len(result.Counts) != 0 || strings.Contains(string(encoded), marker) || strings.Contains(string(encoded), root) || strings.Contains(string(encoded), filepath.Base(input)) {
		t.Fatal("unsupported profile disclosed source details or untrusted counts")
	}
	entries, err := os.ReadDir(output)
	if err != nil || len(entries) != 0 {
		t.Fatal("unsupported profile produced reverse files")
	}
}

// Native manifests do not interpret the Beads kind registry. This valid
// transport envelope deliberately includes an untrusted registry entry.
func nativeEnvelopeWithKind(t *testing.T, kind string) []byte {
	t.Helper()
	paths := []string{"changesets.ndjson", "config/sources.json", "records/native.ndjson", "versions.ndjson"}
	members := map[string][]byte{"config/sources.json": []byte("{}\n")}
	manifest := dumpformat.Manifest{
		Format: dumpformat.Format, Schema: 1, Scope: "plug-native-store", History: "head",
		NativeStoreID: "public-native-store", NativeIncarnation: "public-incarnation", NativeHead: "0",
		Kinds: []dumpformat.Kind{{Name: kind, Count: 7}}, Files: map[string]dumpformat.File{},
		Projects: []dumpformat.Project{}, Sources: []dumpformat.Disposition{}, Warnings: []string{}, Verification: map[string]string{},
	}
	h := sha256.New()
	for _, path := range paths {
		data := members[path]
		sum := sha256.Sum256(data)
		manifest.Files[path] = dumpformat.File{SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data))}
		_, _ = h.Write([]byte(path))
		_, _ = h.Write(data)
	}
	manifest.NativeDigest = hex.EncodeToString(h.Sum(nil))
	var err error
	members["manifest.json"], err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	for _, path := range append([]string{"manifest.json"}, paths...) {
		body := members[path]
		if err := tw.WriteHeader(&tar.Header{Name: path, Mode: 0o600, Size: int64(len(body)), ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}
