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
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
)

func TestHarnessHistoryModesAndPrivateOutput(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "plug-roundtrip-public-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	for _, engine := range []string{"memory", "sqlite"} {
		for _, mode := range []string{"full", "partial", "snapshot"} {
			t.Run(engine+"/"+mode, func(t *testing.T) {
				output := filepath.Join(root, engine+"-"+mode)
				result := run([]string{"--input", "../../internal/integration/offlinehistory/testdata/dolt-source-partial.tar", "--output-dir", output, "--history-mode", mode, "--engine", engine, "--all"})
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
				if result.Status != "WAIT" || result.Stage != "beads_artifact" || result.Reason != "pinned_beads_artifact_required" || result.BeadsReimport != "WAIT" || result.BeadsReadback != "WAIT" || result.Writes != 1 || result.Claims != 1 || result.BaselineSHA256 == result.ReverseSHA256 || result.HistoryLossItems == 0 {
					t.Fatalf("public runtime workload: %+v", result)
				}
				encoded, _ := json.Marshal(result)
				t.Logf("PUBLIC_CONVERTER_OUTCOME %s", encoded)
				if result.SourceHistoryComplete || result.SourceHistoryFirst != 3 || result.SourceHistoryHead != 3 || len(result.SourceHistoryGaps) != 2 || result.RestoredIncarnation == "" || result.ReverseIncarnation != result.RestoredIncarnation || result.ReverseAt == "" || result.HistoryFloor != "1" {
					t.Fatalf("history boundary: %+v", result)
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
}

func TestHarnessDefaultExportDoesNotExerciseOmittedBeads(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "plug-roundtrip-default-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	for _, engine := range []string{"memory", "sqlite"} {
		for _, exercise := range []bool{true, false} {
			name := "no-exercise"
			if exercise {
				name = "exercise"
			}
			t.Run(engine+"/"+name, func(t *testing.T) {
				output := filepath.Join(root, engine+"-"+name)
				assertDefaultEphemeralOutcome(t, output, []string{"--engine", engine}, exercise)
			})
		}
	}
}

func assertDefaultEphemeralOutcome(t *testing.T, output string, args []string, exercise bool) {
	t.Helper()
	args = append(args, "--input", "../../internal/integration/offlinehistory/testdata/dolt-source-partial.tar", "--output-dir", output, "--history-mode", "partial")
	if !exercise {
		args = append(args, "--exercise=false")
	}
	result := run(args)
	if result.Claims != 0 || result.Writes != 0 || result.BaselineSHA256 == "" {
		t.Fatalf("default omission performed a workload: %+v", result)
	}
	if exercise {
		if result.Status != "BLOCK" || result.Stage != "workload" || result.Reason != "workload_requires_exported_bead" || result.ReverseSHA256 != "" {
			t.Fatalf("omitted workload was not refused: %+v", result)
		}
		if _, err := os.Stat(filepath.Join(output, "reverse-live.jsonl")); !os.IsNotExist(err) {
			t.Fatal("refused workload published a live reverse")
		}
		return
	}
	if result.Status != "WAIT" || result.Stage != "beads_artifact" || result.Reason != "pinned_beads_artifact_required" || result.ReverseSHA256 != result.BaselineSHA256 || result.ReverseIncarnation != result.RestoredIncarnation || result.ReverseAt == "" {
		t.Fatalf("default read-only reverse: %+v", result)
	}
	data, err := os.ReadFile(filepath.Join(output, "reverse-live.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var line map[string]any
		if json.Unmarshal([]byte(raw), &line) != nil || line["_type"] == "issue" {
			t.Fatal("default export emitted an omitted bead")
		}
	}
	data, err = os.ReadFile(filepath.Join(output, "reverse-report.json"))
	var report reverseconverter.Report
	if err != nil || json.Unmarshal(data, &report) != nil || len(report.OmittedEphemeralRows) != 3 {
		t.Fatal("default export lost its explicit omissions")
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

func TestHarnessGenuinePublicCompleteAndPartialProducerControls(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "plug-roundtrip-producer-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	for _, engine := range []string{"memory", "sqlite"} {
		for _, test := range publicProducerCases() {
			t.Run(engine+"/"+test.name, func(t *testing.T) {
				args := []string{"--input", "../../internal/integration/converterroundtrip/testdata/" + test.file, "--output-dir", filepath.Join(root, engine+"-"+test.name), "--engine", engine}
				if !test.complete {
					args = append(args, "--all")
				}
				if test.mode != "" {
					args = append(args, "--history-mode", test.mode)
				}
				assertPublicProducerOutcome(t, test, run(args))
			})
		}
	}
}

type publicProducerCase struct {
	name, file, mode, head, floor string
	complete                      bool
	first, sourceHead             uint64
	gaps                          int
}

func publicProducerCases() []publicProducerCase {
	return []publicProducerCase{
		{"complete-default", "public-complete-root-990eb800.tar", "", "8", "0", true, 1, 7, 0},
		{"complete-snapshot", "public-complete-root-990eb800.tar", "snapshot", "1", "1", true, 1, 7, 0},
		{"partial-default-refused", "public-partial-990eb800.tar", "", "", "", false, 4, 4, 3},
		{"partial", "public-partial-990eb800.tar", "partial", "3", "1", false, 4, 4, 3},
		{"partial-snapshot", "public-partial-990eb800.tar", "snapshot", "1", "1", false, 4, 4, 3},
	}
}

func assertPublicProducerOutcome(t *testing.T, test publicProducerCase, got outcome) {
	t.Helper()
	if test.name == "partial-default-refused" {
		if got.Status != "BLOCK" || got.Stage != "history" || got.Reason != "history_non-contiguous" || got.ReverseSHA256 != "" || got.RestoredIncarnation != "" {
			t.Fatalf("strict genuine partial boundary: %+v", got)
		}
		return
	}
	if got.Status != "WAIT" || got.Reason != "pinned_beads_artifact_required" || got.BeadsReimport != "WAIT" || got.BeadsReadback != "WAIT" || got.Claims != 1 || got.Writes != 1 || got.RestoredHead != test.head || got.HistoryFloor != test.floor || got.SourceHistoryComplete != test.complete || got.SourceHistoryFirst != test.first || got.SourceHistoryHead != test.sourceHead || len(got.SourceHistoryGaps) != test.gaps || got.RestoredIncarnation == "" || got.RestoredIncarnation != got.ReverseIncarnation || got.ReverseAt == "" || got.BaselineSHA256 == got.ReverseSHA256 || got.HistoryLossItems == 0 {
		t.Fatalf("genuine producer runtime boundary: %+v", got)
	}
	encoded, _ := json.Marshal(got)
	t.Logf("PUBLIC_PRODUCER_RUNTIME %s", encoded)
}
