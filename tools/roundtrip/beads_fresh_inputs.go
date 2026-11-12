package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// The public/synthetic packet is an authenticated repository runfile. Only a
// disposable copy is unpacked; no ambient local fixture path is accepted.
const freshNativeInputsDirectory = "tools/roundtrip/testdata/fresh-native/id-style-a-inputs"
const freshNativeInputsRunfile = freshNativeInputsDirectory + ".tar.gz"
const freshNativeInputsProvenanceRunfile = "tools/roundtrip/testdata/fresh-native/id-style-a-inputs.provenance.json"

const freshNativeInputsMaxBytes = 20 << 20

type freshCommittedNativeInputs struct {
	Root, WorkspaceArchive, CaptureSet, Oracle string
	ReaderDirectory                            string
	Requests                                   map[string]string
	CaptureSHA256, LedgerSHA256, StatesSHA256  string
	WorkspaceSHA256                            string
	RestoreTime                                string
}

type freshInputFileBinding struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	Mode   string `json:"mode"`
}

// freshRunfile resolves a declared runfile and rejects a missing or changed
// symlink target. Bazel supplies source-relative names for committed inputs,
// while rlocationpath values may already carry TEST_WORKSPACE (for example,
// "_main/bazel-out/..."). Handle both forms without duplicating the workspace
// component. Absolute names are accepted for existing producer/consumer
// runfiles, but committed native inputs use the constant above and therefore
// always resolve from TEST_SRCDIR/TEST_WORKSPACE.
func freshRunfile(t *testing.T, name string) string {
	t.Helper()
	if filepath.IsAbs(name) {
		return name
	}
	if name == "" || !filepath.IsLocal(filepath.ToSlash(name)) || filepath.ToSlash(filepath.Clean(name)) != name {
		t.Fatal("required declared runfile absent")
	}
	root, workspace := os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE")
	if root == "" || workspace == "" || workspace == "." || workspace == ".." || strings.ContainsAny(workspace, "/\\") {
		// Source-only tests can run from tools/roundtrip without Bazel. Runtime
		// tests always have TEST_SRCDIR and take the checked-in runfile path.
		local := filepath.FromSlash(name)
		if _, err := os.Stat(local); err != nil {
			t.Fatal("required declared runfile absent")
		}
		return local
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		t.Fatal("Bazel runfiles root is unavailable")
	}
	base := filepath.Join(root, workspace)
	if strings.HasPrefix(name, workspace+"/") {
		// $(rlocationpath ...) already includes the workspace name. Joining it
		// below base would produce the observed TEST_SRCDIR/_main/_main/... path.
		base = root
	}
	path := filepath.Join(base, filepath.FromSlash(name))
	path, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func loadFreshCommittedNativeInputs(t *testing.T) freshCommittedNativeInputs {
	t.Helper()
	archiveBytes, err := os.ReadFile(freshRunfile(t, freshNativeInputsRunfile))
	if err != nil || hash(archiveBytes) != freshNativeInputsArchiveSHA {
		t.Fatal("committed native input archive binding changed", err)
	}
	sidecar, err := os.ReadFile(freshRunfile(t, freshNativeInputsProvenanceRunfile))
	if err != nil || hash(sidecar) != freshNativeInputsProvenanceSHA {
		t.Fatal("committed native input provenance binding changed", err)
	}
	// Preserve the original repository-relative request paths, including the
	// exact capture-set/oracle locations. The existing reader uses this owned
	// directory as its cwd; no request byte or shared reader is rewritten.
	readerDirectory := t.TempDir()
	root := filepath.Join(readerDirectory, filepath.FromSlash(freshNativeInputsDirectory))
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := unpackFreshNativeInputs(archiveBytes, root, freshNativeInputsMaxBytes, freshNativeInputsArchiveFiles); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatal("committed native input root is not a regular directory")
	}
	read := func(name string) []byte {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(name))
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil || !filepath.IsLocal(filepath.ToSlash(rel)) || filepath.ToSlash(filepath.Clean(rel)) != name {
			t.Fatal("committed native input path escapes root")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read committed native input %s: %v", name, err)
		}
		return data
	}
	if !bytes.Equal(read("provenance.json"), sidecar) {
		t.Fatal("archived native provenance differs from committed sidecar")
	}
	var provenance struct {
		Schema        string         `json:"schema"`
		Kind          string         `json:"kind"`
		Qualification string         `json:"qualification"`
		Source        map[string]any `json:"source"`
		Limits        struct {
			MaxAggregateBytes int64 `json:"max_aggregate_bytes"`
			AggregateBytes    int64 `json:"aggregate_bytes"`
			RegularFileCount  int   `json:"regular_file_count"`
		} `json:"limits"`
		Workspace struct {
			Path                      string `json:"path"`
			SHA256                    string `json:"sha256"`
			Size                      int64  `json:"size"`
			Mode                      string `json:"mode"`
			ExtractOnlyDisposableCopy bool   `json:"extract_only_disposable_copy"`
		} `json:"workspace"`
		CaptureSet struct {
			Path              string                  `json:"path"`
			ReadAPI           string                  `json:"read_api"`
			ManifestSHA256    string                  `json:"manifest_sha256"`
			StreamSHA256      string                  `json:"stream_sha256"`
			StreamBytes       int64                   `json:"stream_bytes"`
			PartCount         int                     `json:"part_count"`
			HeadArchiveSHA256 string                  `json:"head_archive_declared_sha256"`
			HeadArchiveSize   int64                   `json:"head_archive_declared_size"`
			Files             []freshInputFileBinding `json:"files"`
		} `json:"capture_set"`
		Oracle struct {
			Path                            string `json:"path"`
			Ledger                          string `json:"ledger"`
			LedgerSHA256                    string `json:"ledger_sha256"`
			States                          string `json:"states"`
			StatesSHA256                    string `json:"states_sha256"`
			CompleteAuthenticatedStateCount int    `json:"complete_authenticated_state_count"`
			TableFiles                      int    `json:"table_files"`
		} `json:"oracle"`
		RequestTemplates map[string]string       `json:"request_templates"`
		GeneratedOutputs map[string]any          `json:"generated_outputs"`
		Files            []freshInputFileBinding `json:"files"`
	}
	if err := strictTypedJSON(read("provenance.json"), &provenance); err != nil || provenance.Schema != "plug-id-style-a-native-inputs/1" || provenance.Kind != "public-synthetic-native-fixture" {
		t.Fatal("committed native provenance schema changed")
	}
	if provenance.Limits.MaxAggregateBytes != freshNativeInputsMaxBytes || provenance.Limits.AggregateBytes < 1 || provenance.Limits.AggregateBytes > freshNativeInputsMaxBytes || provenance.Limits.RegularFileCount != len(provenance.Files) {
		t.Fatal("committed native input aggregate limit/census changed")
	}
	bindings := make(map[string]freshInputFileBinding, len(provenance.Files))
	var aggregate int64
	for _, binding := range provenance.Files {
		if binding.Path == "" || binding.Path == "provenance.json" || !filepath.IsLocal(binding.Path) || filepath.ToSlash(filepath.Clean(binding.Path)) != binding.Path || len(binding.SHA256) != 64 || binding.Size < 0 || binding.Mode != "0644" {
			t.Fatalf("invalid committed native file binding: %+v", binding)
		}
		if _, exists := bindings[binding.Path]; exists {
			t.Fatal("duplicate committed native file binding")
		}
		path := filepath.Join(root, filepath.FromSlash(binding.Path))
		// Bazel materializes data files as runfile symlinks. Stat follows that
		// runfile indirection while retaining the committed mode/size contract.
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || fmt.Sprintf("%04o", info.Mode().Perm()) != binding.Mode || info.Size() != binding.Size {
			t.Fatalf("committed native file mode/size/type changed: %s", binding.Path)
		}
		data := read(binding.Path)
		if int64(len(data)) != binding.Size || hash(data) != binding.SHA256 {
			t.Fatalf("committed native file digest changed: %s", binding.Path)
		}
		aggregate += binding.Size
		bindings[binding.Path] = binding
	}
	if aggregate != provenance.Limits.AggregateBytes {
		t.Fatal("committed native aggregate byte count changed")
	}
	var actualFiles int
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if info.IsDir() {
				return fmt.Errorf("committed native input contains a symlinked directory: %s", path)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("committed native input contains a symlink to a nonregular file: %s", path)
			}
		} else if entry.IsDir() {
			return nil
		} else if !entry.Type().IsRegular() {
			return fmt.Errorf("committed native input contains a nonregular file: %s", path)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "provenance.json" {
			return nil
		}
		actualFiles++
		if _, ok := bindings[filepath.ToSlash(rel)]; !ok {
			return fmt.Errorf("committed native file is not in provenance: %s", rel)
		}
		return nil
	})
	if err != nil || actualFiles != len(bindings) {
		t.Fatal("committed native file census differs", err)
	}
	manifest := read("capture-set/capture-set.json")
	var capture struct {
		Format      string   `json:"format"`
		Contract    string   `json:"contract"`
		StoreID     string   `json:"store_id"`
		Projects    []string `json:"projects"`
		Head        string   `json:"head"`
		HeadArchive struct {
			SHA256 string `json:"sha256"`
			Size   int64  `json:"size"`
		} `json:"head_archive"`
		PartCount       int    `json:"part_count"`
		FinalPartSHA256 string `json:"final_part_sha256"`
		StreamSHA256    string `json:"stream_sha256"`
		StreamBytes     int64  `json:"stream_bytes"`
		Totals          struct {
			Schemas            int `json:"schemas"`
			Commits            int `json:"commits"`
			TableTransactions  int `json:"table_transactions"`
			RowOperations      int `json:"row_operations"`
			ReconstructedRows  int `json:"reconstructed_rows"`
			ReconstructedCells int `json:"reconstructed_cells"`
			FullImageBytes     int `json:"full_image_bytes"`
		} `json:"totals"`
	}
	if err := strictTypedJSON(manifest, &capture); err != nil || capture.Format != "beads-native-capture-set/2" || capture.Contract != "beads-native-delta/2" || hash(manifest) != provenance.CaptureSet.ManifestSHA256 || capture.StoreID == "" || len(capture.Projects) != 1 || capture.Head == "" || capture.PartCount != provenance.CaptureSet.PartCount || capture.StreamSHA256 != provenance.CaptureSet.StreamSHA256 || capture.StreamBytes != provenance.CaptureSet.StreamBytes || capture.HeadArchive.SHA256 != provenance.CaptureSet.HeadArchiveSHA256 || capture.HeadArchive.Size != provenance.CaptureSet.HeadArchiveSize || capture.Totals.Commits < 1 {
		t.Fatal("authenticated capture-set manifest binding changed")
	}
	if provenance.CaptureSet.Path != "capture-set" || provenance.CaptureSet.ReadAPI != "dumpformat.ReadNativeCaptureSetDirectory" || len(provenance.CaptureSet.Files) != 3 || provenance.CaptureSet.PartCount != 1 {
		t.Fatal("authenticated capture-set file census changed")
	}
	for _, binding := range provenance.CaptureSet.Files {
		if got, ok := bindings[binding.Path]; !ok || got != binding {
			t.Fatal("capture-set inner binding is not in complete provenance", binding.Path)
		}
	}
	archive, err := readFreshCommittedCaptureSet(t, root, provenance.CaptureSet.PartCount)
	if err != nil {
		t.Fatalf("committed capture-set authentication failed: %v", err)
	}
	archiveSHA := archive.SHA256()
	proof, proofOK := archive.NativeDeltaVerification()
	archive.ReleaseNativeDecoded()
	if !proofOK || proof.Totals.Commits != uint64(capture.Totals.Commits) || archiveSHA != provenance.CaptureSet.ManifestSHA256 {
		t.Fatal("authenticated capture-set proof changed")
	}
	for mode, name := range map[string]string{"full": "requests/full.json", "partial": "requests/partial.json", "snapshot": "requests/snapshot.json"} {
		if provenance.RequestTemplates[mode] != name {
			t.Fatalf("missing committed %s request template", mode)
		}
		raw := read(name)
		var request struct {
			Format               string          `json:"format"`
			Capture              string          `json:"capture"`
			CaptureSHA256        string          `json:"capture_sha256"`
			Source               map[string]any  `json:"source_identity"`
			ProducerHead         string          `json:"producer_head"`
			Oracle               string          `json:"oracle"`
			LedgerSHA256         string          `json:"ledger_sha256"`
			StatesSHA256         string          `json:"states_sha256"`
			RestoreTime          string          `json:"restore_time"`
			RestoreMode          string          `json:"restore_mode"`
			RestoreFirstSequence json.RawMessage `json:"restore_first_sequence"`
			JournalTable         string          `json:"journal_table"`
			Commits              int             `json:"commits"`
		}
		if err := strictTypedJSON(raw, &request); err != nil || request.Format != "public-native-capture-proof/1" || request.Capture != freshNativeInputsDirectory+"/capture-set" || request.CaptureSHA256 != provenance.CaptureSet.ManifestSHA256 || request.Oracle != freshNativeInputsDirectory+"/oracle" || request.LedgerSHA256 != provenance.Oracle.LedgerSHA256 || request.StatesSHA256 != provenance.Oracle.StatesSHA256 || request.RestoreMode != mode || request.JournalTable != "bd_events_journal" || request.Commits != capture.Totals.Commits || request.RestoreTime == "" || strings.Contains(string(raw), "/tmp/") {
			t.Fatalf("committed %s request template is not repository-relative", mode)
		}
		if mode == "partial" && string(request.RestoreFirstSequence) != "2" || mode != "partial" && len(request.RestoreFirstSequence) != 0 {
			t.Fatalf("committed %s source ordinal changed", mode)
		}
	}
	var fullRequest map[string]any
	if err := strictTypedJSON(read("requests/full.json"), &fullRequest); err != nil {
		t.Fatal("committed restore clock binding is missing")
	}
	restoreTime, ok := fullRequest["restore_time"].(string)
	if !ok || restoreTime == "" {
		t.Fatal("committed restore clock binding is missing")
	}
	if provenance.Workspace.Path != "workspace-root.tar" || provenance.Workspace.SHA256 != hash(read(provenance.Workspace.Path)) || provenance.Workspace.Size != int64(len(read(provenance.Workspace.Path))) || provenance.Workspace.Mode != "0644" {
		t.Fatal("workspace input binding changed")
	}
	if provenance.Oracle.Path != "oracle" || provenance.Oracle.Ledger != "tables.ndjson" || provenance.Oracle.States != "states.ndjson" || provenance.Oracle.CompleteAuthenticatedStateCount != capture.Totals.Commits+2 || provenance.Oracle.LedgerSHA256 != hash(read("oracle/tables.ndjson")) || provenance.Oracle.StatesSHA256 != hash(read("oracle/states.ndjson")) {
		t.Fatal("independent oracle binding changed")
	}
	return freshCommittedNativeInputs{
		Root: root, WorkspaceArchive: filepath.Join(root, "workspace-root.tar"), CaptureSet: filepath.Join(root, "capture-set"), Oracle: filepath.Join(root, "oracle"),
		ReaderDirectory: readerDirectory,
		Requests:        map[string]string{"full": filepath.Join(root, "requests/full.json"), "partial": filepath.Join(root, "requests/partial.json"), "snapshot": filepath.Join(root, "requests/snapshot.json")},
		CaptureSHA256:   provenance.CaptureSet.ManifestSHA256, LedgerSHA256: provenance.Oracle.LedgerSHA256, StatesSHA256: provenance.Oracle.StatesSHA256,
		WorkspaceSHA256: provenance.Workspace.SHA256, RestoreTime: restoreTime,
	}
}

// The plan's command vectors are an independent input contract. Decode every
// declared field without discarding unknown fields, then require the original
// root/child/comment/comment/child order and native command for each step.
func decodeFreshContinuationPlan(data []byte) ([]string, error) {
	var plan struct {
		Schema                   string `json:"schema"`
		Status                   string `json:"status"`
		GeneratedRequest         string `json:"generated_request"`
		GeneratedRequestIsOutput bool   `json:"generated_request_is_output"`
		Note                     string `json:"note"`
		Operations               []struct {
			Kind          string   `json:"kind"`
			NativeCommand []string `json:"native_command"`
		} `json:"operations"`
	}
	if err := strictTypedJSON(data, &plan); err != nil {
		return nil, fmt.Errorf("committed continuation plan decode: %w", err)
	}
	if plan.Schema != "public-id-style-a-continuation-plan/1" || plan.Status != "PUBLIC_SYNTHETIC_INPUT_PLAN" || plan.GeneratedRequest != "native-continuation-request.json" || !plan.GeneratedRequestIsOutput || plan.Note == "" || len(plan.Operations) != 5 {
		return nil, fmt.Errorf("committed continuation plan changed")
	}
	want := []string{"root", "child", "comment", "comment", "child"}
	commands := [][]string{{"create"}, {"create", "--parent"}, {"comments", "add"}, {"comments", "add"}, {"create", "--parent"}}
	steps := make([]string, len(want))
	for i, operation := range plan.Operations {
		if operation.Kind != want[i] || len(operation.NativeCommand) != len(commands[i]) {
			return nil, fmt.Errorf("committed continuation operation %d changed", i)
		}
		for j, token := range operation.NativeCommand {
			if token != commands[i][j] {
				return nil, fmt.Errorf("committed continuation command %d changed", i)
			}
		}
		steps[i] = operation.Kind
	}
	return steps, nil
}

// Bazel's runfiles tree exposes declared files through symlinks, while the
// directory reader deliberately rejects links to protect production inputs
// from path races. Authenticate those same committed bytes through the stream
// reader in that environment; a materialized source tree still exercises the
// descriptor-scoped directory reader.
func readFreshCommittedCaptureSet(t *testing.T, root string, partCount int) (*dumpformat.Archive, error) {
	t.Helper()
	captureRoot := filepath.Join(root, "capture-set")
	linked := false
	for _, name := range []string{"capture-set.json", "head.tar", "parts"} {
		info, err := os.Lstat(filepath.Join(captureRoot, name))
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			linked = true
		}
	}
	if !linked {
		return dumpformat.ReadNativeCaptureSetDirectory(context.Background(), captureRoot, dumpformat.NativeCaptureSetOptions{})
	}
	read := func(name string) ([]byte, error) {
		path := filepath.Join(captureRoot, filepath.FromSlash(name))
		return os.ReadFile(path)
	}
	rootBytes, err := read("capture-set.json")
	if err != nil {
		return nil, err
	}
	headBytes, err := read("head.tar")
	if err != nil {
		return nil, err
	}
	open := func(ctx context.Context, ordinal uint64) (io.ReadCloser, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if ordinal == 0 || ordinal > uint64(partCount) {
			return nil, fmt.Errorf("native delta part ordinal differs")
		}
		return os.Open(filepath.Join(captureRoot, "parts", fmt.Sprintf("%020d.part", ordinal)))
	}
	return dumpformat.ReadNativeCaptureSet(context.Background(), bytes.NewReader(rootBytes), bytes.NewReader(headBytes), open, dumpformat.NativeCaptureSetOptions{})
}
