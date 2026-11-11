package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// The shared file is adopted byte for byte from this owner-supplied source.
// This identity grants no runtime or landing qualification to this caller.
const freshSharedCaptureSource = "3b041a43f4639ffb755c8d893d42e926b23ff1fb"

func freshBuildProducer(t *testing.T, root string) string {
	return (publicnative.SourceBuilder{T: t, Root: root, SourceDirectory: "testdata/fresh-native", Target: "//tools/native-physical-capture:native-physical-capture"}).Build(publicnative.CanonicalSource{
		Name: "beads-1a75f7", Commit: "1a75f75feae282890c4b9b8769c54e8b6a23c2f4", Tree: "6ee2f640b693e1104155013b46e2769a9b55d9ad", Parent: "295ba4e44f527deb51b94d69570e5f49a4316974",
		ArchiveSHA: "76ae171a1c7c1d2904a258807d94cbe15bba15bce24742dd5d5f5cbd01340925", ManifestSHA: "566e102e8909114fb20043da8438264e90d664ac273ec12d4be2fc07dc484954", Entries: 5248,
	})
}

func freshRunfile(t *testing.T, name string) string {
	t.Helper()
	if filepath.IsAbs(name) {
		return name
	}
	if name == "" {
		t.Fatal("required declared runfile absent")
	}
	path := filepath.Join(os.Getenv("TEST_SRCDIR"), name)
	path, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func freshRetainConsumer(t *testing.T, root string) string {
	t.Helper()
	source := freshRunfile(t, os.Getenv("PLUG_ID_STYLE_A_CONSUMER"))
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	magic := make([]byte, 4)
	if _, err := io.ReadFull(in, magic); err != nil || !bytes.Equal(magic, []byte{0x7f, 'E', 'L', 'F'}) {
		t.Fatal("same-package consumer runfile is not a real ELF")
	}
	if _, err := in.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "composed-id-style-a-test")
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0555)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatal(copyErr, closeErr)
	}
	if freshFileSHA(t, path) != freshFileSHA(t, source) {
		t.Fatal("composed consumer ELF changed on export")
	}
	freshJSON(t, filepath.Join(root, "consumer-elf.json"), map[string]any{"source_runfile": source, "export": path, "sha256": freshFileSHA(t, path), "source_head": os.Getenv("GITHUB_SHA"), "run_id": os.Getenv("GITHUB_RUN_ID"), "entry": "TestPublicIDStyleASameRunContinuation", "shared_dependency": freshSharedCaptureSource})
	return path
}

func freshNativeCase(t *testing.T, root string, source publicnative.SourceSession, item freshCase, producer, consumer string, result *freshResult) {
	t.Helper()
	freshGuard(t)
	caseRoot := filepath.Join(root, item.Engine+"-"+item.Name)
	if err := os.Mkdir(caseRoot, 0700); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(caseRoot, "workspace")
	for _, name := range []string{workspace, filepath.Join(workspace, "home"), filepath.Join(workspace, ".beads"), filepath.Join(caseRoot, "capture-output"), filepath.Join(caseRoot, "native-evidence")} {
		if err := os.Mkdir(name, 0700); err != nil {
			t.Fatal(err)
		}
	}
	// The unchanged native recipe accepts only a fresh child of a private,
	// canonical /tmp/plug-swarm directory. Keep evidence in declared outputs;
	// the complete quiescent source is archived there before capture/mutation.
	retainedParent, err := os.MkdirTemp("/tmp/plug-swarm", "id-style-a-native-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(retainedParent); err != nil {
			t.Errorf("owned native recipe source cleanup: %v", err)
		}
	})
	retained := filepath.Join(retainedParent, "clean-export-root")
	if err := freshRecordRecipe(t, source, item, caseRoot, retained, result); err != nil {
		t.Fatal(err)
	}
	result.SourceRoot = retained
	freshTreeManifest(t, retained, filepath.Join(caseRoot, "clean-export-manifest-before-capture.json"))
	archive := filepath.Join(caseRoot, "clean-export-root.tar")
	if _, err := freshCommand(t, caseRoot, "archive-clean-export", caseRoot, "/usr/bin/tar", []string{"-cf", archive, "-C", retained, "."}, freshChildEnv(workspace), 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	result.RetainedArchiveSHA256 = freshFileSHA(t, archive)
	mutable := filepath.Join(caseRoot, "native-continuation-root")
	freshCopyTree(t, retained, mutable)
	freshGuard(t)
	_, err = freshCommand(t, caseRoot, "native-delta-capture", workspace, producer, []string{"--data-dir", retained, "--database", "beads_test", "--beads-dir", filepath.Join(workspace, ".beads"), "--output-dir", filepath.Join(caseRoot, "capture-output"), "--disposable", "--native-delta"}, freshChildEnv(workspace), 25*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	freshTreeManifest(t, retained, filepath.Join(caseRoot, "clean-export-manifest-after-capture.json"))
	capture := filepath.Join(caseRoot, "capture-output", "capture-set")
	result.CaptureSHA256 = freshFileSHA(t, filepath.Join(capture, "capture-set.json"))
	// The oracle is authored from this capture before any target import. Each
	// case gets a new directory, scratch replay, ledger, clock and request.
	oracle := filepath.Join(caseRoot, "independent")
	restoreTime := time.Now().UTC().Format(time.RFC3339Nano)
	helper := freshRunfile(t, filepath.Join(os.Getenv("TEST_WORKSPACE"), "tools/roundtrip/testdata/fresh-native/independent-row-replay.tar.gz"))
	adapter := freshRunfile(t, filepath.Join(os.Getenv("TEST_WORKSPACE"), "tools/roundtrip/fresh_oracle.py"))
	oracleArgs := []string{adapter, "--capture", capture, "--output", oracle, "--helpers", helper, "--helpers-sha256", "b826c9e069e000007cc1f3a7c5c137a2d5113cb51276b84ceec433636076ff02", "--restore-time", restoreTime, "--producer-head", "1a75f75feae282890c4b9b8769c54e8b6a23c2f4", "--restore-mode", item.Mode}
	var firstSequence json.RawMessage
	if item.Mode == "partial" {
		// A source ordinal, fixed before target creation. The independent
		// oracle must authenticate both this image and its predecessor.
		firstSequence = json.RawMessage("2")
		oracleArgs = append(oracleArgs, "--restore-first-sequence", "2")
	}
	_, err = freshCommand(t, caseRoot, "independent-oracle", workspace, "/usr/bin/python3", oracleArgs, freshChildEnv(workspace), 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	result.NativeContinuation = "STARTED"
	request := freshNativeContinuation(t, item, filepath.Join(caseRoot, "native-evidence"), workspace, mutable, source.Binary, result.CaptureSHA256, freshFileSHA(t, producer), firstSequence, *result)
	result.NativeContinuation = "COMPLETED"
	requestPath := filepath.Join(caseRoot, "native-evidence", "native-continuation-request.json")
	result.RequestSHA256 = freshFileSHA(t, requestPath)
	result.ConsumerELFSHA256 = freshFileSHA(t, consumer)
	requestBinding := map[string]any{"format": "public-id-style-a-case-input-binding/1", "case": request.Case, "run_id": os.Getenv("GITHUB_RUN_ID"), "capture_sha256": result.CaptureSHA256, "native_request_sha256": result.RequestSHA256, "consumer_elf_sha256": result.ConsumerELFSHA256, "restore_time": restoreTime, "shared_dependency": freshSharedCaptureSource, "plug_execution": "NOT_RUN", "equality_credit": 0}
	requestBinding["restore_mode"] = item.Mode
	if firstSequence != nil {
		requestBinding["restore_first_sequence"] = firstSequence
	}
	freshJSON(t, filepath.Join(caseRoot, "FROZEN-CASE-INPUTS.json"), requestBinding)
	for _, file := range []string{requestPath, filepath.Join(caseRoot, "FROZEN-CASE-INPUTS.json")} {
		if err := os.Chmod(file, 0444); err != nil {
			t.Fatal(err)
		}
	}
	freshGuard(t)
	proofRequest := filepath.Join(oracle, "request.json")
	fieldProfile := freshRunfile(t, filepath.Join(os.Getenv("TEST_WORKSPACE"), "tools/roundtrip/testdata/fresh-native/continuation-fields.json"))
	env := []string{}
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "TEST_SHARD_") && !strings.HasPrefix(entry, "TEST_TOTAL_SHARDS=") && !strings.HasPrefix(entry, "PLUG_PUBLIC_PROOF_") && !strings.HasPrefix(entry, "PLUG_ID_STYLE_A_") {
			env = append(env, entry)
		}
	}
	env = append(env, "PLUG_ID_STYLE_A_RUN=1", "PLUG_ID_STYLE_A_REQUEST="+requestPath, "PLUG_ID_STYLE_A_REQUEST_SHA256="+result.RequestSHA256, "PLUG_ID_STYLE_A_FIELD_PROFILE="+fieldProfile,
		"PLUG_PUBLIC_PROOF_DRIVER="+item.Engine, "PLUG_PUBLIC_PROOF_ELF_SHA256="+result.ConsumerELFSHA256, "PLUG_PUBLIC_PROOF_REQUEST="+proofRequest, "PLUG_PUBLIC_PROOF_REQUEST_SHA256="+freshFileSHA(t, proofRequest), "PLUG_PUBLIC_PROOF_OUTPUT="+filepath.Join(caseRoot, "plug-proof"))
	result.PlugRestore, result.PlugContinuation = "STARTED", "STARTED"
	cwd := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip")
	raw, err := freshCommand(t, caseRoot, "plug-same-process-continuation", cwd, consumer, []string{"-test.run=^TestPublicIDStyleASameRunContinuation$", "-test.v", "-test.timeout=2h"}, env, 125*time.Minute)
	if err != nil {
		t.Fatal("actual Plug import/continuation failed; original output retains any typed refusal: ", err)
	}
	if bytes.Count(raw, []byte("--- PASS: TestPublicIDStyleASameRunContinuation (")) != 1 || bytes.Contains(raw, []byte("--- SKIP:")) || bytes.Contains(raw, []byte("--- FAIL:")) {
		t.Fatal("actual selected consumer did not run/pass exactly once")
	}
	for _, name := range []string{"SUCCESS.json", "id-result.json", "post-continuation-reopen.json"} {
		data, err := os.ReadFile(filepath.Join(caseRoot, "plug-proof", name))
		if err != nil {
			t.Fatal(err)
		}
		var observed map[string]any
		if strictTypedJSON(data, &observed) != nil {
			t.Fatal("invalid original consumer success record")
		}
		if name == "SUCCESS.json" && (observed["capture_sha256"] != result.CaptureSHA256 || observed["elf_sha256"] != result.ConsumerELFSHA256 || observed["post_continuation_reopen_asserted"] != true || observed["restore_mode"] != item.Mode) {
			t.Fatal("shared original success binding differs")
		}
		if name == "SUCCESS.json" {
			evidence, supplied := observed["native_delta_history"]
			if item.Mode == "full" {
				if supplied {
					t.Fatal("Full acquired a selected-mode evidence declaration")
				}
			} else {
				selected, ok := evidence.(map[string]any)
				if !ok || selected["mode"] != item.Mode || selected["archiveSha256"] != result.CaptureSHA256 {
					t.Fatal("actual selected-mode evidence/source differs")
				}
				wantFirst := json.Number("0")
				if item.Mode == "partial" {
					wantFirst = json.Number("2")
				}
				if selected["firstSequence"] != wantFirst {
					t.Fatal("actual selected-mode evidence changed the source ordinal")
				}
			}
		}
		if name == "id-result.json" && (observed["case"] != request.Case || observed["restore_mode"] != item.Mode || observed["equality"] != true || observed["post_write_reopen_asserted"] != true) {
			t.Fatal("actual native equality incomplete")
		}
		if name == "id-result.json" {
			var wantFirst any
			if item.Mode == "partial" {
				wantFirst = json.Number("2")
			}
			if observed["restore_first_sequence"] != wantFirst {
				t.Fatal("actual consumer compared a different source ordinal")
			}
		}
		if name == "post-continuation-reopen.json" && (observed["driver"] != item.Engine || observed["restore_mode"] != item.Mode || observed["completed"] != true) {
			t.Fatal("actual post-write reopen mode/driver differs")
		}
	}
	result.PlugRestore, result.PlugContinuation = "COMPLETED", "COMPLETED"
	result.Status, result.EqualityCredit = "PASS", 1
}
