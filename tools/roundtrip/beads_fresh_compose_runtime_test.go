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

// The actual merged receiver and matching shared lifecycle are composed from
// this main commit without modifying their source. This caller earns runtime
// qualification only from its own executed continuations and readback.
const freshSharedCaptureSource = "8c32ea2f26d527f64f0f6747e8599373d3f9e7b6"

func freshBuildProducer(t *testing.T, root string) string {
	return (publicnative.SourceBuilder{T: t, Root: root, SourceDirectory: "testdata/fresh-native", Target: "//tools/native-physical-capture:native-physical-capture"}).Build(publicnative.CanonicalSource{
		Name: "beads-1a75f7", Commit: "1a75f75feae282890c4b9b8769c54e8b6a23c2f4", Tree: "6ee2f640b693e1104155013b46e2769a9b55d9ad", Parent: "295ba4e44f527deb51b94d69570e5f49a4316974",
		ArchiveSHA: "76ae171a1c7c1d2904a258807d94cbe15bba15bce24742dd5d5f5cbd01340925", ManifestSHA: "566e102e8909114fb20043da8438264e90d664ac273ec12d4be2fc07dc484954", Entries: 5248,
	})
}

func freshRetainConsumer(t *testing.T, root string) string {
	t.Helper()
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(checkout) {
		t.Fatal("actual consumer source checkout required")
	}
	identity, err := freshCommand(t, root, "consumer-source-identity", checkout, "/usr/bin/git", []string{"rev-parse", "HEAD", "HEAD^{tree}"}, nil, 30*time.Second)
	parts := strings.Fields(string(identity))
	if err != nil || len(parts) != 2 || len(parts[0]) != 40 || len(parts[1]) != 40 {
		t.Fatal("actual consumer source head/tree unavailable")
	}
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
	freshJSON(t, filepath.Join(root, "consumer-elf.json"), map[string]any{"source_runfile": source, "export": path, "sha256": freshFileSHA(t, path), "source_head": parts[0], "source_tree": parts[1], "workflow_head": os.Getenv("GITHUB_SHA"), "run_id": os.Getenv("GITHUB_RUN_ID"), "entry": "TestPublicIDStyleASameRunContinuation", "shared_dependency": freshSharedCaptureSource})
	return path
}

func freshNativeCase(t *testing.T, root string, source publicnative.SourceSession, item freshCase, inputs freshCommittedNativeInputs, producer, consumer string, result *freshResult) {
	t.Helper()
	freshGuard(t)
	caseRoot := filepath.Join(root, item.Engine+"-"+item.Name)
	if err := os.Mkdir(caseRoot, 0700); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(caseRoot, "workspace")
	for _, name := range []string{workspace, filepath.Join(workspace, "home"), filepath.Join(workspace, ".beads"), filepath.Join(caseRoot, "native-evidence")} {
		if err := os.Mkdir(name, 0700); err != nil {
			t.Fatal(err)
		}
	}
	// The source-side capture and independent oracle are immutable repository
	// inputs. Only the disposable extracted workspace and evidence are created
	// below the test's declared output root.
	mutable := filepath.Join(caseRoot, "native-continuation-root")
	if err := os.Mkdir(mutable, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := freshCommand(t, caseRoot, "extract-committed-workspace", caseRoot, "/usr/bin/tar", []string{"-xf", inputs.WorkspaceArchive, "-C", mutable}, freshChildEnv(workspace), 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	result.SourceRoot = mutable
	result.RetainedArchiveSHA256 = inputs.WorkspaceSHA256
	result.RecipeExportSHA256 = inputs.WorkspaceSHA256
	freshTreeManifest(t, mutable, filepath.Join(caseRoot, "committed-workspace-manifest.json"))
	freshGuard(t)
	result.CaptureSHA256 = inputs.CaptureSHA256
	// The committed request supplies the capture-set/oracle inputs. The
	// five-operation continuation request below remains generated evidence so
	// IDs, clocks, responses and SQL journal rows come from this run.
	proofRequest := inputs.Requests[item.Mode]
	if freshFileSHA(t, proofRequest) == "" {
		t.Fatal("committed native proof request missing")
	}
	restoreTime := inputs.RestoreTime
	var firstSequence json.RawMessage
	if item.Mode == "partial" {
		// A source ordinal, fixed before target creation. The consumer must
		// authenticate both this image and its predecessor.
		firstSequence = json.RawMessage("2")
	}
	result.NativeContinuation = "STARTED"
	request := freshNativeContinuation(t, item, filepath.Join(caseRoot, "native-evidence"), workspace, mutable, source.Binary, result.CaptureSHA256, freshFileSHA(t, producer), filepath.Join(inputs.Root, "requests", "continuation-plan.json"), firstSequence, *result)
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
	fieldProfile := freshRunfile(t, "tools/roundtrip/testdata/fresh-native/continuation-fields.json")
	env := []string{}
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "TEST_SHARD_") && !strings.HasPrefix(entry, "TEST_TOTAL_SHARDS=") && !strings.HasPrefix(entry, "PLUG_PUBLIC_PROOF_") && !strings.HasPrefix(entry, "PLUG_ID_STYLE_A_") {
			env = append(env, entry)
		}
	}
	env = append(env, "PLUG_ID_STYLE_A_RUN=1", "PLUG_ID_STYLE_A_REQUEST="+requestPath, "PLUG_ID_STYLE_A_REQUEST_SHA256="+result.RequestSHA256, "PLUG_ID_STYLE_A_FIELD_PROFILE="+fieldProfile,
		"PLUG_PUBLIC_PROOF_DRIVER="+item.Engine, "PLUG_PUBLIC_PROOF_ELF_SHA256="+result.ConsumerELFSHA256, "PLUG_PUBLIC_PROOF_REQUEST="+proofRequest, "PLUG_PUBLIC_PROOF_REQUEST_SHA256="+freshFileSHA(t, proofRequest), "PLUG_PUBLIC_PROOF_OUTPUT="+filepath.Join(caseRoot, "plug-proof"))
	result.PlugRestore, result.PlugContinuation = "STARTED", "STARTED"
	cwd := inputs.ReaderDirectory
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
