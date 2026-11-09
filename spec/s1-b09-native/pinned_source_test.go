package b09native

import (
	"bytes"
	"context"
	"crypto/sha1"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	styleAPin         = "9d6fff760b7a314b476539198504d77ce0959656"
	styleATree        = "cde121d6e4119e454857ad1c3ca293e2791be840"
	styleAParent      = "cadad184cefce7da159ea4b18c9bd44865e72f6b"
	styleAArchiveSHA  = "8aa8d07655ec6903703a4c94a4b1aa12d4fb509ab647cd1958f73f8342d3fe6e"
	styleAManifestSHA = "23d84a56302ad19c50d9d25af733b2da50f9137aaf9308b273cab039b6023ae3"
	styleAActor       = "user:b09-native-review"
)

type styleASource struct {
	Name, Commit, Tree, Parent, ArchiveSHA, ManifestSHA string
	Entries                                             int
}

var styleABehaviorSource = styleASource{"beads-9d6fff", styleAPin, styleATree, styleAParent, styleAArchiveSHA, styleAManifestSHA, 5087}
var styleAExporterSource = styleASource{
	"beads-446dbb", "446dbbdaf978385099aeb8625e08016889fffccf", "64f0d22c8622c2a20da2bfe8c3ad49458fd12b06", "a367be65df60baa1ee8444f1e437904122f25f87",
	"32034b6d242f252144f6353f0d9d2f8b7d50739faf7973d62de93cafe25f3959", "a2456b25849b281d86ac57a461e21e0cc244cd80602c20269d88e7b9e28bc246", 5158,
}

// This manual fixture builds production //cmd/bd:bd from an exact, declared
// source archive. It neither reuses bd_for_tests nor changes trusted bundle
// admission, and is separate from the producer's official bundle run.
func TestRecordNativeIDStyleA9d6(t *testing.T) {
	if !*nativeRecording {
		t.Fatal("explicit manual recording target required")
	}
	parent := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(parent) {
		t.Fatal("absolute recording output directory required")
	}
	root, err := os.MkdirTemp(parent, "id-style-a-9d6-")
	if err != nil {
		t.Fatal(err)
	}
	r := &recorder{t: t, root: root}
	if hashBytes(nativeInputBytes) != nativeInputsSHA256 || json.Unmarshal(nativeInputBytes, &r.inputs) != nil {
		t.Fatal("external native protocol inputs changed")
	}
	for _, key := range []string{"TEST_TARGET", "PLUG_ID_STYLE_A_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing native request/run binding %s", key)
		}
	}
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(checkout) {
		t.Fatal("actual checked-out candidate root required")
	}
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if actual != os.Getenv("PLUG_ID_STYLE_A_REQUEST_SHA") {
		t.Fatalf("candidate request/checkout mismatch: %s", actual)
	}
	inventory, err := os.ReadFile(filepath.Join(checkout, "tools/lexicon/source_manifest.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "candidate-source-manifest.txt"), inventory, 0600); err != nil {
		t.Fatal(err)
	}
	r.writeJSON(filepath.Join(root, "request-run-binding.json"), map[string]any{
		"target": os.Getenv("TEST_TARGET"), "request_sha": os.Getenv("PLUG_ID_STYLE_A_REQUEST_SHA"),
		"run_id": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"),
		"workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"),
		"behavior_pin": styleAPin, "behavior_tree": styleATree, "behavior_parent": styleAParent,
		"exporter_source": styleAExporterSource, "candidate_source_inventory_sha256": hashBytes(inventory),
	})
	r.binary = r.buildCanonicalSource(styleABehaviorSource)
	r.exporter = r.buildCanonicalSource(styleAExporterSource) // Export capture only; never the behavior oracle.
	w := r.styleAWorkspace("pinned-native")
	versionBytes := r.run(w, "pinned-version", r.binary, []string{"version", "--json"}, 0)
	var version struct{ Commit, Build string }
	if json.Unmarshal(versionBytes, &version) != nil || version.Commit != styleAPin || version.Build != "id-style-a-fixture-"+styleAPin {
		t.Fatalf("executed behavior binary version mismatch: %+v", version)
	}
	var ids []string
	for i := 0; i < 3; i++ {
		id := r.create(w, fmt.Sprintf("pinned-default-%d", i), fmt.Sprintf("Pinned default %d", i))
		r.requireIDLength(id, 3)
		ids = append(ids, id)
	}
	initial := r.capture(w, "pinned-default-three", 3)
	r.requireIDs(initial, ids...)
	r.replayNative(initial)
	// Actual public config changes select configured and count-driven lengths.
	r.setConfig(w, "min_hash_length", "5")
	configured := r.create(w, "pinned-configured-five", "Configured width five")
	r.requireIDLength(configured, 5)
	r.setConfig(w, "min_hash_length", "3")
	r.setConfig(w, "max_collision_prob", "0.000001")
	adaptive := r.create(w, "pinned-adaptive-count-five", "Adaptive count width five")
	r.requireIDLength(adaptive, 5) // Four roots cross the configured probability threshold.
	// Explicit source children seed/repair counters, nested descendants count
	// only for their immediate parent, and closed children keep their number.
	parentID := ids[0]
	explicit := r.createExplicit(w, "pinned-explicit-eight", parentID+".8")
	child := r.createArgs(w, "pinned-next-nine", []string{"create", "--parent", parentID, "--title", "Ninth child", "--type", "task", "--json"})
	if child != parentID+".9" {
		t.Fatalf("native counter did not follow explicit .8: %q", child)
	}
	grandchild := r.createArgs(w, "pinned-grandchild-one", []string{"create", "--parent", child, "--title", "Grandchild", "--type", "task", "--json"})
	if grandchild != child+".1" {
		t.Fatalf("native nested counter mismatch: %q", grandchild)
	}
	r.run(w, "pinned-close-child", r.binary, []string{"close", explicit, "--json"}, 0)
	next := r.createArgs(w, "pinned-next-ten", []string{"create", "--parent", parentID, "--title", "Tenth child", "--type", "task", "--json"})
	if next != parentID+".10" {
		t.Fatalf("native reused closed child or counted grandchild: %q", next)
	}
	final := r.capture(w, "pinned-adaptive-children", 9)
	pairedChild := r.createArgs(w, "pinned-paired-eleven", []string{"create", "--parent", parentID, "--title", "Paired next child", "--type", "task", "--json"})
	if pairedChild != parentID+".11" {
		t.Fatalf("native next child after capture: %s", pairedChild)
	}
	input := driver.HashInput{Title: "Paired adaptive root", Description: "Public timestamp input", Creator: styleAActor, Timestamp: time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)}
	wantAdaptive, _ := driver.HashCandidate("b09", input, 5, 0)
	r.importStyleARows(w, "pinned-paired-adaptive", []map[string]any{styleAIssue(input, "")}, 0, wantAdaptive)
	r.replayStyleA(final, input, wantAdaptive, parentID, pairedChild, false)
	r.recordStyleACollisions(input)
	r.recordStyleANamespaces(input)
	if t.Failed() {
		t.Fatal("native/Plug paired controls failed; no completed native receipt")
	}
	r.writeJSON(filepath.Join(root, "native-id-style-a.json"), map[string]any{
		"status": "RECORDED_BOUNDED", "behavior_pin": styleAPin,
		"default_capture": initial, "final_capture": final,
		"configured_id": configured, "adaptive_id": adaptive,
		"explicit_child": explicit, "next_child": child, "grandchild": grandchild, "next_after_close": next,
		"paired_next_child": pairedChild, "paired_adaptive_root": wantAdaptive,
		"limits": []string{"public fixture evidence only; no real-store, official bundle admission or switch credit", "import collision route uses pinned legacy issueops at lengths 3 through 8; no beyond-eight oracle"},
	})
}

func (r *recorder) buildCanonicalSource(spec styleASource) string {
	r.t.Helper()
	archive, err := filepath.Abs("testdata/" + spec.Name + ".tar.gz")
	if err != nil {
		r.t.Fatal(err)
	}
	archiveSHA, archiveSize, err := hashFile(archive)
	if err != nil || archiveSHA != spec.ArchiveSHA {
		r.t.Fatalf("declared source archive mismatch: %s %v", archiveSHA, err)
	}
	manifest, err := os.ReadFile("testdata/" + spec.Name + ".git-tree")
	if err != nil || hashBytes(manifest) != spec.ManifestSHA {
		r.t.Fatal("declared plain Git source manifest mismatch")
	}
	source := filepath.Join(r.t.TempDir(), "beads-source")
	if err := os.Mkdir(source, 0700); err != nil {
		r.t.Fatal(err)
	}
	r.buildCommand(source, spec.Name+"-extract-source", "/usr/bin/tar", []string{"-xzf", archive, "-C", source})
	entries := strings.Split(strings.TrimSuffix(string(manifest), "\n"), "\n")
	if len(entries) != spec.Entries {
		r.t.Fatalf("source manifest entries=%d", len(entries))
	}
	for _, entry := range entries {
		identity, name, ok := strings.Cut(entry, "\t")
		parts := strings.Fields(identity)
		if !ok || len(parts) != 3 || parts[1] != "blob" || filepath.IsAbs(name) || filepath.Clean(name) != name || strings.HasPrefix(name, "../") {
			r.t.Fatal("invalid pinned source manifest entry")
		}
		path := filepath.Join(source, name)
		var data []byte
		if parts[0] == "120000" {
			target, err := os.Readlink(path)
			if err != nil {
				r.t.Fatal(err)
			}
			data = []byte(target)
		} else {
			info, err := os.Lstat(path)
			if err != nil || !info.Mode().IsRegular() || parts[0] != "100644" && parts[0] != "100755" || (info.Mode().Perm()&0111 != 0) != (parts[0] == "100755") {
				r.t.Fatalf("source mode mismatch: %s %v", name, err)
			}
			data, err = os.ReadFile(path)
			if err != nil {
				r.t.Fatal(err)
			}
		}
		h := sha1.New()
		fmt.Fprintf(h, "blob %d\x00", len(data))
		h.Write(data)
		if hex.EncodeToString(h.Sum(nil)) != parts[2] {
			r.t.Fatalf("source Git blob mismatch: %s", name)
		}
	}
	bazel := os.Getenv("BAZEL_BIN")
	if !filepath.IsAbs(bazel) {
		r.t.Fatal("trusted RunsOn Bazel binary path required")
	}
	if got := strings.TrimSpace(string(r.buildCommand(source, spec.Name+"-bazel-version", bazel, []string{"--version"}))); got != "bazel 9.3.0" {
		r.t.Fatalf("Bazel version=%q", got)
	}
	outputBase := "--output_base=" + filepath.Join(r.t.TempDir(), "bazel")
	settings := []string{"--repo_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1", "--extra_execution_platforms=//platforms:rbe_worker", "--incompatible_strict_action_env", "--@rules_go//go/config:tags=gms_pure_go", "--@rules_go//go/config:gc_linkopts=-X=main.Commit=" + spec.Commit + ",-X=main.Build=id-style-a-fixture-" + spec.Commit}
	config := append([]string{outputBase, "build", "--jobs=2", "--local_resources=memory=8192", "--run_validations", "--lockfile_mode=error"}, settings...)
	config = append(config, r.inputs.ExporterLabel)
	r.buildCommand(source, spec.Name+"-canonical-bazel-build", bazel, config)
	query := append([]string{outputBase, "cquery"}, settings...)
	query = append(query, "--output=files", r.inputs.ExporterLabel)
	outputs := strings.Fields(string(r.buildCommand(source, spec.Name+"-canonical-cquery", bazel, query)))
	if len(outputs) != 1 {
		r.t.Fatalf("canonical bd outputs=%v", outputs)
	}
	input, err := os.Open(filepath.Join(source, outputs[0]))
	if err != nil {
		r.t.Fatal(err)
	}
	defer input.Close()
	binary := filepath.Join(r.root, spec.Name+"-cli")
	output, err := os.OpenFile(binary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
	if err != nil {
		r.t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		r.t.Fatalf("retain actual canonical binary: %v %v", copyErr, closeErr)
	}
	sha, size, err := hashFile(binary)
	if err != nil {
		r.t.Fatal(err)
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil || info.GoVersion != "go1.26.10" {
		r.t.Fatalf("actual canonical binary Go toolchain: %+v %v", info, err)
	}
	wrapper, err := os.ReadFile(bazel)
	if err != nil || !bytes.Contains(wrapper, []byte("--nosystem_rc --nohome_rc --noworkspace_rc")) {
		r.t.Fatalf("trusted RunsOn Bazel wrapper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(r.root, spec.Name+"-runs-on-bazel-wrapper.sh"), wrapper, 0600); err != nil {
		r.t.Fatal(err)
	}
	rcPath := os.Getenv("BAZEL_RC")
	rc, err := os.ReadFile(rcPath)
	if err != nil {
		r.t.Fatalf("trusted RunsOn rc required: %v", err)
	}
	if err := os.WriteFile(filepath.Join(r.root, spec.Name+"-runs-on.bazelrc"), rc, 0600); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.root, spec.Name+".git-tree"), manifest, 0600); err != nil {
		r.t.Fatal(err)
	}
	for _, name := range []string{".bazelversion", ".bazelrc", "MODULE.bazel", "MODULE.bazel.lock"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			r.t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(r.root, spec.Name+"-"+strings.TrimPrefix(name, ".")), data, 0600); err != nil {
			r.t.Fatal(err)
		}
	}
	r.writeJSON(filepath.Join(r.root, spec.Name+"-build.json"), map[string]any{
		"commit": spec.Commit, "tree": spec.Tree, "parent": spec.Parent,
		"source_archive_sha256": archiveSHA, "source_archive_size": archiveSize,
		"plain_git_manifest_sha256": spec.ManifestSHA, "source_entries": len(entries),
		"binary_path": binary, "binary_sha256": sha, "binary_size": size, "binary_mode": "0755", "target": r.inputs.ExporterLabel,
		"build_argv":    append([]string{bazel}, config...),
		"bazel_version": "9.3.0", "go_version": info.GoVersion, "go_build_info": info,
		"wrapper_sha256": hashBytes(wrapper), "rc_sha256": hashBytes(rc), "canonical_validations": true,
	})
	return binary
}

func (r *recorder) buildCommand(dir, label, binary string, args []string) []byte {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -2
		var failure *exec.ExitError
		if errors.As(err, &failure) {
			exit = failure.ExitCode()
		}
	}
	outPath, errPath := filepath.Join(r.root, label+".stdout"), filepath.Join(r.root, label+".stderr")
	if writeErr := os.WriteFile(outPath, stdout.Bytes(), 0600); writeErr != nil {
		r.t.Fatal(writeErr)
	}
	if writeErr := os.WriteFile(errPath, stderr.Bytes(), 0600); writeErr != nil {
		r.t.Fatal(writeErr)
	}
	r.writeJSON(filepath.Join(r.root, label+".request.json"), map[string]any{"argv": append([]string{binary}, args...), "directory": dir, "exit": exit, "error": fmt.Sprint(err), "stdout": outPath, "stderr": errPath, "stdout_sha256": hashBytes(stdout.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes())})
	if err != nil {
		r.t.Fatalf("%s: %v\n%s", label, err, stderr.Bytes())
	}
	return stdout.Bytes()
}

func (r *recorder) styleAWorkspace(name string) workspace {
	w := r.newWorkspace(name)
	for i, entry := range w.Env {
		if strings.HasPrefix(entry, "BEADS_ACTOR=") {
			w.Env[i] = "BEADS_ACTOR=" + styleAActor
		}
	}
	r.run(w, name+"-git-init", "/usr/bin/git", []string{"init", "--quiet", "--template="}, 0)
	r.run(w, name+"-init", r.binary, []string{"init", "--prefix", "b09", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, 0)
	r.writeNativeConfig(w)
	return w
}

func styleAIssue(input driver.HashInput, id string) map[string]any {
	return map[string]any{"id": id, "title": input.Title, "description": input.Description, "issue_type": "task", "status": "open", "priority": 2, "created_at": input.Timestamp.Format(time.RFC3339Nano), "updated_at": input.Timestamp.Format(time.RFC3339Nano), "created_by": input.Creator}
}

// Public JSONL import preserves a supplied timestamp and mints an absent ID.
// This controls the real pinned CLI's input tuple without modifying its clock
// or generator. Explicit occupants are persisted by that same public CLI.
func (r *recorder) importStyleARows(w workspace, label string, rows []map[string]any, wantExit int, wantID string) {
	path := filepath.Join(r.root, label+".jsonl")
	var data bytes.Buffer
	for _, row := range rows {
		if err := json.NewEncoder(&data).Encode(row); err != nil {
			r.t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, data.Bytes(), 0600); err != nil {
		r.t.Fatal(err)
	}
	r.writeJSON(path+".input.json", map[string]any{"path": path, "sha256": hashBytes(data.Bytes()), "rows": len(rows), "actor": styleAActor})
	response := r.run(w, label, r.binary, []string{"import", path, "--json"}, wantExit)
	if wantID != "" {
		var result struct {
			ImportedIDs []string `json:"ids"`
		}
		if err := json.Unmarshal(response, &result); err != nil || !reflect.DeepEqual(result.ImportedIDs, []string{wantID}) {
			r.t.Fatalf("native timestamp-bound import: %s want=%s err=%v", response, wantID, err)
		}
	}
}

func (r *recorder) recordStyleACollisions(input driver.HashInput) {
	r.t.Helper()
	for _, exhausted := range []bool{false, true} {
		name := "pinned-ten-nonce-retry"
		max := uint32(3)
		if exhausted {
			name, max = "pinned-all-nonces-exhausted", 8
		}
		w := r.styleAWorkspace(name)
		var rows []map[string]any
		seen := map[string]bool{}
		for width := uint32(3); width <= max; width++ {
			for nonce := uint32(0); nonce < 10; nonce++ {
				id, err := driver.HashCandidate("b09", input, width, nonce)
				if err != nil || seen[id] {
					r.t.Fatalf("collision input corpus: %s %v", id, err)
				}
				seen[id] = true
				row := styleAIssue(input, id)
				row["title"] = "Occupied " + id
				rows = append(rows, row)
			}
		}
		r.importStyleARows(w, name+"-seed", rows, 0, "")
		before := r.capture(w, name+"-before", len(rows))
		want, _ := driver.HashCandidate("b09", input, 4, 0)
		wantExit := 0
		if exhausted {
			want, wantExit = "", -1
		}
		r.importStyleARows(w, name+"-mint", []map[string]any{styleAIssue(input, "")}, wantExit, want)
		count := len(rows)
		if !exhausted {
			count++
		}
		after := r.capture(w, name+"-after", count)
		if exhausted {
			r.requireConserved(before, after, "pinned native nonce exhaustion")
		} else {
			r.requireIDs(after, want)
		}
		r.replayStyleA(before, input, want, "", "", exhausted)
	}
}

func (r *recorder) recordStyleANamespaces(input driver.HashInput) {
	w := r.styleAWorkspace("pinned-namespaced-parent")
	parent := r.createExplicit(w, "pinned-namespaced-root", "b09-native-team-4de")
	child := r.createArgs(w, "pinned-namespaced-child", []string{"create", "--parent", parent, "--title", "Namespaced child", "--type", "task", "--json"})
	if child != parent+".1" {
		r.t.Fatalf("native namespaced child changed: %s", child)
	}
	grandchild := r.createArgs(w, "pinned-namespaced-grandchild", []string{"create", "--parent", child, "--title", "Namespaced grandchild", "--type", "task", "--json"})
	if grandchild != child+".1" {
		r.t.Fatalf("native namespaced grandchild changed: %s", grandchild)
	}
	before := r.capture(w, "pinned-namespaced-before", 3)
	next := r.createArgs(w, "pinned-namespaced-next", []string{"create", "--parent", parent, "--title", "Next namespaced child", "--type", "task", "--json"})
	if next != parent+".2" {
		r.t.Fatalf("native namespace counter changed: %s", next)
	}
	want, _ := driver.HashCandidate("b09", input, 3, 0)
	r.importStyleARows(w, "pinned-namespaced-paired-root", []map[string]any{styleAIssue(input, "")}, 0, want)
	r.replayStyleA(before, input, want, parent, next, false)
}

func (r *recorder) replayStyleA(source captureReceipt, input driver.HashInput, wantRoot, parent, wantChild string, exhausted bool) {
	r.t.Helper()
	ctx := context.Background()
	data, err := os.ReadFile(source.ArchivePath)
	if err != nil || hashBytes(data) != source.ArchiveSHA256 {
		r.t.Fatalf("same-run archive changed: %v", err)
	}
	inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
	if err != nil {
		r.t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		r.t.Run(source.Label+"/"+engine, func(t *testing.T) {
			target := openB09ReplayTarget(t, engine)
			target.now = input.Timestamp
			target.output = filepath.Join(r.root, "style-a-paired", source.Label, engine)
			if err := os.MkdirAll(target.output, 0700); err != nil {
				t.Fatal(err)
			}
			profile := driver.RestoreImportProfile{
				Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: b09Principal, Authority: "restore-admin", AdmissionDigest: strings.Repeat("c", 64)},
				SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: hashBytes([]byte(source.Label + engine)),
				ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
			}
			if _, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), target.store, target.lease, profile); err != nil {
				t.Fatal(err)
			}
			if err := pipeline.BuildSourceIndexes(ctx, target.store, target.lease); err != nil {
				t.Fatal(err)
			}
			before := target.stage("restored", source.BeadIDs, nil)
			if !reflect.DeepEqual(before.BeadIDs, source.BeadIDs) {
				t.Fatal("restore changed native IDs")
			}
			server := styleARPC(t, target)
			params := map[string]any{"type": "issue", "fields": map[string]any{"title": input.Title, "description": input.Description}, "_meta": map[string]any{"project": "b09", "idempotencyKey": hashBytes([]byte("style-a-root-" + source.Label + engine))}}
			request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "bead.create", "params": params})
			if err != nil {
				t.Fatal(err)
			}
			callCtx := rpc.WithPrincipal(ctx, b09Principal)
			response := server.Dispatch(callCtx, request)
			r.writeJSON(filepath.Join(target.output, "rpc-root.json"), map[string]any{"request": json.RawMessage(request), "response": json.RawMessage(response), "native_want": wantRoot, "native_exhausted": exhausted, "input": input})
			var reply struct {
				Result model.Commit    `json:"result"`
				Error  json.RawMessage `json:"error"`
			}
			if err := json.Unmarshal(response, &reply); err != nil {
				t.Fatal(err)
			}
			if exhausted {
				if len(reply.Error) == 0 {
					t.Fatalf("native exhaustion became success: %s", response)
				}
				writer, _ := target.bind()
				_, cause := writer.Apply(ctx, b09Principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: "b09", Fields: map[string]any{"title": input.Title, "description": input.Description}}}}}, model.WriteMeta{Method: "bead.create", Project: "b09", IdempotencyKey: hashBytes([]byte("exhaustion-cause-" + engine))})
				if !errors.Is(cause, driver.ErrUnsupported) || !strings.Contains(cause.Error(), "hash candidates exhausted") {
					t.Fatalf("public refusal was not actual hash exhaustion: %v", cause)
				}
				r.writeJSON(filepath.Join(target.output, "exhaustion-cause.json"), map[string]any{"error": cause.Error(), "unsupported": true})
				after := target.stage("exhausted", source.BeadIDs, nil)
				if before.Meta != after.Meta || !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.Versions, after.Versions) || !reflect.DeepEqual(before.Allocator, after.Allocator) || !reflect.DeepEqual(before.Changesets, after.Changesets) {
					t.Fatal("exhausted public request published allocator, records or history")
				}
				return
			}
			if len(reply.Error) != 0 || len(reply.Result.Results) != 1 || reply.Result.Results[0].Ref.ID != wantRoot {
				t.Fatalf("native/Plug ID mismatch: %s native=%s", response, wantRoot)
			}
			if err := target.reopen(); err != nil {
				t.Fatal(err)
			}
			server = styleARPC(t, target)
			if replay := server.Dispatch(callCtx, request); !bytes.Equal(response, replay) {
				t.Fatalf("reopen RPC replay changed result: %s -> %s", response, replay)
			}
			newIDs := []string{wantRoot}
			if parent != "" {
				params["parent"] = model.RefByID(parent, model.Natural64(0))
				params["fields"] = map[string]any{"title": "Paired next child"}
				params["_meta"] = map[string]any{"project": "b09", "idempotencyKey": hashBytes([]byte("style-a-child-" + source.Label + engine))}
				request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 2, "method": "bead.create", "params": params})
				if err != nil {
					t.Fatal(err)
				}
				response := server.Dispatch(callCtx, request)
				r.writeJSON(filepath.Join(target.output, "rpc-child.json"), map[string]any{"request": json.RawMessage(request), "response": json.RawMessage(response), "native_want": wantChild})
				reply.Error, reply.Result = nil, model.Commit{}
				if err := json.Unmarshal(response, &reply); err != nil || len(reply.Error) != 0 || len(reply.Result.Results) != 1 || reply.Result.Results[0].Ref.ID != wantChild {
					t.Fatalf("native/Plug child mismatch: %s native=%s err=%v", response, wantChild, err)
				}
				newIDs = append(newIDs, wantChild)
			}
			after := target.stage("continued-after-reopen", source.BeadIDs, newIDs)
			for id, record := range before.Records {
				if !reflect.DeepEqual(record, after.Records[id]) {
					t.Fatalf("new allocation changed restored record %s", id)
				}
			}
		})
	}
}

func styleARPC(t *testing.T, target *b09ReplayTarget) *rpc.Server {
	t.Helper()
	_, domain := target.bind()
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	meta, err := target.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "b09", CursorKey: bytes.Repeat([]byte{0x51}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	return server
}
