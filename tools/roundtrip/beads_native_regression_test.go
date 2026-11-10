package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// This control runs before the original mandatory native-equality assertion.
// Passing it proves refusal evidence only; the leaf must still fail unless
// the actual native readback equals the intended data.
func assertPublicReadbackRefusalEvidence(t *testing.T, output string, got outcome) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(output, "beads-readback-refusal.json"))
	if err != nil || got.BeadsRefusalSHA256 == "" || hash(data) != got.BeadsRefusalSHA256 {
		t.Fatal("actual readback refusal evidence missing or unbound")
	}
	info, err := os.Stat(filepath.Join(output, "beads-readback-refusal.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("actual readback refusal evidence is not private")
	}
	var refusal beadsReadbackRefusal
	if decodeNumberJSON(data, &refusal) != nil || refusal.Schema != "plug-public-readback-refusal/1" || refusal.Reason != got.Reason || refusal.SourceSHA256 != got.ArchiveSHA256 || refusal.SubmittedSHA256 != got.ReverseSHA256 || refusal.ImporterCommit != got.BeadsCommit || refusal.ImporterTree != got.BeadsTree || refusal.ImporterBinarySHA256 != got.BeadsBinarySHA256 || refusal.ImporterBuildRun != got.BeadsBuildRun || refusal.CaptureCommit != got.BeadsCaptureCommit || refusal.CaptureTree != got.BeadsCaptureTree || refusal.CaptureBinarySHA256 != got.BeadsCaptureBinarySHA256 || refusal.CaptureBuildRun != got.BeadsCaptureBuildRun {
		t.Fatal("actual refusal source/import/capture bindings changed")
	}
	raw, err := os.ReadFile(filepath.Join(output, "beads-raw-readback.tar"))
	if err != nil || hash(raw) != refusal.ProducedSHA256 {
		t.Fatal("actual refusal lost its original produced bytes")
	}
	if got.BeadsReadbackSHA256 == "" {
		if refusal.Validation != "FAIL" || refusal.NativeComparison != "NOT_MEASURED" || refusal.AcceptedSHA256 != "" || len(refusal.CellRefusals) != 0 || len(refusal.FieldRows) != 0 || refusal.Identity == nil || refusal.Identity.Code != "edge_metadata_identity_mismatch" || refusal.Identity.Table != "wisp_dependencies" || refusal.Identity.RawID != "f589ea2e-9ce6-5427-abb0-e7ff1c64eb6b" || refusal.Identity.Envelope.Value != "edge-fixture" || refusal.ValidationError != "source plane dependency metadata: edge metadata envelope identity mismatch" {
			t.Fatal("rejected native archive acquired equality or lost its exact identity refusal")
		}
	} else {
		if refusal.Validation != "PASS" || refusal.NativeComparison != "MEASURED" || refusal.AcceptedSHA256 != got.BeadsReadbackSHA256 || refusal.AcceptedSHA256 != refusal.ProducedSHA256 || refusal.Identity != nil || len(refusal.CellRefusals) != 9 || len(refusal.FieldRows) != 9 {
			t.Fatal("measured native refusal lost row/cell evidence")
		}
		fields := map[string]bool{}
		for _, cell := range refusal.CellRefusals {
			if cell.Row != "demo-42v" || !cell.Expected.Present || !cell.Actual.Present {
				t.Fatal("actual refusal lost its measured row/presence")
			}
			fields[cell.Domain+"/"+cell.Field] = true
		}
		for _, field := range []string{"native/agent_state", "native/ephemeral", "native/hook_bead", "native/is_template", "native/payload", "native/role_bead", "native/role_type", "native/timeout_ns", "metadata/beads.timeout_ns"} {
			if !fields[field] {
				t.Fatal("actual refusal concealed a measured native/metadata field")
			}
		}
	}
	t.Logf("PUBLIC_READBACK_REFUSAL_CONTROL PASS validation=%s native=%s cells=%d sha256=%s", refusal.Validation, refusal.NativeComparison, len(refusal.CellRefusals), got.BeadsRefusalSHA256)
}

// The child is the consumer's own test process, solely to observe a fatal
// manifest refusal. It never impersonates bd or supplies a capable exporter.
func TestPublicBeadsPreparedSourcePinRefusals(t *testing.T) {
	const probe = "PLUG_ROUNDTRIP_SOURCE_PIN_REFUSAL_PROBE"
	if os.Getenv(probe) == "1" {
		deliveredPublicBeads(t)
		return
	}
	if !*publicBeadsRuntime {
		t.Fatal("source-pin regression requires the explicit public runtime target")
	}
	versionKey := publicNativeProtocolVersionKey(t)
	deliveredPublicBeads(t)
	original, err := os.ReadFile(os.Getenv("PLUG_PUBLIC_BEADS_MANIFEST"))
	if err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		path  []string
		value string
	}{
		{"source-commit", []string{"source", "beads", "commit"}, strings.Repeat("0", 40)},
		{"source-tree", []string{"source", "beads", "tree"}, strings.Repeat("0", 40)},
		{"version-commit", []string{"resolution", versionKey, "commit"}, strings.Repeat("0", 40)},
		{"version-build", []string{"resolution", versionKey, "build"}, "unadmitted-build"},
		{"profile", []string{"schema"}, "unadmitted-public-profile"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var manifest map[string]any
			decoder := json.NewDecoder(bytes.NewReader(original))
			decoder.UseNumber()
			if err := decoder.Decode(&manifest); err != nil {
				t.Fatal(err)
			}
			node := manifest
			for _, key := range test.path[:len(test.path)-1] {
				next, ok := node[key].(map[string]any)
				if !ok {
					t.Fatalf("delivered manifest lacks source-pin field %v", test.path)
				}
				node = next
			}
			key := test.path[len(test.path)-1]
			if value, ok := node[key].(string); !ok || value == "" || value == test.value {
				t.Fatalf("source-pin mutation did not change its intended field: %v", test.path)
			}
			node[key] = test.value
			changed, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "refused-manifest.json")
			if err := os.WriteFile(path, changed, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PLUG_PUBLIC_BEADS_MANIFEST", path)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, self, "-test.run=^TestPublicBeadsPreparedSourcePinRefusals$", "-test.v", "-public-beads-runtime")
			cmd.Env = append(os.Environ(), probe+"=1")
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || !bytes.Contains(output, []byte("prepared public artifact identity does not match its manifest")) {
				t.Fatalf("consumer source-pin guard did not refuse %v: %v\n%s", test.path, err, output)
			}
			t.Logf("PUBLIC_BEADS_SOURCE_PIN_REFUSAL field=%s original_sha256=%s changed_sha256=%s exit=%d", strings.Join(test.path, "."), hash(original), hash(changed), exit.ExitCode())
		})
	}
}

// Called once for each original three-engine/history runtime leaf. All bytes
// come from the real hash-pinned importer and the separate declared capture
// component observing that same fresh workspace. No preparer pin is changed.
func assertPublicNativeCaptureReadback(t *testing.T, options beadsOptions, output string, got outcome) {
	t.Helper()
	pinned := publicNativePrivateFile(t, filepath.Join(output, "beads-pinned"), 0500)
	if hash(pinned) != options.SHA256 || got.BeadsBinarySHA256 != options.SHA256 || got.BeadsBuildRun != options.BuildRun {
		t.Fatal("executed private copy is not bound to the delivered bytes/build")
	}
	capture := publicNativePrivateFile(t, filepath.Join(output, "beads-capture-pinned"), 0500)
	if hash(capture) != options.CaptureSHA256 || got.BeadsCaptureBinarySHA256 != options.CaptureSHA256 || got.BeadsCaptureBuildRun != options.CaptureBuildRun || got.BeadsCaptureCommit != publicCaptureCommit || got.BeadsCaptureTree != publicCaptureTree {
		t.Fatal("actual separate capture executable/source/build differs")
	}
	var captureVersion struct{ Commit, Build string }
	if err := json.Unmarshal(publicNativePrivateFile(t, filepath.Join(output, "beads-4.stdout"), 0600), &captureVersion); err != nil || captureVersion.Commit != publicCaptureCommit || captureVersion.Build != "id-style-a-fixture-"+publicCaptureCommit {
		t.Fatalf("actual separate capture version differs: %+v %v", captureVersion, err)
	}
	var version struct {
		Commit string `json:"commit"`
		Build  string `json:"build"`
	}
	if err := json.Unmarshal(publicNativePrivateFile(t, filepath.Join(output, "beads-1.stdout"), 0600), &version); err != nil || version.Commit != beadsCommit || version.Build != "public-"+beadsCommit {
		t.Fatalf("actual executable source pin differs: %+v %v", version, err)
	}
	generated := publicNativePrivateFile(t, filepath.Join(output, "beads-generated-inventory.json"), 0600)
	copied := publicNativePrivateFile(t, filepath.Join(output, "beads-inventory.json"), 0600)
	if len(generated) == 0 || !bytes.Equal(generated, copied) {
		t.Fatal("raw exporter did not consume the exact real fresh-target inventory")
	}
	var inventory struct {
		Version         string            `json:"version"`
		StoreID         string            `json:"store_id"`
		Quiesced        bool              `json:"quiesced"`
		RemotesMerged   bool              `json:"remotes_merged"`
		WorkingSetClean bool              `json:"working_set_clean"`
		ConfigPolicy    map[string]string `json:"config_policy"`
		External        []struct {
			Family string `json:"family"`
			State  string `json:"state"`
			Path   string `json:"path"`
		} `json:"external"`
	}
	if err := json.Unmarshal(generated, &inventory); err != nil || inventory.Version != "beads-inventory/1" || !inventory.Quiesced || !inventory.RemotesMerged || !inventory.WorkingSetClean {
		t.Fatalf("real native inventory declarations are incomplete: %v", err)
	}
	wantFamilies := publicNativeProtocolFamilies(t)
	remaining := map[string]bool{}
	for _, family := range wantFamilies {
		remaining[family] = true
	}
	physical, err := filepath.EvalSymlinks(output)
	if err != nil {
		t.Fatal(err)
	}
	sidecars := filepath.Join(physical, "beads-generated-inventory.json.sources")
	if info, err := os.Lstat(sidecars); err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatalf("actual inventory sidecar directory is not private: %v", err)
	}
	if len(inventory.External) != len(wantFamilies) {
		t.Fatalf("native external family count=%d want %d", len(inventory.External), len(wantFamilies))
	}
	for _, input := range inventory.External {
		if !remaining[input.Family] || input.State != "json-config" || input.Path != filepath.Join(sidecars, input.Family+".json") {
			t.Fatalf("fresh-target external family missing, duplicated or relocated: %+v", input)
		}
		delete(remaining, input.Family)
		data := publicNativePrivateFile(t, input.Path, 0600)
		if !json.Valid(data) {
			t.Fatalf("actual native sidecar %s is not JSON", input.Family)
		}
		t.Logf("PUBLIC_BEADS_NATIVE_SIDECAR family=%s sha256=%s bytes=%d", input.Family, hash(data), len(data))
	}
	readback := publicNativePrivateFile(t, filepath.Join(output, "beads-raw-readback.tar"), 0600)
	archive, err := dumpformat.Read(bytes.NewReader(readback))
	if err != nil || hash(readback) != got.BeadsReadbackSHA256 || archive.SHA256() != got.BeadsReadbackSHA256 {
		t.Fatalf("actual raw readback archive/digest differs: %v", err)
	}
	// The declared capture hashes its actual StoreLocator path, and retains
	// both directory observations. Bind those physical observations to this
	// importer's owned embedded target and to the resulting raw archive.
	var storage struct {
		Observations []struct {
			Path, State, Contents string
			Mode                  uint32
		} `json:"observations"`
		Quiesced  bool   `json:"operator_quiesced"`
		Clean     bool   `json:"sql_clean"`
		Atomicity string `json:"filesystem_atomicity"`
	}
	storageBytes := publicNativePrivateFile(t, filepath.Join(sidecars, "store-files-and-quiescence.json"), 0600)
	if err := json.Unmarshal(storageBytes, &storage); err != nil || len(storage.Observations) != 2 || !storage.Quiesced || !storage.Clean || storage.Atomicity != "not-verified" {
		t.Fatalf("actual native physical-root observations are incomplete: %v", err)
	}
	beadsDir := filepath.Join(physical, "beads-target", ".beads")
	storeDir := storage.Observations[1].Path
	if storage.Observations[0].Path != beadsDir || filepath.Dir(storeDir) != filepath.Join(beadsDir, "embeddeddolt") || inventory.StoreID != "native/"+hash([]byte(storeDir)) || archive.Manifest().StoreID != inventory.StoreID {
		t.Fatal("inventory/raw archive is not bound to this fresh embedded import target")
	}
	for _, observation := range storage.Observations {
		info, err := os.Lstat(observation.Path)
		if err != nil || !info.IsDir() || observation.State != "directory" || observation.Contents != "not-read" || uint32(info.Mode().Perm()) != observation.Mode {
			t.Fatalf("actual owned storage directory differs from capture observation: %+v %v", observation, err)
		}
	}
	policies := map[string]bool{}
	// The exporter keeps config/kv.memory.* source rows in the memory kind.
	// Both kinds belong to the same complete raw config/metadata policy census.
	for _, row := range append(archive.Records("config"), archive.Records("memory")...) {
		key, ok := row.Source.Fields["key"].(string)
		qualified := row.Source.Table + "/" + key
		if !ok || key == "" || policies[qualified] || inventory.ConfigPolicy[qualified] == "" || row.Body["classification"] != inventory.ConfigPolicy[qualified] {
			t.Fatal("actual raw configuration row differs from captured policy census")
		}
		policies[qualified] = true
	}
	if len(policies) == 0 || len(policies) != len(inventory.ConfigPolicy) {
		t.Fatal("captured configuration policy census has missing or unmeasured keys")
	}
	t.Logf("PUBLIC_BEADS_NATIVE_TARGET store_id=%s store_dir=%s beads_dir=%s config_keys=%d physical_atomicity=%s", inventory.StoreID, storeDir, beadsDir, len(policies), storage.Atomicity)
	live := publicNativePrivateFile(t, filepath.Join(output, "reverse-live.jsonl"), 0600)
	if hash(live) != got.ReverseSHA256 || got.BaselineSHA256 == got.ReverseSHA256 {
		t.Fatal("readback expectation is not the changed production Writer image")
	}
	decoder := json.NewDecoder(bytes.NewReader(live))
	decoder.UseNumber()
	expected := map[string]map[string]any{}
	for {
		var row map[string]any
		if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if row["_type"] != "issue" {
			continue
		}
		id, ok := row["id"].(string)
		if !ok || id == "" || expected[id] != nil {
			t.Fatal("actual submitted live issue census is invalid")
		}
		expected[id] = row
	}
	if len(expected) != got.Counts["bead"] || len(archive.Records("bead")) != len(expected) {
		t.Fatal("raw target issue census differs from actual submitted live image")
	}
	changed := 0
	for _, row := range archive.Records("bead") {
		want, ok := expected[row.ID]
		if !ok {
			t.Fatalf("raw target has an extra or duplicate issue %s", row.ID)
		}
		delete(expected, row.ID)
		for _, field := range []string{"id", "title", "description", "status", "priority", "issue_type", "metadata"} {
			if value, present := want[field]; present {
				a, errA := json.Marshal(value)
				b, errB := json.Marshal(row.Source.Fields[field])
				if errA != nil || errB != nil || !bytes.Equal(a, b) {
					t.Fatalf("independent raw target %s field %s differs from submitted bytes", row.ID, field)
				}
			}
		}
		if row.Source.Fields["description"] == "Disposable round-trip live edit" {
			changed++
		}
	}
	if len(expected) != 0 || changed != 1 {
		t.Fatal("raw readback substituted a source/pre-workload image for the real changed target")
	}
	t.Logf("PUBLIC_BEADS_NATIVE_READBACK build=%s binary=%s inventory=%s readback=%s issue_rows=%d changed_rows=%d", options.BuildRun, options.SHA256, hash(generated), hash(readback), got.Counts["bead"], changed)
}

// These expected wire values come from existing independent protocol data.
// Bind complete source bytes; never discover expectations from runtime output.
func publicNativeProtocolExporterLabel(t *testing.T) string {
	t.Helper()
	data := publicNativeProtocolInput(t,
		"tools/roundtrip/testdata/public-companion-profile.json",
		"9389b60ccf64dd755a16b00367318b78866455cb0c1493a0b0c0532e1a6aae40")
	var profile struct {
		Schema   string                 `json:"schema"`
		Exporter struct{ Label string } `json:"exporter"`
	}
	if err := json.Unmarshal(data, &profile); err != nil || profile.Schema != "plug-public-companion/1" || strings.TrimSpace(profile.Exporter.Label) == "" {
		t.Fatal("pinned public companion exporter label is invalid")
	}
	return profile.Exporter.Label
}

func publicNativeProtocolVersionKey(t *testing.T) string {
	t.Helper()
	data := publicNativeProtocolInput(t,
		"tools/roundtrip/testdata/public-companion-profile.json",
		"9389b60ccf64dd755a16b00367318b78866455cb0c1493a0b0c0532e1a6aae40")
	var profile struct {
		Schema     string `json:"schema"`
		VersionKey string `json:"version_key"`
	}
	if err := json.Unmarshal(data, &profile); err != nil || profile.Schema != "plug-public-companion/1" || strings.TrimSpace(profile.VersionKey) == "" {
		t.Fatal("pinned public companion protocol data is invalid")
	}
	return profile.VersionKey
}

func publicNativeProtocolFamilies(t *testing.T) []string {
	t.Helper()
	data := publicNativeProtocolInput(t,
		"spec/s1-b25-b28/testdata/public-recording-fixture.json",
		"ecd42d10e57cbac6156ce2cf962494ffc7c205cd6599055c5e21a6292dd91570")
	var fixture struct {
		Schema           string   `json:"schema"`
		SourceCommit     string   `json:"source_commit"`
		SourceTree       string   `json:"source_tree"`
		ExternalFamilies []string `json:"external_families"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil || fixture.Schema != "plug-s1-b25-b28-public-recording/1" || fixture.SourceCommit != beadsCommit || fixture.SourceTree != beadsTree || len(fixture.ExternalFamilies) != 26 {
		t.Fatal("pinned public family declaration is invalid")
	}
	seen := map[string]bool{}
	for _, family := range fixture.ExternalFamilies {
		if strings.TrimSpace(family) == "" || seen[family] {
			t.Fatal("pinned public family declaration has an empty or duplicate name")
		}
		seen[family] = true
	}
	return fixture.ExternalFamilies
}

func publicNativeProtocolInput(t *testing.T, name, wantSHA256 string) []byte {
	t.Helper()
	root, workspace := os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE")
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || workspace == "" || workspace == "." || workspace == ".." || strings.ContainsAny(workspace, "/\\") || !filepath.IsLocal(name) || filepath.ToSlash(filepath.Clean(name)) != name {
		t.Fatal("declared protocol input requires valid Bazel runfiles roots and a local source path")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		t.Fatal("Bazel runfiles root is unavailable")
	}
	data, err := os.ReadFile(filepath.Join(root, workspace, filepath.FromSlash(name)))
	if err != nil || hash(data) != wantSHA256 {
		t.Fatalf("declared protocol input is unavailable or changed: %s", name)
	}
	return data
}

func publicNativePrivateFile(t *testing.T, path string, mode os.FileMode) []byte {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != mode {
		t.Fatalf("real native evidence is missing, linked or has wrong permissions: %s: %v", path, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
