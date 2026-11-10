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
		{"companion-profile", []string{"schema"}, "plug-public-companion/1"},
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
// come from the real hash-pinned executable and its fresh imported workspace.
// No extra capture, executable factory, preparer, descriptor or pin is added.
func assertPublicNativeCaptureReadback(t *testing.T, options beadsOptions, output string, got outcome) {
	t.Helper()
	pinned := publicNativePrivateFile(t, filepath.Join(output, "beads-pinned"), 0500)
	if hash(pinned) != options.SHA256 || got.BeadsBinarySHA256 != options.SHA256 || got.BeadsBuildRun != options.BuildRun {
		t.Fatal("executed private copy is not bound to the delivered bytes/build")
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
		Version         string `json:"version"`
		Quiesced        bool   `json:"quiesced"`
		RemotesMerged   bool   `json:"remotes_merged"`
		WorkingSetClean bool   `json:"working_set_clean"`
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
		for _, field := range []string{"id", "title", "description", "design", "acceptance_criteria", "notes", "spec_id", "status", "priority", "issue_type", "assignee", "owner", "created_by", "external_ref", "source_system", "close_reason", "closed_by_session", "metadata"} {
			if value, present := want[field]; present {
				a, errA := json.Marshal(value)
				b, errB := json.Marshal(row.Source.Fields[field])
				if errA != nil || errB != nil || !bytes.Equal(a, b) {
					t.Fatalf("independent raw target %s field %s differs from submitted bytes: expected_type=%T expected_sha256=%s expected=%q actual_type=%T actual_sha256=%s actual=%q marshal_errors=%v/%v", row.ID, field, value, hash(a), a, row.Source.Fields[field], hash(b), b, errA, errB)
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
func publicNativeProtocolVersionKey(t *testing.T) string {
	t.Helper()
	data := publicNativeProtocolInput(t,
		"tools/roundtrip/testdata/native-public-profile.json",
		"67491cd9ce55d652ee99ce80144298648ce60af4c93d35f266ccf7a6fbfd739d")
	var profile struct {
		Schema     string `json:"schema"`
		VersionKey string `json:"version_key"`
	}
	if err := json.Unmarshal(data, &profile); err != nil || profile.Schema != "plug-public-native/1" || strings.TrimSpace(profile.VersionKey) == "" {
		t.Fatal("pinned public native protocol data is invalid")
	}
	return profile.VersionKey
}

func publicNativeProtocolFamilies(t *testing.T) []string {
	t.Helper()
	data := publicNativeProtocolInput(t,
		"tools/roundtrip/testdata/native-public-recording-fixture.json",
		"5cb91bc8b5f6612d1907202c7210fc2532d9711d1fd23c7e5fb977b6d6e575b0")
	var fixture struct {
		Schema           string   `json:"schema"`
		SourceCommit     string   `json:"source_commit"`
		SourceTree       string   `json:"source_tree"`
		ExternalFamilies []string `json:"external_families"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil || fixture.Schema != "plug-s1-b25-b27-native-recording/2" || fixture.SourceCommit != beadsCommit || fixture.SourceTree != beadsTree || len(fixture.ExternalFamilies) != 26 {
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
