package s1b25b28_test

// This is an opt-in runtime recording.  It deliberately consumes the public
// companion artifact prepared by RunsOn instead of building, downloading, or
// faking a Beads executable in the test. Raw evidence goes to Bazel's undeclared
// outputs; executable copies and disposable stores stay under /tmp/plug-swarm/work.
// The remote workflow must collect undeclared outputs before retention is proven.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

const (
	publicBeadsCommit = "682ca798dab448803ea9b74f7da3715c21d8c3ae"
	publicBeadsTree   = "9d026de307ee915953b247cf42758d0c53303574"
	publicChildID     = "pub-root.1"
)

var requirePublicRecording = flag.Bool("public-recording", false, "Require the independently delivered public Beads recording artifact")

type recordingProfile struct {
	Schema   string `json:"schema"`
	Exporter struct {
		TargetKey string `json:"target_key"`
		Label     string `json:"label"`
		Binary    string `json:"binary"`
	} `json:"exporter"`
	VersionKey string `json:"version_key"`
}

type recordingFixture struct {
	Schema           string   `json:"schema"`
	SourceCommit     string   `json:"source_commit"`
	SourceTree       string   `json:"source_tree"`
	Commands         []string `json:"commands"`
	ProjectionFields []string `json:"projection_fields"`
	IDs              []string `json:"ids"`
	MemoryKey        string   `json:"memory_key"`
	ConfigKey        string   `json:"config_key"`
}

type publicArtifact struct {
	Binary       string
	SHA256       string
	BuildRun     string
	ManifestPath string
	ManifestSHA  string
	Size         int64
}

type commandReceipt struct {
	Label        string   `json:"label"`
	Store        string   `json:"store"`
	Argv         []string `json:"argv"`
	StdinSHA256  string   `json:"stdin_sha256,omitempty"`
	StdoutSHA256 string   `json:"stdout_sha256"`
	StderrSHA256 string   `json:"stderr_sha256"`
	Exit         int      `json:"exit"`
	StdoutPath   string   `json:"stdout_path"`
	StderrPath   string   `json:"stderr_path"`
}

type publicRun struct {
	Name                string
	Root                string
	SourceExportPath    string
	TargetExportPath    string
	SourceInventoryPath string
	TargetInventoryPath string
	SourceArchivePath   string
	TargetArchivePath   string
	SourceExportSHA256  string
	TargetExportSHA256  string
	SourceArchiveSHA256 string
	TargetArchiveSHA256 string
	SourceProjectionSHA string
	TargetProjectionSHA string
	Commands            []commandReceipt
}

type semanticProjection struct {
	Issues   map[string]map[string]any `json:"issues"`
	Edges    []map[string]any          `json:"edges"`
	Comments []map[string]any          `json:"comments"`
	Memories map[string]any            `json:"memories"`
}

// TestPublicPairedRecording is intentionally manual.  The target fails when
// the trusted bundle environment is absent; a missing artifact is never
// converted into a skip or a successful fixture result.
func TestPublicPairedRecording(t *testing.T) {
	if !*requirePublicRecording {
		t.Fatal("public recording target requires -public-recording")
	}
	profile := readRecordingProfile(t)
	fixture := readRecordingFixture(t)
	if fixture.SourceCommit != publicBeadsCommit || fixture.SourceTree != publicBeadsTree || fixture.Schema == "" || len(fixture.Commands) == 0 || len(fixture.ProjectionFields) == 0 {
		t.Fatalf("public recording fixture is not pinned: %+v", fixture)
	}
	artifact := verifyPublicArtifact(t, profile)
	root := makeRecordingRoot(t)
	t.Logf("PUBLIC_RECORDING_OUTPUT %s", root)
	storeRoot := makeRecordingDirectory(t, "/tmp/plug-swarm/work", "s1-b25-b28-public-stores-")
	t.Cleanup(func() {
		if err := os.RemoveAll(storeRoot); err != nil {
			t.Errorf("remove disposable public stores: %v", err)
		}
	})
	manifest, err := os.ReadFile(artifact.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(manifest) != artifact.ManifestSHA {
		t.Fatal("public manifest changed after artifact admission")
	}
	writeRecordingBytes(t, filepath.Join(root, "public-companion-manifest.json"), manifest)
	buildInfo := recordBuildInfo(t, root, artifact)
	t.Logf("PUBLIC_RECORDING_ARTIFACT build_run=%s binary_sha256=%s manifest_sha256=%s build_info_sha256=%s", artifact.BuildRun, artifact.SHA256, artifact.ManifestSHA, hashBytes(buildInfo))

	run1 := recordPublicRun(t, root, storeRoot, "run1", artifact, fixture)
	run2 := recordPublicRun(t, root, storeRoot, "run2", artifact, fixture)
	if run1.SourceProjectionSHA != run2.SourceProjectionSHA || run1.TargetProjectionSHA != run2.TargetProjectionSHA {
		t.Fatalf("paired public projections differ: run1 source=%s target=%s, run2 source=%s target=%s", run1.SourceProjectionSHA, run1.TargetProjectionSHA, run2.SourceProjectionSHA, run2.TargetProjectionSHA)
	}
	if run1.SourceArchiveSHA256 == "" || run2.SourceArchiveSHA256 == "" || run1.TargetArchiveSHA256 == "" || run2.TargetArchiveSHA256 == "" {
		t.Fatal("paired recording did not retain both validated archives")
	}
	provenance := map[string]any{
		"schema":            fixture.Schema,
		"source_commit":     publicBeadsCommit,
		"source_tree":       publicBeadsTree,
		"build_run":         artifact.BuildRun,
		"binary_sha256":     artifact.SHA256,
		"manifest_sha256":   artifact.ManifestSHA,
		"build_info_sha256": hashBytes(buildInfo),
		"projection_sha256": map[string]string{
			"source": run1.SourceProjectionSHA,
			"target": run1.TargetProjectionSHA,
		},
		"runs": []any{run1, run2},
	}
	data, err := json.MarshalIndent(provenance, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "provenance.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("PUBLIC_RECORDING_OUTPUT %s", root)
	t.Logf("PUBLIC_RECORDING_PROVENANCE %s", filepath.Join(root, "provenance.json"))
}

func readRecordingProfile(t *testing.T) recordingProfile {
	t.Helper()
	var profile recordingProfile
	data := recordingRunfile(t, "testdata/public-companion-profile.json")
	if err := json.Unmarshal(data, &profile); err != nil || profile.Schema == "" || profile.Exporter.TargetKey == "" || profile.Exporter.Label == "" || profile.Exporter.Binary == "" || profile.VersionKey == "" {
		t.Fatalf("public companion profile is invalid: %v", err)
	}
	return profile
}

func readRecordingFixture(t *testing.T) recordingFixture {
	t.Helper()
	var fixture recordingFixture
	data := recordingRunfile(t, "testdata/public-recording-fixture.json")
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("public recording fixture is invalid: %v", err)
	}
	return fixture
}

func recordingRunfile(t *testing.T, name string) []byte {
	t.Helper()
	candidates := []string{
		filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "spec/s1-b25-b28", name),
		filepath.Join("spec", "s1-b25-b28", name),
		name,
	}
	for _, path := range candidates {
		if path == "" || path == filepath.Join("", name) {
			continue
		}
		if data, err := os.ReadFile(path); err == nil {
			return data
		}
	}
	t.Fatalf("recording runfile %q is unavailable", name)
	return nil
}

func verifyPublicArtifact(t *testing.T, profile recordingProfile) publicArtifact {
	t.Helper()
	artifact := publicArtifact{Binary: os.Getenv("PLUG_PUBLIC_BEADS_BINARY"), SHA256: os.Getenv("PLUG_PUBLIC_BEADS_SHA256"), BuildRun: os.Getenv("PLUG_PUBLIC_BEADS_BUILD_RUN"), ManifestPath: os.Getenv("PLUG_PUBLIC_BEADS_MANIFEST")}
	for name, value := range map[string]string{"PLUG_PUBLIC_BEADS_BINARY": artifact.Binary, "PLUG_PUBLIC_BEADS_SHA256": artifact.SHA256, "PLUG_PUBLIC_BEADS_BUILD_RUN": artifact.BuildRun, "PLUG_PUBLIC_BEADS_MANIFEST": artifact.ManifestPath} {
		if value == "" {
			t.Fatalf("required public artifact evidence is missing: %s", name)
		}
	}
	if !filepath.IsAbs(artifact.Binary) || !filepath.IsAbs(artifact.ManifestPath) || len(artifact.SHA256) != 64 || !isLowerHex(artifact.SHA256) {
		t.Fatal("public artifact paths or digest are invalid")
	}
	if run, err := strconv.ParseUint(artifact.BuildRun, 10, 64); err != nil || run == 0 || strconv.FormatUint(run, 10) != artifact.BuildRun {
		t.Fatal("public artifact build run is not an immutable decimal run ID")
	}
	manifestData, err := os.ReadFile(artifact.ManifestPath)
	if err != nil || len(manifestData) == 0 || len(manifestData) > 1<<20 {
		t.Fatalf("public artifact manifest is unavailable: %v", err)
	}
	artifact.ManifestSHA = hashBytes(manifestData)
	var manifest struct {
		Schema    string                                    `json:"schema"`
		Source    map[string]struct{ Commit, Tree string }  `json:"source"`
		Targets   map[string]struct{ Label, Binary string } `json:"targets"`
		Artifacts map[string]struct {
			SHA256 string `json:"sha256"`
			Size   int64  `json:"size"`
			Mode   string `json:"mode"`
		} `json:"artifacts"`
		Build struct {
			RunID string `json:"run_id"`
		} `json:"build"`
		Resolution map[string]json.RawMessage `json:"resolution"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("public artifact manifest is invalid: %v", err)
	}
	target := manifest.Targets[profile.Exporter.TargetKey]
	binaryInfo := manifest.Artifacts[profile.Exporter.Binary]
	var version struct {
		Commit string `json:"commit"`
		Build  string `json:"build"`
	}
	if rawVersion, ok := manifest.Resolution[profile.VersionKey]; !ok || json.Unmarshal(rawVersion, &version) != nil {
		t.Fatal("public artifact version probe is missing or malformed")
	}
	beadsSource := manifest.Source["beads"]
	if manifest.Schema != profile.Schema || beadsSource.Commit != publicBeadsCommit || beadsSource.Tree != publicBeadsTree || target.Label != profile.Exporter.Label || target.Binary != profile.Exporter.Binary || binaryInfo.SHA256 != artifact.SHA256 || binaryInfo.Mode != "0o755" || binaryInfo.Size <= 0 || manifest.Build.RunID != artifact.BuildRun || version.Commit != publicBeadsCommit || version.Build != "public-"+publicBeadsCommit {
		t.Fatalf("public artifact identity does not match profile: schema=%q source=%+v target=%+v binary=%+v build=%+v version=%+v", manifest.Schema, beadsSource, target, binaryInfo, manifest.Build, version)
	}
	info, err := os.Lstat(artifact.Binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o755 || info.Size() != binaryInfo.Size {
		t.Fatalf("public executable mode or size is not manifest-bound: %v", err)
	}
	artifact.Size = info.Size()
	if got := hashFile(artifact.Binary); got != artifact.SHA256 {
		t.Fatalf("public executable digest changed: got %s want %s", got, artifact.SHA256)
	}
	return artifact
}

func makeRecordingRoot(t *testing.T) string {
	t.Helper()
	parent := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if parent == "" {
		parent = "/tmp/plug-swarm/work"
	}
	if !filepath.IsAbs(parent) {
		t.Fatal("public recording output directory must be absolute")
	}
	return makeRecordingDirectory(t, parent, "s1-b25-b28-public-recording-")
}

func makeRecordingDirectory(t *testing.T, parent, pattern string) string {
	t.Helper()
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(parent, pattern)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func writeRecordingBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func recordBuildInfo(t *testing.T, root string, artifact publicArtifact) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "go", "version", "-m", artifact.Binary).CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("github.com/steveyegge/beads")) {
		t.Fatalf("pinned Beads build info is unavailable: %v\n%s", err, output)
	}
	path := filepath.Join(root, "build-info.txt")
	if err := os.WriteFile(path, output, 0o600); err != nil {
		t.Fatal(err)
	}
	return output
}

func recordPublicRun(t *testing.T, root, storeRoot, name string, artifact publicArtifact, fixture recordingFixture) publicRun {
	t.Helper()
	run := publicRun{Name: name, Root: filepath.Join(root, name)}
	if err := os.MkdirAll(run.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	storeRoot = filepath.Join(storeRoot, name)
	source := filepath.Join(storeRoot, "source")
	target := filepath.Join(storeRoot, "target")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{source, target} {
		home := filepath.Join(dir, "home")
		if err := os.MkdirAll(filepath.Join(home, ".config"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	pinned := filepath.Join(storeRoot, "bd-pinned")
	copyPinned(t, artifact.Binary, pinned, artifact.SHA256, artifact.Size)
	env := recordingEnv(source)
	commandGit := func(label, store string, dir string, argv []string) []byte {
		return runCommand(t, run.Root, &run.Commands, label, store, dir, env(dir), append([]string{"git"}, argv...), nil)
	}
	commandBD := func(label, store, dir string, argv []string, stdin []byte) []byte {
		return runCommand(t, run.Root, &run.Commands, label, store, dir, env(dir), append([]string{pinned}, argv...), stdin)
	}
	commandGit("source-git-init", "source", source, []string{"-c", "init.defaultBranch=main", "init", "--quiet"})
	version := commandBD("source-version", "source", source, []string{"version", "--json"}, nil)
	var identity struct {
		Commit string `json:"commit"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(version), &identity); err != nil || identity.Commit != publicBeadsCommit {
		t.Fatalf("pinned version identity changed: %v %s", err, version)
	}
	commandBD("source-init", "source", source, []string{"init", "--prefix", "pub", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, nil)
	commandBD("source-config", "source", source, []string{"--json", "config", "set", fixture.ConfigKey, "paired-public"}, nil)
	configOutput := commandBD("source-config-list", "source", source, []string{"--json", "config", "list"}, nil)
	var settings map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(configOutput), &settings); err != nil {
		t.Fatalf("real config list is invalid JSON: %v %s", err, configOutput)
	}
	rawSetting, ok := settings[fixture.ConfigKey]
	if !ok {
		t.Fatalf("real config list omitted %s: key is absent %s", fixture.ConfigKey, configOutput)
	}
	var configured string
	if err := json.Unmarshal(rawSetting, &configured); err != nil || configured != "paired-public" {
		t.Fatalf("real config list omitted %s: %v %s", fixture.ConfigKey, err, configOutput)
	}
	commandBD("source-remember", "source", source, []string{"--json", "remember", "paired-store memory", "--key", fixture.MemoryKey}, nil)
	commandBD("source-create-root", "source", source, []string{"--json", "create", "--id", "pub-root", "--type", "task", "--title", "Public root", "--description", "relational root", "--metadata", `{"card":"B27","public":true}`, "--labels", "alpha,beta", "--spec-id", "s1/b27"}, nil)
	commandBD("source-create-child", "source", source, []string{"--json", "create", "--type", "bug", "--title", "Public child", "--description", "parented child", "--parent", "pub-root", "--metadata", `{"child":true}`, "--labels", "child,public", "--priority", "1"}, nil)
	commandBD("source-create-dependency", "source", source, []string{"--json", "create", "--id", "pub-dependency", "--type", "task", "--title", "Public dependency", "--description", "dependency row"}, nil)
	commandBD("source-dependency", "source", source, []string{"--json", "dep", "add", publicChildID, "pub-dependency"}, nil)
	commandBD("source-label", "source", source, []string{"--json", "label", "add", "pub-root", "reviewed"}, nil)
	commandBD("source-comment", "source", source, []string{"--json", "comment", "pub-root", "preserve this public comment"}, nil)
	commandBD("source-close", "source", source, []string{"--json", "close", "pub-dependency", "--reason", "dependency complete"}, nil)

	run.SourceExportPath = filepath.Join(run.Root, "source-export.jsonl")
	commandBD("source-export", "source", source, []string{"export", "--include-memories", "--output", run.SourceExportPath}, nil)
	sourceExport, err := os.ReadFile(run.SourceExportPath)
	if err != nil {
		t.Fatal(err)
	}
	run.SourceExportSHA256 = hashBytes(sourceExport)
	sourceRawProjection, err := projectJSONL(sourceExport, fixture.ProjectionFields)
	if err != nil {
		t.Fatalf("source JSONL projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "source-jsonl-projection.json"), sourceRawProjection)
	validateSourceRows(t, sourceRawProjection, fixture)
	validateInventoryAndArchive(t, &run, commandBD, "source", source, run.Root, fixture)

	commandGit("target-git-init", "target", target, []string{"-c", "init.defaultBranch=main", "init", "--quiet"})
	commandBD("target-version", "target", target, []string{"version", "--json"}, nil)
	commandBD("target-init", "target", target, []string{"init", "--prefix", "pub", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, nil)
	commandBD("target-create-keep", "target", target, []string{"--json", "create", "--id", "pub-keep", "--type", "task", "--title", "Keep target row", "--description", "must survive import"}, nil)
	commandBD("target-import", "target", target, []string{"--json", "import", run.SourceExportPath}, nil)
	run.TargetExportPath = filepath.Join(run.Root, "target-export.jsonl")
	commandBD("target-export", "target", target, []string{"export", "--include-memories", "--output", run.TargetExportPath}, nil)
	targetExport, err := os.ReadFile(run.TargetExportPath)
	if err != nil {
		t.Fatal(err)
	}
	run.TargetExportSHA256 = hashBytes(targetExport)
	targetRawProjection, err := projectJSONL(targetExport, fixture.ProjectionFields)
	if err != nil {
		t.Fatalf("target JSONL projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "target-jsonl-projection.json"), targetRawProjection)
	assertTargetContainsSource(t, sourceRawProjection, targetRawProjection, fixture)
	validateInventoryAndArchive(t, &run, commandBD, "target", target, run.Root, fixture)

	sourceArchive, err := dumpformat.ReadFile(run.SourceArchivePath)
	if err != nil {
		t.Fatalf("source plug-dump validation: %v", err)
	}
	targetArchive, err := dumpformat.ReadFile(run.TargetArchivePath)
	if err != nil {
		t.Fatalf("target plug-dump validation: %v", err)
	}
	sourceArchiveProjection, err := projectArchive(sourceArchive, fixture.ProjectionFields)
	if err != nil {
		t.Fatalf("source archive projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "source-archive-projection.json"), sourceArchiveProjection)
	targetArchiveProjection, err := projectArchive(targetArchive, fixture.ProjectionFields)
	if err != nil {
		t.Fatalf("target archive projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "target-archive-projection.json"), targetArchiveProjection)
	if !bytes.Equal(sourceRawProjection, sourceArchiveProjection) || !bytes.Equal(targetRawProjection, targetArchiveProjection) {
		t.Fatalf("independent JSONL/archive projections differ: source=%s/%s target=%s/%s", hashBytes(sourceRawProjection), hashBytes(sourceArchiveProjection), hashBytes(targetRawProjection), hashBytes(targetArchiveProjection))
	}
	run.SourceProjectionSHA = hashBytes(sourceRawProjection)
	run.TargetProjectionSHA = hashBytes(targetRawProjection)
	labels := make([]string, len(run.Commands))
	for i, receipt := range run.Commands {
		labels[i] = receipt.Label
		if receipt.Exit != 0 || receipt.StdoutSHA256 == "" || receipt.StderrSHA256 == "" {
			t.Fatalf("command %s did not retain a complete successful receipt", receipt.Label)
		}
	}
	if !equalJSON(labels, fixture.Commands) {
		t.Fatalf("recorded command sequence changed: got=%v want=%v", labels, fixture.Commands)
	}
	writeCommandReceipts(t, run.Root, run.Commands)
	return run
}

func validateInventoryAndArchive(t *testing.T, run *publicRun, commandBD func(string, string, string, []string, []byte) []byte, label, store, root string, fixture recordingFixture) {
	t.Helper()
	path := filepath.Join(root, label+"-inventory.json")
	commandBD(label+"-inventory", label, store, []string{"--readonly", "export", "--format", "beads-inventory/1", "--output", path}, nil)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s inventory was not published: %v", label, err)
	}
	if err := validateInventory(data, fixture); err != nil {
		t.Fatalf("%s inventory is not a truthful complete public census: %v", label, err)
	}
	if _, err := os.Stat(path + ".sources"); err != nil {
		t.Fatalf("%s inventory sidecars were not retained: %v", label, err)
	}
	archivePath := filepath.Join(root, label+"-plug-dump.tar")
	commandBD(label+"-dump", label, store, []string{"--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", path, "--output", archivePath}, nil)
	archive, err := dumpformat.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("%s raw plug-dump is invalid: %v", label, err)
	}
	manifest := archive.Manifest()
	if manifest.Format != dumpformat.Format || manifest.Schema != 1 || manifest.Reference != dumpformat.BeadsReference || manifest.Scope != "offline-head-and-source-archives" || len(manifest.Projects) != 1 || len(archive.Records("bead")) < 3 {
		t.Fatalf("%s archive is not the pinned public head-state format: %+v", label, manifest)
	}
	if label == "source" {
		run.SourceInventoryPath, run.SourceArchivePath = path, archivePath
		run.SourceArchiveSHA256 = archive.SHA256()
	} else {
		run.TargetInventoryPath, run.TargetArchivePath = path, archivePath
		run.TargetArchiveSHA256 = archive.SHA256()
	}
}

func validateInventory(data []byte, fixture recordingFixture) error {
	var inventory struct {
		Version         string                           `json:"version"`
		Quiesced        bool                             `json:"quiesced"`
		RemotesMerged   bool                             `json:"remotes_merged"`
		WorkingSetClean bool                             `json:"working_set_clean"`
		History         string                           `json:"history"`
		External        []struct{ Family, State string } `json:"external"`
		ConfigPolicy    map[string]string                `json:"config_policy"`
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		return err
	}
	if inventory.Version != "beads-inventory/1" || !inventory.Quiesced || !inventory.RemotesMerged || !inventory.WorkingSetClean || (inventory.History != "unavailable" && inventory.History != "available-unverified") {
		return fmt.Errorf("invalid capture declarations: %+v", inventory)
	}
	expected := map[string]bool{}
	for _, family := range []string{"user-legacy-config", "user-xdg-config", "user-native-config", "project-config", "beads-dir-config", "project-local-config", "environment-and-flags", "metadata-json", "legacy-config-json", "beads-hooks", "legacy-hooks", "git-hooks", "external-hook-managers", "routes-jsonl", "formulas", "formula-search-paths", "issues-jsonl", "interactions-jsonl", "dolt-ignore", "dolt-nonlocal-tables", "dolt-working-set", "dolt-commit-graph", "dolt-remotes", "dolt-conflicts", "dolt-history-maintenance", "store-files-and-quiescence"} {
		expected[family] = true
	}
	if len(inventory.External) != len(expected) {
		return fmt.Errorf("external family count=%d want=%d", len(inventory.External), len(expected))
	}
	for _, entry := range inventory.External {
		if !expected[entry.Family] || (entry.State != "absent" && entry.State != "json-config") {
			return fmt.Errorf("unexpected external entry: %+v", entry)
		}
		delete(expected, entry.Family)
	}
	if len(expected) != 0 || len(inventory.ConfigPolicy) == 0 {
		return fmt.Errorf("external/config policy census is incomplete: missing=%v policy=%v", expected, inventory.ConfigPolicy)
	}
	foundConfig := false
	for key, policy := range inventory.ConfigPolicy {
		if policy != "public" && policy != "secret" {
			return fmt.Errorf("invalid config policy %s=%s", key, policy)
		}
		if strings.HasSuffix(key, "/"+fixture.ConfigKey) {
			foundConfig = true
		}
	}
	if !foundConfig {
		return fmt.Errorf("actual config policy does not include %s", fixture.ConfigKey)
	}
	return nil
}

func projectJSONL(data []byte, fields []string) ([]byte, error) {
	projection := semanticProjection{Issues: map[string]map[string]any{}, Memories: map[string]any{}, Edges: []map[string]any{}, Comments: []map[string]any{}}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	for {
		var row map[string]any
		if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		if row["_type"] == "memory" {
			key, _ := row["key"].(string)
			if key == "" {
				return nil, fmt.Errorf("memory row has no key")
			}
			projection.Memories[key] = row["value"]
			continue
		}
		if row["_type"] != "issue" {
			return nil, fmt.Errorf("unsupported JSONL row type %v", row["_type"])
		}
		id, _ := row["id"].(string)
		if id == "" {
			return nil, fmt.Errorf("issue row has no id")
		}
		item := map[string]any{}
		for _, field := range fields {
			switch field {
			case "status_category":
				item[field] = statusCategory(row["status"])
			case "source_type":
				item[field] = row["issue_type"]
			default:
				item[field] = row[field]
			}
		}
		projection.Issues[id] = item
		projection.Edges = append(projection.Edges, projectedDependencies(row["dependencies"])...)
		projection.Comments = append(projection.Comments, projectedNested(row["comments"], []string{"id", "issue_id", "author", "created_by", "text", "body"})...)
	}
	sortProjection(&projection)
	return json.Marshal(projection)
}

func projectArchive(archive *dumpformat.Archive, fields []string) ([]byte, error) {
	projection := semanticProjection{Issues: map[string]map[string]any{}, Memories: map[string]any{}, Edges: []map[string]any{}, Comments: []map[string]any{}}
	for _, record := range archive.Records("bead") {
		item := map[string]any{}
		for _, field := range fields {
			item[field] = record.Body[field]
		}
		projection.Issues[record.ID] = item
	}
	for _, record := range archive.Records("edge") {
		projection.Edges = append(projection.Edges, projectMap(record.Body, []string{"from", "to", "target_kind", "type"}))
	}
	for _, record := range archive.Records("comment") {
		projection.Comments = append(projection.Comments, projectMap(record.Body, []string{"id", "issue_id", "author", "created_by", "text", "body"}))
	}
	for _, record := range archive.Records("memory") {
		key, _ := record.Body["key"].(string)
		if key == "" {
			key = record.ID
		}
		projection.Memories[key] = record.Body["value"]
	}
	sortProjection(&projection)
	return json.Marshal(projection)
}

func projectedNested(value any, fields []string) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if ok {
			result = append(result, projectMap(object, fields))
		}
	}
	return result
}

func projectedDependencies(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			continue
		}
		edge := map[string]any{"from": object["issue_id"], "type": object["type"]}
		if target, ok := object["depends_on_id"]; ok {
			edge["to"], edge["target_kind"] = target, "depends_on_issue_id"
		} else if target, ok := object["depends_on_external"]; ok {
			edge["to"], edge["target_kind"] = target, "depends_on_external"
		}
		result = append(result, edge)
	}
	return result
}

func projectMap(input map[string]any, fields []string) map[string]any {
	result := map[string]any{}
	for _, field := range fields {
		if value, ok := input[field]; ok {
			result[field] = value
		}
	}
	return result
}

func sortProjection(projection *semanticProjection) {
	sort.SliceStable(projection.Edges, func(i, j int) bool { return canonical(projection.Edges[i]) < canonical(projection.Edges[j]) })
	sort.SliceStable(projection.Comments, func(i, j int) bool { return canonical(projection.Comments[i]) < canonical(projection.Comments[j]) })
}

func validateSourceRows(t *testing.T, projection []byte, fixture recordingFixture) {
	t.Helper()
	var value semanticProjection
	if err := json.Unmarshal(projection, &value); err != nil {
		t.Fatal(err)
	}
	for _, id := range fixture.IDs {
		if id == "pub-keep" {
			continue
		}
		if _, ok := value.Issues[id]; !ok {
			t.Fatalf("source recording omitted issue %s", id)
		}
	}
	if value.Memories[fixture.MemoryKey] != "paired-store memory" {
		t.Fatalf("source recording omitted memory %s: %+v", fixture.MemoryKey, value.Memories)
	}
	wantEdges := []map[string]any{
		{"from": publicChildID, "to": "pub-dependency", "target_kind": "depends_on_issue_id", "type": "blocks"},
		{"from": publicChildID, "to": "pub-root", "target_kind": "depends_on_issue_id", "type": "parent-child"},
	}
	if !equalJSON(value.Edges, wantEdges) {
		t.Fatalf("source recording changed dependency/parent relations: got=%s want=%s", canonical(value.Edges), canonical(wantEdges))
	}
	if len(value.Comments) != 1 || value.Comments[0]["issue_id"] != "pub-root" || value.Comments[0]["text"] != "preserve this public comment" {
		t.Fatalf("source recording changed the public comment: %s", canonical(value.Comments))
	}
}

func assertTargetContainsSource(t *testing.T, source, target []byte, fixture recordingFixture) {
	t.Helper()
	if err := comparePublicImport(source, target, fixture); err != nil {
		t.Fatal(err)
	}
}

// This comparison deliberately checks relation contents and multiplicity. Equal
// counts would miss an import that replaces an endpoint, type, author, or body.
func comparePublicImport(source, target []byte, fixture recordingFixture) error {
	left, err := decodePublicProjection(source)
	if err != nil {
		return fmt.Errorf("source projection: %w", err)
	}
	right, err := decodePublicProjection(target)
	if err != nil {
		return fmt.Errorf("target projection: %w", err)
	}
	if _, ok := right.Issues["pub-keep"]; !ok {
		return fmt.Errorf("target import deleted the pre-existing target issue")
	}
	for id, issue := range left.Issues {
		if !equalJSON(issue, right.Issues[id]) {
			return fmt.Errorf("target import changed source issue %s", id)
		}
	}
	if !equalJSON(left.Memories, right.Memories) {
		return fmt.Errorf("target import changed source memories")
	}
	if !equalJSON(left.Edges, right.Edges) {
		return fmt.Errorf("target import changed dependency/parent relations: source=%s target=%s", canonical(left.Edges), canonical(right.Edges))
	}
	if !equalJSON(left.Comments, right.Comments) {
		return fmt.Errorf("target import changed comment contents: source=%s target=%s", canonical(left.Comments), canonical(right.Comments))
	}
	for _, id := range fixture.IDs {
		if id != "pub-keep" && right.Issues[id] == nil {
			return fmt.Errorf("target import omitted source issue %s", id)
		}
	}
	return nil
}

func decodePublicProjection(data []byte) (semanticProjection, error) {
	var projection semanticProjection
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&projection); err != nil {
		return projection, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return projection, fmt.Errorf("projection has trailing data: %v", err)
	}
	return projection, nil
}

// Authored measurement controls, not recordings: prove that the import
// comparison rejects substituted relations even when the row counts match.
func TestPublicProjectionRejectsRelationCorruption(t *testing.T) {
	source := semanticProjection{
		Issues: map[string]map[string]any{"pub-root": {"title": "Public root"}, publicChildID: {"title": "Public child"}, "pub-dependency": {"title": "Public dependency"}},
		Edges: []map[string]any{
			{"from": publicChildID, "to": "pub-dependency", "target_kind": "depends_on_issue_id", "type": "blocks"},
			{"from": publicChildID, "to": "pub-root", "target_kind": "depends_on_issue_id", "type": "parent-child"},
		},
		Comments: []map[string]any{{"id": json.Number("9007199254740993"), "issue_id": "pub-root", "author": "public-recorder", "text": "preserve this public comment"}},
		Memories: map[string]any{"public-memory": "paired-store memory"},
	}
	fixture := recordingFixture{IDs: []string{"pub-root", publicChildID, "pub-dependency", "pub-keep"}}
	sourceBytes, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*semanticProjection)
		want   string
	}{
		{"unchanged", func(*semanticProjection) {}, ""},
		{"dependency_endpoint", func(p *semanticProjection) { p.Edges[0]["to"] = "pub-keep" }, "dependency/parent relations"},
		{"dependency_type", func(p *semanticProjection) { p.Edges[0]["type"] = "relates-to" }, "dependency/parent relations"},
		{"parent_endpoint", func(p *semanticProjection) { p.Edges[1]["to"] = "pub-keep" }, "dependency/parent relations"},
		{"duplicate_edge", func(p *semanticProjection) { p.Edges = append(p.Edges, p.Edges[0]) }, "dependency/parent relations"},
		{"comment_text", func(p *semanticProjection) { p.Comments[0]["text"] = "replaced" }, "comment contents"},
		{"comment_author", func(p *semanticProjection) { p.Comments[0]["author"] = "another-actor" }, "comment contents"},
		{"comment_issue", func(p *semanticProjection) { p.Comments[0]["issue_id"] = publicChildID }, "comment contents"},
		{"comment_integer_precision", func(p *semanticProjection) { p.Comments[0]["id"] = json.Number("9007199254740992") }, "comment contents"},
		{"duplicate_comment", func(p *semanticProjection) { p.Comments = append(p.Comments, p.Comments[0]) }, "comment contents"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target, err := decodePublicProjection(sourceBytes)
			if err != nil {
				t.Fatal(err)
			}
			target.Issues["pub-keep"] = map[string]any{"title": "Keep target row"}
			tc.mutate(&target)
			targetBytes, err := json.Marshal(target)
			if err != nil {
				t.Fatal(err)
			}
			err = comparePublicImport(sourceBytes, targetBytes, fixture)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("corrupt import error=%v, want %s", err, tc.want)
			}
		})
	}
}

func copyPinned(t *testing.T, source, destination, expected string, expectedSize int64) {
	t.Helper()
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o500)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, h), in)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil || n != expectedSize || hex.EncodeToString(h.Sum(nil)) != expected {
		t.Fatalf("pinned executable copy failed: bytes=%d/%d copy=%v close=%v digest=%s/%s", n, expectedSize, copyErr, closeErr, hex.EncodeToString(h.Sum(nil)), expected)
	}
}

func recordingEnv(store string) func(string) []string {
	return func(dir string) []string {
		home := filepath.Join(dir, "home")
		return []string{"PATH=/usr/bin:/bin", "HOME=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "BEADS_DIR=" + filepath.Join(dir, ".beads"), "LANG=C.UTF-8", "TZ=UTC", "CI=true", "DO_NOT_TRACK=1", "BEADS_NO_DAEMON=1", "BEADS_ACTOR=public-recorder", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Public recorder", "GIT_AUTHOR_EMAIL=public@example.invalid", "GIT_COMMITTER_NAME=Public recorder", "GIT_COMMITTER_EMAIL=public@example.invalid"}
	}
}

func runCommand(t *testing.T, root string, receipts *[]commandReceipt, label, store, dir string, env []string, argv []string, stdin []byte) []byte {
	t.Helper()
	if len(argv) == 0 {
		t.Fatal("empty command argv")
	}
	command := argv[0]
	args := argv[1:]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir, cmd.Env = dir, env
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exit = exitErr.ExitCode()
		}
	}
	n := len(*receipts) + 1
	base := fmt.Sprintf("%03d-%s", n, strings.ReplaceAll(label, "/", "_"))
	stdoutPath, stderrPath := filepath.Join(root, base+".stdout"), filepath.Join(root, base+".stderr")
	if writeErr := os.WriteFile(stdoutPath, stdout.Bytes(), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if writeErr := os.WriteFile(stderrPath, stderr.Bytes(), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	receipt := commandReceipt{Label: label, Store: store, Argv: append([]string(nil), args...), StdoutSHA256: hashBytes(stdout.Bytes()), StderrSHA256: hashBytes(stderr.Bytes()), Exit: exit, StdoutPath: stdoutPath, StderrPath: stderrPath}
	if stdin != nil {
		receipt.StdinSHA256 = hashBytes(stdin)
	}
	*receipts = append(*receipts, receipt)
	// Flush before failing so the failing command and every preceding command
	// retain their argv, exit status and raw-output hashes in Bazel outputs.
	writeCommandReceipts(t, root, *receipts)
	if err != nil {
		t.Fatalf("public command %s failed (exit %d): %v\nstdout=%s\nstderr=%s", label, exit, err, stdout.String(), stderr.String())
	}
	return stdout.Bytes()
}

func writeCommandReceipts(t *testing.T, root string, commands []commandReceipt) {
	t.Helper()
	data, err := json.MarshalIndent(commands, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "commands.json"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hashFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return hashBytes(data)
}

func canonical(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func equalJSON(left, right any) bool {
	a, err := json.Marshal(left)
	if err != nil {
		return false
	}
	b, err := json.Marshal(right)
	return err == nil && bytes.Equal(a, b)
}

func statusCategory(value any) string {
	switch value {
	case "open":
		return "unstarted"
	case "in_progress", "hooked":
		return "started"
	case "blocked", "deferred", "pinned", "frozen":
		return "backlog"
	case "closed":
		return "completed"
	default:
		return fmt.Sprint(value)
	}
}

func isLowerHex(value string) bool {
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}
