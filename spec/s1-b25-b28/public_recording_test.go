package s1b25b28_test

// This is an opt-in runtime recording.  It deliberately consumes the public
// native artifact prepared by RunsOn instead of building, downloading, or
// faking a Beads executable in the test. Raw evidence goes to Bazel's undeclared
// outputs; executable copies and disposable stores stay under /tmp/plug-swarm/work.
// RunsOn retains undeclared outputs; each recording still needs its own receipt.

import (
	"archive/tar"
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
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/publicnative"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const (
	publicBeadsCommit = "305fec3afac83224f46c54f982959fd99db136fa"
	publicBeadsTree   = "48e3545b7c5c6a5cdc0119e34dfa379354456a6e"
	publicChildID     = "pub-root.1"
)

var requirePublicRecording = flag.Bool("public-recording", false, "Require the independently delivered public Beads recording artifact")
var publicRecordingID = flag.String("public-recording-id", "", "One independent RunsOn recording: recording1 or recording2")

type recordingProfile struct {
	Schema   string `json:"schema"`
	Exporter struct {
		TargetKey string `json:"target_key"`
		Label     string `json:"label"`
		Binary    string `json:"binary"`
	} `json:"exporter"`
	VersionKey     string `json:"version_key"`
	GoModuleSHA256 string `json:"go_module_sha256"`
	Producer       struct {
		RunID          string `json:"run_id"`
		ManifestSHA256 string `json:"manifest_sha256"`
		WorkflowSHA    string `json:"workflow_sha"`
		WorkflowBranch string `json:"workflow_branch"`
	} `json:"producer"`
}

type recordingFixture struct {
	Schema           string            `json:"schema"`
	SourceCommit     string            `json:"source_commit"`
	SourceTree       string            `json:"source_tree"`
	Commands         []string          `json:"commands"`
	ProjectionFields []string          `json:"projection_fields"`
	IDs              []string          `json:"ids"`
	MemoryKey        string            `json:"memory_key"`
	ConfigKey        string            `json:"config_key"`
	ExternalFamilies []string          `json:"external_families"`
	StatusCategories map[string]string `json:"status_categories"`
}

type publicArtifact struct {
	Binary       string
	SHA256       string
	BuildRun     string
	ManifestPath string
	ManifestSHA  string
	Size         int64
}

type publicNativeServer struct {
	Port     int    `json:"port"`
	PID      int    `json:"pid"`
	DataRoot string `json:"data_root"`
	provider *publicnative.Server
}

// The producer owns provisioning, listener identity and lifecycle. The caller
// consumes its in-process binding; no endpoint is supplied by a wrapper.
func requireNativePublicServer(t *testing.T, output string) publicNativeServer {
	t.Helper()
	server := publicnative.StartDolt(t, output)
	return publicNativeServer{Port: server.Port, PID: server.PID, DataRoot: server.DataRoot, provider: server}
}

type commandReceipt struct {
	Label        string   `json:"label"`
	Store        string   `json:"store"`
	Argv         []string `json:"argv"`
	Directory    string   `json:"directory"`
	Environment  []string `json:"environment"`
	StdinSHA256  string   `json:"stdin_sha256,omitempty"`
	StdoutSHA256 string   `json:"stdout_sha256"`
	StderrSHA256 string   `json:"stderr_sha256"`
	Exit         int      `json:"exit"`
	StdoutPath   string   `json:"stdout_path"`
	StderrPath   string   `json:"stderr_path"`
}

type publicRun struct {
	Name                 string
	Root                 string
	SourceExportPath     string
	TargetExportPath     string
	SourceInventoryPath  string
	TargetInventoryPath  string
	SourceArchivePath    string
	TargetArchivePath    string
	SourceExportSHA256   string
	TargetExportSHA256   string
	SourceArchiveSHA256  string
	TargetArchiveSHA256  string
	SourceProjectionSHA  string
	TargetProjectionSHA  string
	PlugSourceProjection map[string]string
	PlugTargetProjection map[string]string
	Commands             []commandReceipt
}

type semanticProjection struct {
	Issues   map[string]map[string]any `json:"issues"`
	Edges    []map[string]any          `json:"edges"`
	Comments []map[string]any          `json:"comments"`
	Memories map[string]any            `json:"memories"`
}

// Observations come from source readbacks/setup, never from the archive or
// reverse converter. The public corpus declares public values only. A future
// admitted observer may supply metadata or exact secret references explicitly.
type publicConfigObservation struct {
	Table          string `json:"table"`
	Key            string `json:"key"`
	Value          any    `json:"value"`
	Classification string `json:"classification"`
}

type publicConfigExpectation struct {
	Config     map[string]dumpformat.Record `json:"config"`
	UserMemory map[string]dumpformat.Record `json:"user_memory"`
	Policy     map[string]string            `json:"policy"`
	Memories   map[string]any               `json:"reverse_memories"`
	References []string                     `json:"config_references"`
}

// TestPublicNativeRecording is intentionally manual. The target fails when
// the trusted bundle environment is absent; a missing artifact is never
// converted into a skip or a successful fixture result.
func TestPublicNativeRecording(t *testing.T) {
	if !*requirePublicRecording {
		t.Fatal("public recording target requires -public-recording")
	}
	if *publicRecordingID != "recording1" && *publicRecordingID != "recording2" {
		t.Fatal("native recording requires a recording1/recording2 identity")
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
	writeRecordingBytes(t, filepath.Join(root, "native-public-manifest.json"), manifest)
	buildInfo := recordBuildInfo(t, root, artifact, profile)
	t.Logf("PUBLIC_RECORDING_ARTIFACT build_run=%s binary_sha256=%s manifest_sha256=%s build_info_sha256=%s", artifact.BuildRun, artifact.SHA256, artifact.ManifestSHA, hashBytes(buildInfo))

	server := requireNativePublicServer(t, root)
	run := recordPublicRun(t, root, storeRoot, *publicRecordingID, artifact, fixture, server)
	if run.SourceArchiveSHA256 == "" || run.TargetArchiveSHA256 == "" {
		t.Fatal("recording did not retain both validated archives")
	}
	if err := server.provider.Stop(); err != nil {
		t.Fatalf("producer-owned server cleanup: %v", err)
	}
	provenance := map[string]any{
		"schema":            fixture.Schema,
		"recording_id":      *publicRecordingID,
		"pair_status":       "requires-independent-second-run-and-comparison",
		"source_commit":     publicBeadsCommit,
		"source_tree":       publicBeadsTree,
		"build_run":         artifact.BuildRun,
		"binary_sha256":     artifact.SHA256,
		"manifest_sha256":   artifact.ManifestSHA,
		"build_info_sha256": hashBytes(buildInfo),
		"projection_sha256": map[string]string{
			"source": run.SourceProjectionSHA,
			"target": run.TargetProjectionSHA,
		},
		"runs": []any{run},
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
	data := recordingRunfile(t, "testdata/native-public-profile.json")
	if err := json.Unmarshal(data, &profile); err != nil || profile.Schema != "plug-public-native/1" || profile.Exporter.TargetKey == "" || profile.Exporter.Label == "" || profile.Exporter.Binary == "" || profile.VersionKey == "" || len(profile.GoModuleSHA256) != 64 || !isLowerHex(profile.GoModuleSHA256) || profile.Producer.RunID == "" || len(profile.Producer.ManifestSHA256) != 64 || !isLowerHex(profile.Producer.ManifestSHA256) || len(profile.Producer.WorkflowSHA) != 40 || !isLowerHex(profile.Producer.WorkflowSHA) || profile.Producer.WorkflowBranch == "" {
		t.Fatalf("native public profile is invalid: %v", err)
	}
	return profile
}

func readRecordingFixture(t *testing.T) recordingFixture {
	t.Helper()
	var fixture recordingFixture
	data := recordingRunfile(t, "testdata/native-public-recording-fixture.json")
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("public recording fixture is invalid: %v", err)
	}
	families := map[string]bool{}
	for _, family := range fixture.ExternalFamilies {
		if strings.TrimSpace(family) == "" || families[family] {
			t.Fatal("public recording fixture has an empty or duplicate source family")
		}
		families[family] = true
	}
	if len(families) != 26 || len(fixture.StatusCategories) != 8 {
		t.Fatal("public recording fixture lacks the complete pinned source profile")
	}
	for status, category := range fixture.StatusCategories {
		if status == "" || category != "unstarted" && category != "started" && category != "backlog" && category != "completed" {
			t.Fatal("public recording fixture has an invalid status projection")
		}
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
	if artifact.ManifestSHA != profile.Producer.ManifestSHA256 || artifact.BuildRun != profile.Producer.RunID {
		t.Fatal("native public manifest or producer run differs from the reviewed profile")
	}
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
		Workflow struct {
			Ref     string `json:"ref"`
			SHA     string `json:"sha"`
			Profile string `json:"profile"`
		} `json:"workflow"`
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
	if manifest.Schema != profile.Schema || beadsSource.Commit != publicBeadsCommit || beadsSource.Tree != publicBeadsTree || target.Label != profile.Exporter.Label || target.Binary != profile.Exporter.Binary || binaryInfo.SHA256 != artifact.SHA256 || binaryInfo.Mode != "0o755" || binaryInfo.Size <= 0 || manifest.Build.RunID != artifact.BuildRun || manifest.Workflow.SHA != profile.Producer.WorkflowSHA || manifest.Workflow.Ref != "refs/heads/"+profile.Producer.WorkflowBranch || manifest.Workflow.Profile != profile.Schema || version.Commit != publicBeadsCommit || version.Build != "public-"+publicBeadsCommit {
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

func recordBuildInfo(t *testing.T, root string, artifact publicArtifact, profile recordingProfile) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, runErr := exec.CommandContext(ctx, "go", "version", "-m", artifact.Binary).CombinedOutput()
	path := filepath.Join(root, "build-info.txt")
	if err := os.WriteFile(path, output, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("PUBLIC_BUILD_INFO path=%s sha256=%s", path, hashBytes(output))
	if runErr != nil {
		t.Fatalf("pinned Beads build info is unavailable: %v\n%s", runErr, output)
	}
	if err := checkPublicBuildInfo(output, profile.GoModuleSHA256, profile.Exporter.Label); err != nil {
		t.Fatalf("pinned Beads main package is not profile-bound: %v\n%s", err, output)
	}
	return output
}

// Bazel's Go binary records its main package path without a mod record. Bind
// that complete path to the sealed exporter label and the unchanged module
// hash. Any present mod record must agree; dependencies cannot supply either.
func checkPublicBuildInfo(data []byte, expectedModuleSHA256, exporterLabel string) error {
	if len(expectedModuleSHA256) != 64 || !isLowerHex(expectedModuleSHA256) {
		return errors.New("invalid source module hash pin")
	}
	targetPackage, target, ok := strings.Cut(exporterLabel, ":")
	if !ok || !strings.HasPrefix(targetPackage, "//") || target == "" || strings.ContainsAny(target, ":/\\ \t\r\n") {
		return errors.New("invalid sealed exporter label")
	}
	relativePackage := strings.TrimPrefix(targetPackage, "//")
	for _, part := range strings.Split(relativePackage, "/") {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, "\\ \t\r\n") {
			return errors.New("invalid sealed exporter package")
		}
	}
	packageSuffix := "/" + relativePackage
	pathCount, moduleCount := 0, 0
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "path":
			pathCount++
			if len(fields) != 2 || !strings.HasSuffix(fields[1], packageSuffix) {
				return errors.New("main package does not match the sealed exporter label")
			}
			module := strings.TrimSuffix(fields[1], packageSuffix)
			if hashBytes([]byte(module)) != expectedModuleSHA256 {
				return errors.New("main package module does not match the sealed source profile")
			}
		case "mod":
			moduleCount++
			if len(fields) < 3 || hashBytes([]byte(fields[1])) != expectedModuleSHA256 {
				return errors.New("main module does not match the sealed source profile")
			}
		}
	}
	if pathCount != 1 || moduleCount > 1 {
		return errors.New("build info must contain exactly one main package and at most one matching module")
	}
	return nil
}

func recordPublicRun(t *testing.T, root, storeRoot, name string, artifact publicArtifact, fixture recordingFixture, server publicNativeServer) publicRun {
	t.Helper()
	run := publicRun{Name: name, Root: filepath.Join(root, name)}
	if err := os.MkdirAll(run.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	storeRoot = filepath.Join(storeRoot, name)
	// Use supported nested workspaces so project and active source locations
	// remain distinct. Repository-root capture retains its separate refusal.
	source := filepath.Join(storeRoot, "source", "workspace")
	target := filepath.Join(storeRoot, "target", "workspace")
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
	pinned := filepath.Join(storeRoot, "native-exporter-pinned")
	copyPinned(t, artifact.Binary, pinned, artifact.SHA256, artifact.Size)
	if server.Port < 1 || server.Port > 65535 || server.PID < 1 || !filepath.IsAbs(server.DataRoot) {
		t.Fatal("producer helper did not return a complete in-process server binding")
	}
	dataRoot, err := filepath.EvalSymlinks(server.DataRoot)
	if err != nil || dataRoot != server.DataRoot {
		t.Fatal("producer data root must be the actual physical directory")
	}
	baseEnv := recordingEnv(server.DataRoot)
	env := func(dir string) []string { return server.provider.ChildEnv(baseEnv(dir)) }
	commandGit := func(label, store string, dir string, argv []string) []byte {
		return runCommand(t, run.Root, &run.Commands, label, store, dir, env(dir), append([]string{"git"}, argv...), nil)
	}
	commandReference := func(label, store, dir string, argv []string, stdin []byte) []byte {
		return runCommand(t, run.Root, &run.Commands, label, store, dir, env(dir), append([]string{pinned}, argv...), stdin)
	}
	// Config list is the stored-settings readback. Use the CLI's explicit JSON
	// envelope so its numeric schema_version cannot masquerade as a setting.
	commandConfigList := func(label, store, dir string) []byte {
		return runCommand(t, run.Root, &run.Commands, label, store, dir,
			append(env(dir), "BD_JSON_ENVELOPE=1"), []string{pinned, "--json", "config", "list"}, nil)
	}
	commandMetadata := func(label, store, dir string) []byte {
		return runCommand(t, run.Root, &run.Commands, label, store, dir,
			append(env(dir), "BD_JSON_ENVELOPE=1"), []string{pinned, "--readonly", "--json", "sql", "SELECT `key`, `value` FROM metadata ORDER BY `key`"}, nil)
	}
	commandGit("source-git-init", "source", source, []string{"-C", "..", "-c", "init.defaultBranch=main", "init", "--quiet"})
	version := commandReference("source-version", "source", source, []string{"version", "--json"}, nil)
	var identity struct {
		Commit string `json:"commit"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(version), &identity); err != nil || identity.Commit != publicBeadsCommit {
		t.Fatalf("pinned version identity changed: %v %s", err, version)
	}
	commandReference("source-init", "source", source, nativeInitArgs(source, server.Port), nil)
	recordNativeDatabaseBinding(t, run.Root, "source", source, server)
	// Record the existing disabled auto-export default as real parsed YAML.
	commandReference("source-export-config", "source", source, []string{"--json", "config", "set", "export.auto", "false"}, nil)
	commandReference("source-config", "source", source, []string{"--json", "config", "set", fixture.ConfigKey, "paired-public"}, nil)
	configOutput := commandConfigList("source-config-list", "source", source)
	settings, err := readPublicConfigSettings(configOutput)
	if err != nil {
		t.Fatalf("real config list is invalid: %v %s", err, configOutput)
	}
	configured, found := "", false
	for _, setting := range settings {
		if setting.Key == fixture.ConfigKey {
			configured, found = setting.Value.(string), true // The readback parser requires string settings.
			break
		}
	}
	if !found || configured != "paired-public" {
		t.Fatalf("real config list omitted %s: %s", fixture.ConfigKey, configOutput)
	}
	commandReference("source-remember", "source", source, []string{"--json", "remember", "paired-store memory", "--key", fixture.MemoryKey}, nil)
	commandReference("source-create-root", "source", source, []string{"--json", "create", "--id", "pub-root", "--type", "task", "--title", "Public root", "--description", "relational root", "--metadata", `{"card":"B27","public":true}`, "--labels", "alpha,beta", "--spec-id", "s1/b27"}, nil)
	commandReference("source-create-child", "source", source, []string{"--json", "create", "--type", "bug", "--title", "Public child", "--description", "parented child", "--parent", "pub-root", "--metadata", `{"child":true}`, "--labels", "child,public", "--priority", "1"}, nil)
	commandReference("source-create-dependency", "source", source, []string{"--json", "create", "--id", "pub-dependency", "--type", "task", "--title", "Public dependency", "--description", "dependency row"}, nil)
	commandReference("source-dependency", "source", source, []string{"--json", "dep", "add", publicChildID, "pub-dependency"}, nil)
	commandReference("source-label", "source", source, []string{"--json", "label", "add", "pub-root", "reviewed"}, nil)
	commandReference("source-comment", "source", source, []string{"--json", "comment", "pub-root", "preserve this public comment"}, nil)
	commandReference("source-close", "source", source, []string{"--json", "close", "pub-dependency", "--reason", "dependency complete"}, nil)
	commandReference("source-commit", "source", source, []string{"dolt", "commit"}, nil)

	run.SourceExportPath = filepath.Join(run.Root, "source-export.jsonl")
	commandReference("source-export", "source", source, []string{"--readonly", "export", "--include-memories", "--output", run.SourceExportPath}, nil)
	sourceExport, err := os.ReadFile(run.SourceExportPath)
	if err != nil {
		t.Fatal(err)
	}
	run.SourceExportSHA256 = hashBytes(sourceExport)
	sourceRawProjection, err := projectJSONL(sourceExport, fixture.ProjectionFields, fixture.StatusCategories)
	if err != nil {
		t.Fatalf("source JSONL projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "source-jsonl-projection.json"), sourceRawProjection)
	validateSourceRows(t, sourceRawProjection, fixture)
	sourceConfig := commandConfigList("source-config-readback", "source", source)
	sourceMetadata := commandMetadata("source-metadata-readback", "source", source)
	sourceExpected := observePublicConfig(t, run.Root, "source", sourceConfig, sourceMetadata, sourceRawProjection)
	if observed, ok := sourceExpected.Config["config/"+fixture.ConfigKey]; !ok || observed.Source.Fields["value"] != configured {
		t.Fatal("source setup config changed before independent archive observation")
	}
	validateInventoryAndArchive(t, &run, commandReference, "source", source, run.Root, fixture)

	commandGit("target-git-init", "target", target, []string{"-C", "..", "-c", "init.defaultBranch=main", "init", "--quiet"})
	commandReference("target-version", "target", target, []string{"version", "--json"}, nil)
	commandReference("target-init", "target", target, nativeInitArgs(target, server.Port), nil)
	recordNativeDatabaseBinding(t, run.Root, "target", target, server)
	commandReference("target-export-config", "target", target, []string{"--json", "config", "set", "export.auto", "false"}, nil)
	commandReference("target-create-keep", "target", target, []string{"--json", "create", "--id", "pub-keep", "--type", "task", "--title", "Keep target row", "--description", "must survive import"}, nil)
	commandReference("target-import", "target", target, []string{"--json", "import", run.SourceExportPath}, nil)
	commandReference("target-commit", "target", target, []string{"dolt", "commit"}, nil)
	run.TargetExportPath = filepath.Join(run.Root, "target-export.jsonl")
	commandReference("target-export", "target", target, []string{"--readonly", "export", "--include-memories", "--output", run.TargetExportPath}, nil)
	targetExport, err := os.ReadFile(run.TargetExportPath)
	if err != nil {
		t.Fatal(err)
	}
	run.TargetExportSHA256 = hashBytes(targetExport)
	targetRawProjection, err := projectJSONL(targetExport, fixture.ProjectionFields, fixture.StatusCategories)
	if err != nil {
		t.Fatalf("target JSONL projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "target-jsonl-projection.json"), targetRawProjection)
	assertTargetContainsSource(t, sourceRawProjection, targetRawProjection, fixture)
	targetConfig := commandConfigList("target-config-readback", "target", target)
	targetMetadata := commandMetadata("target-metadata-readback", "target", target)
	targetExpected := observePublicConfig(t, run.Root, "target", targetConfig, targetMetadata, targetRawProjection)
	validateInventoryAndArchive(t, &run, commandReference, "target", target, run.Root, fixture)

	sourceArchive, err := dumpformat.ReadFile(run.SourceArchivePath)
	if err != nil {
		t.Fatalf("source plug-dump validation: %v", err)
	}
	targetArchive, err := dumpformat.ReadFile(run.TargetArchivePath)
	if err != nil {
		t.Fatalf("target plug-dump validation: %v", err)
	}
	assertPublicConfigArchive(t, sourceArchive, run.SourceInventoryPath, sourceExpected)
	assertPublicConfigArchive(t, targetArchive, run.TargetInventoryPath, targetExpected)
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
	run.PlugSourceProjection = replayPublicArchive(t, run.Root, storeRoot, "source", sourceArchive, sourceRawProjection, sourceExpected, fixture)
	run.PlugTargetProjection = replayPublicArchive(t, run.Root, storeRoot, "target", targetArchive, targetRawProjection, targetExpected, fixture)
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

// The producer owns the server executable, provisioning and lifecycle. The
// consumer can address only its explicit loopback port and creates fresh,
// per-workspace database names. No external DSN or producer pin is accepted.
func nativeDatabaseName(store string) string { return "b27_" + hashBytes([]byte(store))[:24] }

func nativeInitArgs(store string, port int) []string {
	return []string{"init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(port), "--database", nativeDatabaseName(store), "--prefix", "pub", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive"}
}

func recordNativeDatabaseBinding(t *testing.T, output, label, store string, server publicNativeServer) {
	t.Helper()
	dir := server.provider.RequireDatabaseDir(t, nativeDatabaseName(store))
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("actual Dolt database directory is missing or is a link: %s: %v", dir, err)
	}
	data, err := json.MarshalIndent(map[string]any{"server": server, "database": nativeDatabaseName(store), "database_directory": dir, "mode": info.Mode().String()}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeRecordingBytes(t, filepath.Join(output, label+"-server-binding.json"), append(data, '\n'))
}

// Replay consumes the exact native producer envelope. It never manufactures an
// archive or treats a converter refusal as success. Snapshot is an explicit
// head-state slice under P5c/P44/P22/P46; this does not qualify source history.
func replayPublicArchive(t *testing.T, output, storeRoot, label string, archive *dumpformat.Archive, expected []byte, config publicConfigExpectation, fixture recordingFixture) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	data := archive.TarBytes()
	inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("%s actual archive cannot enter Plug replay: %v", label, err)
	}
	profile := driver.RestoreImportProfile{
		Admission: driver.RestoreImportAdmission{
			Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "public-recorder"},
			Authority: "restore-admin", AdmissionDigest: hashBytes([]byte("s1-b27-disposable-public-import")),
		},
		SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import",
		IdempotencyKey: "s1-b27-public-" + label, ArchiveDigest: inspection.ArchiveSHA256,
		DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
	hashes := map[string]string{}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		if !t.Run(label+"-plug-"+engine, func(t *testing.T) {
			var store driver.Store
			path := filepath.Join(storeRoot, "plug-"+label+".sqlite")
			var sqlDriver driver.Driver
			dsn := path
			var err error
			if engine == "memory" {
				store, err = memory.NewWithOptions(memory.Options{StoreID: driver.StoreID("s1-b27-public-" + label), Incarnation: "s1-b27-public-" + label})
			} else {
				sqlDriver = sqlite.Driver{}
				if engine == "postgres" {
					sqlDriver = postgres.Driver{}
					dsn = testclock.PostgresDSN(t)
				}
				store, err = sqlDriver.Open(ctx, driver.Config{DSN: dsn})
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if store != nil {
					if err := store.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			if _, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), store, profile); err != nil {
				t.Fatalf("%s real Plug restore: %v", engine, err)
			}
			compare := func(suffix string) {
				live, err := converterroundtrip.ReverseRestored(ctx, store)
				if err != nil {
					t.Fatalf("%s live Plug projection%s: %v", engine, suffix, err)
				}
				base := filepath.Join(output, label+"-plug-"+engine+suffix)
				writeRecordingBytes(t, base+".jsonl", live.Result.JSONL)
				report, err := json.MarshalIndent(live.Result.Report, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				writeRecordingBytes(t, base+"-report.json", report)
				projection, err := projectJSONL(live.Result.JSONL, fixture.ProjectionFields, fixture.StatusCategories)
				if err != nil {
					t.Fatalf("%s actual Plug JSONL projection%s: %v", engine, suffix, err)
				}
				writeRecordingBytes(t, base+"-projection.json", projection)
				if err := comparePublicReverse(expected, projection, config, live.Result.Report.ConfigReferences, live.Result.Report.ConfigAsMemories); err != nil {
					t.Fatalf("%s Plug replay%s changed recorded semantics: %v", engine, suffix, err)
				}
				hashes[engine+suffix] = hashBytes(projection)
			}
			compare("")
			if sqlDriver != nil {
				err := store.Close()
				store = nil
				if err != nil {
					t.Fatal(err)
				}
				store, err = sqlDriver.Open(ctx, driver.Config{DSN: dsn})
				if err != nil {
					t.Fatal(err)
				}
				compare("-reopen")
			}
			recordPublicSubscriber(t, ctx, store, filepath.Join(output, label+"-plug-"+engine+"-subscriber.json"), fixture.IDs[:3])
		}) {
			t.Fatalf("%s Plug replay stopped at %s; no later engine qualifies", label, engine)
		}
	}
	return hashes
}

type publicPostObservation struct {
	Change   port.CommittedChange `json:"change"`
	Snapshot driver.View          `json:"snapshot"`
	Rows     []driver.Record      `json:"rows"`
}

type publicPostRecorder struct {
	mu           sync.Mutex
	Observations []publicPostObservation
}

func (p *publicPostRecorder) After(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
	view := reader.Snapshot()
	if view.At == nil || view.At.String() != change.Revision.String() || len(change.Changes) != 1 {
		return errors.New("public subscriber did not receive the committed single-row snapshot")
	}
	c := change.Changes[0]
	rows, err := reader.Get(ctx, []model.Ref{c.Resource.Ref})
	if err != nil {
		return err
	}
	if len(rows) != 1 || !bytes.Equal(rows[0].Body, c.After) {
		return errors.New("public subscriber readback differs from the committed after image")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Observations = append(p.Observations, publicPostObservation{change, view, rows})
	return nil
}

// These are real public service writes on the just-imported native rows. The
// offline import itself makes no subscriber claim. Evidence is flushed before
// every assertion, including a refused or unexpectedly published operation.
func recordPublicSubscriber(t *testing.T, ctx context.Context, store driver.Store, path string, ids []string) {
	t.Helper()
	if len(ids) != 3 {
		t.Fatal("public subscriber recording requires exact root, child and dependency IDs")
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	ontologyRows, err := r.DerivedRows(ctx, driver.DerivedOntology, "")
	closeErr := r.Close()
	if err != nil || closeErr != nil || len(ontologyRows) != 1 || ontologyRows[0].Key != "builtin" {
		t.Fatalf("native replay stored ontology: %v / %v", err, closeErr)
	}
	doc, err := ontology.Decode(ontologyRows[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("native replay stored ontology compilation: %v", problems)
	}
	lease, err := store.Lease(ctx, "s1-native-public-subscriber", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	observer := &publicPostRecorder{}
	rules := service.NewOntology(snapshot.Effective)
	w := pipeline.New(store, lease, pipeline.WithOntology(rules), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "s1-native-public-observer", Subscriber: observer}))
	domain := service.New(store, lease, w, service.WithOntology(rules))
	actor := model.PrincipalRef{Kind: "user", ID: "public-recorder"}
	read := func() map[string]pipeline.Document {
		meta, err := store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		refs := []model.Ref{model.RefByID(ids[0], model.Natural64(0)), model.RefByID(ids[1], model.Natural64(0)), model.RefByID(ids[2], model.Natural64(0))}
		rows, err := r.Get(ctx, refs)
		if err != nil || len(rows) != len(refs) {
			t.Fatalf("native public readback: %v", err)
		}
		out := map[string]pipeline.Document{}
		for _, row := range rows {
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				t.Fatal(err)
			}
			out[doc.ID] = doc
		}
		return out
	}
	var receipts []json.RawMessage
	postCount := func() int {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		return len(observer.Observations)
	}
	appendReceipt := func(v any) {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, data)
	}
	flush := func() {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		data, err := json.MarshalIndent(map[string]any{"schema": "plug-s1-native-public-subscriber/1", "surface": "service.UpdateWithOutcome", "operations": receipts, "posts": observer.Observations}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		writeRecordingBytes(t, path, append(data, '\n'))
	}
	requests := []service.UpdateRequest{
		{ID: model.RefByID(ids[0], model.Natural64(0)), Fields: map[string]any{"title": "Public root after scalar update"}},
		{ID: model.RefByID(ids[1], model.Natural64(0)), Fields: map[string]any{"title": "Public child after scalar update"}},
		{ID: model.RefByID(ids[0], model.Natural64(0)), Fields: map[string]any{"labels": []string{"native-reviewed"}}},
	}
	var last port.WriteOutcome
	for i, req := range requests {
		before := read()
		meta := model.WriteMeta{Project: before[req.ID.ID].Project, IdempotencyKey: hashBytes([]byte(fmt.Sprintf("s1-native-public-%d", i)))}
		outcome, err := domain.UpdateWithOutcome(ctx, actor, req, meta)
		after := read()
		appendReceipt(map[string]any{"method": "bead.update", "request": req, "meta": meta, "outcome": outcome, "error": fmt.Sprint(err), "before": before, "after": after})
		flush()
		if err != nil || outcome.Replayed || len(outcome.Commit.Warnings) != 0 || postCount() != i+1 {
			t.Fatalf("native public update/subscriber: %+v err=%v", outcome, err)
		}
		expected := before[req.ID.ID]
		for field, value := range req.Fields {
			expected.Fields[field] = value
			if field == "labels" {
				expected.Labels = value.([]string)
			}
		}
		before[req.ID.ID] = expected
		if !equalJSON(before, after) {
			t.Fatal("native public update changed unpatched fields, metadata, parent, links, comments or close provenance")
		}
		replayed, replayErr := domain.UpdateWithOutcome(ctx, actor, req, meta)
		appendReceipt(map[string]any{"method": "bead.update", "request": req, "meta": meta, "outcome": replayed, "error": fmt.Sprint(replayErr), "after": read()})
		flush()
		if replayErr != nil || !replayed.Replayed || !equalJSON(outcome.Commit, replayed.Commit) || postCount() != i+1 || !equalJSON(after, read()) {
			t.Fatal("native public replay changed state, receipt or subscriber count")
		}
		last = outcome
	}
	before := read()
	noopMeta := model.WriteMeta{Project: before[ids[0]].Project, IdempotencyKey: hashBytes([]byte("s1-native-public-equal-labels"))}
	noop, err := domain.UpdateWithOutcome(ctx, actor, requests[2], noopMeta)
	appendReceipt(map[string]any{"method": "bead.update", "request": requests[2], "meta": noopMeta, "outcome": noop, "error": fmt.Sprint(err), "after": read()})
	flush()
	if err != nil || noop.Replayed || noop.Commit.Revision.String() != last.Commit.Revision.String() || postCount() != 3 || !equalJSON(before, read()) {
		t.Fatal("native public equal replacement created a revision, subscriber call or changed state")
	}
	t.Logf("PUBLIC_SUBSCRIBER_RECORDING path=%s posts=%d scalar=2 replacement=1 replay=3 noop=1", path, postCount())
}

// This control derives a declared hash-allocator variant of the explicitly
// synthetic archive. The original sequential allocator remains unsupported;
// none of these authored bytes is a native recording or producer evidence.
func authoredSubscriberArchive(t *testing.T, original []byte) []byte {
	t.Helper()
	if hashBytes(original) != "c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c" {
		t.Fatal("original synthetic control fixture changed")
	}
	archive, err := dumpformat.Read(bytes.NewReader(original))
	if err != nil {
		t.Fatal(err)
	}
	manifest := archive.Manifest()
	if len(manifest.Projects) != 1 || manifest.Projects[0].IDMode != "sequential" || manifest.Projects[0].HashLength != 0 {
		t.Fatal("original synthetic allocator declaration changed")
	}
	var authoredManifest map[string]json.RawMessage
	if err := json.Unmarshal(archiveMember(t, original, "manifest.json"), &authoredManifest); err != nil {
		t.Fatal(err)
	}
	var projects []map[string]json.RawMessage
	if err := json.Unmarshal(authoredManifest["projects"], &projects); err != nil {
		t.Fatal(err)
	}
	projects[0]["id_mode"], projects[0]["hash_length"] = json.RawMessage(`"hash"`), json.RawMessage(`1`)
	authoredManifest["projects"], err = json.Marshal(projects)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := json.Marshal(authoredManifest)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	reader := tar.NewReader(bytes.NewReader(original))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "manifest.json" {
			data = manifestBytes
		}
		header.Size = int64(len(data))
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := dumpformat.Read(bytes.NewReader(output.Bytes())); err != nil {
		t.Fatalf("authored synthetic control validation: %v", err)
	}
	t.Logf("AUTHORED_SUBSCRIBER_INPUT original_sha256=%s authored_sha256=%s only_delta=manifest_allocator_mode_and_width native=false", hashBytes(original), hashBytes(output.Bytes()))
	return output.Bytes()
}

func archiveMember(t *testing.T, data []byte, name string) []byte {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err != nil {
			t.Fatalf("authored fixture member %s: %v", name, err)
		}
		if header.Name == name {
			data, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			return data
		}
	}
}

// Exercise actual restore/service/subscribers on three engines. The manual
// native target selects only TestPublicNativeRecording and excludes this input.
func TestPublicProjectionAuthoredImportPublicSubscriberAcrossDrivers(t *testing.T) {
	path := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = authoredSubscriberArchive(t, data)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			var store driver.Store
			var err error
			switch engine {
			case "memory":
				store = memory.New()
			case "sqlite":
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "control.sqlite")})
			case "postgres":
				store, err = (postgres.Driver{}).Open(ctx, driver.Config{DSN: testclock.PostgresDSN(t)})
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			profile := driver.RestoreImportProfile{
				Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "authored-measurement"}, Authority: "restore-admin", AdmissionDigest: hashBytes([]byte("authored-measurement"))},
				SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: "authored-subscriber-control", ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
			}
			if _, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), store, profile); err != nil {
				t.Fatal(err)
			}
			output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if output == "" {
				output = t.TempDir()
			}
			recordPublicSubscriber(t, ctx, store, filepath.Join(output, "authored-subscriber-control-"+engine+".json"), []string{"demo-7", "demo-7.4", "demo-8"})
		})
	}
}

func validateInventoryAndArchive(t *testing.T, run *publicRun, commandReference func(string, string, string, []string, []byte) []byte, label, store, root string, fixture recordingFixture) {
	t.Helper()
	path := filepath.Join(root, label+"-inventory.json")
	commandReference(label+"-inventory", label, store, []string{"--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", path}, nil)
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
	commandReference(label+"-dump", label, store, []string{"--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", path, "-o", archivePath}, nil)
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
	for _, family := range fixture.ExternalFamilies {
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
	for key, policy := range inventory.ConfigPolicy {
		if policy != "public" && policy != "secret" {
			return fmt.Errorf("invalid config policy %s=%s", key, policy)
		}
	}
	return nil
}

func observePublicConfig(t *testing.T, output, label string, settings, metadata, ordinary []byte) publicConfigExpectation {
	t.Helper()
	observations, err := readPublicConfigSettings(settings)
	if err != nil {
		t.Fatalf("%s independent config readback: %v", label, err)
	}
	metadataRows, err := readPublicMetadata(metadata)
	if err != nil {
		t.Fatalf("%s independent metadata readback: %v", label, err)
	}
	observations = append(observations, metadataRows...)
	expected, err := expectPublicConfig(observations, ordinary)
	if err != nil {
		t.Fatalf("%s independent config expectations: %v", label, err)
	}
	data, err := json.MarshalIndent(expected, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeRecordingBytes(t, filepath.Join(output, label+"-config-expectation.json"), data)
	return expected
}

// The genuine config-list command returns stored settings in the explicit
// JSON data envelope, excluding the KV plane. Retain every data key and reject
// duplicate keys in either object rather than allowing map decoding to conceal
// them. The envelope's numeric schema_version is protocol metadata, not a
// setting. User memories are observed in ordinary JSONL independently.
// Metadata has its own read-only SQL observation; unobserved entries cannot pass.
func readPublicConfigSettings(data []byte) ([]publicConfigObservation, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("config readback must be an envelope object")
	}
	observations := []publicConfigObservation{}
	seenEnvelope := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok || seenEnvelope[key] {
			return nil, errors.New("duplicate or invalid config readback envelope key")
		}
		seenEnvelope[key] = true
		switch key {
		case "schema_version":
			var version any
			if err := decoder.Decode(&version); err != nil {
				return nil, errors.New("unsupported config readback schema_version")
			}
			number, ok := version.(json.Number)
			if !ok || number != "1" {
				return nil, errors.New("unsupported config readback schema_version")
			}
		case "data":
			start, err := decoder.Token()
			if err != nil || start != json.Delim('{') {
				return nil, errors.New("config readback data must be an object")
			}
			seenSettings := map[string]bool{}
			for decoder.More() {
				token, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				setting, ok := token.(string)
				if !ok || setting == "" || seenSettings[setting] || strings.HasPrefix(setting, "kv.") {
					return nil, errors.New("empty, duplicate or non-setting config readback key")
				}
				var rawValue any
				if err := decoder.Decode(&rawValue); err != nil {
					return nil, fmt.Errorf("config readback value must be a string: %w", err)
				}
				value, ok := rawValue.(string)
				if !ok {
					return nil, errors.New("config readback value must be a string")
				}
				seenSettings[setting] = true
				observations = append(observations, publicConfigObservation{Table: "config", Key: setting, Value: value, Classification: "public"})
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return nil, errors.New("config readback data is not closed")
			}
		default:
			return nil, errors.New("unexpected config readback envelope key")
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !seenEnvelope["schema_version"] || !seenEnvelope["data"] {
		return nil, errors.New("config readback envelope is incomplete")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("config readback has trailing data")
	}
	return observations, nil
}

// Observe the fresh public corpus through the pinned CLI's read-only SQL
// command, independently of inventory and archive bytes. Retain every metadata
// row and reject ambiguous envelopes, duplicate rows and non-string values.
func readPublicMetadata(data []byte) ([]publicConfigObservation, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("metadata readback must be an envelope object")
	}
	observations := []publicConfigObservation{}
	seenEnvelope, seenKeys := map[string]bool{}, map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok || seenEnvelope[key] {
			return nil, errors.New("duplicate or invalid metadata readback envelope key")
		}
		seenEnvelope[key] = true
		switch key {
		case "schema_version":
			var version any
			if err := decoder.Decode(&version); err != nil {
				return nil, err
			}
			if number, ok := version.(json.Number); !ok || number != "1" {
				return nil, errors.New("unsupported metadata readback schema_version")
			}
		case "data":
			start, err := decoder.Token()
			if err != nil || start != json.Delim('[') {
				return nil, errors.New("metadata readback data must be an array")
			}
			for decoder.More() {
				start, err := decoder.Token()
				if err != nil || start != json.Delim('{') {
					return nil, errors.New("metadata readback row must be an object")
				}
				row := map[string]string{}
				for decoder.More() {
					token, err := decoder.Token()
					if err != nil {
						return nil, err
					}
					field, ok := token.(string)
					if _, duplicate := row[field]; !ok || duplicate || (field != "key" && field != "value") {
						return nil, errors.New("duplicate or unexpected metadata readback field")
					}
					var rawValue any
					if err := decoder.Decode(&rawValue); err != nil {
						return nil, err
					}
					value, ok := rawValue.(string)
					if !ok {
						return nil, errors.New("metadata readback fields must be strings")
					}
					row[field] = value
				}
				end, err := decoder.Token()
				if err != nil || end != json.Delim('}') || len(row) != 2 || row["key"] == "" || seenKeys[row["key"]] {
					return nil, errors.New("incomplete or duplicate metadata readback row")
				}
				seenKeys[row["key"]] = true
				observations = append(observations, publicConfigObservation{Table: "metadata", Key: row["key"], Value: row["value"], Classification: "public"})
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return nil, errors.New("metadata readback array is not closed")
			}
		default:
			return nil, errors.New("unexpected metadata readback envelope key")
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !seenEnvelope["schema_version"] || !seenEnvelope["data"] {
		return nil, errors.New("metadata readback envelope is incomplete")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("metadata readback has trailing data")
	}
	return observations, nil
}

func expectPublicConfig(observations []publicConfigObservation, ordinary []byte) (publicConfigExpectation, error) {
	expected := publicConfigExpectation{
		Config: map[string]dumpformat.Record{}, UserMemory: map[string]dumpformat.Record{},
		Policy: map[string]string{}, Memories: map[string]any{}, References: []string{},
	}
	projection, err := decodePublicProjection(ordinary)
	if err != nil {
		return expected, err
	}
	for key, value := range projection.Memories {
		if _, ok := value.(string); key == "" || !ok {
			return expected, errors.New("ordinary memory observation requires a key and string value")
		}
		expected.Memories[key] = value
		expected.Policy["config/kv.memory."+key] = "public"
		expected.UserMemory[key] = dumpformat.Record{Kind: "memory", ID: key,
			Body:   dumpformat.Row{"key": key, "value": value, "classification": "public"},
			Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": "kv.memory." + key, "value": value}}}
	}
	references := map[string]bool{}
	var collectReferences func(any)
	collectReferences = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			if ref, ok := value["$secret"].(string); ok && len(value) == 1 && ref != "" {
				references[ref] = true
			}
			for _, item := range value {
				collectReferences(item)
			}
		case []any:
			for _, item := range value {
				collectReferences(item)
			}
		}
	}
	for _, observed := range observations {
		id := observed.Table + "/" + observed.Key
		if (observed.Table != "config" && observed.Table != "metadata") || observed.Key == "" || (observed.Table == "config" && strings.HasPrefix(observed.Key, "kv.memory.")) {
			return expected, errors.New("unsupported independent config source identity")
		}
		if _, duplicate := expected.Config[id]; duplicate {
			return expected, fmt.Errorf("duplicate config observation %q", id)
		}
		if observed.Classification != "public" && observed.Classification != "secret" {
			return expected, fmt.Errorf("unclassified config observation %q", id)
		}
		if observed.Classification == "secret" && !equalJSON(observed.Value, map[string]any{"$secret": id}) {
			return expected, fmt.Errorf("config observation %q requires its exact secret reference", id)
		}
		value, ok := observed.Value.(string)
		if !ok {
			encoded, err := json.Marshal(observed.Value)
			if err != nil {
				return expected, err
			}
			value = string(encoded)
		}
		key := "plug.config/" + observed.Key
		if _, collision := expected.Memories[key]; collision {
			return expected, fmt.Errorf("reserved config memory-key collision %q", key)
		}
		expected.Memories[key] = value
		expected.Policy[id] = observed.Classification
		expected.Config[id] = dumpformat.Record{Kind: "config", ID: id,
			Body:   dumpformat.Row{"key": id, "value": observed.Value, "classification": observed.Classification},
			Source: dumpformat.Provenance{Table: observed.Table, Fields: dumpformat.Row{"key": observed.Key, "value": observed.Value}}}
		references[id] = true
		collectReferences(observed.Value)
	}
	for ref := range references {
		expected.References = append(expected.References, ref)
	}
	sort.Strings(expected.References)
	return expected, nil
}

func assertPublicConfigArchive(t *testing.T, archive *dumpformat.Archive, inventoryPath string, expected publicConfigExpectation) {
	t.Helper()
	data, err := os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatal(err)
	}
	var inventory struct {
		ConfigPolicy map[string]string `json:"config_policy"`
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Fatal(err)
	}
	if err := comparePublicConfigSources(archive.Records("config"), archive.Records("memory"), inventory.ConfigPolicy, expected); err != nil {
		t.Fatal(err)
	}
}

func comparePublicConfigSources(config, memories []dumpformat.Record, policy map[string]string, expected publicConfigExpectation) error {
	for _, group := range []struct {
		kind string
		got  []dumpformat.Record
		want map[string]dumpformat.Record
	}{{"config", config, expected.Config}, {"memory", memories, expected.UserMemory}} {
		seen := map[string]bool{}
		for _, record := range group.got {
			if seen[record.ID] {
				return fmt.Errorf("duplicate archive %s ID %q", group.kind, record.ID)
			}
			seen[record.ID] = true
			want, ok := group.want[record.ID]
			if !ok || !equalJSON(record, want) {
				return fmt.Errorf("archive %s source/body differs from independent observation %q", group.kind, record.ID)
			}
		}
		if len(seen) != len(group.want) {
			return fmt.Errorf("archive %s source census is incomplete", group.kind)
		}
	}
	if !equalJSON(policy, expected.Policy) {
		return errors.New("config policy differs from complete independent observations")
	}
	return nil
}

// Ordinary import retains exactly its original user memories. Reverse adds an
// explicitly enumerated config-memory set; no prefix is discarded or treated
// as effective source configuration. Report checks are independent of JSONL.
func comparePublicReverse(ordinary, actual []byte, expected publicConfigExpectation, references []string, configCount int) error {
	want, err := decodePublicProjection(ordinary)
	if err != nil {
		return err
	}
	got, err := decodePublicProjection(actual)
	if err != nil {
		return err
	}
	if !equalJSON(got.Issues, want.Issues) || !equalJSON(got.Edges, want.Edges) || !equalJSON(got.Comments, want.Comments) {
		return errors.New("reverse changed issue/relation/comment semantics")
	}
	if !equalJSON(got.Memories, expected.Memories) {
		return errors.New("reverse config/user-memory set or values differ")
	}
	if configCount != len(expected.Config) || len(references) != len(expected.References) || len(references) > 0 && !equalJSON(references, expected.References) {
		return errors.New("reverse config report differs from independent observations")
	}
	return nil
}

func projectJSONL(data []byte, fields []string, categories map[string]string) ([]byte, error) {
	projection := semanticProjection{Issues: map[string]map[string]any{}, Memories: map[string]any{}, Edges: []map[string]any{}, Comments: []map[string]any{}}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	first := true
	for {
		var row map[string]any
		if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		if _, header := row["_schema"]; header {
			if !first || len(row) != 2 || row["_schema"] != "beads-jsonl/1" || row["_sort"] != "stable-v1" {
				return nil, fmt.Errorf("unsupported or misplaced JSONL header")
			}
			first = false
			continue
		}
		first = false
		if row["_type"] == "memory" {
			key, _ := row["key"].(string)
			if key == "" {
				return nil, fmt.Errorf("memory row has no key")
			}
			if _, duplicate := projection.Memories[key]; duplicate {
				return nil, fmt.Errorf("duplicate memory key %q", key)
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
				item[field] = statusCategory(row["status"], categories)
			case "source_type":
				item[field] = row["issue_type"]
			default:
				value, present := row[field]
				// The pinned CLI omits these typed empty values from JSONL.
				// Normalize only absent keys; explicit null stays observable.
				if !present {
					switch field {
					case "labels":
						value = []any{}
					case "metadata":
						value = map[string]any{}
					case "spec_id", "close_reason":
						value = ""
					}
				}
				item[field] = value
			}
		}
		projection.Issues[id] = item
		projection.Edges = append(projection.Edges, projectedDependencies(row["dependencies"])...)
		if parent, ok := row["parent"].(string); ok && parent != "" {
			projection.Edges = append(projection.Edges, map[string]any{"from": id, "to": parent, "target_kind": "depends_on_issue_id", "type": "parent-child"})
		}
		projection.Comments = append(projection.Comments, projectedNested(row["comments"], []string{"id", "issue_id", "author", "created_by", "text", "body"})...)
	}
	sortProjection(&projection)
	return json.Marshal(projection)
}

func projectArchive(archive *dumpformat.Archive, fields []string) ([]byte, error) {
	projection := semanticProjection{Issues: map[string]map[string]any{}, Memories: map[string]any{}, Edges: []map[string]any{}, Comments: []map[string]any{}}
	for _, record := range archive.Records("bead") {
		projection.Issues[record.ID] = projectArchiveBead(record, fields)
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

func projectArchiveBead(record dumpformat.Record, fields []string) map[string]any {
	item := map[string]any{}
	for _, field := range fields {
		if field == "close_reason" {
			// The canonical normalized body omits this column; the exact source
			// provenance retains it. Never turn a real reason into a null match.
			item[field] = record.Source.Fields[field]
		} else {
			item[field] = record.Body[field]
		}
	}
	return item
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
	for id := range right.Issues {
		if id != "pub-keep" {
			if _, source := left.Issues[id]; !source {
				return fmt.Errorf("target import introduced issue %s", id)
			}
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
		{"extra_issue", func(p *semanticProjection) {
			p.Issues["pub-extra"] = map[string]any{"title": "unexpected target issue"}
		}, "target import introduced issue"},
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

func TestPublicProjectionArchiveCloseReasonAndReverseHeader(t *testing.T) {
	fields := []string{"title", "close_reason"}
	raw := []byte(`{"_type":"issue","id":"pub-dependency","title":"Public dependency","close_reason":"dependency complete"}` + "\n")
	want, err := projectJSONL(raw, fields, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodePublicProjection(want)
	if err != nil {
		t.Fatal(err)
	}
	// This is an authored projection control, not an executable recording or
	// an archive delivered to restore. The canonical normalized body omits the
	// close reason while its independent raw source column retains the value.
	record := dumpformat.Record{
		Body:   dumpformat.Row{"title": "Public dependency"},
		Source: dumpformat.Provenance{Fields: dumpformat.Row{"close_reason": "dependency complete"}},
	}
	if got := projectArchiveBead(record, fields); !equalJSON(got, decoded.Issues["pub-dependency"]) {
		t.Fatalf("archive projection lost the retained close reason: %s", canonical(got))
	}
	header := []byte(`{"_schema":"beads-jsonl/1","_sort":"stable-v1"}` + "\n")
	for _, tc := range []struct {
		name string
		data []byte
		ok   bool
	}{
		{"supported_header", append(append([]byte{}, header...), raw...), true},
		{"unknown_schema", []byte(`{"_schema":"unknown/1","_sort":"stable-v1"}` + "\n"), false},
		{"duplicate_header", append(append([]byte{}, header...), header...), false},
		{"late_header", append(append([]byte{}, raw...), header...), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := projectJSONL(tc.data, fields, nil)
			if tc.ok {
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("versioned reverse framing changed semantics: err=%v got=%s", err, got)
				}
			} else if err == nil {
				t.Fatal("unsupported framing was silently discarded")
			}
		})
	}
}

// All config controls below use independently enumerated authored observations
// and output rows. They do not invoke a producer, converter, restore or store.
type publicConfigControl struct {
	Ordinary     []byte
	Observations []publicConfigObservation
	Config       []dumpformat.Record
	Memory       []dumpformat.Record
	Policy       map[string]string
	Rows         []map[string]any
	References   []string
	Count        int
}

func newPublicConfigControl(t *testing.T) publicConfigControl {
	t.Helper()
	ordinary, err := projectJSONL([]byte(`{"_type":"issue","id":"pub-root","title":"Public root"}
{"_type":"memory","key":"public-memory","value":"user value"}
{"_type":"memory","key":"plug.config/user-owned","value":"keep this user memory"}
`), []string{"title"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return publicConfigControl{
		Ordinary: ordinary,
		Observations: []publicConfigObservation{
			{Table: "config", Key: "custom.public_recording", Value: "paired-public", Classification: "public"},
			{Table: "metadata", Key: "api_token", Value: map[string]any{"$secret": "metadata/api_token"}, Classification: "secret"},
		},
		Config: []dumpformat.Record{
			{Kind: "config", ID: "config/custom.public_recording", Body: dumpformat.Row{"key": "config/custom.public_recording", "value": "paired-public", "classification": "public"}, Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": "custom.public_recording", "value": "paired-public"}}},
			{Kind: "config", ID: "metadata/api_token", Body: dumpformat.Row{"key": "metadata/api_token", "value": map[string]any{"$secret": "metadata/api_token"}, "classification": "secret"}, Source: dumpformat.Provenance{Table: "metadata", Fields: dumpformat.Row{"key": "api_token", "value": map[string]any{"$secret": "metadata/api_token"}}}},
		},
		Memory: []dumpformat.Record{
			{Kind: "memory", ID: "public-memory", Body: dumpformat.Row{"key": "public-memory", "value": "user value", "classification": "public"}, Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": "kv.memory.public-memory", "value": "user value"}}},
			{Kind: "memory", ID: "plug.config/user-owned", Body: dumpformat.Row{"key": "plug.config/user-owned", "value": "keep this user memory", "classification": "public"}, Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": "kv.memory.plug.config/user-owned", "value": "keep this user memory"}}},
		},
		Policy: map[string]string{"config/custom.public_recording": "public", "metadata/api_token": "secret", "config/kv.memory.public-memory": "public", "config/kv.memory.plug.config/user-owned": "public"},
		Rows: []map[string]any{
			{"_type": "issue", "id": "pub-root", "title": "Public root"},
			{"_type": "memory", "key": "public-memory", "value": "user value"},
			{"_type": "memory", "key": "plug.config/user-owned", "value": "keep this user memory"},
			{"_type": "memory", "key": "plug.config/custom.public_recording", "value": "paired-public"},
			{"_type": "memory", "key": "plug.config/api_token", "value": `{"$secret":"metadata/api_token"}`},
		},
		References: []string{"config/custom.public_recording", "metadata/api_token"}, Count: 2,
	}
}

func TestPublicProjectionConfigSources(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*publicConfigControl)
	}{
		{"unchanged", func(*publicConfigControl) {}},
		{"dropped_config", func(c *publicConfigControl) { c.Config = c.Config[1:] }},
		{"extra_config", func(c *publicConfigControl) { r := c.Config[0]; r.ID = "config/extra"; c.Config = append(c.Config, r) }},
		{"duplicate_config_id", func(c *publicConfigControl) { c.Config = append(c.Config, c.Config[0]) }},
		{"same_count_substitution", func(c *publicConfigControl) { c.Config[0].ID = "config/other" }},
		{"classification", func(c *publicConfigControl) { c.Config[0].Body["classification"] = "secret" }},
		{"body_value", func(c *publicConfigControl) { c.Config[0].Body["value"] = "substituted" }},
		{"source_value", func(c *publicConfigControl) { c.Config[0].Source.Fields["value"] = "substituted" }},
		{"body_source_substitution", func(c *publicConfigControl) {
			c.Config[0].Body["value"] = "substituted"
			c.Config[0].Source.Fields["value"] = "substituted"
		}},
		{"source_key", func(c *publicConfigControl) { c.Config[0].Source.Fields["key"] = "other" }},
		{"source_table", func(c *publicConfigControl) { c.Config[0].Source.Table = "metadata" }},
		{"missing_source_relations_identical", func(c *publicConfigControl) { c.Config[0].Source.Fields = nil }},
		{"extra_source_field", func(c *publicConfigControl) { c.Config[0].Source.Fields["extra"] = "hidden" }},
		{"body_key", func(c *publicConfigControl) { c.Config[0].Body["key"] = "config/other" }},
		{"secret_reference", func(c *publicConfigControl) { c.Config[1].Body["value"] = map[string]any{"$secret": "metadata/other"} }},
		{"dropped_user_memory", func(c *publicConfigControl) { c.Memory = c.Memory[1:] }},
		{"duplicate_user_memory", func(c *publicConfigControl) { c.Memory = append(c.Memory, c.Memory[0]) }},
		{"user_memory_value", func(c *publicConfigControl) { c.Memory[0].Body["value"] = "substituted" }},
		{"policy_missing", func(c *publicConfigControl) { delete(c.Policy, "config/custom.public_recording") }},
		{"policy_extra", func(c *publicConfigControl) { c.Policy["config/extra"] = "public" }},
		{"policy_classification", func(c *publicConfigControl) { c.Policy["metadata/api_token"] = "public" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newPublicConfigControl(t)
			expected, err := expectPublicConfig(c.Observations, c.Ordinary)
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&c)
			err = comparePublicConfigSources(c.Config, c.Memory, c.Policy, expected)
			if (err == nil) != (tc.name == "unchanged") {
				t.Fatalf("source control %s: %v", tc.name, err)
			}
		})
	}
}

func TestPublicProjectionReverseConfigMemories(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*publicConfigControl)
	}{
		{"unchanged", func(*publicConfigControl) {}},
		{"dropped_config", func(c *publicConfigControl) { c.Rows = c.Rows[:4] }},
		{"extra_reserved", func(c *publicConfigControl) {
			c.Rows = append(c.Rows, map[string]any{"_type": "memory", "key": "plug.config/unobserved", "value": "extra"})
		}},
		{"same_count_key_substitution", func(c *publicConfigControl) { c.Rows[3]["key"] = "plug.config/other" }},
		{"same_count_value_substitution", func(c *publicConfigControl) { c.Rows[3]["value"] = "substituted" }},
		{"duplicate_config_memory", func(c *publicConfigControl) { c.Rows = append(c.Rows, c.Rows[3]) }},
		{"duplicate_user_memory", func(c *publicConfigControl) { c.Rows = append(c.Rows, c.Rows[1]) }},
		{"reserved_collision", func(c *publicConfigControl) { c.Rows[3]["key"] = "plug.config/user-owned" }},
		{"changed_user_memory", func(c *publicConfigControl) { c.Rows[1]["value"] = "substituted" }},
		{"dropped_reserved_user_memory", func(c *publicConfigControl) { c.Rows = append(c.Rows[:2], c.Rows[3:]...) }},
		{"noncanonical_secret_string", func(c *publicConfigControl) { c.Rows[4]["value"] = `{ "$secret": "metadata/api_token" }` }},
		{"nonstring_config_memory", func(c *publicConfigControl) { c.Rows[4]["value"] = map[string]any{"$secret": "metadata/api_token"} }},
		{"changed_issue", func(c *publicConfigControl) { c.Rows[0]["title"] = "substituted" }},
		{"wrong_count", func(c *publicConfigControl) { c.Count-- }},
		{"missing_reference", func(c *publicConfigControl) { c.References = c.References[:1] }},
		{"extra_reference", func(c *publicConfigControl) { c.References = append(c.References, "metadata/unobserved") }},
		{"duplicate_reference", func(c *publicConfigControl) { c.References[1] = c.References[0] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newPublicConfigControl(t)
			expected, err := expectPublicConfig(c.Observations, c.Ordinary)
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&c)
			var data bytes.Buffer
			encoder := json.NewEncoder(&data)
			for _, row := range c.Rows {
				if err := encoder.Encode(row); err != nil {
					t.Fatal(err)
				}
			}
			projection, err := projectJSONL(data.Bytes(), []string{"title"}, nil)
			if err == nil {
				err = comparePublicReverse(c.Ordinary, projection, expected, c.References, c.Count)
			}
			if (err == nil) != (tc.name == "unchanged") {
				t.Fatalf("reverse control %s: %v", tc.name, err)
			}
		})
	}
}

func TestPublicProjectionConfigObservationRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*publicConfigControl)
	}{
		{"duplicate_id", func(c *publicConfigControl) { c.Observations = append(c.Observations, c.Observations[0]) }},
		{"ambiguous_source_keys", func(c *publicConfigControl) {
			c.Observations[1].Key = c.Observations[0].Key
			c.Observations[1].Classification = "public"
		}},
		{"user_reserved_collision", func(c *publicConfigControl) { c.Observations[0].Key = "user-owned" }},
		{"unknown_table", func(c *publicConfigControl) { c.Observations[0].Table = "other" }},
		{"missing_classification", func(c *publicConfigControl) { c.Observations[0].Classification = "" }},
		{"wrong_secret_reference", func(c *publicConfigControl) { c.Observations[1].Value = map[string]any{"$secret": "metadata/other"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newPublicConfigControl(t)
			tc.edit(&c)
			if _, err := expectPublicConfig(c.Observations, c.Ordinary); err == nil {
				t.Fatal("ambiguous or unclassified observation admitted")
			}
		})
	}
}

func TestPublicProjectionConfigSettingsReadback(t *testing.T) {
	observations, err := readPublicConfigSettings([]byte(`{"schema_version":1,"data":{"custom.public_recording":"paired-public","schema_version":"stored-version","stored-only":"9007199254740993"}}`))
	if err != nil || len(observations) != 3 || observations[0].Value != "paired-public" || observations[1].Key != "schema_version" || observations[1].Value != "stored-version" || observations[2].Value != "9007199254740993" {
		t.Fatalf("independent stored-settings readback: %v, %+v", err, observations)
	}
	empty, err := readPublicConfigSettings([]byte(`{"data":{},"schema_version":1}`))
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty stored-settings readback: %v, %+v", err, empty)
	}
	for _, data := range []string{
		`[]`, `{"custom.public_recording":"paired-public","schema_version":1}`,
		`{"data":{"a":"x"}}`, `{"schema_version":2,"data":{"a":"x"}}`,
		`{"schema_version":"1","data":{"a":"x"}}`,
		`{"schema_version":{},"data":{}}`,
		`{"schema_version":1,"schema_version":1,"data":{}}`,
		`{"schema_version":1,"data":{},"data":{}}`,
		`{"schema_version":1,"data":{"a":"x"},"other":true}`,
		`{"schema_version":1,"data":[]}`, `{"schema_version":1,"data":null}`,
		`{"schema_version":1,"data":{"a":"x","a":"y"}}`,
		`{"schema_version":1,"data":{"a":null}}`,
		`{"schema_version":1,"data":{"a":42}}`,
		`{"schema_version":1,"data":{"a":false}}`,
		`{"schema_version":1,"data":{"a":{}}}`,
		`{"schema_version":1,"data":{"kv.memory.hidden":"x"}}`,
		`{"schema_version":1,"data":{"a":"x"}} {}`,
	} {
		if _, err := readPublicConfigSettings([]byte(data)); err == nil {
			t.Fatal("invalid/duplicate/non-setting readback admitted")
		}
	}
}

func TestPublicProjectionOmittedTypedDefaults(t *testing.T) {
	fields := []string{"labels", "metadata", "spec_id", "close_reason"}
	project := func(raw string) map[string]any {
		t.Helper()
		data, err := projectJSONL([]byte(raw), fields, nil)
		if err != nil {
			t.Fatal(err)
		}
		projection, err := decodePublicProjection(data)
		if err != nil {
			t.Fatal(err)
		}
		return projection.Issues["sample"]
	}
	omitted := project(`{"_type":"issue","id":"sample"}`)
	if !equalJSON(omitted, map[string]any{"labels": []any{}, "metadata": map[string]any{}, "spec_id": "", "close_reason": ""}) {
		t.Fatalf("omitted CLI defaults differ: %+v", omitted)
	}
	explicitNull := project(`{"_type":"issue","id":"sample","labels":null,"metadata":null,"spec_id":null,"close_reason":null}`)
	for _, field := range fields {
		if explicitNull[field] != nil {
			t.Fatalf("explicit null %s was replaced with a default", field)
		}
	}
	if equalJSON(omitted, explicitNull) {
		t.Fatal("explicit null and omitted typed defaults became equal")
	}
	populated := project(`{"_type":"issue","id":"sample","labels":["kept"],"metadata":{"kept":true},"spec_id":"kept-spec","close_reason":"real reason"}`)
	if !equalJSON(populated, map[string]any{"labels": []any{"kept"}, "metadata": map[string]any{"kept": true}, "spec_id": "kept-spec", "close_reason": "real reason"}) {
		t.Fatal("populated optional fields changed")
	}
}

func TestPublicProjectionMetadataReadback(t *testing.T) {
	observations, err := readPublicMetadata([]byte(`{"schema_version":1,"data":[{"key":"_project_id","value":"9007199254740993"},{"value":"","key":"empty"}]}`))
	if err != nil || len(observations) != 2 || observations[0].Table != "metadata" || observations[0].Key != "_project_id" || observations[0].Value != "9007199254740993" || observations[0].Classification != "public" || observations[1].Value != "" {
		t.Fatalf("independent metadata readback: %v, %+v", err, observations)
	}
	empty, err := readPublicMetadata([]byte(`{"data":[],"schema_version":1}`))
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty metadata readback: %v, %+v", err, empty)
	}
	for _, data := range []string{
		`[]`, `{"data":[]}`, `{"schema_version":1}`,
		`{"schema_version":2,"data":[]}`, `{"schema_version":"1","data":[]}`,
		`{"schema_version":1,"schema_version":1,"data":[]}`,
		`{"schema_version":1,"data":[],"data":[]}`,
		`{"schema_version":1,"data":[],"other":true}`,
		`{"schema_version":1,"data":{}}`, `{"schema_version":1,"data":null}`,
		`{"schema_version":1,"data":[null]}`, `{"schema_version":1,"data":[[]]}`,
		`{"schema_version":1,"data":[{}]}`,
		`{"schema_version":1,"data":[{"key":"a"}]}`,
		`{"schema_version":1,"data":[{"value":"x"}]}`,
		`{"schema_version":1,"data":[{"key":"","value":"x"}]}`,
		`{"schema_version":1,"data":[{"key":"a","key":"b","value":"x"}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":"x","value":"y"}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":"x","other":"y"}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":"x"},{"key":"a","value":"y"}]}`,
		`{"schema_version":1,"data":[{"key":42,"value":"x"}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":42}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":null}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":false}]}`,
		`{"schema_version":1,"data":[]} {}`,
	} {
		if _, err := readPublicMetadata([]byte(data)); err == nil {
			t.Fatal("invalid or ambiguous metadata readback admitted")
		}
	}
}

// Authored build-info controls do not execute or stand in for a reference binary.
func TestPublicProjectionBuildInfoRequiresMainModulePin(t *testing.T) {
	module := "example.invalid/reference"
	pin := hashBytes([]byte(module))
	label := "//cmd/reference:reference"
	mainPath := "\tpath\t" + module + "/cmd/reference\n"
	mainModule := "\tmod\t" + module + "\t(devel)\n"
	for _, tc := range []struct {
		name, info, pin, label string
		ok                     bool
	}{
		{"main_module", "reference: go1.26.10\n" + mainPath + mainModule, pin, label, true},
		{"bazel_main_package", "reference: go1.26.10\n" + mainPath + "\tdep\texample.invalid/dependency\tv1.0.0\n", pin, label, true},
		{"wrong_module", mainPath + "\tmod\texample.invalid/other\t(devel)\n", pin, label, false},
		{"dependency_only", "\tdep\t" + module + "\tv1.0.0\n", pin, label, false},
		{"substring_only", mainPath + "\tmod\t" + module + "-other\t(devel)\n", pin, label, false},
		{"substring_path", "\tpath\t" + module + "-other/cmd/reference\n", pin, label, false},
		{"wrong_package", "\tpath\t" + module + "/cmd/other\n" + mainModule, pin, label, false},
		{"package_suffix", "\tpath\t" + module + "/cmd/reference/other\n" + mainModule, pin, label, false},
		{"missing_path", mainModule, pin, label, false},
		{"malformed_path", mainPath + "\tpath\n", pin, label, false},
		{"duplicate_path", mainPath + mainPath + mainModule, pin, label, false},
		{"malformed_module", mainPath + "\tmod\t" + module + "\n", pin, label, false},
		{"duplicate_module", mainPath + mainModule + mainModule, pin, label, false},
		{"missing_pin", mainPath + mainModule, "", label, false},
		{"invalid_label", mainPath + mainModule, pin, "cmd/reference:reference", false},
		{"extra_label_target", mainPath + mainModule, pin, label + ":other", false},
		{"noncanonical_package", mainPath + mainModule, pin, "//cmd/../cmd/reference:reference", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkPublicBuildInfo([]byte(tc.info), tc.pin, tc.label)
			if (err == nil) != tc.ok {
				t.Fatalf("main package binding: %v", err)
			}
		})
	}
	profile := readRecordingProfile(t)
	fixture := readRecordingFixture(t)
	if len(profile.GoModuleSHA256) != 64 || len(fixture.ExternalFamilies) != 26 || len(fixture.StatusCategories) != 8 {
		t.Fatal("recording runfiles lost the sealed source profile")
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

// Resolve the fresh workspace from each command CWD. The accepted CLI skips
// local metadata creation for explicit BEADS_DIR with an external data root.
func recordingEnv(dataRoot string) func(string) []string {
	return func(dir string) []string {
		home := filepath.Join(dir, "home")
		return []string{"PATH=/usr/bin:/bin", "HOME=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "BEADS_DOLT_DATA_DIR=" + dataRoot, "LANG=C.UTF-8", "TZ=UTC", "CI=true", "DO_NOT_TRACK=1", "BEADS_NO_DAEMON=1", "BEADS_ACTOR=public-recorder", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Public recorder", "GIT_AUTHOR_EMAIL=public@example.invalid", "GIT_COMMITTER_NAME=Public recorder", "GIT_COMMITTER_EMAIL=public@example.invalid"}
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
	receipt := commandReceipt{Label: label, Store: store, Argv: append([]string(nil), argv...), Directory: dir, Environment: append([]string(nil), env...), StdoutSHA256: hashBytes(stdout.Bytes()), StderrSHA256: hashBytes(stderr.Bytes()), Exit: exit, StdoutPath: stdoutPath, StderrPath: stderrPath}
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

func statusCategory(value any, categories map[string]string) string {
	status := fmt.Sprint(value)
	if category, ok := categories[status]; ok {
		return category
	}
	return status
}

func isLowerHex(value string) bool {
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}
