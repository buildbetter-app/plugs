package b09native

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// These are the accepted public-bundle inputs, not an executable factory or a
// replacement preparer. The committed main trusted workflow supplies the four
// PLUG_PUBLIC_BEADS_* exports only after its pinned helper has admitted them.
const (
	beadsBuildRun        = "41800330346"
	beadsCommit          = "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724"
	beadsTree            = "930405e863284a3955cbd35e73a6097b606b91a5"
	binarySHA256         = "b734da8ba8b6f50d7fd32239e405b8f8ed9d171b7be5fbbccb63a923d257a123"
	manifestSHA256       = "ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a"
	nativeDescriptor     = "41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a"
	nativeWorkflowSHA    = "d758fdf42a426e9e2a4b707dbcebb22ec68b264e"
	nativeWorkflowBranch = "swarm/native-producer-census-control-c8eb-20261104"
	nativeInputsSHA256   = "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2"
	maxSnapshotSize      = 512 << 20
)

var nativeRecording = flag.Bool("b09-native-recording", false, "explicit accepted Beads B09 recording")

// The reviewed external protocol names are data, separate from Go identifiers.
// Embed and retain the exact committed bytes; no runtime input can replace them.
//
//go:embed native_inputs.json
var nativeInputBytes []byte

type nativeInputs struct {
	SourceFamilies            []string `json:"source_families"`
	ExporterArtifact          string   `json:"exporter_artifact"`
	ExporterLabel             string   `json:"exporter_label"`
	VersionResolution         string   `json:"version_resolution"`
	NoninteractiveEnvironment string   `json:"noninteractive_environment"`
}

type commandReceipt struct {
	Label        string   `json:"label"`
	Argv         []string `json:"argv"`
	Directory    string   `json:"directory"`
	Environment  []string `json:"environment"`
	Exit         int      `json:"exit"`
	StdoutPath   string   `json:"stdout_path"`
	StderrPath   string   `json:"stderr_path"`
	StdoutSHA256 string   `json:"stdout_sha256"`
	StderrSHA256 string   `json:"stderr_sha256"`
	ResultType   string   `json:"result_type"`
	Result       any      `json:"result,omitempty"`
	ErrorType    string   `json:"error_type"`
	ErrorResult  any      `json:"error_result,omitempty"`
}

type fileReceipt struct {
	Path   string `json:"path"`
	Type   string `json:"type"`
	Mode   string `json:"mode"`
	Size   int64  `json:"size,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
}

type captureReceipt struct {
	Label              string            `json:"label"`
	InventoryPath      string            `json:"inventory_path"`
	InventorySHA256    string            `json:"inventory_sha256"`
	SourceConfigPath   string            `json:"source_config_path"`
	SourceConfigBytes  string            `json:"source_config_bytes"`
	SourceConfigSHA256 string            `json:"source_config_sha256"`
	Sidecars           map[string]string `json:"sidecar_sha256"`
	NormalizedSidecars map[string]string `json:"normalized_sidecar_sha256"`
	ArchivePath        string            `json:"archive_path"`
	ArchiveSHA256      string            `json:"archive_sha256"`
	ArchiveStateSHA    string            `json:"archive_state_sha256"`
	FilesystemPath     string            `json:"filesystem_path"`
	FilesystemSHA      string            `json:"filesystem_sha256"`
	BeadIDs            []string          `json:"bead_ids"`
}

type workspace struct {
	Name string
	Root string
	Dir  string
	Home string
	Data string
	Temp string
	Env  []string
}

type recorder struct {
	t        *testing.T
	root     string
	binary   string
	exporter string
	inputs   nativeInputs
	commands []commandReceipt
	captures []captureReceipt
}

func TestRecordNativeB09(t *testing.T) {
	if !*nativeRecording {
		t.Fatal("B09 native recorder requires the explicit manual target")
	}
	parent := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(parent) {
		t.Fatal("native recording requires an absolute declared output directory")
	}
	root, err := os.MkdirTemp(parent, "b09-native-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	r := &recorder{t: t, root: root}
	if hashBytes(nativeInputBytes) != nativeInputsSHA256 {
		t.Fatal("committed external protocol input digest changed")
	}
	decoder := json.NewDecoder(bytes.NewReader(nativeInputBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r.inputs); err != nil {
		t.Fatalf("committed external protocol input: %v", err)
	}
	if len(r.inputs.SourceFamilies) != 26 || r.inputs.ExporterArtifact == "" || r.inputs.ExporterLabel == "" || r.inputs.VersionResolution == "" || r.inputs.NoninteractiveEnvironment == "" {
		t.Fatal("committed external protocol input is incomplete")
	}
	if err := os.WriteFile(filepath.Join(root, "native-inputs.json"), nativeInputBytes, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("B09_NATIVE_OUTPUT %s", root)
	r.binary = r.admitBinary()
	adaptive := r.newWorkspace("adaptive")
	r.run(adaptive, "adaptive-git-init", "/usr/bin/git", []string{"init", "--quiet", "--template="}, 0)
	r.run(adaptive, "adaptive-init", r.binary, []string{"init", "--prefix", "b09", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, 0)
	r.version(adaptive)
	r.writeNativeConfig(adaptive)
	if help := r.run(adaptive, "adaptive-config-help", r.binary, []string{"config", "--help"}, 0); len(bytes.TrimSpace(help)) == 0 {
		t.Fatal("native config help was empty")
	}
	first := r.create(adaptive, "default-length-3", "Adaptive first")
	r.requireIDLength(first, 3)
	firstState := r.capture(adaptive, "adaptive-first", 1)
	second := r.create(adaptive, "native-second", "Native second")
	r.requireIDLength(second, 3)
	if first == second {
		t.Fatal("native mints reused an ID")
	}
	secondState := r.capture(adaptive, "adaptive-second", 2)
	maximum := r.create(adaptive, "native-third", "Native third")
	r.requireIDLength(maximum, 3)
	maxState := r.capture(adaptive, "native-three", 3)
	r.requireIDs(maxState, first, second, maximum)
	r.requireListMatches(adaptive, maxState)
	r.replayNative(maxState)
	if len(firstState.BeadIDs) != 1 || len(secondState.BeadIDs) != 2 {
		t.Fatal("adaptive archive did not conserve the earlier native issues")
	}
	beforeDuplicate := r.capture(adaptive, "before-explicit-refusal", 3)
	refusal := r.run(adaptive, "explicit-id-refusal", r.binary,
		[]string{"create", "--id", first, "--title", "Duplicate ID", "--type", "task", "--json"}, -1)
	if !r.lastOutputContains(refusal, "already exists") {
		t.Fatal("explicit ID refusal did not report the occupied ID")
	}
	afterDuplicate := r.capture(adaptive, "after-explicit-refusal", 3)
	r.requireConserved(beforeDuplicate, afterDuplicate, "explicit-ID refusal")

	// A second real Beads workspace imports JSONL exported from the first.
	// This observes native continuation against authentic IDs; JSONL import
	// is not a substitute for Plug's snapshot restore path.
	transfer := filepath.Join(root, "native-transfer")
	if err := os.Mkdir(transfer, 0700); err != nil {
		t.Fatal(err)
	}
	exportPath := filepath.Join(transfer, "source.jsonl")
	r.run(adaptive, "native-jsonl-export", r.binary,
		[]string{"--readonly", "export", "--include-memories", "--output", exportPath}, 0)
	exportInfo, err := os.Lstat(exportPath)
	if err != nil || !exportInfo.Mode().IsRegular() || exportInfo.Size() == 0 {
		t.Fatalf("native JSONL export was not retained: %v", err)
	}
	exportSHA, _, err := hashFile(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	afterExport := r.capture(adaptive, "after-native-jsonl-export", 3)
	r.requireConserved(afterDuplicate, afterExport, "readonly native JSONL export")
	imported := r.newWorkspace("native-jsonl-import-continuation")
	r.run(imported, "imported-git-init", "/usr/bin/git", []string{"init", "--quiet", "--template="}, 0)
	r.run(imported, "imported-init", r.binary, []string{"init", "--prefix", "b09", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, 0)
	r.version(imported)
	r.writeNativeConfig(imported)
	r.run(imported, "native-jsonl-import", r.binary, []string{"--json", "import", exportPath}, 0)
	importedState := r.capture(imported, "native-imported", 3)
	r.requireIDs(importedState, first, second, maximum)
	continued := r.create(imported, "native-after-import", "Native continuation")
	if continued == first || continued == second || continued == maximum {
		t.Fatal("native continuation reused an imported issue ID")
	}
	continuedState := r.capture(imported, "native-import-continued", 4)
	r.requireIDs(continuedState, first, second, maximum, continued)
	r.requireListMatches(imported, continuedState)

	r.writeJSON(filepath.Join(root, "disposition.json"), map[string]any{
		"schema":        "plug-s1-b09-accepted-native-recording/1",
		"status":        "RECORDED_BOUNDED",
		"source_commit": beadsCommit, "source_tree": beadsTree,
		"build_run": beadsBuildRun, "binary_sha256": binarySHA256,
		"manifest_sha256": manifestSHA256,
		"setup_case":      "configured-yaml-object",
		"default_id":      first, "second_id": second, "third_id": maximum,
		"native_jsonl_export_sha256": exportSHA, "native_import_continuation_id": continued,
		"explicit_refusal_conserved": true,
		"limits":                     []string{"this recording uses the native default three-character allocator and does not exercise configured adaptive length or nonce exhaustion", "native JSONL import is not Plug snapshot restore", "native recording alone does not prove Plug parity, restored-ID safety or full-card admission"},
		"captures":                   r.captures,
	})
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hashFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	h := sha256.New()
	n, err := io.Copy(h, file)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

func (r *recorder) writeJSON(path string, value any) {
	r.t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		r.t.Fatal(err)
	}
}

func (r *recorder) admitBinary() string {
	r.t.Helper()
	binary, manifestPath := os.Getenv("PLUG_PUBLIC_BEADS_BINARY"), os.Getenv("PLUG_PUBLIC_BEADS_MANIFEST")
	if os.Getenv("PLUG_PUBLIC_BEADS_BUILD_RUN") != beadsBuildRun || os.Getenv("PLUG_PUBLIC_BEADS_SHA256") != binarySHA256 || !filepath.IsAbs(binary) || !filepath.IsAbs(manifestPath) {
		r.t.Fatal("trusted public artifact exports do not match accepted B09 inputs")
	}
	info, err := os.Lstat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0755 {
		r.t.Fatalf("trusted public binary is not a regular executable: %v", err)
	}
	manifestInfo, err := os.Lstat(manifestPath)
	if err != nil || !manifestInfo.Mode().IsRegular() || manifestInfo.Size() <= 0 || manifestInfo.Size() > 1<<20 {
		r.t.Fatalf("trusted public manifest is unavailable: %v", err)
	}
	manifest, err := os.ReadFile(manifestPath)
	if err != nil || hashBytes(manifest) != manifestSHA256 {
		r.t.Fatal("trusted public manifest digest differs from the accepted producer")
	}
	var identity struct {
		Schema   string `json:"schema"`
		Workflow struct {
			Ref     string `json:"ref"`
			SHA     string `json:"sha"`
			Profile string `json:"profile"`
		} `json:"workflow"`
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
	if err := json.Unmarshal(manifest, &identity); err != nil {
		r.t.Fatal(err)
	}
	if got := os.Getenv("PLUG_PUBLIC_BUNDLE"); got != nativeDescriptor {
		r.t.Fatalf("trusted public descriptor differs from admitted preparation: %q", got)
	}
	target := identity.Targets["beads_exporter"]
	artifact := identity.Artifacts[r.inputs.ExporterArtifact]
	var version struct{ Commit, Build string }
	if err := json.Unmarshal(identity.Resolution[r.inputs.VersionResolution], &version); err != nil ||
		identity.Schema != "plug-public-native/1" || identity.Source["beads"].Commit != beadsCommit || identity.Source["beads"].Tree != beadsTree ||
		target.Label != r.inputs.ExporterLabel || target.Binary != r.inputs.ExporterArtifact || artifact.SHA256 != binarySHA256 || artifact.Size != info.Size() || artifact.Mode != "0o755" ||
		identity.Build.RunID != beadsBuildRun || version.Commit != beadsCommit || version.Build != "public-"+beadsCommit ||
		identity.Workflow.SHA != nativeWorkflowSHA || identity.Workflow.Ref != "refs/heads/"+nativeWorkflowBranch || identity.Workflow.Profile != "plug-public-native/1" {
		r.t.Fatal("trusted public manifest fields do not bind the accepted source and executable")
	}
	receiptPath := filepath.Join(filepath.Dir(manifestPath), "receipt.json")
	receipt, err := os.ReadFile(receiptPath)
	if err != nil || !bytes.Equal(receipt, manifest) {
		r.t.Fatalf("trusted public preparation receipt differs from manifest: %v", err)
	}
	digest, size, err := hashFile(binary)
	if err != nil || digest != binarySHA256 || size != artifact.Size {
		r.t.Fatalf("trusted public executable digest differs: %v", err)
	}
	copyPath := filepath.Join(r.root, "native-pinned")
	src, err := os.Open(binary)
	if err != nil {
		r.t.Fatal(err)
	}
	defer src.Close()
	dst, err := os.OpenFile(copyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0500)
	if err != nil {
		r.t.Fatal(err)
	}
	n, copyErr := io.Copy(dst, src)
	closeErr := dst.Close()
	if copyErr != nil || closeErr != nil || n != artifact.Size {
		r.t.Fatalf("private executable copy failed: bytes=%d err=%v close=%v", n, copyErr, closeErr)
	}
	if digest, _, err = hashFile(copyPath); err != nil || digest != binarySHA256 {
		r.t.Fatal("private executable bytes differ from accepted public binary")
	}
	if err := os.WriteFile(filepath.Join(r.root, "accepted-manifest.json"), manifest, 0600); err != nil {
		r.t.Fatal(err)
	}
	return copyPath
}

func (r *recorder) newWorkspace(name string) workspace {
	r.t.Helper()
	base := filepath.Join(r.root, "scenarios", name)
	w := workspace{Name: name, Root: base, Dir: filepath.Join(base, "workspace"), Home: filepath.Join(base, "home"), Data: filepath.Join(base, "dolt"), Temp: filepath.Join(base, "tmp")}
	disabledHooks := filepath.Join(w.Home, ".git-hooks-disabled")
	for _, path := range []string{w.Dir, w.Home, w.Data, w.Temp, disabledHooks, filepath.Join(w.Home, ".config"), filepath.Join(w.Home, ".cache"), filepath.Join(w.Home, ".local"), filepath.Join(w.Home, ".local", "share"), filepath.Join(w.Home, ".local", "state")} {
		if err := os.MkdirAll(path, 0700); err != nil {
			r.t.Fatal(err)
		}
		if err := os.Chmod(path, 0700); err != nil {
			r.t.Fatal(err)
		}
	}
	w.Env = []string{
		"PATH=/usr/bin:/bin", "HOME=" + w.Home, "USER=b09-native", "LOGNAME=b09-native",
		"XDG_CONFIG_HOME=" + filepath.Join(w.Home, ".config"), "XDG_CACHE_HOME=" + filepath.Join(w.Home, ".cache"),
		"XDG_DATA_HOME=" + filepath.Join(w.Home, ".local", "share"), "XDG_STATE_HOME=" + filepath.Join(w.Home, ".local", "state"),
		"TMPDIR=" + w.Temp, "BEADS_DOLT_DATA_DIR=" + w.Data,
		"BEADS_ACTOR=b09-native", "BEADS_NO_DAEMON=1", r.inputs.NoninteractiveEnvironment, "DO_NOT_TRACK=1",
		"LANG=C", "LC_ALL=C", "TZ=UTC", "TERM=dumb", "CI=true", "NO_COLOR=1",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_TERMINAL_PROMPT=0", "GIT_ASKPASS=/bin/false", "SSH_ASKPASS=/bin/false", "GIT_SSH_COMMAND=/bin/false",
		"GIT_AUTHOR_NAME=B09 Native", "GIT_AUTHOR_EMAIL=b09-native@example.invalid",
		"GIT_COMMITTER_NAME=B09 Native", "GIT_COMMITTER_EMAIL=b09-native@example.invalid",
		"AWS_EC2_METADATA_DISABLED=true", "AWS_CONFIG_FILE=/dev/null", "AWS_SHARED_CREDENTIALS_FILE=/dev/null",
	}
	gitConfig := [][2]string{
		{"credential.helper", ""}, {"credential.interactive", "false"},
		{"core.askPass", "/bin/false"}, {"core.sshCommand", "/bin/false"},
		{"core.hooksPath", disabledHooks}, {"core.fsmonitor", "false"},
		{"core.untrackedCache", "false"}, {"init.templateDir", "/dev/null"},
		{"init.defaultBranch", "main"}, {"protocol.allow", "never"},
		{"user.name", "B09 Native"}, {"user.email", "b09-native@example.invalid"},
	}
	w.Env = append(w.Env, fmt.Sprintf("GIT_CONFIG_COUNT=%d", len(gitConfig)))
	for i, item := range gitConfig {
		w.Env = append(w.Env, fmt.Sprintf("GIT_CONFIG_KEY_%d=%s", i, item[0]), fmt.Sprintf("GIT_CONFIG_VALUE_%d=%s", i, item[1]))
	}
	return w
}

func typedReturn(data []byte) (string, any) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return "empty", nil
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err == nil {
		var trailing any
		if decoder.Decode(&trailing) == io.EOF {
			switch value.(type) {
			case map[string]any:
				return "json-object", value
			case []any:
				return "json-array", value
			case string:
				return "json-string", value
			case json.Number:
				return "json-number", value
			case bool:
				return "json-boolean", value
			default:
				return "json-null", nil
			}
		}
	}
	return "text", string(data)
}

// wantExit is zero for success and -1 for any observed nonzero refusal.
func (r *recorder) run(w workspace, label, binary string, args []string, wantExit int) []byte {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	argv := append([]string{binary}, args...)
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = w.Dir, append([]string(nil), w.Env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -2
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exit = exitError.ExitCode()
		}
	}
	base := fmt.Sprintf("%03d-%s", len(r.commands)+1, label)
	streams := filepath.Join(r.root, "streams")
	if err := os.MkdirAll(streams, 0700); err != nil {
		r.t.Fatal(err)
	}
	outPath, errPath := filepath.Join(streams, base+".stdout"), filepath.Join(streams, base+".stderr")
	if err := os.WriteFile(outPath, stdout.Bytes(), 0600); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(errPath, stderr.Bytes(), 0600); err != nil {
		r.t.Fatal(err)
	}
	typ, value := typedReturn(stdout.Bytes())
	errType, errValue := typedReturn(stderr.Bytes())
	r.commands = append(r.commands, commandReceipt{
		Label: label, Argv: argv, Directory: w.Dir, Environment: append([]string(nil), w.Env...), Exit: exit,
		StdoutPath: outPath, StderrPath: errPath, StdoutSHA256: hashBytes(stdout.Bytes()), StderrSHA256: hashBytes(stderr.Bytes()),
		ResultType: typ, Result: value, ErrorType: errType, ErrorResult: errValue,
	})
	r.writeJSON(filepath.Join(r.root, "commands.json"), r.commands)
	if ctx.Err() != nil || (wantExit == 0 && exit != 0) || (wantExit == -1 && exit <= 0) {
		r.t.Fatalf("native command %s exit=%d want=%d err=%v; raw streams retained in %s", label, exit, wantExit, err, streams)
	}
	return stdout.Bytes()
}

func (r *recorder) lastOutputContains(stdout []byte, phrase string) bool {
	r.t.Helper()
	if bytes.Contains(bytes.ToLower(stdout), []byte(phrase)) {
		return true
	}
	last := r.commands[len(r.commands)-1]
	stderr, err := os.ReadFile(last.StderrPath)
	if err != nil {
		r.t.Fatal(err)
	}
	return bytes.Contains(bytes.ToLower(stderr), []byte(phrase))
}

func (r *recorder) version(w workspace) {
	r.t.Helper()
	data := r.run(w, w.Name+"-version", r.binary, []string{"version", "--json"}, 0)
	var version struct{ Commit, Build string }
	if json.Unmarshal(data, &version) != nil || version.Commit != beadsCommit || version.Build != "public-"+beadsCommit {
		r.t.Fatalf("executed native version differs from accepted source: %+v", version)
	}
}

func (r *recorder) setConfig(w workspace, key, value string) {
	r.t.Helper()
	data := r.run(w, w.Name+"-set-"+key+"-"+value, r.binary, []string{"config", "set", key, value, "--json"}, 0)
	last := r.commands[len(r.commands)-1]
	stderr, err := os.ReadFile(last.StderrPath)
	if err != nil {
		r.t.Fatal(err)
	}
	if bytes.Contains(stderr, []byte("not a recognized config key")) {
		r.t.Fatalf("native config key %q was refused by the admitted producer", key)
	}
	var response map[string]any
	if json.Unmarshal(data, &response) != nil || response["key"] != key || response["value"] != value {
		r.t.Fatalf("native config set did not return its typed value for %s", key)
	}
}

func (r *recorder) writeNativeConfig(w workspace) {
	r.t.Helper()
	path := filepath.Join(w.Dir, ".beads", "config.yaml")
	data := []byte("export:\n  auto: false\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		r.t.Fatalf("write native YAML configuration: %v", err)
	}
	r.commands = append(r.commands, commandReceipt{
		Label: w.Name + "-write-native-yaml",
		Argv:  []string{"source-file", "write", "--path", path, "--bytes", string(data)}, Directory: w.Dir,
		Environment: append([]string(nil), w.Env...), Exit: 0,
		StdoutSHA256: hashBytes(nil), StderrSHA256: hashBytes(nil),
		ResultType: "file-write", Result: map[string]any{"path": path, "bytes": string(data), "sha256": hashBytes(data), "size": len(data)},
	})
	r.writeJSON(filepath.Join(r.root, "commands.json"), r.commands)
}

func (r *recorder) create(w workspace, label, title string) string {
	r.t.Helper()
	return r.createArgs(w, label, []string{"create", "--title", title, "--description", "B09 accepted native recorder", "--type", "task", "--json"})
}

func (r *recorder) createExplicit(w workspace, label, id string) string {
	r.t.Helper()
	return r.createArgs(w, label, []string{"create", "--id", id, "--title", "Occupied " + id, "--type", "task", "--json"})
}

func (r *recorder) createArgs(w workspace, label string, args []string) string {
	r.t.Helper()
	data := r.run(w, label, r.binary, args, 0)
	var response map[string]any
	if json.Unmarshal(data, &response) != nil {
		r.t.Fatalf("native create %s returned no typed JSON object", label)
	}
	id, ok := response["id"].(string)
	if !ok || id == "" {
		r.t.Fatalf("native create %s returned no ID", label)
	}
	return id
}

func (r *recorder) requireIDLength(id string, length int) {
	r.t.Helper()
	pattern := regexp.MustCompile(fmt.Sprintf(`^b09-[0-9a-z]{%d}$`, length))
	if !pattern.MatchString(id) {
		r.t.Fatalf("native hash ID %q does not have configured length %d", id, length)
	}
}

func (r *recorder) capture(w workspace, label string, wantBeads int) captureReceipt {
	r.t.Helper()
	exporter := r.exporter
	if exporter == "" {
		exporter = r.binary
	}
	root := filepath.Join(r.root, "captures", label)
	if err := os.MkdirAll(root, 0700); err != nil {
		r.t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		r.t.Fatal(err)
	}
	inventoryPath := filepath.Join(root, "inventory.json")
	r.run(w, label+"-inventory", exporter, []string{"--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", inventoryPath}, 0)
	inventoryInfo, err := os.Lstat(inventoryPath)
	if err != nil || !inventoryInfo.Mode().IsRegular() || inventoryInfo.Mode().Perm() != 0600 {
		r.t.Fatalf("native inventory is not a private regular file: %v", err)
	}
	inventoryBytes, err := os.ReadFile(inventoryPath)
	if err != nil {
		r.t.Fatal(err)
	}
	var inventory struct {
		Version         string `json:"version"`
		StoreID         string `json:"store_id"`
		Quiesced        bool   `json:"quiesced"`
		RemotesMerged   bool   `json:"remotes_merged"`
		WorkingSetClean bool   `json:"working_set_clean"`
		History         string `json:"history"`
		External        []struct {
			Family string `json:"family"`
			State  string `json:"state"`
			Path   string `json:"path"`
		} `json:"external"`
		ConfigPolicy map[string]string `json:"config_policy"`
	}
	if json.Unmarshal(inventoryBytes, &inventory) != nil || inventory.Version != "beads-inventory/1" || inventory.StoreID == "" ||
		!inventory.Quiesced || !inventory.RemotesMerged || !inventory.WorkingSetClean ||
		(inventory.History != "unavailable" && inventory.History != "available-unverified") || len(inventory.ConfigPolicy) == 0 {
		r.t.Fatal("native inventory has incomplete or unsupported declarations")
	}
	sidecarDir := inventoryPath + ".sources"
	dirInfo, err := os.Lstat(sidecarDir)
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode().Perm() != 0700 || len(inventory.External) != len(r.inputs.SourceFamilies) {
		r.t.Fatalf("native inventory sidecar directory or family census is incomplete: %v", err)
	}
	wantFamilies := map[string]bool{}
	for _, family := range r.inputs.SourceFamilies {
		wantFamilies[family] = true
	}
	sidecars := map[string]string{}
	normalizedSidecars := map[string]string{}
	for _, item := range inventory.External {
		if !wantFamilies[item.Family] || item.State != "json-config" || item.Path != filepath.Join(sidecarDir, item.Family+".json") {
			r.t.Fatalf("native inventory family is missing, repeated, or relocated: %+v", item)
		}
		delete(wantFamilies, item.Family)
		info, err := os.Lstat(item.Path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() <= 0 || info.Size() > 4<<20 {
			r.t.Fatalf("native sidecar %s is not a bounded private file: %v", item.Family, err)
		}
		data, err := os.ReadFile(item.Path)
		if err != nil || !json.Valid(data) {
			r.t.Fatalf("native sidecar %s is not complete JSON: %v", item.Family, err)
		}
		sidecars[item.Family] = hashBytes(data)
		normalizedSidecars[item.Family] = r.normalizedJSONDigest(data)
	}
	if len(wantFamilies) != 0 {
		r.t.Fatalf("native family census omitted %v", wantFamilies)
	}
	archivePath := filepath.Join(root, "raw-plug-dump.tar")
	r.run(w, label+"-dump", exporter, []string{"--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "--output", archivePath}, 0)
	archiveInfo, err := os.Lstat(archivePath)
	if err != nil || !archiveInfo.Mode().IsRegular() || archiveInfo.Mode().Perm() != 0600 {
		r.t.Fatalf("native raw archive is not a private regular file: %v", err)
	}
	archive, err := dumpformat.ReadFile(archivePath)
	if err != nil || archive.Native() {
		r.t.Fatalf("native Beads raw archive is invalid: %v", err)
	}
	m := archive.Manifest()
	if m.Format != dumpformat.Format || m.Schema != 1 || m.Reference != dumpformat.BeadsReference || m.Scope != dumpformat.Scope || m.StoreID != inventory.StoreID || len(m.Projects) != 1 || len(m.Kinds) == 0 {
		r.t.Fatal("native archive manifest differs from its complete source inventory")
	}
	rows := archive.Records("bead")
	if len(rows) != wantBeads {
		r.t.Fatalf("native source bead count=%d, want=%d", len(rows), wantBeads)
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.ID == "" || row.Source.Table != "issues" || len(row.Source.Fields) == 0 || row.Body == nil {
			r.t.Fatal("native archive lacks a full physical issue row")
		}
		ids = append(ids, row.ID)
	}
	sort.Strings(ids)
	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[i-1] {
			r.t.Fatal("native archive contains a repeated issue ID")
		}
	}
	configPath := filepath.Join(w.Dir, ".beads", "config.yaml")
	configBytes, err := os.ReadFile(configPath)
	if err != nil || len(configBytes) == 0 {
		r.t.Fatalf("native source YAML config is not retained at capture %s: %v", label, err)
	}
	filesystemPath, filesystemSHA := r.snapshot(w, root)
	receipt := captureReceipt{
		Label: label, InventoryPath: inventoryPath, InventorySHA256: hashBytes(inventoryBytes), Sidecars: sidecars, NormalizedSidecars: normalizedSidecars,
		SourceConfigPath: configPath, SourceConfigBytes: string(configBytes), SourceConfigSHA256: hashBytes(configBytes),
		ArchivePath: archivePath, ArchiveSHA256: archive.SHA256(), ArchiveStateSHA: r.archiveStateSHA(archive),
		FilesystemPath: filesystemPath, FilesystemSHA: filesystemSHA, BeadIDs: ids,
	}
	r.captures = append(r.captures, receipt)
	r.writeJSON(filepath.Join(r.root, "captures.json"), r.captures)
	return receipt
}

// Exclude only the capture destination's absolute paths in config/sources.json.
// Every source row, history member, ontology and project setting remains in
// the comparison; the complete validated tar bytes are retained separately.
func (r *recorder) archiveStateSHA(archive *dumpformat.Archive) string {
	m := archive.Manifest()
	members := map[string]string{}
	for path := range m.Files {
		data, ok := archive.Member(path)
		if !ok {
			panic("validated archive lost a manifest member")
		}
		if path == "config/sources.json" {
			data = r.normalizedJSON(data)
		}
		members[path] = hashBytes(data)
	}
	data, err := json.Marshal(struct {
		StoreID  string                         `json:"store_id"`
		History  string                         `json:"history"`
		Projects []dumpformat.Project           `json:"projects"`
		Ontology []dumpformat.OntologyDirective `json:"ontology"`
		Kinds    []dumpformat.Kind              `json:"kinds"`
		Sources  []dumpformat.Disposition       `json:"sources"`
		Members  map[string]string              `json:"members"`
	}{m.StoreID, m.History, m.Projects, m.Ontology, m.Kinds, m.Sources, members})
	if err != nil {
		panic(err)
	}
	return hashBytes(data)
}

// normalizedJSON retains every source/flag/value field while replacing only
// run-local capture paths and the native gate's process PID. Raw sidecars and
// archive bytes remain in the receipt; this digest is solely the conservation
// comparison across captures that intentionally use fresh output paths.
func (r *recorder) normalizedJSON(data []byte) []byte {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		r.t.Fatalf("source JSON normalization decode: %v", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		r.t.Fatalf("source JSON normalization has trailing data: %v", err)
	}
	var normalize func(any, string) any
	normalize = func(input any, key string) any {
		switch value := input.(type) {
		case map[string]any:
			out := make(map[string]any, len(value))
			for name, child := range value {
				out[name] = normalize(child, name)
			}
			return out
		case []any:
			out := make([]any, len(value))
			for i, child := range value {
				out[i] = normalize(child, key)
			}
			return out
		case string:
			if key == "BEADS_GATE_SHARED_HOLDER_PID" {
				return "<native-gate-process-id>"
			}
			if strings.HasPrefix(value, r.root+string(os.PathSeparator)+"captures"+string(os.PathSeparator)) {
				return "<native-capture-output>"
			}
			return value
		default:
			return input
		}
	}
	canonical, err := json.Marshal(normalize(value, ""))
	if err != nil {
		r.t.Fatalf("source JSON normalization encode: %v", err)
	}
	return canonical
}

func (r *recorder) normalizedJSONDigest(data []byte) string {
	return hashBytes(r.normalizedJSON(data))
}

func (r *recorder) snapshot(w workspace, output string) (string, string) {
	r.t.Helper()
	path := filepath.Join(output, "physical-state")
	if err := os.Mkdir(path, 0700); err != nil {
		r.t.Fatal(err)
	}
	var files []fileReceipt
	var total int64
	for _, root := range []struct{ name, path string }{{"workspace", w.Dir}, {"home", w.Home}, {"dolt", w.Data}, {"tmp", w.Temp}} {
		err := filepath.WalkDir(root.path, func(source string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			rel, err := filepath.Rel(root.path, source)
			if err != nil {
				return err
			}
			name := root.name
			if rel != "." {
				name = filepath.Join(name, rel)
			}
			info, err := os.Lstat(source)
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if err := os.MkdirAll(filepath.Join(path, name), 0700); err != nil {
					return err
				}
				files = append(files, fileReceipt{Path: name, Type: "directory", Mode: fmt.Sprintf("%04o", info.Mode().Perm())})
				return nil
			}
			if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxSnapshotSize-total {
				return fmt.Errorf("native physical state contains an unsupported entry or exceeds the complete-copy bound: %s", name)
			}
			total += info.Size()
			input, err := os.Open(source)
			if err != nil {
				return err
			}
			defer input.Close()
			destination := filepath.Join(path, name)
			if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
				return err
			}
			copyFile, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return err
			}
			h := sha256.New()
			n, copyErr := io.Copy(io.MultiWriter(copyFile, h), input)
			closeErr := copyFile.Close()
			if copyErr != nil || closeErr != nil || n != info.Size() {
				return fmt.Errorf("native physical copy failed for %s: bytes=%d err=%v close=%v", name, n, copyErr, closeErr)
			}
			files = append(files, fileReceipt{Path: name, Type: "file", Mode: fmt.Sprintf("%04o", info.Mode().Perm()), Size: n, SHA256: hex.EncodeToString(h.Sum(nil))})
			return nil
		})
		if err != nil {
			r.t.Fatal(err)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	data, err := json.Marshal(files)
	if err != nil {
		r.t.Fatal(err)
	}
	r.writeJSON(filepath.Join(path, "file-manifest.json"), files)
	return path, hashBytes(data)
}

func (r *recorder) requireConfig(c captureReceipt, key, value string) {
	r.t.Helper()
	archive, err := dumpformat.ReadFile(c.ArchivePath)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, row := range archive.Records("config") {
		if row.Source.Table == "config" && row.Source.Fields["key"] == key && row.Source.Fields["value"] == value {
			return
		}
	}
	r.t.Fatalf("native raw archive %s did not retain config %s=%s", c.Label, key, value)
}

func (r *recorder) requireIDs(c captureReceipt, ids ...string) {
	r.t.Helper()
	want := append([]string(nil), ids...)
	sort.Strings(want)
	if strings.Join(want, "\x00") != strings.Join(c.BeadIDs, "\x00") {
		r.t.Fatalf("native archive %s issue IDs differ: have %v want %v", c.Label, c.BeadIDs, want)
	}
}

func (r *recorder) requireListMatches(w workspace, c captureReceipt) {
	r.t.Helper()
	data := r.run(w, c.Label+"-public-list", r.binary, []string{"list", "--all", "--limit", "0", "--json"}, 0)
	var rows []map[string]any
	if err := json.Unmarshal(data, &rows); err != nil || len(rows) != len(c.BeadIDs) {
		r.t.Fatalf("native public list did not return the archive's complete issue set: %v", err)
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		id, ok := row["id"].(string)
		if !ok || id == "" {
			r.t.Fatal("native public list omitted a typed issue ID")
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if strings.Join(ids, "\x00") != strings.Join(c.BeadIDs, "\x00") {
		r.t.Fatalf("native public list and complete raw archive disagree for %s", c.Label)
	}
}

func (r *recorder) requireConserved(before, after captureReceipt, label string) {
	r.t.Helper()
	if before.ArchiveStateSHA != after.ArchiveStateSHA || before.FilesystemSHA != after.FilesystemSHA ||
		strings.Join(before.BeadIDs, "\x00") != strings.Join(after.BeadIDs, "\x00") {
		r.t.Fatalf("%s changed native source rows or physical workspace/home bytes; before=%s after=%s", label, before.Label, after.Label)
	}
	// The native inventory captures the rest of the source census. Compare the
	// normalized digest for every family; only explicit capture paths and the
	// native gate PID are expected to vary. Raw sidecar hashes remain available
	// for an independent physical census.
	for family, digest := range before.NormalizedSidecars {
		if after.NormalizedSidecars[family] != digest {
			r.t.Fatalf("%s changed native sidecar family %s", label, family)
		}
	}
}
