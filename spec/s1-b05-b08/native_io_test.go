package s1b05b08

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

//go:embed native_inputs_b06_b08.json
var nativeInputsBytes []byte

type nativeInputs struct {
	Consumer, ConsumerTree, HelperSource, HelperTree string
	Source, SourceTree, SourceManifest               string
	SourceEntries                                    int
	BinarySHA256                                     string
	BinaryBytes                                      int64
	BuildRun, Profile, ManifestSHA256, WorkflowSHA   string
	Descriptor, AdoptedIndefiniteDecision            string
	ClockScope                                       string
	Vectors                                          []nativeVector
}

type nativeVector struct {
	ID, Mode  string
	SourceIDs map[string]string
	Until     *string
	Runs      []nativeRun
}

type nativeRun struct{ ID, Database string }

var nativeRoles = []string{"parent", "child", "grandchild", "free"}

func nativeWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func nativeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	nativeWrite(t, path, append(data, '\n'))
}

func nativeEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required from reviewed trusted RunsOn preparation", name)
	}
	return value
}

func nativeDigestFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(hash, file)
	return hex.EncodeToString(hash.Sum(nil)), size, errors.Join(copyErr, file.Close())
}

// Validate the actual four preparer values and their bytes before executing
// anything. The trusted preparer owns run/artifact/admission authentication;
// this consumer adds exact source, manifest, executable and linked-build checks.
func nativePrepared(t *testing.T, evidence string, inputs nativeInputs) string {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Fatal("the native profile requires Linux/amd64")
	}
	binary := nativeEnv(t, "PLUG_PUBLIC_BEADS_BINARY")
	if nativeEnv(t, "PLUG_PUBLIC_BEADS_BUILD_RUN") != inputs.BuildRun ||
		nativeEnv(t, "PLUG_PUBLIC_BEADS_SHA256") != inputs.BinarySHA256 {
		t.Fatal("prepared run/digest differs from the embedded native input pins")
	}
	manifestBytes, err := os.ReadFile(nativeEnv(t, "PLUG_PUBLIC_BEADS_MANIFEST"))
	if err != nil {
		t.Fatal(err)
	}
	nativeWrite(t, filepath.Join(evidence, "prepared-manifest.json"), manifestBytes)
	if b068SHA(manifestBytes) != inputs.ManifestSHA256 {
		t.Fatal("actual prepared manifest digest differs from the admitted descriptor")
	}
	var manifest struct {
		Schema string
		Source map[string]struct {
			Commit, Tree   string
			ManifestSHA256 string `json:"manifest_sha256"`
			Entries        int    `json:"manifest_entries"`
		}
		Workflow struct{ SHA, Profile string }
		Build    struct {
			Run string `json:"run_id"`
		}
		Artifacts map[string]struct {
			SHA256 string
			Size   int64
		}
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	source := manifest.Source["beads"]
	if manifest.Schema != inputs.Profile || manifest.Workflow.Profile != inputs.Profile ||
		manifest.Workflow.SHA != inputs.WorkflowSHA || manifest.Build.Run != inputs.BuildRun ||
		source.Commit != inputs.Source || source.Tree != inputs.SourceTree ||
		source.ManifestSHA256 != inputs.SourceManifest || source.Entries != inputs.SourceEntries {
		t.Fatal("prepared manifest source/tree/profile/build bindings differ")
	}
	matches := 0
	for _, artifact := range manifest.Artifacts {
		if artifact.SHA256 == inputs.BinarySHA256 && artifact.Size == inputs.BinaryBytes {
			matches++
		}
	}
	info, err := os.Lstat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 || !filepath.IsAbs(binary) {
		t.Fatalf("prepared executable is not an absolute regular executable: %v", err)
	}
	digest, size, err := nativeDigestFile(binary)
	nativeJSON(t, filepath.Join(evidence, "prepared-executable.json"), map[string]any{
		"path": binary, "sha256": digest, "bytes": size, "manifestMatches": matches,
		"descriptor": inputs.Descriptor, "consumerBase": inputs.Consumer, "helperSource": inputs.HelperSource,
	})
	if err != nil || matches != 1 || digest != inputs.BinarySHA256 || size != inputs.BinaryBytes {
		t.Fatalf("actual prepared executable differs: %s/%d: %v", digest, size, err)
	}
	return binary
}

type nativeCommand struct {
	t                         *testing.T
	binary, workspace, output string
	env                       []string
	count                     int
}

// Retain exact requests, both original streams and failure receipts before
// asserting success. The environment is newly constructed and contains no
// inherited credential or database locator.
func (c *nativeCommand) run(label string, args ...string) []byte { return c.runExit(label, 0, args...) }

func (c *nativeCommand) runExit(label string, want int, args ...string) []byte {
	c.t.Helper()
	c.count++
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, c.binary, args...)
	command.Dir, command.Env = c.workspace, c.env
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }
	command.WaitDelay = 5 * time.Second
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	started := time.Now().UTC()
	err := command.Run()
	ended := time.Now().UTC()
	exit := 0
	errorText := ""
	if err != nil {
		exit, errorText = -1, err.Error()
		var failure *exec.ExitError
		if errors.As(err, &failure) {
			exit = failure.ExitCode()
		}
	}
	name := fmt.Sprintf("%02d-%s", c.count, label)
	for suffix, data := range map[string][]byte{"stdout": stdout.Bytes(), "stderr": stderr.Bytes()} {
		nativeWrite(c.t, filepath.Join(c.output, name+"."+suffix), data)
	}
	receipt := map[string]any{
		"label": label, "argv": append([]string{c.binary}, args...), "cwd": c.workspace, "env": c.env,
		"startedUTC": started, "endedUTC": ended, "exit": exit, "error": errorText,
		"timedOut": ctx.Err() != nil, "stdoutBytes": stdout.Len(), "stderrBytes": stderr.Len(),
		"stdoutSHA256": b068SHA(stdout.Bytes()), "stderrSHA256": b068SHA(stderr.Bytes()),
	}
	nativeJSON(c.t, filepath.Join(c.output, name+".receipt.json"), receipt)
	c.t.Logf("B06_B08_NATIVE_CLI label=%s argv0=%s exit=%d start=%s end=%s", label, c.binary, exit, started.Format(time.RFC3339Nano), ended.Format(time.RFC3339Nano))
	if exit != want || ctx.Err() != nil {
		c.t.Fatalf("native command %s failed: %s, context %v; original streams retained", label, errorText, ctx.Err())
	}
	return append([]byte(nil), stdout.Bytes()...)
}

// No symlink following. The poll log is excluded by name before stat/read/hash.
// Empty directories and empty files remain distinguishable in the inventory.
func nativeInventory(root string) ([]map[string]any, error) {
	var entries []map[string]any
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if entry != nil && entry.Name() == "poll.log" {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		item := map[string]any{"path": filepath.ToSlash(rel), "mode": info.Mode().String(), "bytes": info.Size()}
		switch {
		case info.Mode().IsRegular():
			digest, size, err := nativeDigestFile(path)
			if err != nil {
				return err
			}
			item["type"], item["sha256"], item["bytes"] = "file", digest, size
		case info.IsDir():
			item["type"] = "directory"
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			item["type"], item["target"], item["qualification"] = "symlink", target, "refused-as-source-bytes"
		default:
			return fmt.Errorf("refused output file type at %s", rel)
		}
		entries = append(entries, item)
		return nil
	})
	return entries, err
}

func nativeOutputState(path string) map[string]any {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]any{"path": filepath.Base(path), "state": "absent"}
	}
	if err != nil {
		return map[string]any{"path": filepath.Base(path), "state": "refused", "error": err.Error()}
	}
	state := "present-nonempty"
	if !info.Mode().IsRegular() && !info.IsDir() {
		state = "unexpected-type"
	}
	if info.Mode().IsRegular() && info.Size() == 0 {
		state = "present-empty"
	}
	if info.IsDir() {
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			return map[string]any{"path": filepath.Base(path), "state": "refused", "error": readErr.Error()}
		}
		if len(entries) == 0 {
			state = "present-empty"
		}
	}
	return map[string]any{"path": filepath.Base(path), "state": state, "mode": info.Mode().String(), "bytes": info.Size()}
}

func nativeDecode(t *testing.T, data []byte, target any) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("native JSON decode: %v", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		t.Fatalf("native JSON has trailing/invalid bytes: %v", err)
	}
}

func nativeExportStates(t *testing.T, output, stage string) {
	t.Helper()
	states := []map[string]any{}
	for _, name := range []string{"inventory.json", "inventory.json.sources", "readback.tar"} {
		states = append(states, nativeOutputState(filepath.Join(output, name)))
	}
	nativeJSON(t, filepath.Join(output, "export-states-"+stage+".json"), map[string]any{
		"outputs": states, "testFailed": t.Failed(),
		"commandDisposition": "consult exact command receipt, exit and original streams; filesystem state alone does not establish refusal or success",
	})
}

func b068SHA(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
