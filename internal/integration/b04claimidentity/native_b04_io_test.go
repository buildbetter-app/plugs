package b04claimidentity_test

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
	"strings"
	"syscall"
	"testing"
	"time"
)

//go:embed native_b04_inputs.json
var b04NativeInputsBytes []byte

type b04NativeInputs struct {
	Format             string   `json:"format"`
	Consumer           string   `json:"consumer"`
	ConsumerTree       string   `json:"consumerTree"`
	ConsumerParent     string   `json:"consumerParent"`
	Package            string   `json:"package"`
	PackageBuildBlob   string   `json:"packageBuildBlob"`
	PackageBuildSHA256 string   `json:"packageBuildSHA256"`
	Source             string   `json:"source"`
	SourceTree         string   `json:"sourceTree"`
	HelperSource       string   `json:"helperSource"`
	HelperSHA256       string   `json:"helperSHA256"`
	BuildRun           string   `json:"buildRun"`
	Artifact           string   `json:"artifact"`
	ArtifactSHA256     string   `json:"artifactSHA256"`
	ArtifactBytes      int64    `json:"artifactBytes"`
	BinarySHA256       string   `json:"binarySHA256"`
	BinaryBytes        int64    `json:"binaryBytes"`
	Descriptor         string   `json:"descriptor"`
	ManifestSHA256     string   `json:"manifestSHA256"`
	WorkflowSHA        string   `json:"workflowSHA"`
	Control            string   `json:"control"`
	Flags              []string `json:"flags"`
	Vector             struct {
		ID             string            `json:"id"`
		DatabasePrefix string            `json:"databasePrefix"`
		SourceIDs      map[string]string `json:"sourceIDs"`
		Runs           []struct {
			ID       string `json:"id"`
			Database string `json:"database"`
		} `json:"runs"`
	} `json:"vector"`
	CommandContract map[string]string `json:"commandContract"`
	Qualification   string            `json:"qualification"`
	ClockScope      string            `json:"clockScope"`
	Boundaries      []string          `json:"boundaries"`
}

func loadB04NativeInputs(t *testing.T) b04NativeInputs {
	t.Helper()
	var inputs b04NativeInputs
	if err := json.Unmarshal(b04NativeInputsBytes, &inputs); err != nil {
		t.Fatalf("decode B04 native input: %v", err)
	}
	return inputs
}

func b04NativeWrite(t testing.TB, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func b04NativeJSON(t testing.TB, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
	b04NativeWrite(t, path, append(data, '\n'))
}

func b04NativeSHA(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func b04NativeFileSHA(path string) (string, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", 0, err
	}
	return b04NativeSHA(data), int64(len(data)), nil
}

func b04NativeEnv(t testing.TB, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required from reviewed RunsOn preparation", name)
	}
	return value
}

// b04NativePrepared checks the producer's actual executable, manifest, source
// and four preparation bindings before any native database is opened.
func b04NativePrepared(t *testing.T, evidence string, inputs b04NativeInputs) string {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Fatal("B04 native admission requires Linux/amd64")
	}
	binary := b04NativeEnv(t, "PLUG_PUBLIC_BEADS_BINARY")
	if b04NativeEnv(t, "PLUG_PUBLIC_BEADS_BUILD_RUN") != inputs.BuildRun || b04NativeEnv(t, "PLUG_PUBLIC_BEADS_SHA256") != inputs.BinarySHA256 {
		t.Fatal("prepared run or executable digest differs from the admitted B04 input")
	}
	manifestPath := b04NativeEnv(t, "PLUG_PUBLIC_BEADS_MANIFEST")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	b04NativeWrite(t, filepath.Join(evidence, "prepared-manifest.json"), manifestBytes)
	if got := b04NativeSHA(manifestBytes); got != inputs.ManifestSHA256 {
		t.Fatalf("prepared manifest SHA=%s want=%s", got, inputs.ManifestSHA256)
	}
	var manifest struct {
		Schema string `json:"schema"`
		Source map[string]struct {
			Commit         string `json:"commit"`
			Tree           string `json:"tree"`
			ManifestSHA256 string `json:"manifest_sha256"`
			Entries        int    `json:"manifest_entries"`
		} `json:"source"`
		Workflow struct {
			SHA     string `json:"sha"`
			Profile string `json:"profile"`
		} `json:"workflow"`
		Build struct {
			Run string `json:"run_id"`
		} `json:"build"`
		Artifacts map[string]struct {
			SHA256 string `json:"sha256"`
			Size   int64  `json:"size"`
		} `json:"artifacts"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("prepared manifest JSON: %v", err)
	}
	beads := manifest.Source["beads"]
	if manifest.Workflow.SHA != inputs.WorkflowSHA || manifest.Build.Run != inputs.BuildRun || beads.Commit != inputs.Source || beads.Tree != inputs.SourceTree {
		t.Fatalf("prepared source/workflow/build bindings differ: %+v", manifest)
	}
	matches := 0
	for _, artifact := range manifest.Artifacts {
		if artifact.SHA256 == inputs.BinarySHA256 && artifact.Size == inputs.BinaryBytes {
			matches++
		}
	}
	digest, size, err := b04NativeFileSHA(binary)
	if err != nil || digest != inputs.BinarySHA256 || size != inputs.BinaryBytes || matches != 1 {
		t.Fatalf("prepared executable digest=%s bytes=%d manifestMatches=%d err=%v", digest, size, matches, err)
	}
	b04NativeJSON(t, filepath.Join(evidence, "prepared-executable.json"), map[string]any{
		"path": binary, "sha256": digest, "bytes": size, "buildRun": inputs.BuildRun,
		"descriptor": inputs.Descriptor, "consumer": inputs.Consumer, "consumerTree": inputs.ConsumerTree,
		"helperSource": inputs.HelperSource, "helperSHA256": inputs.HelperSHA256,
	})
	return binary
}

type b04NativeCommand struct {
	t                         *testing.T
	binary, workspace, output string
	env                       []string
	count                     int
}

type b04NativeResult struct {
	Label      string    `json:"label"`
	Argv       []string  `json:"argv"`
	Exit       int       `json:"exit"`
	Stdout     []byte    `json:"-"`
	Stderr     []byte    `json:"-"`
	TimedOut   bool      `json:"timedOut"`
	StartedUTC time.Time `json:"startedUTC"`
	EndedUTC   time.Time `json:"endedUTC"`
}

func (c *b04NativeCommand) execute(label string, args ...string) b04NativeResult {
	c.t.Helper()
	c.count++
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.binary, args...)
	cmd.Dir, cmd.Env = c.workspace, c.env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now().UTC()
	err := cmd.Run()
	ended := time.Now().UTC()
	exit := 0
	if err != nil {
		exit = -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exit = exitErr.ExitCode()
		}
	}
	name := fmt.Sprintf("%03d-%s", c.count, label)
	b04NativeWrite(c.t, filepath.Join(c.output, name+".stdout"), stdout.Bytes())
	b04NativeWrite(c.t, filepath.Join(c.output, name+".stderr"), stderr.Bytes())
	b04NativeJSON(c.t, filepath.Join(c.output, name+".receipt.json"), map[string]any{
		"label": label, "argv": append([]string{c.binary}, args...), "cwd": c.workspace, "env": c.env,
		"exit": exit, "startedUTC": started, "endedUTC": ended, "timedOut": ctx.Err() != nil,
		"stdoutBytes": stdout.Len(), "stderrBytes": stderr.Len(),
		"stdoutSHA256": b04NativeSHA(stdout.Bytes()), "stderrSHA256": b04NativeSHA(stderr.Bytes()),
	})
	result := b04NativeResult{Label: label, Argv: append([]string{c.binary}, args...), Exit: exit, Stdout: append([]byte(nil), stdout.Bytes()...), Stderr: append([]byte(nil), stderr.Bytes()...), TimedOut: ctx.Err() != nil, StartedUTC: started, EndedUTC: ended}
	c.t.Logf("B04_NATIVE_CLI label=%s exit=%d start=%s end=%s", label, exit, started.Format(time.RFC3339Nano), ended.Format(time.RFC3339Nano))
	return result
}

func (c *b04NativeCommand) run(label string, args ...string) []byte {
	c.t.Helper()
	result := c.execute(label, args...)
	if result.Exit != 0 || result.TimedOut {
		c.t.Fatalf("native command %s failed exit=%d timeout=%v; original streams retained", label, result.Exit, result.TimedOut)
	}
	return result.Stdout
}

func (c *b04NativeCommand) expect(label string, wantExit int, args ...string) b04NativeResult {
	c.t.Helper()
	result := c.execute(label, args...)
	if result.TimedOut {
		c.t.Fatalf("expected native command %s to finish; original streams retained", label)
	}
	if result.Exit != wantExit {
		c.t.Fatalf("native command %s exit=%d want=%d; stderr=%q", label, result.Exit, wantExit, result.Stderr)
	}
	return result
}

func b04NativeDecode(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing native JSON: %v", err)
	}
	return nil
}

func b04NativeInventory(root string) ([]map[string]any, error) {
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
			digest, size, err := b04NativeFileSHA(path)
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
			return fmt.Errorf("unsupported native output type at %s", rel)
		}
		entries = append(entries, item)
		return nil
	})
	return entries, err
}

func b04NativeOutputState(path string) map[string]any {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]any{"path": filepath.Base(path), "state": "absent"}
	}
	if err != nil {
		return map[string]any{"path": filepath.Base(path), "state": "refused", "error": err.Error()}
	}
	state := "present-nonempty"
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

func b04NativeSafeID(id string) bool {
	if id == "" || len(id) > 64 || strings.HasPrefix(id, "-") || strings.HasSuffix(id, "-") {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '-' {
			return false
		}
	}
	return true
}
