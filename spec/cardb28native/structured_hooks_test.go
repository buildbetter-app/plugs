package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// Only this semantic projection is compared across independently initialized
// native stores. Original temporal values remain in the actual stdin and
// public readback files; they are not invented Plug revisions or timestamps.
type b28NativeHook struct {
	Phase        string                `json:"phase"`
	Event        string                `json:"event"`
	ID           string                `json:"id"`
	Title        string                `json:"title"`
	Description  string                `json:"description"`
	Status       string                `json:"status"`
	Priority     json.Number           `json:"priority"`
	Dependencies []b28NativeDependency `json:"dependencies"`
}

type b28NativeDependency struct {
	IssueID     string `json:"issue_id"`
	DependsOnID string `json:"depends_on_id"`
	Type        string `json:"type"`
	CreatedBy   string `json:"created_by"`
}

type b28Native struct {
	t      *testing.T
	r      *nativeRecorder
	dir    string
	binary string
	env    []string
	log    string
}

func (n *b28Native) command(label string, wantFailure bool, args ...string) []byte {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Dir = n.dir
	cmd.Env = append(append([]string(nil), n.env...), "B28_HOOK_PHASE="+label)
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
	for suffix, data := range map[string][]byte{"stdout": stdout.Bytes(), "stderr": stderr.Bytes()} {
		if err := os.WriteFile(filepath.Join(n.r.root, label+"."+suffix), data, 0600); err != nil {
			n.t.Fatal(err)
		}
	}
	n.r.writeJSON(filepath.Join(n.r.root, label+".request.json"), map[string]any{"argv": cmd.Args, "directory": n.dir, "exit": exit, "stdout_sha256": hashBytes(stdout.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes()), "error": fmt.Sprint(err)})
	if ctx.Err() != nil || exit < 0 || wantFailure && exit == 0 || !wantFailure && exit != 0 {
		n.t.Fatalf("native %s exit=%d err=%v stderr=%s", label, exit, err, stderr.Bytes())
	}
	return append([]byte(nil), stdout.Bytes()...)
}

func b28NativeRows(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	var rows []map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&rows); err != nil {
		t.Fatalf("native public rows: %v: %s", err, raw)
	}
	return rows
}

func (n *b28Native) callbacks() string {
	n.t.Helper()
	raw, err := os.ReadFile(n.log)
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		n.t.Fatal(err)
	}
	return string(raw)
}

func (n *b28Native) hook(phase, event, title, description, status, priority string, dependencies []b28NativeDependency) b28NativeHook {
	n.t.Helper()
	payloadPath := filepath.Join(n.r.root, phase+".hook.stdin.json")
	raw, err := os.ReadFile(payloadPath)
	if err != nil {
		n.t.Fatalf("actual script stdin absent for%s: %v", phase, err)
	}
	var observed b28NativeHook
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&observed); err != nil {
		n.t.Fatal(err)
	}
	observed.Phase = phase
	observed.Event = event
	if observed.ID != "b28-first" || observed.Title != title || observed.Description != description || observed.Status != status || observed.Priority.String() != priority || !reflect.DeepEqual(observed.Dependencies, dependencies) {
		n.t.Fatalf("independent native hook expectation differs: %+v deps=%+v raw=%s", observed, dependencies, raw)
	}
	argv, err := os.ReadFile(filepath.Join(n.r.root, phase+".hook.argv"))
	if err != nil || string(argv) != "b28-first\n"+event+"\n" {
		n.t.Fatalf("actual script argv=%q err=%v", argv, err)
	}
	readback, err := os.ReadFile(filepath.Join(n.r.root, phase+".hook.readback.json"))
	if err != nil {
		n.t.Fatal(err)
	}
	rows := b28NativeRows(n.t, readback)
	if len(rows) != 1 || rows[0]["id"] != "b28-first" || rows[0]["title"] != title || rows[0]["description"] != description || rows[0]["status"] != status || fmt.Sprint(rows[0]["priority"]) != priority {
		n.t.Fatalf("hook did not read actual committed public values: %s", readback)
	}
	// Public show hydrates each dependency as the target issue plus relation
	// metadata; script stdin carries the detached dependency record instead.
	publicDependencies, _ := rows[0]["dependencies"].([]any)
	if len(publicDependencies) != len(dependencies) {
		n.t.Fatalf("native public dependency count=%d want=%d: %s", len(publicDependencies), len(dependencies), readback)
	}
	for i, dependency := range dependencies {
		current, ok := publicDependencies[i].(map[string]any)
		if !ok || current["id"] != dependency.DependsOnID || current["dependency_type"] != dependency.Type {
			n.t.Fatalf("native public dependency differs: got=%+v want=%+v", publicDependencies[i], dependency)
		}
	}
	var complete map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&complete); err != nil {
		n.t.Fatal(err)
	}
	for _, field := range []string{"created_at", "updated_at"} {
		text, ok := complete[field].(string)
		if _, err := time.Parse(time.RFC3339Nano, text); !ok || err != nil || rows[0][field] != text {
			n.t.Fatalf("actual temporal snapshot/readback mismatch field=%s hook=%v current=%v", field, complete[field], rows[0][field])
		}
	}
	return observed
}

func b28RecordNative(t *testing.T, binary, root, name string) []b28NativeHook {
	t.Helper()
	if err := os.MkdirAll("/tmp/plug-swarm/work", 0700); err != nil {
		t.Fatal(err)
	}
	workspace, err := os.MkdirTemp("/tmp/plug-swarm/work", "b28-native-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(workspace); err != nil {
			t.Error(err)
		}
	})
	for _, dir := range []string{"home", "project", "tmp"} {
		if err := os.Mkdir(filepath.Join(workspace, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	server := publicnative.StartDolt(t, root)
	base := []string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + filepath.Join(workspace, "home"), "TMPDIR=" + filepath.Join(workspace, "tmp"), "USER=b28-native", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B28 Public", "GIT_AUTHOR_EMAIL=b28@example.invalid", "GIT_COMMITTER_NAME=B28 Public", "GIT_COMMITTER_EMAIL=b28@example.invalid", "BEADS_ACTOR=b28-native", "BEADS_NO_DAEMON=1", "B28_HOOK_ROOT=" + root, "B28_HOOK_BINARY=" + binary}
	n := &b28Native{t: t, r: &nativeRecorder{t: t, root: root}, dir: filepath.Join(workspace, "project"), binary: "/usr/bin/git", env: server.ChildEnv(base), log: filepath.Join(root, "hooks.log")}
	n.command("git-init", false, "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	n.binary = binary
	var version struct{ Commit, Build string }
	if json.Unmarshal(n.command("native-version", false, "version", "--json"), &version) != nil || version.Commit != pinnedSource.Commit || version.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatal("executed source identity differs")
	}
	database := "b28_hooks_" + name
	n.command("init", false, "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", "b28", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, database)
	// Setup has no callbacks. The actual recorded operations start only after
	// installing disposable scripts; no external plugin destination exists.
	for _, id := range []string{"b28-target", "b28-delete"} {
		n.command("setup-"+id, false, "create", "--id", id, "--title", id, "--type", "task", "--silent")
	}
	hooks := filepath.Join(n.dir, ".beads", "hooks")
	if err := os.MkdirAll(hooks, 0700); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
set -eu
export PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin
stem="$B28_HOOK_ROOT/$B28_HOOK_PHASE.hook"
printf '%s\n%s\n' "$1" "$2" > "$stem.argv"
cat > "$stem.stdin.json"
"$B28_HOOK_BINARY" --readonly show "$1" --json > "$stem.readback.json"
printf '%s|%s|%s\n' "$B28_HOOK_PHASE" "$2" "$1" >> "$B28_HOOK_ROOT/hooks.log"
if test "$B28_HOOK_PHASE" = failing-hook; then exit 73; fi
`
	for _, hook := range []string{"on_create", "on_update", "on_close"} {
		if err := os.WriteFile(filepath.Join(hooks, hook), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	var observations []b28NativeHook
	description := "rich description α\nsecond line"
	n.command("rich-create", false, "create", "--id", "b28-first", "--title", "first", "--description", description, "--type", "task", "--priority", "0", "--silent")
	observations = append(observations, n.hook("rich-create", "create", "first", description, "open", "0", nil))
	n.command("rich-update", false, "update", "b28-first", "--title", "edited", "--json")
	observations = append(observations, n.hook("rich-update", "update", "edited", description, "open", "0", nil))
	n.command("noop", false, "update", "b28-first", "--title", "edited", "--json")
	// This guarded native Update completes on every successful result, even
	// Changed=false. Plug's adopted write pipeline suppresses its no-op posts.
	observations = append(observations, n.hook("noop", "update", "edited", description, "open", "0", nil))
	n.command("comment", false, "comments", "add", "b28-first", "exact comment α\nsecond line", "--json")
	// This public comment path rereads GetIssue; it does not invent a comment
	// event or hydrate comment rows into the script issue snapshot.
	observations = append(observations, n.hook("comment", "update", "edited", description, "open", "0", nil))
	comments := b28NativeRows(t, n.command("comment-readback", false, "comments", "b28-first", "--json"))
	if len(comments) != 1 || comments[0]["text"] != "exact comment α\nsecond line" || comments[0]["author"] != "b28-native" {
		t.Fatalf("native exact comment readback=%+v", comments)
	}
	n.command("dependency", false, "dep", "add", "b28-first", "b28-target", "--type", "relates-to", "--json")
	dep := []b28NativeDependency{{IssueID: "b28-first", DependsOnID: "b28-target", Type: "relates-to", CreatedBy: "b28-native"}}
	observations = append(observations, n.hook("dependency", "update", "edited", description, "open", "0", dep))
	before := n.callbacks()
	n.command("invalid-priority", true, "update", "b28-first", "--priority", "9", "--json")
	if n.callbacks() != before {
		t.Fatal("native invalid ordinary write emitted a hook")
	}
	n.command("close", false, "close", "b28-first", "--reason", "native complete", "--json")
	observations = append(observations, n.hook("close", "close", "edited", description, "closed", "0", dep))
	n.command("failing-hook", false, "update", "b28-first", "--title", "committed despite hook failure", "--json")
	observations = append(observations, n.hook("failing-hook", "update", "committed despite hook failure", description, "closed", "0", dep))
	before = n.callbacks()
	n.command("delete", false, "delete", "b28-delete", "--force", "--json")
	if n.callbacks() != before {
		t.Fatal("native delete unexpectedly emitted a script hook")
	}
	want := "rich-create|create|b28-first\nrich-update|update|b28-first\nnoop|update|b28-first\ncomment|update|b28-first\ndependency|update|b28-first\nclose|close|b28-first\nfailing-hook|update|b28-first\n"
	if n.callbacks() != want {
		t.Fatalf("native operation order/multiplicity=%q want=%q", n.callbacks(), want)
	}
	final := b28NativeRows(t, n.command("new-process-current-readback", false, "show", "b28-first", "--json"))
	if len(final) != 1 || final[0]["title"] != "committed despite hook failure" || final[0]["status"] != "closed" {
		t.Fatal("failing native script vetoed committed data")
	}
	n.r.writeJSON(filepath.Join(root, "semantic-projection.json"), observations)
	n.r.writeJSON(filepath.Join(root, "dispositions.json"), map[string]any{"native_public_path": "canonical CLI guarded issue operations and HookFiringStore", "native_delete": "no script hook", "native_successful_noop_update": "successful guarded Update emits update hook even when Changed=false", "plug_noop": "adopted write pipeline suppresses post delivery and graph revisions for unchanged writes", "comment": "update issue snapshot plus independently read comment row", "native_hook_error": "cannot veto committed mutation; script exit73", "plug_pre_mutation": "explicit adopted configured mutator changes title before validation/commit", "plug_comment_dependency": "typed operations and structured before/after document relations", "plug_post_error": "committed result.warnings plus deadletter and actual owner-supplied alert port; requires separate alert acceptance evidence", "idempotent_response_replay": "Plug-only stored response contract; no invented native idempotency equivalence", "official": "UNSPENT", "private": "HARD_DISABLED"})
	return observations
}

func TestB28CanonicalNativeStructuredHookSnapshots(t *testing.T) {
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	root, checkout := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) {
		t.Fatal("RunsOn declared evidence and source root required")
	}
	r := &nativeRecorder{t: t, root: root}
	input, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(input) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("existing canonical source protocol changed")
	}
	var protocol struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(input, &protocol) != nil || protocol.ExporterLabel == "" {
		t.Fatal("canonical source target absent")
	}
	r.target = protocol.ExporterLabel
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	commit := r.buildCommand(checkout, "candidate-commit", "/usr/bin/git", []string{"cat-file", "commit", actual})
	for _, key := range []string{"GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing original run binding%s", key)
		}
	}
	r.writeJSON(filepath.Join(root, "request-run-binding.json"), map[string]any{"candidate": actual, "raw_commit_sha256": hashBytes(commit), "native_source": pinnedSource, "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "target": os.Getenv("TEST_TARGET"), "qualification": "PUBLIC_NATIVE_STRUCTURED_HOOK_OBSERVATIONS_ONLY"})
	binary := r.buildCanonicalSource(pinnedSource)
	var twins [][]b28NativeHook
	for _, name := range []string{"a", "b"} {
		if !t.Run("independent_"+name, func(t *testing.T) {
			dir := filepath.Join(root, "native-"+name)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			twins = append(twins, b28RecordNative(t, binary, dir, name))
		}) {
			t.Fatal("structured native observation incomplete")
		}
	}
	if len(twins) != 2 || !reflect.DeepEqual(twins[0], twins[1]) {
		t.Fatalf("independent native semantic observations diverged: %+v", twins)
	}
	r.writeJSON(filepath.Join(root, "independent-native-twins.json"), twins)
}
