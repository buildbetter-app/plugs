package localclisession_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

const disposableWarning = "warning: memory storage is disposable; all data is discarded when this process exits\n"

func plugBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("PLUG_BINARY")
	if binary == "" {
		t.Fatal("Bazel must supply the real plug executable")
	}
	if !filepath.IsAbs(binary) {
		binary = filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), binary)
	}
	return binary
}

func runPlug(t *testing.T, dir, input string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, plugBinary(t), args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PLUG_SERVER=", "PLUG_TOKEN=")
	cmd.Stdin = strings.NewReader(input)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("plug process timed out: %v; stderr=%s", ctx.Err(), errOut.String())
	}
	return out.String(), errOut.String(), err
}

func decodeLines(t *testing.T, stdout string) []map[string]any {
	t.Helper()
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return nil
	}
	lines := strings.Split(trimmed, "\n")
	results := make([]map[string]any, 0, len(lines))
	for i, line := range lines {
		decoded, err := wire.DecodeJSON([]byte(line))
		if err != nil {
			t.Fatalf("result %d is not canonical JSON: %v\n%s", i+1, err, line)
		}
		object, ok := decoded.(map[string]any)
		if !ok {
			t.Fatalf("result %d is not a JSON object: %T", i+1, decoded)
		}
		results = append(results, object)
	}
	return results
}

func objectAt(t *testing.T, value any, path string) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s: want object, got %T", path, value)
	}
	return object
}

func arrayAt(t *testing.T, object map[string]any, key string) []any {
	t.Helper()
	value, ok := object[key]
	if !ok {
		t.Fatalf("response is missing %q: %v", key, object)
	}
	array, ok := value.([]any)
	if !ok {
		t.Fatalf("response %q: want array, got %T", key, value)
	}
	return array
}

func stringAt(t *testing.T, object map[string]any, key string) string {
	t.Helper()
	value, ok := object[key].(string)
	if !ok {
		t.Fatalf("response %q: want string, got %T", key, object[key])
	}
	return value
}

func numberAt(t *testing.T, object map[string]any, key string) json.Number {
	t.Helper()
	value, ok := object[key].(json.Number)
	if !ok {
		t.Fatalf("response %q: want lossless number, got %T", key, object[key])
	}
	return value
}

func requireRevision(t *testing.T, result map[string]any, want string) {
	t.Helper()
	if got := numberAt(t, result, "revision").String(); got != want {
		t.Fatalf("revision: got %s, want %s", got, want)
	}
}

func requireEmptyWarnings(t *testing.T, result map[string]any) {
	t.Helper()
	warnings := arrayAt(t, result, "warnings")
	if len(warnings) != 0 {
		t.Fatalf("unexpected write warnings: %v", warnings)
	}
}

func exitCode(err error) int {
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return 0
	}
	return exit.ExitCode()
}

func TestRealBuiltMemorySessionLifecycleAndDisposableLifetime(t *testing.T) {
	commands := strings.Join([]string{
		`["ready"]`,
		`["create","--title","Original","--idempotency-key","00000000000000000000000000000001"]`,
		`["claim","@last","--expect","@revision","--idempotency-key","00000000000000000000000000000002"]`,
		`["update","@last","--title","Updated","--expect","@revision","--idempotency-key","00000000000000000000000000000003"]`,
		`["comment","@last","--body","Evidence","--expect","@revision","--idempotency-key","00000000000000000000000000000004"]`,
		`["close","@last","--reason","Done","--expect","@revision","--idempotency-key","00000000000000000000000000000005"]`,
		`["history","--as-of","@revision","--id","@last"]`,
		`["ready"]`,
		`["get","@last","--as-of","1"]`,
	}, "\n") + "\n"
	args := []string{"--memory", "--project", "lifetime", "--agent", "worker", "--json", "session"}
	stdout, stderr, err := runPlug(t, "", commands, args...)
	if err != nil {
		t.Fatalf("successful local session exited %d: %v\nstdout=%s\nstderr=%s", exitCode(err), err, stdout, stderr)
	}
	if stderr != disposableWarning {
		t.Fatalf("local session warnings changed:\n%s", stderr)
	}
	results := decodeLines(t, stdout)
	if len(results) != 9 {
		t.Fatalf("one result per session line required: got %d\n%s", len(results), stdout)
	}
	if got := len(arrayAt(t, results[0], "items")); got != 0 {
		t.Fatalf("fresh ready returned %d rows", got)
	}

	created := arrayAt(t, results[1], "results")
	if len(created) != 1 {
		t.Fatalf("create returned %d results", len(created))
	}
	createRow := objectAt(t, created[0], "create result")
	createRef := objectAt(t, createRow["ref"], "create ref")
	id := stringAt(t, createRef, "id")
	if id == "" {
		t.Fatal("create did not return an actual ID")
	}
	requireRevision(t, results[1], "1")
	requireEmptyWarnings(t, results[1])

	claimRow := objectAt(t, arrayAt(t, results[2], "results")[0], "claim result")
	if stringAt(t, objectAt(t, claimRow["ref"], "claim ref"), "id") != id {
		t.Fatalf("claim returned a different ID: %v", claimRow)
	}
	claimRecord := objectAt(t, claimRow["record"], "claim record")
	claimFields := objectAt(t, claimRecord["fields"], "claim fields")
	if stringAt(t, claimFields, "status") != "in_progress" {
		t.Fatalf("claim did not move the actual bead to in_progress: %v", claimFields)
	}
	claimLease := objectAt(t, claimRecord["lease"], "claim lease")
	if stringAt(t, claimLease, "agent") != "worker" || stringAt(t, objectAt(t, claimLease["principal"], "claim principal"), "kind") != "anonymous" {
		t.Fatalf("claim lease did not retain the local agent/principal: %v", claimLease)
	}
	requireRevision(t, results[2], "2")
	requireEmptyWarnings(t, results[2])

	updatedRow := objectAt(t, arrayAt(t, results[3], "results")[0], "update result")
	updatedRecord := objectAt(t, updatedRow["record"], "update record")
	if stringAt(t, objectAt(t, updatedRecord["fields"], "update fields"), "title") != "Updated" {
		t.Fatalf("update did not change the actual title: %v", updatedRecord)
	}
	requireRevision(t, results[3], "3")
	requireEmptyWarnings(t, results[3])

	commentRecord := objectAt(t, objectAt(t, arrayAt(t, results[4], "results")[0], "comment result")["record"], "comment record")
	comments := arrayAt(t, commentRecord, "comments")
	if len(comments) != 1 || stringAt(t, objectAt(t, comments[0], "comment"), "body") != "Evidence" {
		t.Fatalf("comment was not returned in the committed record: %v", commentRecord)
	}
	requireRevision(t, results[4], "4")
	requireEmptyWarnings(t, results[4])

	closeRows := arrayAt(t, results[5], "results")
	if len(closeRows) == 0 {
		t.Fatal("close returned no actual records")
	}
	closeRecord := objectAt(t, objectAt(t, closeRows[0], "close result")["record"], "close record")
	if stringAt(t, objectAt(t, closeRecord["fields"], "close fields"), "status") != "done" {
		t.Fatalf("close did not settle the actual bead: %v", closeRecord)
	}
	requireRevision(t, results[5], "5")
	requireEmptyWarnings(t, results[5])

	historyItems := arrayAt(t, results[6], "items")
	if len(historyItems) != 1 {
		t.Fatalf("history returned %d items, want one actual bead", len(historyItems))
	}
	history := objectAt(t, historyItems[0], "history item")
	if stringAt(t, history, "id") != id || stringAt(t, history, "status") != "done" || numberAt(t, history, "revision").String() != "5" || stringAt(t, objectAt(t, history["fields"], "history fields"), "title") != "Updated" {
		t.Fatalf("history did not return the final committed state: %v", history)
	}
	if got := len(arrayAt(t, results[7], "items")); got != 0 {
		t.Fatalf("closed bead remained ready: %d rows", got)
	}
	historical := objectAt(t, results[8]["bead"], "historical bead")
	if stringAt(t, historical, "id") != id || stringAt(t, historical, "status") != "todo" || numberAt(t, historical, "revision").String() != "1" || stringAt(t, objectAt(t, historical["fields"], "historical fields"), "title") != "Original" {
		t.Fatalf("as-of read substituted current state: %v", historical)
	}
	t.Logf("actual built local session: ID=%s revisions=1..5, history and ready state verified", id)

	// The same project and agent in a new process still see an empty store.
	// Keep the invocation explicit to make the disposable boundary reviewable.
	fresh, freshErrOut, err := runPlug(t, "", "", "--memory", "--project", "lifetime", "--agent", "worker", "--json", "ready")
	if err != nil || freshErrOut != disposableWarning {
		t.Fatalf("fresh disposable invocation failed: exit=%d stderr=%s", exitCode(err), freshErrOut)
	}
	freshResults := decodeLines(t, fresh)
	if len(freshResults) != 1 || len(arrayAt(t, freshResults[0], "items")) != 0 {
		t.Fatalf("new process retained local state: %s", fresh)
	}
}

func TestLocalCLIRefusesUnknownCommands(t *testing.T) {
	stdout, stderr, err := runPlug(t, "", "", "--memory", "--json", "unknown")
	if exitCode(err) != 2 || stdout != "" || !strings.Contains(stderr, `unsupported command "unknown"`) || strings.Contains(stderr, "disposable") {
		t.Fatalf("unknown command was not refused before opening local storage: exit=%d stdout=%q stderr=%q", exitCode(err), stdout, stderr)
	}

	stdout, stderr, err = runPlug(t, "", "[\"create\",\"--title\",\"First\"]\n[\"unknown\"]\n[\"create\",\"--title\",\"Must not run\"]\n", "--memory", "--json", "session")
	if exitCode(err) != 1 || len(decodeLines(t, stdout)) != 1 || !strings.Contains(stderr, "session line 2") || !strings.Contains(stderr, `unsupported command "unknown"`) || strings.Count(stderr, "disposable") != 1 {
		t.Fatalf("session did not stop at strict unknown-command refusal: exit=%d stdout=%q stderr=%q", exitCode(err), stdout, stderr)
	}
}

func TestLocalCLIPersistsSQLiteAcrossProcessesAndHonorsMemoryOptIn(t *testing.T) {
	dir := t.TempDir()
	project := "persistent"
	createOut, createErr, err := runPlug(t, dir, "", "--project", project, "--json", "create", "--title", "Original", "--idempotency-key", strings.Repeat("a", 64))
	if err != nil || createErr != "" {
		t.Fatalf("persistent create failed: exit=%d stdout=%s stderr=%s", exitCode(err), createOut, createErr)
	}
	created := decodeLines(t, createOut)
	if len(created) != 1 {
		t.Fatalf("persistent create returned %d results", len(created))
	}
	createRows := arrayAt(t, created[0], "results")
	if len(createRows) != 1 {
		t.Fatalf("persistent create returned %d rows", len(createRows))
	}
	id := stringAt(t, objectAt(t, objectAt(t, createRows[0], "create result")["ref"], "create ref"), "id")
	requireRevision(t, created[0], "1")

	readyOut, readyErr, err := runPlug(t, dir, "", "--project", project, "--json", "ready")
	if err != nil || readyErr != "" {
		t.Fatalf("persistent list failed after restart: exit=%d stdout=%s stderr=%s", exitCode(err), readyOut, readyErr)
	}
	ready := decodeLines(t, readyOut)
	if len(ready) != 1 || len(arrayAt(t, ready[0], "items")) != 1 {
		t.Fatalf("persistent list lost created bead: %s", readyOut)
	}
	readyItem := objectAt(t, arrayAt(t, ready[0], "items")[0], "ready item")
	if stringAt(t, readyItem, "id") != id || stringAt(t, objectAt(t, readyItem["fields"], "ready fields"), "title") != "Original" {
		t.Fatalf("persistent list returned wrong bead: %s", readyOut)
	}

	updateOut, updateErr, err := runPlug(t, dir, "", "--project", project, "--json", "update", id, "--title", "Updated", "--expect", "1")
	if err != nil || updateErr != "" {
		t.Fatalf("persistent update failed: exit=%d stdout=%s stderr=%s", exitCode(err), updateOut, updateErr)
	}
	updated := decodeLines(t, updateOut)
	if len(updated) != 1 {
		t.Fatalf("persistent update returned %d results", len(updated))
	}
	requireRevision(t, updated[0], "2")

	closeOut, closeErr, err := runPlug(t, dir, "", "--project", project, "--json", "close", id, "--reason", "Done", "--expect", "2")
	if err != nil || closeErr != "" {
		t.Fatalf("persistent close failed: exit=%d stdout=%s stderr=%s", exitCode(err), closeOut, closeErr)
	}
	closed := decodeLines(t, closeOut)
	if len(closed) != 1 {
		t.Fatalf("persistent close returned %d results", len(closed))
	}
	requireRevision(t, closed[0], "3")

	historyOut, historyErr, err := runPlug(t, dir, "", "--project", project, "--json", "history", "--as-of", "3", "--id", id)
	if err != nil || historyErr != "" {
		t.Fatalf("persistent history failed after restart: exit=%d stdout=%s stderr=%s", exitCode(err), historyOut, historyErr)
	}
	history := decodeLines(t, historyOut)
	if len(history) != 1 || len(arrayAt(t, history[0], "items")) != 1 {
		t.Fatalf("persistent history was not retained: %s", historyOut)
	}
	historyItem := objectAt(t, arrayAt(t, history[0], "items")[0], "history item")
	if stringAt(t, historyItem, "id") != id || stringAt(t, historyItem, "status") != "done" || stringAt(t, objectAt(t, historyItem["fields"], "history fields"), "title") != "Updated" {
		t.Fatalf("persistent history returned wrong state: %s", historyOut)
	}
	if _, err := os.Stat(filepath.Join(dir, ".plug", "plug.db")); err != nil {
		t.Fatalf("default SQLite file was not created: %v", err)
	}
	configuredDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(configuredDir, ".plug"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configuredDir, ".plug", "config.yaml"), []byte("storage:\n  driver: sqlite\n  path: .plug/custom/state.db\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configuredOut, configuredErr, err := runPlug(t, configuredDir, "", "--json", "create", "--title", "Configured")
	if err != nil || configuredErr != "" || len(decodeLines(t, configuredOut)) != 1 {
		t.Fatalf("configured SQLite path failed: exit=%d stdout=%s stderr=%s", exitCode(err), configuredOut, configuredErr)
	}
	if _, err := os.Stat(filepath.Join(configuredDir, ".plug", "custom", "state.db")); err != nil {
		t.Fatalf("configured SQLite parent was not created: %v", err)
	}

	memoryDir := t.TempDir()
	memoryCreate, memoryErr, err := runPlug(t, memoryDir, "", "--memory", "--json", "create", "--title", "Ephemeral")
	if err != nil || memoryErr != disposableWarning {
		t.Fatalf("explicit memory create failed: exit=%d stdout=%s stderr=%s", exitCode(err), memoryCreate, memoryErr)
	}
	memoryReady, memoryReadyErr, err := runPlug(t, memoryDir, "", "--memory", "--json", "ready")
	if err != nil || memoryReadyErr != disposableWarning || len(decodeLines(t, memoryReady)) != 1 || len(arrayAt(t, decodeLines(t, memoryReady)[0], "items")) != 0 {
		t.Fatalf("explicit memory mode retained state across processes: exit=%d stdout=%s stderr=%s", exitCode(err), memoryReady, memoryReadyErr)
	}
}

func TestLocalCLIRejectsMalformedConfig(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".plug"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".plug", "config.yaml"), []byte("storage:\n  driver: sqlite\n  unknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := runPlug(t, dir, "", "--json", "ready")
	if exitCode(err) != 2 || stdout != "" || !strings.Contains(stderr, "unsupported storage key") || strings.Contains(stderr, "memory storage is disposable") {
		t.Fatalf("malformed config did not fail closed: exit=%d stdout=%q stderr=%q", exitCode(err), stdout, stderr)
	}

	stdout, stderr, err = runPlug(t, dir, "", "--memory", "--json", "ready")
	if exitCode(err) != 2 || stdout != "" || !strings.Contains(stderr, "unsupported storage key") || strings.Contains(stderr, "disposable") {
		t.Fatalf("malformed config was not rejected in explicit memory mode: exit=%d stdout=%q stderr=%q", exitCode(err), stdout, stderr)
	}
}
