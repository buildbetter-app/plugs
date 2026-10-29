package spec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/acceptance"
)

var plugBinary = flag.String("plug-binary", "", "Bazel runfile of the actual plug executable")

func runCLI(t *testing.T, commands ...[]string) ([]json.RawMessage, string, error) {
	t.Helper()
	if *plugBinary == "" {
		t.Fatal("the cli_acceptance target must supply the actual plug executable")
	}
	var input bytes.Buffer
	for _, command := range commands {
		if err := json.NewEncoder(&input).Encode(command); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	path := *plugBinary
	if !filepath.IsAbs(path) {
		path = filepath.Join(os.Getenv("TEST_SRCDIR"), path)
	}
	cmd := exec.CommandContext(ctx, path, "--memory", "--project", "acceptance", "--agent", "acceptance-worker", "--json", "session")
	cmd.Stdin = &input
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	runErr := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("plug session did not terminate: %v", ctx.Err())
	}
	var results []json.RawMessage
	for _, line := range bytes.Split(bytes.TrimSpace(stdout.Bytes()), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		if _, err := acceptance.CanonicalJSON(line); err != nil {
			t.Fatalf("plug emitted invalid or duplicate-member JSON: %s: %v", line, err)
		}
		results = append(results, append(json.RawMessage(nil), line...))
	}
	return results, stderr.String(), runErr
}

func TestCLIRealMemorySession(t *testing.T) {
	closeKey := "00000000000000000000000000000005"
	results, stderr, err := runCLI(t,
		[]string{"ready"},
		[]string{"create", "--title", "original", "--idempotency-key", "00000000000000000000000000000001"},
		[]string{"ready"},
		[]string{"claim", "@last", "--expect", "@revision", "--idempotency-key", "00000000000000000000000000000002"},
		[]string{"update", "@last", "--title", "edited", "--expect", "@revision", "--idempotency-key", "00000000000000000000000000000003"},
		[]string{"comment", "@last", "--body", "  verbatim\ncomment\n", "--expect", "@revision", "--idempotency-key", "00000000000000000000000000000004"},
		[]string{"close", "@last", "--reason", "done with note", "--expect", "@revision", "--idempotency-key", closeKey},
		[]string{"get", "@last"},
		[]string{"history", "--as-of", "1", "--id", "@last"},
		[]string{"ready"},
		[]string{"get", "@last", "--as-of", "3"},
		[]string{"close", "@last", "--reason", "done with note", "--expect", "4", "--idempotency-key", closeKey},
	)
	if err != nil || stderr != "" {
		t.Fatalf("actual plug session failed: %v\nstderr: %s", err, stderr)
	}
	if len(results) != 12 {
		t.Fatalf("want one result per command, got %d", len(results))
	}
	assertPaths(t, results[0], map[string]any{"/items": []any{}})
	id := valueAt[string](t, results[1], "/results/0/ref/id")
	if id == "" {
		t.Fatal("actual Writer did not mint an ID")
	}
	assertPaths(t, results[2], map[string]any{"/items/0/id": id, "/items/0/status": "todo"})
	if count := len(valueAt[[]any](t, results[2], "/items")); count != 1 {
		t.Fatalf("ready page has %d items after one create", count)
	}
	for i, index := range []int{1, 3, 4, 5, 6} {
		assertPaths(t, results[index], map[string]any{"/revision": i + 1, "/results/0/ref/id": id, "/warnings": []any{}})
	}
	assertPaths(t, results[3], map[string]any{"/leaseTtlMs": 300000})
	if _, err := time.Parse(time.RFC3339Nano, valueAt[string](t, results[3], "/leaseExpiresAt")); err != nil {
		t.Fatalf("claim deadline is not a timestamp: %v", err)
	}
	assertPaths(t, results[7], map[string]any{"/bead/id": id, "/bead/fields/title": "edited", "/bead/status": "done", "/bead/revision": 5})
	assertPaths(t, results[8], map[string]any{"/items/0/id": id, "/items/0/fields/title": "original", "/items/0/status": "todo", "/items/0/revision": 1})
	assertPaths(t, results[9], map[string]any{"/items": []any{}})
	assertPaths(t, results[10], map[string]any{"/bead/id": id, "/bead/fields/title": "edited", "/bead/status": "in_progress", "/bead/revision": 3})
	if err := acceptance.EqualJSON(results[6], results[11]); err != nil {
		t.Fatalf("CLI did not return the stored close result on replay: %v", err)
	}
	t.Logf("actual plug process: 12 commands, five committed revisions, ID %s", id)
}

func TestCLIStaleCASIsTypedAndStopsSession(t *testing.T) {
	results, stderr, err := runCLI(t,
		[]string{"create", "--title", "original", "--idempotency-key", "00000000000000000000000000000011"},
		[]string{"update", "@last", "--title", "refused", "--expect", "0", "--idempotency-key", "00000000000000000000000000000012"},
		[]string{"get", "@last"},
	)
	var exit *exec.ExitError
	if !strings.Contains(stderr, "precondition_failed") || !strings.Contains(stderr, "session line 2") {
		t.Fatalf("stale CAS did not reach a typed RPC refusal: error %v, stderr %s", err, stderr)
	}
	if !errors.As(err, &exit) {
		t.Fatalf("want the actual executable's nonzero exit, got %T (%v)", err, err)
	}
	if len(results) != 1 {
		t.Fatalf("session continued past refusal or lost the committed create: %d results", len(results))
	}
	assertPaths(t, results[0], map[string]any{"/revision": 1})
}
