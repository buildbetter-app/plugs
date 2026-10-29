package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

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

func runPlug(t *testing.T, input string, args ...string) (string, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, plugBinary(t), args...)
	cmd.Env = append(os.Environ(), "PLUG_SERVER=", "PLUG_TOKEN=")
	cmd.Stdin = strings.NewReader(input)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("executable timed out: %v; %s", ctx.Err(), errOut.String())
	}
	return out.String(), errOut.String(), err
}

func TestExecutableIdleSessionExitsOnSignalBeforeStdinEOF(t *testing.T) {
	for _, signal := range []os.Signal{syscall.SIGTERM, os.Interrupt} {
		t.Run(signal.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, plugBinary(t), "--memory", "--json", "session")
			cmd.Env = append(os.Environ(), "PLUG_SERVER=", "PLUG_TOKEN=")
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer stdin.Close()
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var errOut bytes.Buffer
			cmd.Stderr = &errOut
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			read := make(chan bool, 1)
			go func() { read <- bufio.NewScanner(stdout).Scan() }()
			if _, err := io.WriteString(stdin, "[\"ready\"]\n"); err != nil {
				_ = cmd.Process.Kill()
				<-done
				t.Fatal(err)
			}
			select {
			case ok := <-read:
				if !ok {
					_ = cmd.Process.Kill()
					<-done
					t.Fatal("session did not return the first command result")
				}
			case <-time.After(2 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Fatal("session did not become idle")
			}
			if err := cmd.Process.Signal(signal); err != nil {
				_ = cmd.Process.Kill()
				<-done
				t.Fatal(err)
			}
			select {
			case err := <-done:
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(errOut.String(), "context canceled") {
					t.Fatalf("signal did not report cancellation: %v; %s", err, errOut.String())
				}
			case <-time.After(2 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Fatal("idle session ignored cancellation while stdin remained open")
			}
		})
	}
}

func TestExecutableMemoryLifecycle(t *testing.T) {
	commands := `
["ready"]
["create","--title","Original"]
["ready"]
["claim","@last"]
["get","@last"]
["update","@last","--title","Updated"]
["comment","@last","--body","Evidence"]
["close","@last","--reason","Done"]
["history","--as-of","@revision","--id","@last"]
["get","@last","--as-of","1"]
["ready"]
`
	out, errOut, err := runPlug(t, commands, "--memory", "--json", "session")
	if err != nil {
		t.Fatalf("plug failed: %v\nstdout: %s\nstderr: %s", err, out, errOut)
	}
	if !strings.Contains(errOut, "memory storage is disposable") {
		t.Fatalf("disposable storage warning missing: %s", errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 11 {
		t.Fatalf("wanted eleven results, got %d: %s", len(lines), out)
	}
	results := make([]map[string]any, len(lines))
	for i, line := range lines {
		v, err := wire.DecodeJSON([]byte(line))
		if err != nil {
			t.Fatalf("result %d: %v", i, err)
		}
		var ok bool
		results[i], ok = v.(map[string]any)
		if !ok {
			t.Fatalf("result %d is not an object", i)
		}
	}
	if len(results[0]["items"].([]any)) != 0 || len(results[2]["items"].([]any)) != 1 || len(results[10]["items"].([]any)) != 0 {
		t.Fatalf("readiness did not follow lifecycle: %s", out)
	}
	for _, pair := range [][2]int{{1, 1}, {3, 2}, {5, 3}, {6, 4}, {7, 5}} {
		if results[pair[0]]["revision"].(json.Number).String() != strconv.Itoa(pair[1]) {
			t.Fatalf("write revision: %v", results[pair[0]])
		}
	}
	lease := results[4]["lease"].(map[string]any)
	if lease["ttlMs"].(json.Number).String() != "300000" || lease["holder"] != "anonymous" {
		t.Fatalf("real persisted lease missing: %v", lease)
	}
	historical := results[8]["items"].([]any)
	if len(historical) != 1 || historical[0].(map[string]any)["fields"].(map[string]any)["title"] != "Updated" {
		t.Fatalf("history did not read committed final state: %s", lines[8])
	}
	if results[9]["bead"].(map[string]any)["fields"].(map[string]any)["title"] != "Original" {
		t.Fatalf("historical get returned current state: %s", lines[9])
	}
	// A new invocation opens a new disposable store, proving the lifetime is
	// explicit even when every command uses the same project and agent label.
	fresh, _, err := runPlug(t, "", "--memory", "--json", "ready")
	if err != nil || !strings.Contains(fresh, `"items":[]`) {
		t.Fatalf("fresh process state: %s %v", fresh, err)
	}
}

func TestExecutableRefusesUnsupportedAndStopsSession(t *testing.T) {
	out, errOut, err := runPlug(t, "", "ready")
	if err == nil || out != "" || !strings.Contains(errOut, "persistent storage is not installed") {
		t.Fatalf("implicit storage fallback: %q %q %v", out, errOut, err)
	}
	input := "[\"create\",\"--title\",\"First\"]\n[\"get\",\"pl-missing\"]\n[\"create\",\"--title\",\"Must not run\"]\n"
	out, errOut, err = runPlug(t, input, "--memory", "--json", "session")
	if err == nil || len(strings.Split(strings.TrimSpace(out), "\n")) != 1 || !strings.Contains(errOut, "not_found") || !strings.Contains(errOut, "session line 2") {
		t.Fatalf("session refusal: %q %q %v", out, errOut, err)
	}
}

func TestExecutableNoopAndClaimReplayPreserveHistoryAndLease(t *testing.T) {
	claimKey := strings.Repeat("c", 64)
	commands := [][]string{
		{"create", "--title", "Stable"},
		{"update", "@last", "--title", "Stable"},
		{"claim", "@last"},
		{"get", "@last"},
		{"claim", "@last", "--idempotency-key", claimKey},
		{"get", "@last"},
		{"comment", "@last", "--body", "Later write"},
		{"claim", "@last", "--idempotency-key", claimKey},
		{"get", "@last"},
		{"history", "--as-of", "3", "--id", "@last"},
	}
	var input strings.Builder
	for _, command := range commands {
		raw, err := json.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		input.Write(raw)
		input.WriteByte('\n')
	}
	out, errOut, err := runPlug(t, input.String(), "--memory", "--json", "session")
	if err != nil {
		t.Fatalf("plug failed: %v\nstdout: %s\nstderr: %s", err, out, errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != len(commands) {
		t.Fatalf("wanted %d results: %s", len(commands), out)
	}
	results := make([]map[string]any, len(lines))
	for i, line := range lines {
		decoded, err := wire.DecodeJSON([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		results[i] = decoded.(map[string]any)
	}
	for _, pair := range [][2]int{{0, 1}, {1, 1}, {2, 2}, {4, 2}, {6, 3}, {7, 2}} {
		if results[pair[0]]["revision"] != json.Number(strconv.Itoa(pair[1])) {
			t.Fatalf("no-op or replay minted a revision: %s", lines[pair[0]])
		}
	}
	for _, i := range []int{4, 7} {
		if results[i]["leaseTtlMs"] != results[2]["leaseTtlMs"] || results[i]["leaseExpiresAt"] != results[2]["leaseExpiresAt"] {
			t.Fatalf("claim changed original TTL or expiry: %s", lines[i])
		}
	}
	lease, _ := json.Marshal(results[3]["lease"])
	for _, i := range []int{5, 8} {
		got, _ := json.Marshal(results[i]["lease"])
		if !bytes.Equal(got, lease) {
			t.Fatalf("durable lease changed after no-op/replay: before %s after %s", lease, got)
		}
	}
	bead := results[8]["bead"].(map[string]any)
	history := results[9]["items"].([]any)
	if bead["revision"] != json.Number("3") || len(history) != 1 || history[0].(map[string]any)["revision"] != json.Number("3") {
		t.Fatalf("no-op/replay changed committed history: %s", out)
	}
}
