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
	return runPlugInDir(t, "", input, args...)
}

func runPlugInDir(t *testing.T, dir, input string, args ...string) (string, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, plugBinary(t), args...)
	if dir != "" {
		cmd.Dir = dir
	}
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

func TestExecutableCommandSucceedsWithDetachedStdin(t *testing.T) {
	// A socket can be inherited as fd 0, but opening /dev/stdin on Linux
	// cannot reopen it. A nil exec.Cmd.Stdin would use /dev/null instead.
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	syscall.CloseOnExec(fds[0])
	syscall.CloseOnExec(fds[1])
	defer syscall.Close(fds[1])
	input := os.NewFile(uintptr(fds[0]), "detached-stdin")
	defer input.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, plugBinary(t), "--memory", "--json", "ready")
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), "PLUG_SERVER=", "PLUG_TOKEN=")
	cmd.Stdin = input
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil || !strings.Contains(out.String(), `"items":[]`) {
		t.Fatalf("plug with detached stdin: %v\nstdout: %s\nstderr: %s", err, &out, &errOut)
	}
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
			read := make(chan string, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				scanner.Scan()
				read <- scanner.Text()
			}()
			if _, err := io.WriteString(stdin, "[\"ready\"]\n"); err != nil {
				_ = cmd.Process.Kill()
				<-done
				t.Fatal(err)
			}
			select {
			case result := <-read:
				if !strings.Contains(result, `"items":[]`) {
					_ = cmd.Process.Kill()
					<-done
					t.Fatalf("session did not execute the command from the pipe: %q", result)
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

func TestExecutablePersistentDefaultAndStopsSession(t *testing.T) {
	dir := t.TempDir()
	out, errOut, err := runPlugInDir(t, dir, "", "--json", "ready")
	if err != nil || errOut != "" || !strings.Contains(out, `"items":[]`) {
		t.Fatalf("implicit persistent storage failed: %q %q %v", out, errOut, err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".plug", "plug.db")); err != nil {
		t.Fatalf("implicit persistent storage did not create SQLite file: %v", err)
	}
}

func TestExecutableMemoryNamedLifecycleCommands(t *testing.T) {
	commands := `
["create","--title","Lifecycle","--idempotency-key","00000000000000000000000000000001"]
["defer","@last","--reason","waiting","--idempotency-key","00000000000000000000000000000002"]
["reopen","@last","--reason","retry","--idempotency-key","00000000000000000000000000000003"]
["claim","@last","--idempotency-key","00000000000000000000000000000004"]
["fail","@last","--reason","failed","--idempotency-key","00000000000000000000000000000005"]
["reopen","@last","--idempotency-key","00000000000000000000000000000006"]
["cancel","@last","--reason","canceled","--idempotency-key","00000000000000000000000000000007"]
["reopen","@last","--idempotency-key","00000000000000000000000000000008"]
["claim","@last","--idempotency-key","00000000000000000000000000000009"]
["close","@last","--reason","done","--idempotency-key","0000000000000000000000000000000a"]
["reopen","@last","--idempotency-key","0000000000000000000000000000000b"]
["fail","@last","--reason","terminal","--idempotency-key","0000000000000000000000000000000c"]
["fail","@last","--reason","terminal","--idempotency-key","0000000000000000000000000000000c"]
["get","@last"]
["history","--as-of","@revision","--id","@last"]
["ready"]
`
	out, errOut, err := runPlug(t, commands, "--memory", "--json", "session")
	if err != nil || !strings.Contains(errOut, "memory storage is disposable") {
		t.Fatalf("named lifecycle session failed: %v\nstdout: %s\nstderr: %s", err, out, errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 16 {
		t.Fatalf("wanted one result per lifecycle command, got %d: %s", len(lines), out)
	}
	if lines[11] != lines[12] {
		t.Fatalf("same keyed fail did not replay identical CLI result:\nfirst=%s\nreplay=%s", lines[11], lines[12])
	}
	get, err := wire.DecodeJSON([]byte(lines[13]))
	if err != nil {
		t.Fatal(err)
	}
	bead := get.(map[string]any)["bead"].(map[string]any)
	if bead["status"] != "failed" {
		t.Fatalf("final CLI lifecycle status=%v", bead["status"])
	}
	history, err := wire.DecodeJSON([]byte(lines[14]))
	if err != nil {
		t.Fatal(err)
	}
	items := history.(map[string]any)["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["status"] != "failed" {
		t.Fatalf("CLI history lost final lifecycle image: %s", lines[14])
	}
	ready, err := wire.DecodeJSON([]byte(lines[15]))
	if err != nil {
		t.Fatal(err)
	}
	if len(ready.(map[string]any)["items"].([]any)) != 0 {
		t.Fatalf("failed bead remained ready: %s", lines[15])
	}
}

func TestExecutableSessionRefusalStopsSession(t *testing.T) {
	input := "[\"create\",\"--title\",\"First\"]\n[\"get\",\"pl-missing\"]\n[\"create\",\"--title\",\"Must not run\"]\n"
	out, errOut, err := runPlugInDir(t, t.TempDir(), input, "--memory", "--json", "session")
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
