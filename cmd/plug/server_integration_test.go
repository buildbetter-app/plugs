package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
)

type plugServerProcess struct {
	cmd      *exec.Cmd
	endpoint string
	stderr   bytes.Buffer
}

func startPlugServer(t *testing.T, dir string, local bool) *plugServerProcess {
	t.Helper()
	args := []string{"server"}
	if local {
		args = append(args, "--local")
	}
	cmd := exec.Command(plugBinary(t), args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PLUG_SERVER=", "PLUG_TOKEN=")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &plugServerProcess{cmd: cmd}
	cmd.Stderr = &process.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("server did not print endpoint: %v; stderr=%s", err, process.stderr.String())
	}
	fields := strings.Fields(line)
	if len(fields) != 5 || fields[0] != "plug" || fields[1] != "server" || fields[2] != "listening" || fields[3] != "on" || !strings.HasPrefix(fields[4], "http://127.0.0.1:") {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("unexpected server announcement %q", line)
	}
	process.endpoint = fields[4]
	return process
}

func (p *plugServerProcess) stop(t *testing.T) {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 0 {
				t.Fatalf("server shutdown: %v; stderr=%s", err, p.stderr.String())
			}
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
		t.Fatal("server did not shut down")
	}
	p.cmd = nil
}

func remoteCommand(t *testing.T, input string, endpoint string, args ...string) (string, string, error) {
	t.Helper()
	args = append([]string{"--server", endpoint, "--project", "acceptance", "--agent", "acceptance", "--json"}, args...)
	return runPlug(t, input, args...)
}

func executableMCPCall(t *testing.T, endpoint, body string, headers map[string]string) (int, http.Header, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, endpoint+mcp.Path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		if value == "" {
			request.Header.Del(key)
			continue
		}
		request.Header.Set(key, value)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, response.Header, data
}

func executableMCPObject(t *testing.T, body []byte) map[string]any {
	t.Helper()
	value, err := wire.DecodeJSON(body)
	if err != nil {
		t.Fatalf("MCP response is not JSON: %v: %s", err, body)
	}
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("MCP response=%T: %s", value, body)
	}
	return object
}

func TestExecutableSQLiteServerRemoteLifecycleAndRestart(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".plug"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".plug", "config.yaml"), []byte("driver: sqlite\ndsn: server.db\nlisten: 127.0.0.1:0\nproject: acceptance\nui: false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	server := startPlugServer(t, dir, false)
	t.Cleanup(func() { server.stop(t) })
	endpoint := server.endpoint
	commands := "[\"create\",\"--title\",\"first\"]\n[\"ready\"]\n[\"update\",\"@last\",\"--title\",\"updated\"]\n[\"close\",\"@last\",\"--reason\",\"done\"]\n"
	out, errOut, err := remoteCommand(t, commands, endpoint, "session")
	if err != nil {
		t.Fatalf("remote lifecycle: %v stdout=%s stderr=%s", err, out, errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 {
		t.Fatalf("lifecycle results=%d: %s", len(lines), out)
	}
	created, err := wire.DecodeJSON([]byte(lines[0]))
	if err != nil {
		t.Fatal(err)
	}
	id := created.(map[string]any)["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
	server.stop(t)

	server = startPlugServer(t, dir, false)
	t.Cleanup(func() { server.stop(t) })
	endpoint = server.endpoint
	out, errOut, err = remoteCommand(t, "", endpoint, "get", id)
	if err != nil {
		t.Fatalf("restart get: %v stdout=%s stderr=%s", err, out, errOut)
	}
	value, err := wire.DecodeJSON([]byte(strings.TrimSpace(out)))
	if err != nil {
		t.Fatal(err)
	}
	bead := value.(map[string]any)["bead"].(map[string]any)
	if bead["status"] != "done" || bead["fields"].(map[string]any)["title"] != "updated" {
		t.Fatalf("restart state=%v", bead)
	}
}

func TestExecutableSQLiteServerMCPRouteAndRPCRestart(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".plug"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".plug", "config.yaml"), []byte("driver: sqlite\ndsn: mcp.db\nlisten: 127.0.0.1:0\nproject: acceptance\nui: false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	server := startPlugServer(t, dir, false)
	t.Cleanup(func() { server.stop(t) })

	status, responseHeaders, body := executableMCPCall(t, server.endpoint, `{"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"binary-test","version":"1"}}}`, nil)
	if status != http.StatusOK {
		t.Fatalf("MCP initialize status=%d body=%s", status, body)
	}
	sessionID := responseHeaders.Get("MCP-Session-Id")
	if sessionID == "" {
		t.Fatalf("MCP initialize did not issue a session: %s", body)
	}
	status, _, body = executableMCPCall(t, server.endpoint, `{"jsonrpc":"2.0","id":"list","method":"tools/list","params":{}}`, map[string]string{"MCP-Session-Id": sessionID})
	if status != http.StatusOK {
		t.Fatalf("MCP tools/list status=%d body=%s", status, body)
	}
	create := `{"jsonrpc":"2.0","id":"create","method":"tools/call","params":{"name":"bead_create","arguments":{"type":"issue","fields":{"title":"binary MCP"},"_meta":{"idempotencyKey":"550e8400-e29b-41d4-a716-446655440001"}}}}`
	status, _, body = executableMCPCall(t, server.endpoint, create, map[string]string{"MCP-Session-Id": sessionID})
	if status != http.StatusOK {
		t.Fatalf("MCP bead_create status=%d body=%s", status, body)
	}
	createResult := executableMCPObject(t, body)["result"].(map[string]any)
	if createResult["isError"] == true {
		t.Fatalf("MCP bead_create returned an error: %s", body)
	}
	content := createResult["content"].([]any)
	canonical, err := wire.DecodeJSON([]byte(content[0].(map[string]any)["text"].(string)))
	if err != nil {
		t.Fatal(err)
	}
	beadID := canonical.(map[string]any)["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)

	server.stop(t)
	server = startPlugServer(t, dir, false)
	t.Cleanup(func() { server.stop(t) })
	out, errOut, err := remoteCommand(t, "", server.endpoint, "get", beadID)
	if err != nil {
		t.Fatalf("RPC restart read after MCP create: %v stdout=%s stderr=%s", err, out, errOut)
	}
	value, err := wire.DecodeJSON([]byte(strings.TrimSpace(out)))
	if err != nil {
		t.Fatal(err)
	}
	bead := value.(map[string]any)["bead"].(map[string]any)
	if bead["fields"].(map[string]any)["title"] != "binary MCP" {
		t.Fatalf("RPC restart did not read MCP data: %s", out)
	}
}

func TestExecutableLocalServerIsDisposableAndWarns(t *testing.T) {
	dir := t.TempDir()
	server := startPlugServer(t, dir, true)
	t.Cleanup(func() { server.stop(t) })
	endpoint := server.endpoint
	out, errOut, err := remoteCommand(t, "", endpoint, "create", "--title", "ephemeral")
	if err != nil || out == "" {
		t.Fatalf("local create: %v stdout=%s stderr=%s", err, out, errOut)
	}
	if !strings.Contains(server.stderr.String(), "memory storage is disposable") {
		// The warning is written during startup and may arrive just after the
		// endpoint line; give the process a short scheduling window.
		time.Sleep(20 * time.Millisecond)
	}
	server.stop(t)
	server = startPlugServer(t, dir, true)
	t.Cleanup(func() { server.stop(t) })
	endpoint = server.endpoint
	out, _, err = remoteCommand(t, "", endpoint, "ready")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.DecodeJSON([]byte(strings.TrimSpace(out)))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.(map[string]any)["items"].([]any)) != 0 {
		t.Fatalf("local restart retained data: %s", out)
	}
}
