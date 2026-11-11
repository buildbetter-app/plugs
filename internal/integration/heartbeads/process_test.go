package heartbeads

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/buildbetter-app/plugs/internal/api/wire"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
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
	cmd.Env = append(os.Environ(), "PLUG_SERVER=", "PLUG_TOKEN=", "PATH=/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
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

func i8FixtureRPC(t *testing.T, endpoint, project, method string, params map[string]any, key string) sqliteRestartResponse {
	t.Helper()
	p := make(map[string]any, len(params)+1)
	for name, value := range params {
		p[name] = value
	}
	if method != "server.hello" {
		meta := map[string]any{"project": project, "agent": "i8-heartbeads-fixture"}
		if key != "" {
			meta["idempotencyKey"] = key
		}
		p["_meta"] = meta
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": p})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/rpc/v1", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s HTTP: %v", method, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status=%d body=%s err=%v", method, resp.StatusCode, raw, err)
	}
	var out sqliteRestartResponse
	if err := wire.DecodeInto(raw, &out); err != nil || out.JSONRPC != "2.0" || out.ID != 1 {
		t.Fatalf("%s response=%s err=%v", method, raw, err)
	}
	return out
}

type sqliteRestartResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *wire.Error     `json:"error,omitempty"`
	Meta    struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta,omitempty"`
}
