package b09native

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// The pinned embedded lifecycle caps its generator at eight. The same exact
// production bd exposes the configured domain allocator through bd serve over
// a SQL server. Reuse the existing pinned, owned Dolt runtime without changing
// its ChildEnv, trusted preparation, source archive or build configuration.
func (r *recorder) styleADomainWorkspace(name string) workspace {
	r.t.Helper()
	server := publicnative.StartDolt(r.t, r.root)
	w := r.newWorkspace(name)
	for i, entry := range w.Env {
		if strings.HasPrefix(entry, "BEADS_ACTOR=") {
			w.Env[i] = "BEADS_ACTOR=" + styleAActor
		}
	}
	w.Env = server.ChildEnv(w.Env)
	w.Data = server.DataRoot
	r.run(w, name+"-git-init", "/usr/bin/git", []string{"init", "--quiet", "--template="}, 0)
	r.run(w, name+"-init", r.binary, []string{"init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b09_domain", "--prefix", "b09", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet"}, 0)
	server.RequireDatabaseDir(r.t, "b09_domain")
	r.writeNativeConfig(w)
	return w
}

// Each public mutation owns a fresh bd serve process. Stop and reap it before
// the caller captures the source, including when an assertion fails. Original
// process streams and HTTP wire bytes remain separate, hash-bound evidence.
func (r *recorder) styleADomainCreate(w workspace, label, title string) []byte {
	r.t.Helper()
	root := filepath.Join(r.root, "domain-http", label)
	if err := os.MkdirAll(root, 0700); err != nil {
		r.t.Fatal(err)
	}
	outPath, errPath := filepath.Join(root, "serve.stdout"), filepath.Join(root, "serve.stderr")
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		r.t.Fatal(err)
	}
	errFile, err := os.OpenFile(errPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		_ = out.Close()
		r.t.Fatal(err)
	}
	cmd := exec.Command(r.binary, "serve", "--addr", "127.0.0.1:0")
	cmd.Dir, cmd.Env = w.Dir, append([]string(nil), w.Env...)
	cmd.Stdout, cmd.Stderr = out, errFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	started := time.Now().UTC()
	if err := cmd.Start(); err != nil {
		_ = out.Close()
		_ = errFile.Close()
		r.t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				r.t.Error("native HTTP server exceeded graceful shutdown budget")
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					r.writeJSON(filepath.Join(root, "serve-process.json"), map[string]any{"argv": cmd.Args, "directory": w.Dir, "pid": cmd.Process.Pid, "reaped": false})
					r.t.Error("native HTTP server did not reap after SIGKILL")
					return
				}
			}
			if err := errors.Join(out.Close(), errFile.Close()); err != nil {
				r.t.Error(err)
			}
			stdout, outErr := os.ReadFile(outPath)
			stderr, errErr := os.ReadFile(errPath)
			if err := errors.Join(outErr, errErr); err != nil {
				r.t.Error(err)
			}
			exit := 0
			if waitErr != nil {
				exit = -2
				var childErr *exec.ExitError
				if errors.As(waitErr, &childErr) {
					exit = childErr.ExitCode()
				}
				r.t.Errorf("native HTTP server shutdown: %v", waitErr)
			}
			typ, result := typedReturn(stdout)
			errType, errResult := typedReturn(stderr)
			r.commands = append(r.commands, commandReceipt{Label: label + "-serve", Argv: cmd.Args, Directory: w.Dir, Environment: append([]string(nil), w.Env...), Exit: exit, StdoutPath: outPath, StderrPath: errPath, StdoutSHA256: hashBytes(stdout), StderrSHA256: hashBytes(stderr), ResultType: typ, Result: result, ErrorType: errType, ErrorResult: errResult})
			r.writeJSON(filepath.Join(r.root, "commands.json"), r.commands)
			r.writeJSON(filepath.Join(root, "serve-process.json"), map[string]any{"argv": cmd.Args, "directory": w.Dir, "environment": w.Env, "pid": cmd.Process.Pid, "started_utc": started, "ended_utc": time.Now().UTC(), "exit": exit, "reaped": true, "stdout_sha256": hashBytes(stdout), "stderr_sha256": hashBytes(stderr)})
		})
	}
	r.t.Cleanup(stop)
	defer stop()
	var address string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(outPath)
		if err != nil {
			r.t.Fatal(err)
		}
		if line, _, ok := strings.Cut(string(data), "\n"); ok {
			var valid bool
			address, valid = strings.CutPrefix(line, "bd serve: listening on http://")
			host, port, err := net.SplitHostPort(address)
			portNumber, numberErr := strconv.Atoi(port)
			if !valid || err != nil || host != "127.0.0.1" || numberErr != nil || portNumber < 1 || portNumber > 65535 {
				r.t.Fatalf("unexpected native HTTP listener: %q", line)
			}
			break
		}
		select {
		case <-done:
			r.t.Fatalf("native HTTP server exited before readiness: %v", waitErr)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if address == "" {
		r.t.Fatal("native HTTP server did not announce its listener")
	}
	body, err := json.Marshal(map[string]any{"title": title, "description": "Actual native domain tuple", "issue_type": "task", "actor": styleAActor})
	if err != nil {
		r.t.Fatal(err)
	}
	url := "http://" + address + "/v0/beads/issues"
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		r.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	response, err := client.Do(request)
	if err != nil {
		r.writeJSON(filepath.Join(root, "http-transport-error.json"), map[string]any{"url": url, "request": string(body), "error": err.Error()})
		r.t.Fatal(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<20+1))
	closeErr := response.Body.Close()
	for name, value := range map[string][]byte{"http.request": body, "http.response": data} {
		if err := os.WriteFile(filepath.Join(root, name), value, 0600); err != nil {
			r.t.Fatal(err)
		}
	}
	r.writeJSON(filepath.Join(root, "http.json"), map[string]any{"method": http.MethodPost, "url": url, "request_headers": request.Header, "response_headers": response.Header, "status": response.StatusCode, "request_sha256": hashBytes(body), "response_sha256": hashBytes(data), "read_error": fmt.Sprint(readErr), "close_error": fmt.Sprint(closeErr)})
	if readErr != nil || closeErr != nil || len(data) > 4<<20 || response.StatusCode != http.StatusOK {
		r.t.Fatalf("native HTTP create: status=%d read=%v close=%v response=%s", response.StatusCode, readErr, closeErr, data)
	}
	return data
}
