// Package testutil runs a pinned local Dolt process for disposable native tests.
package testutil

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/testutil/bazeltest"
	_ "github.com/go-sql-driver/mysql"
)

// IsolatedDoltContainer retains the original control fixture API. The process
// uses the declared pinned runfile and never Docker or a host Dolt executable.
type IsolatedDoltContainer struct {
	Port      string
	directory string
	cmd       *exec.Cmd
	done      chan error
	stop      sync.Once
}

func StartIsolatedDoltContainerHandle(t *testing.T) *IsolatedDoltContainer {
	t.Helper()
	binary, err := bazeltest.RunfileEnv("BEADS_TEST_DOLT_BINARY")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp("/tmp", "native-root-control-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	store := filepath.Join(directory, "beads_test")
	if err := os.Mkdir(store, 0700); err != nil {
		t.Fatal(err)
	}
	// The CLI otherwise starts an unawaited send-metrics child on server exit.
	// Disable that flush so cmd.Wait also bounds writes to the disposable home.
	env := append(os.Environ(), "HOME="+directory, "DOLT_ROOT_PATH="+directory, "DOLT_METRICS_DISABLED=1", "DOLT_DISABLE_EVENT_FLUSH=1")
	init := exec.Command(binary, "init", "--name", "Public source", "--email", "source@example.test")
	init.Dir, init.Env = store, env
	if data, err := init.CombinedOutput(); err != nil {
		t.Fatalf("native init: %v: %s", err, data)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "sql-server", "--host", "127.0.0.1", "--port", port, "--data-dir", directory)
	cmd.Dir, cmd.Env = directory, env
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	server := &IsolatedDoltContainer{Port: port, directory: directory, cmd: cmd, done: make(chan error, 1)}
	go func() { server.done <- cmd.Wait() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:"+port+")/beads_test?timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if err := db.PingContext(ctx); err == nil {
			return server
		}
		select {
		case err := <-server.done:
			t.Fatalf("native server exited: %v: %s", err, output.String())
		case <-ctx.Done():
			t.Fatalf("native server startup: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s *IsolatedDoltContainer) Stop(ctx context.Context) (result error) {
	s.stop.Do(func() {
		if err := s.cmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			result = err
			return
		}
		select {
		case <-s.done:
		case <-ctx.Done():
			result = errors.Join(ctx.Err(), s.cmd.Process.Kill())
			<-s.done
		}
	})
	return result
}

func (s *IsolatedDoltContainer) Exec(_ context.Context, args []string) (int, string, error) {
	if len(args) != 1 || args[0] != "pwd" {
		return 1, "", errors.New("unsupported native fixture command")
	}
	return 0, s.directory + "\n", nil
}
