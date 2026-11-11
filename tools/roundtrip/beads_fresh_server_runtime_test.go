package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// This owns a native SQL process against a COPY of the recipe's retained
// clean root. It has no Plug lifecycle or import implementation.
type freshNativeServer struct {
	db             *sql.DB
	port           int
	cmd            *exec.Cmd
	done           chan struct{}
	waitErr        error
	log            *os.File
	stopOnce       sync.Once
	stopErr        error
	root, evidence string
}

func freshCopyTree(t *testing.T, source, destination string) {
	t.Helper()
	if err := os.Mkdir(destination, 0700); err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		if info.IsDir() {
			return os.Mkdir(target, 0700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular retained native source: %s", rel)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return writePrivate(filepath.Dir(target), filepath.Base(target), data)
	}); err != nil {
		t.Fatal(err)
	}
}

func freshPinnedDolt(t *testing.T) string {
	t.Helper()
	path := os.Getenv("PLUG_PINNED_DOLT_BINARY")
	if !filepath.IsAbs(path) {
		path = filepath.Join(os.Getenv("TEST_SRCDIR"), path)
	}
	path, err := filepath.EvalSymlinks(path)
	if err != nil || freshFileSHA(t, path) != "72352f1d882472a7a8d6124e9c4dc12f80e2c2359ea95f1424c032e9250e35e6" {
		t.Fatal("pinned public Dolt member differs")
	}
	return path
}

func freshStartNativeServer(t *testing.T, root, evidence, workspace string) *freshNativeServer {
	t.Helper()
	for _, name := range []string{"dolt-root", "dolt-root/.dolt", "server-home", "server-tmp"} {
		if err := os.Mkdir(filepath.Join(evidence, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	freshJSON(t, filepath.Join(evidence, "dolt-root/.dolt/config_global.json"), map[string]string{"user.name": "Public roundtrip", "user.email": "public@example.invalid", "metrics.disabled": "true", "versioncheck.disabled": "true"})
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reservation.Addr().(*net.TCPAddr).Port
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	log, err := os.OpenFile(filepath.Join(evidence, "native-server.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	binary := freshPinnedDolt(t)
	args := []string{"sql-server", "-H", "127.0.0.1", "-P", strconv.Itoa(port), "--loglevel=warning"}
	cmd := exec.Command(binary, args...)
	env := freshChildEnv(workspace)
	env = append(env, "DOLT_ROOT_PATH="+filepath.Join(evidence, "dolt-root"), "DOLT_DISABLE_EVENT_FLUSH=1", "TMPDIR="+filepath.Join(evidence, "server-tmp"), "TZ=UTC", "LC_ALL=C", "NO_COLOR=1")
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = root, env, log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	s := &freshNativeServer{port: port, cmd: cmd, done: make(chan struct{}), log: log, root: root, evidence: evidence}
	go func() { s.waitErr = cmd.Wait(); close(s.done) }()
	t.Cleanup(func() {
		if err := s.stop(t); err != nil {
			t.Errorf("native owned server cleanup: %v", err)
		}
	})
	freshJSON(t, filepath.Join(evidence, "native-server-start.json"), map[string]any{"executable": binary, "sha256": freshFileSHA(t, binary), "args": args, "cwd": root, "environment": env, "pid": cmd.Process.Pid, "port": port, "started": time.Now().UTC()})
	config := mysql.NewConfig()
	config.User = "root"
	config.Net = "tcp"
	config.Addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	config.DBName = "beads_test"
	config.Timeout = time.Second
	config.ReadTimeout = 15 * time.Second
	config.WriteTimeout = 15 * time.Second
	connector, err := mysql.NewConnector(config)
	if err != nil {
		t.Fatal(err)
	}
	s.db = sql.OpenDB(connector)
	s.db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	for {
		select {
		case <-s.done:
			t.Fatalf("owned native SQL server exited: %v", s.waitErr)
		default:
		}
		if s.db.PingContext(ctx) == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("owned native SQL server readiness expired")
		}
		time.Sleep(100 * time.Millisecond)
	}
	cwd, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(cmd.Process.Pid), "cwd"))
	if err != nil || cwd != root || !freshOwnsListener(cmd.Process.Pid, port) {
		t.Fatal("native SQL listener/process/root ownership differs")
	}
	freshJSON(t, filepath.Join(evidence, "native-server-ready.json"), map[string]any{"pid": cmd.Process.Pid, "cwd": cwd, "port": port, "owned_proc_listener": true, "ready": time.Now().UTC()})
	return s
}

func freshOwnsListener(pid, port int) bool {
	data, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		return false
	}
	want := fmt.Sprintf("0100007F:%04X", port)
	inodes := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		columns := strings.Fields(line)
		if len(columns) > 9 && columns[1] == want && columns[3] == "0A" {
			inodes[columns[9]] = true
		}
	}
	fds, err := os.ReadDir(filepath.Join("/proc", strconv.Itoa(pid), "fd"))
	if err != nil {
		return false
	}
	for _, fd := range fds {
		link, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "fd", fd.Name()))
		if err == nil && strings.HasPrefix(link, "socket:[") && inodes[strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")] {
			return true
		}
	}
	return false
}

func (s *freshNativeServer) stop(t *testing.T) error {
	s.stopOnce.Do(func() {
		if s.db != nil {
			s.stopErr = s.db.Close()
		}
		select {
		case <-s.done:
			s.stopErr = errors.Join(s.stopErr, fmt.Errorf("native server exited before owned stop: %v", s.waitErr))
		default:
			signalErr := s.cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-s.done:
			case <-time.After(20 * time.Second):
				_ = s.cmd.Process.Kill()
				<-s.done
				s.stopErr = errors.Join(s.stopErr, fmt.Errorf("native SQL server shutdown deadline"))
			}
			s.stopErr = errors.Join(s.stopErr, signalErr)
			if s.waitErr != nil {
				var exit *exec.ExitError
				if !errors.As(s.waitErr, &exit) || !exit.Sys().(syscall.WaitStatus).Signaled() || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
					s.stopErr = errors.Join(s.stopErr, s.waitErr)
				}
			}
		}
		s.stopErr = errors.Join(s.stopErr, s.log.Close())
		freshJSON(t, filepath.Join(s.evidence, "native-server-stop.json"), map[string]any{"pid": s.cmd.Process.Pid, "error": fmt.Sprint(s.stopErr), "wait_error": fmt.Sprint(s.waitErr), "stopped": time.Now().UTC(), "root": s.root})
	})
	return s.stopErr
}
