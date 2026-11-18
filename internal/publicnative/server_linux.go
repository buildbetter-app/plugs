//go:build linux

// Package publicnative owns a disposable, pinned Dolt server for public-native
// RunsOn tests. It does not discover or adopt a server from the runner.
package publicnative

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
)

const (
	doltVersion = "2.2.0"
	// Extracted regular member of the pinned 1f7ad8c2... release archive.
	doltBinarySHA256 = "72352f1d882472a7a8d6124e9c4dc12f80e2c2359ea95f1424c032e9250e35e6"
	startupTimeout   = 60 * time.Second
)

// Server exposes the actual owned listener and physical database root. A
// Beads child must use ChildEnv and the returned Port in --server-port.
type Server struct {
	Port       int
	PID        int
	DataRoot   string
	Root       string
	DoltBinary string
	doltRoot   string

	cmd        *exec.Cmd
	done       chan struct{}
	waitErr    error
	logErr     error
	receipt    string
	evidence   string
	logPath    string
	configPath string
	stopOnce   sync.Once
	stopErr    error
	proxyOnce  sync.Once
	proxyErr   error
	drainProxy func() error
	reapDolt   func() error
}

// RegisterBeadsCleanup registers direct cleanup for a disposable external
// Beads workspace. The cleanup is registered after
// StartDolt, so it runs first (LIFO) and stops the Beads proxy/backend before
// the owned Dolt server is reaped. The command and workspace are caller-owned
// and are matched again in /proc to prove no proxy for this root survived.
func (s *Server) RegisterBeadsCleanup(t testing.TB, binary, dir string, env []string) {
	t.Helper()
	_ = binary
	_ = env
	t.Cleanup(func() {
		root := ""
		for _, e := range env {
			if strings.HasPrefix(e, "BEADS_DOLT_DATA_DIR=") {
				root = strings.TrimPrefix(e, "BEADS_DOLT_DATA_DIR=")
			}
		}
		if root == "" {
			t.Errorf("BEADS_DOLT_DATA_DIR is required for proxy ownership")
			return
		}
		pids, scanErr := beadsProxyPIDs(root)
		if scanErr != nil {
			t.Errorf("scanning Beads proxy processes for %s: %v", dir, scanErr)
		}
		for _, pid := range pids {
			matches, err := beadsProxyPIDMatches(pid, root)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
					continue
				}
				t.Errorf("rechecking Beads proxy pid %d for %s: %v", pid, dir, err)
				continue
			}
			if !matches {
				continue
			}
			if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
				t.Errorf("stopping Beads proxy pid %d for %s: %v", pid, root, err)
			}
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			current, err := beadsProxyPIDs(root)
			if err != nil {
				t.Errorf("scanning Beads proxy processes for %s: %v", dir, err)
				break
			}
			if len(current) == 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		pids, scanErr = beadsProxyPIDs(root)
		if scanErr != nil {
			t.Errorf("final Beads proxy scan for %s: %v", dir, scanErr)
		} else if len(pids) != 0 {
			t.Errorf("Beads proxy survived direct termination for %s: pids=%v", dir, pids)
		}
	})
}

// RegisterDoltProxyCleanup covers external initializers that use StartDolt
// without a site-specific registration; all such proxies use DataRoot.
func (s *Server) RegisterDoltProxyCleanup(t testing.TB) {
	t.Helper()
	t.Cleanup(func() {
		pids, err := beadsProxyPIDs(s.DataRoot)
		if err != nil {
			t.Errorf("scanning Beads proxy processes for %s: %v", s.DataRoot, err)
			return
		}
		for _, pid := range pids {
			match, err := beadsProxyPIDMatches(pid, s.DataRoot)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
					continue
				}
				t.Errorf("rechecking Beads proxy pid %d: %v", pid, err)
				continue
			}
			if match {
				if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
					t.Errorf("stopping Beads proxy pid %d: %v", pid, err)
				}
			}
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			current, err := beadsProxyPIDs(s.DataRoot)
			if err != nil {
				t.Errorf("scanning Beads proxy processes: %v", err)
				return
			}
			if len(current) == 0 {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		current, err := beadsProxyPIDs(s.DataRoot)
		if err != nil {
			t.Errorf("final Beads proxy scan: %v", err)
		} else if len(current) > 0 {
			t.Errorf("Beads proxy survived direct termination: pids=%v", current)
		}
	})
}

func beadsProxyPIDs(root string) ([]int, error) { return beadsProxyPIDsIn("/proc", root) }

func beadsProxyPIDsIn(procDir, root string) ([]int, error) {
	entries, err := os.ReadDir(procDir)
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join(procDir, entry.Name(), "cmdline"))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			return nil, err
		}
		match, err := beadsProxyArgsMatchErr(string(cmdline), root)
		if err != nil {
			return nil, err
		}
		if match {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}

func beadsProxyArgsMatch(raw, root string) bool {
	match, _ := beadsProxyArgsMatchErr(raw, root)
	return match
}

func beadsProxyArgsMatchErr(raw, root string) (bool, error) {
	root, err := canonicalRoot(root)
	if err != nil {
		return false, err
	}
	args := strings.Split(raw, "\x00")
	if len(args) < 2 || args[1] != "db-proxy-child" {
		return false, nil
	}
	for j := 0; j < len(args); j++ {
		var value string
		if args[j] == "--root" && j+1 < len(args) {
			value = args[j+1]
		} else if strings.HasPrefix(args[j], "--root=") {
			value = strings.TrimPrefix(args[j], "--root=")
		}
		if value != "" {
			candidate, err := canonicalRoot(value)
			if err != nil {
				return false, err
			}
			if candidate == root {
				return true, nil
			}
		}
	}
	return false, nil
}

func beadsProxyPIDMatches(pid int, root string) (bool, error) {
	b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return false, err
	}
	return beadsProxyArgsMatchErr(string(b), root)
}

func canonicalRoot(root string) (string, error) {
	clean := filepath.Clean(root)
	if resolved, err := filepath.EvalSymlinks(clean); err == nil {
		return filepath.Clean(resolved), nil
	} else if errors.Is(err, os.ErrNotExist) {
		return clean, nil
	} else {
		return "", fmt.Errorf("resolving proxy root %q: %w", root, err)
	}
}

// StartDolt starts exactly one pinned Dolt 2.2.0 process in a fresh 0700
// directory under evidenceDir. The test owns its lifetime; cleanup records
// termination even after assertion failure. A collision or unreadable /proc
// ownership evidence fails closed, without selecting a second port.
func StartDolt(t testing.TB, evidenceDir string) *Server {
	t.Helper()
	s, err := startDolt(evidenceDir)
	if err != nil {
		t.Fatalf("starting owned public-native Dolt: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Stop(); err != nil {
			t.Errorf("stopping owned public-native Dolt: %v", err)
		}
		t.Logf("NATIVE_DOLT_CLEANUP receipt=%s pid=%d port=%d", s.receipt, s.PID, s.Port)
	})
	s.RegisterDoltProxyCleanup(t)
	t.Logf("NATIVE_DOLT_READY receipt=%s pid=%d port=%d data_root=%s", s.receipt, s.PID, s.Port, s.DataRoot)
	return s
}

func startDolt(evidenceDir string) (_ *Server, retErr error) {
	info, err := os.Lstat(evidenceDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("evidence directory must be a real directory: %q: %v", evidenceDir, err)
	}
	path := os.Getenv("PLUG_PINNED_DOLT_BINARY")
	if path == "" {
		return nil, errors.New("PLUG_PINNED_DOLT_BINARY must come from the pinned Bazel runfile")
	}
	if !filepath.IsAbs(path) {
		root := os.Getenv("TEST_SRCDIR")
		if root == "" {
			return nil, errors.New("TEST_SRCDIR is required for the pinned Dolt runfile")
		}
		path = filepath.Join(root, path)
	}
	if err := checkBinary(path); err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp(evidenceDir, "public-native-dolt-")
	if err != nil {
		return nil, err
	}
	started := false
	s := &Server{Root: root, receipt: filepath.Join(root, "server.jsonl"),
		logPath:    filepath.Join(root, "dolt.log"),
		configPath: filepath.Join(root, "dolt-root", ".dolt", "config_global.json"), evidence: evidenceDir}
	defer func() {
		if !started {
			if err := s.record("startup_failed", map[string]any{"error": fmt.Sprint(retErr), "root": root}); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("recording Dolt startup failure; private root retained at %s: %w", root, err))
				return
			}
			if err := s.retainFiles(); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("retaining Dolt startup evidence; private root retained at %s: %w", root, err))
				_ = s.record("retention_failed", map[string]any{"error": err.Error(), "root": root})
				return // Preserve the private root when evidence could not be retained.
			}
			removeErr := os.RemoveAll(root)
			if err := s.record("cleanup", map[string]any{"root": root,
				"root_removed": removeErr == nil, "started": false,
				"retained_log": s.logPath, "retained_config": s.configPath}); err != nil {
				retErr = errors.Join(retErr, err)
			}
			retErr = errors.Join(retErr, fmt.Errorf("Dolt startup evidence: %s", s.receipt), removeErr)
		}
	}()
	dataRoot := filepath.Join(root, "data")
	home := filepath.Join(root, "home")
	doltRoot := filepath.Join(root, "dolt-root")
	for _, dir := range []string{dataRoot, home, filepath.Join(root, "tmp"), doltRoot, filepath.Join(doltRoot, ".dolt")} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(s.configPath, []byte(`{"user.email":"test@beads.local","user.name":"beads-test","metrics.disabled":"true","versioncheck.disabled":"true"}`+"\n"), 0o600); err != nil {
		return nil, err
	}
	s.DataRoot, s.DoltBinary, s.doltRoot = dataRoot, path, doltRoot
	serverEnv := []string{
		"PATH=" + filepath.Dir(path) + ":/usr/bin:/bin",
		"HOME=" + home,
		"USERPROFILE=" + home,
		"XDG_CONFIG_HOME=" + filepath.Join(home, ".config"),
		"DOLT_ROOT_PATH=" + doltRoot,
		"DOLT_DISABLE_EVENT_FLUSH=1",
		"TMPDIR=" + filepath.Join(root, "tmp"),
		"TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "GIT_CONFIG_NOSYSTEM=1",
	}
	versionCtx, cancelVersion := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelVersion()
	version := exec.CommandContext(versionCtx, path, "version")
	version.WaitDelay = time.Second
	version.Dir, version.Env = dataRoot, serverEnv
	versionStarted := time.Now().UTC()
	versionOutput, err := version.CombinedOutput()
	versionExit := 0
	if err != nil {
		versionExit = -1
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			versionExit = exit.ExitCode()
		}
	}
	if recordErr := s.record("binary_version", map[string]any{"path": path, "sha256": doltBinarySHA256,
		"argv": []string{"dolt", "version"}, "stdout_stderr": string(versionOutput), "exit": versionExit,
		"started_utc": versionStarted.Format(time.RFC3339Nano), "ended_utc": time.Now().UTC().Format(time.RFC3339Nano),
		"config_path": s.configPath}); recordErr != nil {
		return nil, recordErr
	}
	if err != nil || versionCtx.Err() != nil || strings.TrimSpace(string(versionOutput)) != "dolt version "+doltVersion {
		return nil, fmt.Errorf("pinned Dolt version: output %q, exit %d, error %v, context %v", versionOutput, versionExit, err, versionCtx.Err())
	}
	if err := s.record("config", map[string]any{"path": s.configPath, "metrics_disabled": true, "versioncheck_disabled": true,
		"event_flush_disabled": true}); err != nil {
		return nil, err
	}
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s.Port = reservation.Addr().(*net.TCPAddr).Port
	if err := reservation.Close(); err != nil {
		return nil, err
	}
	args := []string{"sql-server", "-H", "127.0.0.1", "-P", strconv.Itoa(s.Port), "--loglevel=warning"}
	log, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	s.cmd = exec.Command(path, args...)
	s.cmd.Dir, s.cmd.Env = dataRoot, serverEnv
	s.cmd.Stdout, s.cmd.Stderr = log, log
	s.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := s.cmd.Start(); err != nil {
		_ = log.Close()
		return nil, err
	}
	s.PID = s.cmd.Process.Pid
	s.done = make(chan struct{})
	started = true
	go func() { s.waitErr = s.cmd.Wait(); s.logErr = log.Close(); close(s.done) }()
	if err := s.record("start", map[string]any{"argv": append([]string{"dolt"}, args...), "pid": s.PID, "port": s.Port, "data_root": dataRoot}); err != nil {
		return nil, errors.Join(err, s.Stop())
	}
	if err := s.awaitReady(); err != nil {
		recordErr := s.record("readiness_failed", map[string]any{"error": err.Error()})
		stopErr := s.Stop()
		return nil, errors.Join(fmt.Errorf("owned pid %d on port %d: %w; receipt %s", s.PID, s.Port, err, s.receipt), recordErr, stopErr)
	}
	actualCWD, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(s.PID), "cwd"))
	if err != nil || actualCWD != dataRoot {
		stopErr := s.Stop()
		return nil, errors.Join(fmt.Errorf("owned Dolt cwd=%q, want real data root %q: %v", actualCWD, dataRoot, err), stopErr)
	}
	if err := s.record("ready", map[string]any{"pid": s.PID, "port": s.Port, "data_root": dataRoot, "listener": "owned_proc_inode_and_mysql_handshake"}); err != nil {
		return nil, errors.Join(err, s.Stop())
	}
	return s, nil
}

func checkBinary(path string) error {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return fmt.Errorf("pinned Dolt member is not an executable regular file: %q: %v", path, err)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, f)
	if err := errors.Join(copyErr, f.Close()); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != doltBinarySHA256 {
		return fmt.Errorf("pinned Dolt member SHA256=%s, want %s", got, doltBinarySHA256)
	}
	return nil
}

func (s *Server) awaitReady() error {
	deadline := time.Now().Add(startupTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-s.done:
			return fmt.Errorf("Dolt child exited before readiness: %v", s.waitErr)
		default:
		}
		owned, listener, err := ownedLoopbackListener(s.PID, s.Port)
		if err != nil {
			return err
		}
		if listener && !owned {
			return errors.New("port is held by an unrelated listener")
		}
		if owned && mysqlHandshake(s.Port) {
			again, present, err := ownedLoopbackListener(s.PID, s.Port)
			if err != nil || !present || !again {
				return fmt.Errorf("owned listener changed during handshake: %v", err)
			}
			select {
			case <-s.done:
				return fmt.Errorf("Dolt child exited after handshake: %v", s.waitErr)
			default:
			}
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timed out waiting for owned Dolt listener and MySQL handshake")
}

// ownedLoopbackListener requires the one LISTEN inode for port to be on the
// owned process's descriptor table. Any other listener or unreadable proc
// evidence is a hard error; a merely open TCP port is never readiness proof.
func ownedLoopbackListener(pid, port int) (owned, present bool, err error) {
	f, err := os.Open("/proc/net/tcp")
	if err != nil {
		return false, false, err
	}
	inodes, err := parseListeners(f, port)
	_ = f.Close()
	if err != nil {
		return false, false, err
	}
	if len(inodes) == 0 {
		return false, false, nil
	}
	if len(inodes) != 1 || inodes[0].address != "0100007F" {
		return false, true, errors.New("port has a non-loopback or multiple TCP listeners")
	}
	// IPv6 listeners on the same port are also foreign to this IPv4-only server.
	v6, err := os.Open("/proc/net/tcp6")
	if err != nil {
		return false, true, err
	}
	other, scanErr := parseListeners(v6, port)
	_ = v6.Close()
	if scanErr != nil {
		return false, true, scanErr
	}
	if len(other) != 0 {
		return false, true, errors.New("port also has an IPv6 listener")
	}
	fds, err := os.ReadDir(filepath.Join("/proc", strconv.Itoa(pid), "fd"))
	if err != nil {
		return false, true, err
	}
	want := "socket:[" + inodes[0].inode + "]"
	for _, fd := range fds {
		link, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "fd", fd.Name()))
		if err == nil && link == want {
			return true, true, nil
		}
	}
	return false, true, nil
}

type listener struct{ address, inode string }

func parseListeners(r io.Reader, port int) ([]listener, error) {
	var found []listener
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 10 || fields[3] != "0A" {
			continue
		}
		parts := strings.SplitN(fields[1], ":", 2)
		if len(parts) != 2 {
			continue
		}
		p, err := strconv.ParseUint(parts[1], 16, 16)
		if err == nil && int(p) == port {
			found = append(found, listener{parts[0], fields[9]})
		}
	}
	return found, scanner.Err()
}

func mysqlHandshake(port int) bool {
	conn, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 300*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var header [5]byte
	_, err = io.ReadFull(conn, header[:])
	return err == nil && header[3] == 0 && header[4] == 10
}

// ChildEnv carries ordinary process and caller-owned workspace settings from
// base. All Dolt routing and credential settings are replaced with the owned
// server values. Apply it to every accepted-Beads child, including exports.
func (s *Server) ChildEnv(base []string) []string {
	out := make([]string, 0, 32)
	path := "/usr/bin:/bin"
	allowed := make(map[string]string)
	for _, entry := range base {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		switch key {
		case "PATH":
			path = value
		case "HOME", "USERPROFILE", "XDG_CONFIG_HOME", "USER", "LANG", "LC_ALL", "TZ", "NO_COLOR",
			"CI", "DO_NOT_TRACK", "TMPDIR", "GIT_CONFIG_NOSYSTEM", "GIT_CONFIG_GLOBAL",
			"GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL",
			"BEADS_DIR", "BEADS_NO_DAEMON", "BEADS_ACTOR":
			allowed[key] = value
		}
	}
	for _, key := range []string{"HOME", "USERPROFILE", "XDG_CONFIG_HOME", "USER", "LANG", "LC_ALL", "TZ", "NO_COLOR",
		"CI", "DO_NOT_TRACK", "TMPDIR", "GIT_CONFIG_NOSYSTEM", "GIT_CONFIG_GLOBAL",
		"GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL",
		"BEADS_DIR", "BEADS_NO_DAEMON", "BEADS_ACTOR"} {
		if value, ok := allowed[key]; ok {
			out = append(out, key+"="+value)
		}
	}
	if s.DoltBinary != "" {
		path = filepath.Dir(s.DoltBinary) + ":" + path
	}
	return append(out, "PATH="+path, "BEADS_DOLT_DATA_DIR="+s.DataRoot, "DOLT_ROOT_PATH="+s.doltRoot,
		"DOLT_DISABLE_EVENT_FLUSH=1", "BEADS_DOLT_AUTO_START=0", "BEADS_DOLT_SHARED_SERVER=0",
		"BEADS_DOLT_SERVER_HOST=127.0.0.1", "BEADS_DOLT_SERVER_PORT="+strconv.Itoa(s.Port),
		"BEADS_DOLT_PORT="+strconv.Itoa(s.Port))
}

// RequireDatabaseDir proves that native StoreDir names the actual server-owned
// directory created by init. It never creates a stand-in or follows a symlink.
func (s *Server) RequireDatabaseDir(t testing.TB, name string) string {
	t.Helper()
	if name == "" || strings.Trim(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != "" {
		t.Fatalf("invalid Dolt database name %q", name)
	}
	path := filepath.Join(s.DataRoot, name)
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("actual server database directory is absent or not a real directory: %s: %v", path, err)
	}
	t.Logf("NATIVE_DOLT_DATABASE path=%s mode=%#o pid=%d port=%d", path, info.Mode().Perm(), s.PID, s.Port)
	return path
}

func (s *Server) record(event string, fields map[string]any) error {
	fields["event"] = event
	fields["at_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
	b, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.receipt, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(b, '\n'))
	return errors.Join(writeErr, f.Sync(), f.Close())
}

// retainFiles moves original startup evidence out of the disposable private
// root before removing it. On a move failure callers keep the root intact.
func (s *Server) retainFiles() error {
	name := filepath.Base(s.Root)
	files := []struct {
		path   *string
		suffix string
	}{
		{&s.receipt, "-server.jsonl"},
		{&s.logPath, "-dolt.log"},
		{&s.configPath, "-config_global.json"},
	}
	for _, file := range files {
		info, err := os.Lstat(*file.path)
		if errors.Is(err, os.ErrNotExist) {
			if file.path == &s.receipt {
				return fmt.Errorf("missing Dolt lifecycle receipt: %s", *file.path)
			}
			*file.path = ""
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("startup evidence is not a regular file: %s", *file.path)
		}
		retained := filepath.Join(s.evidence, name+file.suffix)
		if err := os.Rename(*file.path, retained); err != nil {
			return fmt.Errorf("moving startup evidence %s to %s: %w", *file.path, retained, err)
		}
		*file.path = retained
	}
	return nil
}

// Stop terminates and reaps the owned process group. It is idempotent so a
// consumer may stop explicitly and still retain the registered test cleanup.
func (s *Server) Stop() error {
	s.proxyOnce.Do(func() {
		if s.drainProxy != nil {
			s.proxyErr = s.drainProxy()
		} else {
			s.proxyErr = s.stopBeadsProxy()
		}
	})
	s.stopOnce.Do(func() {
		if s.reapDolt != nil {
			s.stopErr = s.reapDolt()
			return
		}
		if s.cmd == nil || s.done == nil {
			return
		}
		select {
		case <-s.done:
		default:
			_ = syscall.Kill(-s.PID, syscall.SIGTERM)
		}
		reaped := false
		select {
		case <-s.done:
			reaped = true
		case <-time.After(5 * time.Second):
			_ = syscall.Kill(-s.PID, syscall.SIGKILL)
			select {
			case <-s.done:
				reaped = true
			case <-time.After(5 * time.Second):
				s.stopErr = errors.New("owned Dolt child did not exit after SIGKILL")
			}
		}
		waitResult := "unreaped"
		if reaped {
			waitResult = fmt.Sprint(s.waitErr)
		}
		logResult := "unreaped"
		if reaped {
			logResult = fmt.Sprint(s.logErr)
		}
		if err := s.record("stop", map[string]any{"pid": s.PID, "port": s.Port, "wait_error": waitResult,
			"log_close_error": logResult, "reaped": reaped}); err != nil {
			s.stopErr = errors.Join(s.stopErr, err)
		}
		if s.stopErr != nil {
			return // Preserve the data root if the owned process was not reaped.
		}
		if err := s.retainFiles(); err != nil {
			s.stopErr = fmt.Errorf("retaining Dolt lifecycle evidence; private root retained at %s: %w", s.Root, err)
			_ = s.record("retention_failed", map[string]any{"error": s.stopErr.Error(), "root": s.Root})
			return
		}
		removeErr := os.RemoveAll(s.Root)
		recordErr := s.record("cleanup", map[string]any{"pid": s.PID, "port": s.Port,
			"root": s.Root, "root_removed": removeErr == nil,
			"retained_log": s.logPath, "retained_config": s.configPath})
		s.stopErr = errors.Join(removeErr, recordErr, s.logErr)
	})
	return errors.Join(s.proxyErr, s.stopErr)
}

func (s *Server) stopBeadsProxy() error {
	pids, err := beadsProxyPIDs(s.DataRoot)
	if err != nil {
		return err
	}
	for _, pid := range pids {
		match, err := beadsProxyPIDMatches(pid, s.DataRoot)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			return err
		}
		if match {
			if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
				return err
			}
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		left, err := beadsProxyPIDs(s.DataRoot)
		if err != nil {
			return err
		}
		if len(left) == 0 {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	left, err := beadsProxyPIDs(s.DataRoot)
	if err != nil {
		return err
	}
	if len(left) > 0 {
		return fmt.Errorf("Beads proxy survived direct termination: pids=%v", left)
	}
	return nil
}
