//go:build linux

// Package publicnative owns a disposable, pinned Dolt server for public-native
// RunsOn tests. It does not discover or adopt a server from the runner.
package publicnative

import (
	"bufio"
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

	cmd      *exec.Cmd
	done     chan struct{}
	waitErr  error
	receipt  string
	stopOnce sync.Once
	stopErr  error
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
	t.Logf("NATIVE_DOLT_READY receipt=%s pid=%d port=%d data_root=%s", s.receipt, s.PID, s.Port, s.DataRoot)
	return s
}

func startDolt(evidenceDir string) (*Server, error) {
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
	dataRoot := filepath.Join(root, "data")
	home := filepath.Join(root, "home")
	for _, dir := range []string{dataRoot, home, filepath.Join(root, "tmp")} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, err
		}
	}
	s := &Server{DataRoot: dataRoot, Root: root, DoltBinary: path, receipt: filepath.Join(root, "server.jsonl")}
	serverEnv := []string{
		"PATH=" + filepath.Dir(path) + ":/usr/bin:/bin",
		"HOME=" + home,
		"XDG_CONFIG_HOME=" + filepath.Join(home, ".config"),
		"DOLT_ROOT_PATH=" + dataRoot,
		"DOLT_DISABLE_EVENT_FLUSH=1",
		"TMPDIR=" + filepath.Join(root, "tmp"),
		"TZ=UTC", "LC_ALL=C", "NO_COLOR=1",
	}
	version := exec.Command(path, "version")
	version.Dir, version.Env = dataRoot, serverEnv
	versionOutput, err := version.CombinedOutput()
	if err != nil || lastField(firstLine(versionOutput)) != doltVersion {
		return nil, fmt.Errorf("pinned Dolt version: output %q, error %v", versionOutput, err)
	}
	if err := s.record("binary", map[string]any{"path": path, "sha256": doltBinarySHA256, "version": firstLine(versionOutput)}); err != nil {
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
	args := []string{"sql-server", "-H", "127.0.0.1", "-P", strconv.Itoa(s.Port)}
	log, err := os.OpenFile(filepath.Join(root, "dolt.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
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
	go func() { s.waitErr = s.cmd.Wait(); close(s.done); _ = log.Close() }()
	if err := s.record("start", map[string]any{"argv": append([]string{"dolt"}, args...), "pid": s.PID, "port": s.Port, "data_root": dataRoot}); err != nil {
		_ = s.Stop()
		return nil, err
	}
	if err := s.awaitReady(); err != nil {
		_ = s.record("readiness_failed", map[string]any{"error": err.Error()})
		_ = s.Stop()
		return nil, fmt.Errorf("owned pid %d on port %d: %w; log %s", s.PID, s.Port, err, filepath.Join(root, "dolt.log"))
	}
	actualCWD, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(s.PID), "cwd"))
	if err != nil || actualCWD != dataRoot {
		_ = s.Stop()
		return nil, fmt.Errorf("owned Dolt cwd=%q, want real data root %q: %v", actualCWD, dataRoot, err)
	}
	if err := s.record("ready", map[string]any{"pid": s.PID, "port": s.Port, "data_root": dataRoot, "listener": "owned_proc_inode_and_mysql_handshake"}); err != nil {
		_ = s.Stop()
		return nil, err
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

func firstLine(b []byte) string { return strings.TrimSpace(strings.SplitN(string(b), "\n", 2)[0]) }
func lastField(s string) string {
	f := strings.Fields(s)
	if len(f) == 0 {
		return ""
	}
	return f[len(f)-1]
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

// ChildEnv replaces any ambient root/port/autostart settings with the owned
// physical root. Apply it to every accepted-Beads child, including exports.
func (s *Server) ChildEnv(base []string) []string {
	drop := map[string]bool{"BEADS_DOLT_DATA_DIR": true, "DOLT_ROOT_PATH": true, "BEADS_DOLT_AUTO_START": true, "BEADS_DOLT_SHARED_SERVER": true, "PATH": true}
	out := make([]string, 0, len(base)+4)
	path := "/usr/bin:/bin"
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		if key == "PATH" {
			_, path, _ = strings.Cut(entry, "=")
		}
		if !drop[key] {
			out = append(out, entry)
		}
	}
	if s.DoltBinary != "" {
		path = filepath.Dir(s.DoltBinary) + ":" + path
	}
	return append(out, "PATH="+path, "BEADS_DOLT_DATA_DIR="+s.DataRoot, "DOLT_ROOT_PATH="+s.DataRoot, "BEADS_DOLT_AUTO_START=0", "BEADS_DOLT_SHARED_SERVER=0")
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

// Stop terminates and reaps the owned process group. It is idempotent so a
// consumer may stop explicitly and still retain the registered test cleanup.
func (s *Server) Stop() error {
	s.stopOnce.Do(func() {
		if s.cmd == nil || s.done == nil {
			return
		}
		select {
		case <-s.done:
		default:
			_ = syscall.Kill(-s.PID, syscall.SIGTERM)
		}
		select {
		case <-s.done:
		case <-time.After(5 * time.Second):
			_ = syscall.Kill(-s.PID, syscall.SIGKILL)
			select {
			case <-s.done:
			case <-time.After(5 * time.Second):
				s.stopErr = errors.New("owned Dolt child did not exit after SIGKILL")
			}
		}
		if err := s.record("stop", map[string]any{"pid": s.PID, "port": s.Port, "wait_error": fmt.Sprint(s.waitErr), "reaped": s.stopErr == nil}); err != nil {
			s.stopErr = errors.Join(s.stopErr, err)
		}
	})
	return s.stopErr
}
