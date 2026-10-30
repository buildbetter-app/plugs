package pluginbuild

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Compiled with CGO disabled, like the trusted Go tools used by this profile.
// Every probe runs after the real launcher has installed the production
// seccomp filter, Landlock domain, no_new_privs and hard resource ceilings.
const containmentProbe = `package main
import (
 "encoding/json"
 "os"
 "path/filepath"
 "syscall"
 "time"
)
func main() {
 if os.Args[1] == "wait" { for { time.Sleep(time.Second) } }
 if os.Args[1] == "cpu" { for {} }
 workspace, outside, readonly := os.Args[1], os.Args[2], os.Args[3]
 result := map[string]bool{}
 nnp, _, _ := syscall.RawSyscall6(syscall.SYS_PRCTL, 39, 0, 0, 0, 0, 0)
 mode, _, _ := syscall.RawSyscall6(syscall.SYS_PRCTL, 21, 0, 0, 0, 0, 0)
 result["no_new_privs"] = nnp == 1
 result["seccomp_filter"] = mode == 2
 _, readErr := os.ReadFile(outside)
 result["outside_read_denied"] = os.IsPermission(readErr)
 result["outside_write_denied"] = os.IsPermission(os.WriteFile(outside, []byte("changed"), 0600))
 result["outside_truncate_denied"] = os.IsPermission(os.Truncate(outside, 0))
 result["outside_chmod_denied"] = os.IsPermission(os.Chmod(outside, 0777))
 data, err := os.ReadFile(readonly)
 result["declared_read_allowed"] = err == nil && string(data) == "read-only"
 result["declared_write_denied"] = os.IsPermission(os.WriteFile(readonly, []byte("changed"), 0600))
 result["private_write_allowed"] = os.WriteFile(filepath.Join(workspace, "output"), []byte("built"), 0600) == nil
 link := filepath.Join(workspace, "escape")
 if os.Symlink(outside, link) == nil {
  _, err := os.ReadFile(link)
  result["symlink_escape_denied"] = os.IsPermission(err)
 }
 fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
 if err == nil { syscall.Close(fd) }
 result["network_denied"] = err == syscall.EPERM
 result["process_group_escape_denied"] = syscall.Setpgid(0, 0) == syscall.EPERM
 result["outside_signal_denied"] = syscall.Kill(os.Getppid(), 0) == syscall.EPERM
 _, _, errno := syscall.RawSyscall(syscall.SYS_FCNTL, 3, syscall.F_GETFD, 0)
 result["admission_fd_closed"] = errno == syscall.EBADF
 limits := []struct{resource int; max uint64; name string}{
  {syscall.RLIMIT_CPU, 60, "cpu"}, {syscall.RLIMIT_AS, 2<<30, "address_space"},
  {syscall.RLIMIT_FSIZE, 64<<20, "file_size"}, {syscall.RLIMIT_NOFILE, 256, "open_files"},
  {6, 16384, "processes"}, {syscall.RLIMIT_CORE, 0, "core"},
 }
 for _, limit := range limits {
  var value syscall.Rlimit
  err := syscall.Getrlimit(limit.resource, &value)
  result[limit.name+"_ceiling"] = err == nil && value.Cur == value.Max && value.Max <= limit.max
 }
 mapped, err := syscall.Mmap(-1, 0, (2<<30)+4096, syscall.PROT_NONE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
 result["address_space_enforced"] = err == syscall.ENOMEM
 if err == nil { syscall.Munmap(mapped) }
 f, err := os.Create(filepath.Join(workspace, "oversized"))
 if err == nil {
  _, err = f.WriteAt([]byte("x"), 64<<20)
  result["file_size_enforced"] = err != nil
  f.Close()
 }
 var files []*os.File
 for len(files) < 300 {
  f, err := os.Open("/dev/null")
  if err != nil { result["open_files_enforced"] = len(files) < 256; break }
  files = append(files, f)
 }
 for _, f := range files { f.Close() }
 json.NewEncoder(os.Stdout).Encode(result)
}
`

func TestLinuxContainment(t *testing.T) {
	if binary := os.Getenv("PLUG_TEST_CPU_PROBE"); binary != "" {
		// Narrow an inherited limit in an isolated test process. The real
		// launcher must preserve it and the kernel must stop a CPU-bound
		// executable; no configurable production limit or mock is needed.
		if err := unix.Setrlimit(unix.RLIMIT_CPU, &unix.Rlimit{Cur: 1, Max: 1}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, err := runContained(ctx, containedSpec{Binary: binary, Directory: t.TempDir(), Args: []string{"cpu"},
			Read: []string{binary, "/dev/null"}, Execute: []string{binary}, Env: []string{"GODEBUG=asyncpreemptoff=1"}})
		var typed *Error
		if !errors.As(err, &typed) || typed.Code != ErrBuild || !strings.Contains(typed.Detail, "signal: killed") {
			t.Fatalf("inherited hard CPU ceiling was not enforced: %v", err)
		}
		t.Logf("real inherited CPU limit stopped the contained executable: %v", err)
		return
	}
	workspace, outside, readonly := t.TempDir(), filepath.Join(t.TempDir(), "secret"), filepath.Join(t.TempDir(), "dependency")
	for path, value := range map[string]string{outside: "host-only", readonly: "read-only"} {
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	source := filepath.Join(t.TempDir(), "probe.go")
	if err := os.WriteFile(source, []byte(containmentProbe), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "probe")
	goBinary, err := exec.LookPath("go")
	if err != nil {
		for _, candidate := range []string{filepath.Join(runtime.GOROOT(), "bin", "go"), "/home/dev/.nix-profile/bin/go", "/usr/local/go/bin/go", "/usr/bin/go"} {
			if info, statErr := os.Stat(candidate); statErr == nil && info.Mode().IsRegular() && info.Mode()&0o111 != 0 {
				goBinary = candidate
				break
			}
		}
		if goBinary == "" {
			t.Fatal(err)
		}
	}
	command := exec.Command(goBinary, "build", "-p=2", "-o", binary, source)
	command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOTOOLCHAIN=local", "GOWORK=off", "GOFLAGS=")
	if data, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compile real containment probe: %v\n%s", err, data)
	}
	// The same host UID can read/write these files and open a socket before
	// containment; this excludes ambient permissions as the refusal cause.
	if _, err := os.ReadFile(outside); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = unix.Close(fd)
	spec := containedSpec{
		Binary: binary, Directory: workspace, Args: []string{workspace, outside, readonly},
		Env:  []string{"GOMAXPROCS=2", "GODEBUG=asyncpreemptoff=1"},
		Read: []string{binary, readonly, "/dev/null"}, Execute: []string{binary}, Write: []string{workspace, "/dev/null"},
	}
	t.Run("installed_and_enforced", func(t *testing.T) {
		data, err := runContained(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		var results map[string]bool
		if err := json.Unmarshal(data, &results); err != nil {
			t.Fatalf("decode probes: %v\n%s", err, data)
		}
		if len(results) != 23 {
			t.Fatalf("incomplete containment probes (%d): %s", len(results), data)
		}
		for name, passed := range results {
			if !passed {
				t.Errorf("containment probe %s failed", name)
			}
		}
		t.Logf("real Linux containment probes: %s", data)
	})
	for name, mutate := range map[string]func(*containedSpec){
		"missing_path": func(s *containedSpec) { s.Read = append(s.Read, filepath.Join(workspace, "missing")) },
		"broad_path":   func(s *containedSpec) { s.Read = append(s.Read, "/") },
		"exec_denied":  func(s *containedSpec) { s.Execute = nil },
	} {
		t.Run(name, func(t *testing.T) {
			_ = os.Remove(filepath.Join(workspace, "output"))
			bad := spec
			mutate(&bad)
			_, err := runContained(context.Background(), bad)
			var typed *Error
			if !errors.As(err, &typed) || typed.Code != ErrContainment {
				t.Fatalf("uncontained fallback or wrong refusal: %v", err)
			}
			if _, err := os.Stat(filepath.Join(workspace, "output")); !os.IsNotExist(err) {
				t.Fatalf("refused build executed the target: %v", err)
			}
			t.Log(err)
		})
	}
	t.Run("cancelled_process_group", func(t *testing.T) {
		waiting := spec
		waiting.Args = []string{"wait"}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := runContained(ctx, waiting)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 3*time.Second {
			t.Fatalf("contained process did not stop promptly: %v (%v)", err, time.Since(start))
		}
	})
	t.Run("cpu_limit_enforced", func(t *testing.T) {
		self, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(self, "-test.run=^TestLinuxContainment$", "-test.v")
		command.Env = append(os.Environ(), "PLUG_TEST_CPU_PROBE="+binary)
		if data, err := command.CombinedOutput(); err != nil {
			t.Fatalf("CPU limit subprocess: %v\n%s", err, data)
		} else {
			t.Log(string(data))
		}
	})
	if data, err := os.ReadFile(outside); err != nil || string(data) != "host-only" {
		t.Fatalf("contained command modified host content: %s (%v)", data, err)
	}
	if info, err := os.Stat(outside); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("contained command modified host permissions: %v (%v)", info, err)
	}
}

func TestContainmentKernelRefusal(t *testing.T) {
	mode := os.Getenv("PLUG_TEST_CONTAINMENT_UNAVAILABLE")
	if mode == "" {
		self, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []string{"landlock", "seccomp", "no_new_privs", "resource_limits"} {
			t.Run(mode, func(t *testing.T) {
				command := exec.Command(self, "-test.run=^TestContainmentKernelRefusal$", "-test.v")
				command.Env = append(os.Environ(), "PLUG_TEST_CONTAINMENT_UNAVAILABLE="+mode)
				if data, err := command.CombinedOutput(); err != nil {
					t.Fatalf("kernel refusal subprocess: %v\n%s", err, data)
				} else {
					t.Log(string(data))
				}
			})
		}
		return
	}
	// A real outer seccomp filter makes each required installation operation
	// unavailable to the launcher. No production bypass or mocked kernel
	// installation function is involved.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	operations := map[string][]uint32{
		"landlock":        {unix.SYS_LANDLOCK_CREATE_RULESET},
		"seccomp":         {unix.SYS_SECCOMP},
		"no_new_privs":    {unix.SYS_PRCTL},
		"resource_limits": {unix.SYS_PRLIMIT64, unix.SYS_SETRLIMIT},
	}
	filter := []unix.SockFilter{{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0}}
	for _, number := range operations[mode] {
		filter = append(filter,
			unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: number, Jf: 1},
			unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.ENOSYS)},
		)
	}
	filter = append(filter, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW})
	program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if _, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, 0, uintptr(unsafe.Pointer(&program))); errno != 0 {
		t.Fatal(errno)
	}
	_, err := runContained(context.Background(), containedSpec{Binary: "/unreachable", Directory: t.TempDir(), Read: []string{"/dev/null"}})
	var typed *Error
	want := map[string]string{"landlock": "Landlock ABI 3 required", "seccomp": "seccomp:", "no_new_privs": "no_new_privs:", "resource_limits": "resource limit"}[mode]
	if !errors.As(err, &typed) || typed.Code != ErrContainment || !strings.Contains(typed.Detail, want) {
		t.Fatalf("unsupported kernel did not fail closed: %v", err)
	}
	t.Logf("real kernel-installation refusal: %v", err)
}
