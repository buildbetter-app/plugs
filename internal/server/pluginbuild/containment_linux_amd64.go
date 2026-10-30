package pluginbuild

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	containmentChild = "__plugin-build"
	containmentReady = "contained\n"
	buildWallLimit   = 3 * time.Minute
	buildAddressMax  = 2 << 30
	buildFileMax     = 64 << 20
	buildOpenMax     = 256
	buildProcessMax  = 16384 // Per real UID, including other builds on that UID.
	buildCPUSeconds  = 60
)

// A private re-exec entrypoint keeps Linux setup out of the multithreaded
// server. No compiler or source code runs before installation. The helper
// locks its OS thread; exec replaces every other thread, and descendants
// inherit the calling thread's Landlock, seccomp and no_new_privs state.
func init() {
	if len(os.Args) != 2 || os.Args[1] != containmentChild {
		return
	}
	runtime.LockOSThread()
	admission := os.NewFile(3, "build-admission")
	unix.CloseOnExec(3)
	fail := func(step string, err error) {
		_, _ = fmt.Fprintf(admission, "%s: %v\n", step, err)
		os.Exit(125)
	}
	var spec containedSpec
	if err := json.NewDecoder(io.LimitReader(os.Stdin, 64<<10)).Decode(&spec); err != nil {
		fail("configuration", err)
	}
	if err := installContainment(spec); err != nil {
		fail("install", err)
	}
	if _, err := io.WriteString(admission, containmentReady); err != nil {
		os.Exit(125)
	}
	if err := unix.Exec(spec.Binary, append([]string{spec.Binary}, spec.Args...), spec.Env); err != nil {
		fail("exec", err)
	}
}

type containedSpec struct {
	Binary    string
	Args      []string
	Env       []string
	Directory string
	Read      []string
	Execute   []string
	Write     []string
}

func containedGo(binary, source, output, moduleRoot string) (goRunner, error) {
	binary, err := exec.LookPath(binary)
	if err != nil {
		return nil, refusal(ErrContainment, "trusted Go toolchain unavailable: %v", err)
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return nil, refusal(ErrContainment, "resolve trusted Go toolchain: %v", err)
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		return nil, refusal(ErrContainment, "resolve trusted Go toolchain: %v", err)
	}
	// An installed Go SDK has bin/go, pkg/tool and src under this root. Never
	// execute an uncontained `go env` probe to discover the SDK's paths.
	goRoot := filepath.Dir(filepath.Dir(binary))
	for _, name := range []string{"src/runtime", "pkg/tool/linux_amd64"} {
		if info, err := os.Stat(filepath.Join(goRoot, name)); err != nil || !info.IsDir() {
			return nil, refusal(ErrContainment, "trusted Go SDK must contain bin/go, src/runtime and pkg/tool/linux_amd64")
		}
	}
	work := filepath.Join(output, ".build")
	for _, name := range []string{"home", "tmp", "cache", "modules"} {
		if err := os.MkdirAll(filepath.Join(work, name), 0o700); err != nil {
			return nil, refusal(ErrContainment, "create private build workspace: %v", err)
		}
	}
	telemetry := filepath.Join(work, "home", ".config", "go", "telemetry")
	if err := os.MkdirAll(telemetry, 0o700); err != nil {
		return nil, refusal(ErrContainment, "create private toolchain configuration: %v", err)
	}
	if err := os.WriteFile(filepath.Join(telemetry, "mode"), []byte("off\n"), 0o600); err != nil {
		return nil, refusal(ErrContainment, "disable build-tool telemetry: %v", err)
	}
	moduleCache := os.Getenv("GOMODCACHE")
	if moduleCache == "" {
		goPath := strings.Split(os.Getenv("GOPATH"), string(os.PathListSeparator))[0]
		if goPath == "" {
			home, _ := os.UserHomeDir()
			goPath = filepath.Join(home, "go")
		}
		moduleCache = filepath.Join(goPath, "pkg", "mod")
	}
	if _, err := os.Stat(moduleCache); os.IsNotExist(err) {
		moduleCache = filepath.Join(work, "modules")
	}
	read := []string{source, goRoot, moduleCache, "/dev/urandom"}
	if moduleRoot != "" {
		read = append(read, moduleRoot)
	}
	spec := containedSpec{
		Binary: binary, Directory: source, Read: read,
		Execute: []string{binary, filepath.Join(goRoot, "pkg/tool/linux_amd64")},
		Write:   []string{source, output, "/dev/null"},
		Env: append(buildEnvironment(nil, "wasip1", "wasm"),
			"GOROOT="+goRoot, "GOPATH="+filepath.Join(work, "home", "go"),
			"GOMODCACHE="+moduleCache, "GOCACHE="+filepath.Join(work, "cache"),
			"HOME="+filepath.Join(work, "home"), "TMPDIR="+filepath.Join(work, "tmp"),
			"PATH="+filepath.Dir(binary), "GOMAXPROCS=2", "GOMEMLIMIT=512MiB",
			// Signals cannot target processes outside the build on ABI 3
			// kernels. Disable Go's optional signal-based async preemption.
			"GODEBUG=asyncpreemptoff=1",
		),
	}
	return func(ctx context.Context, args ...string) ([]byte, error) {
		spec.Args = args
		return runContained(ctx, spec)
	}, nil
}

func runContained(ctx context.Context, spec containedSpec) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, buildWallLimit)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(spec)
	if err != nil || len(encoded) > 64<<10 {
		return nil, refusal(ErrContainment, "invalid build containment configuration")
	}
	read, write, err := os.Pipe()
	if err != nil {
		return nil, refusal(ErrContainment, "create admission channel: %v", err)
	}
	defer read.Close()
	defer write.Close()
	self, err := os.Executable()
	if err != nil {
		return nil, refusal(ErrContainment, "locate containment helper: %v", err)
	}
	command := exec.CommandContext(ctx, self, containmentChild)
	command.Env = []string{"GOMAXPROCS=1", "GODEBUG=asyncpreemptoff=1"}
	command.Stdin = bytes.NewReader(encoded)
	command.ExtraFiles = []*os.File{write}
	configureBuildProcess(command)
	var output buildOutput
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		return nil, refusal(ErrContainment, "start containment helper: %v", err)
	}
	_ = write.Close()
	err = command.Wait()
	// Reap/kill lingering descendants even if the Go command itself exited.
	_ = unix.Kill(-command.Process.Pid, unix.SIGKILL)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	admission, readErr := io.ReadAll(io.LimitReader(read, 4096))
	if readErr != nil || string(admission) != containmentReady {
		return nil, refusal(ErrContainment, "Linux build containment was not admitted: %s", boundedOutput(admission))
	}
	if err != nil {
		return nil, refusal(ErrBuild, "contained go command failed: %v: %s", err, boundedOutput(output.data.Bytes()))
	}
	return output.data.Bytes(), nil
}

func installContainment(spec containedSpec) error {
	if os.Geteuid() == 0 {
		return fmt.Errorf("local builds require an unprivileged host UID")
	}
	if !filepath.IsAbs(spec.Binary) || !filepath.IsAbs(spec.Directory) {
		return fmt.Errorf("build executable and directory must be absolute")
	}
	if err := os.Chdir(spec.Directory); err != nil {
		return fmt.Errorf("build directory: %w", err)
	}
	for _, limit := range []struct {
		resource int
		value    uint64
	}{
		{unix.RLIMIT_CPU, buildCPUSeconds}, {unix.RLIMIT_AS, buildAddressMax},
		{unix.RLIMIT_FSIZE, buildFileMax}, {unix.RLIMIT_NOFILE, buildOpenMax},
		{unix.RLIMIT_NPROC, buildProcessMax}, {unix.RLIMIT_CORE, 0},
	} {
		var inherited unix.Rlimit
		if err := unix.Getrlimit(limit.resource, &inherited); err != nil {
			return fmt.Errorf("get resource limit %d: %w", limit.resource, err)
		}
		value := min(inherited.Max, limit.value)
		if err := unix.Setrlimit(limit.resource, &unix.Rlimit{Cur: value, Max: value}); err != nil {
			return fmt.Errorf("set resource limit %d: %w", limit.resource, err)
		}
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("no_new_privs: %w", err)
	}
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return fmt.Errorf("disable process tracing: %w", err)
	}
	if err := installLandlock(spec); err != nil {
		return err
	}
	null, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("empty build stdin: %w", err)
	}
	err = unix.Dup3(null, 0, 0)
	_ = unix.Close(null)
	if err != nil {
		return fmt.Errorf("empty build stdin: %w", err)
	}
	filter := buildSeccompFilter()
	program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if _, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, 0, uintptr(unsafe.Pointer(&program))); errno != 0 {
		return fmt.Errorf("seccomp: %w", errno)
	}
	return nil
}

func installLandlock(spec containedSpec) error {
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 {
		return fmt.Errorf("Landlock ABI 3 required: %w", errno)
	}
	if abi < 3 {
		return fmt.Errorf("Landlock ABI 3 required (found %d)", abi)
	}
	// ABI 3 covers all content access, cross-directory reparenting and
	// truncation. Rights not granted below remain denied, including devices,
	// FIFOs and sockets. seccomp separately denies metadata changes and all
	// network/IPC/signal operations that ABI 3 cannot mediate.
	handled := uint64((1 << 15) - 1)
	fd, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, uintptr(unsafe.Pointer(&handled)), unsafe.Sizeof(handled), 0)
	if errno != 0 {
		return fmt.Errorf("Landlock create: %w", errno)
	}
	defer unix.Close(int(fd))
	read := uint64(unix.LANDLOCK_ACCESS_FS_READ_FILE | unix.LANDLOCK_ACCESS_FS_READ_DIR)
	write := read | unix.LANDLOCK_ACCESS_FS_WRITE_FILE | unix.LANDLOCK_ACCESS_FS_REMOVE_DIR |
		unix.LANDLOCK_ACCESS_FS_REMOVE_FILE | unix.LANDLOCK_ACCESS_FS_MAKE_DIR |
		unix.LANDLOCK_ACCESS_FS_MAKE_REG | unix.LANDLOCK_ACCESS_FS_MAKE_SYM |
		unix.LANDLOCK_ACCESS_FS_REFER | unix.LANDLOCK_ACCESS_FS_TRUNCATE
	for _, group := range []struct {
		paths  []string
		rights uint64
	}{{spec.Read, read}, {spec.Write, write}, {spec.Execute, unix.LANDLOCK_ACCESS_FS_EXECUTE}} {
		for _, name := range group.paths {
			resolved, err := filepath.EvalSymlinks(name)
			if err != nil || !filepath.IsAbs(resolved) || resolved == "/" {
				return fmt.Errorf("Landlock path %q must resolve to a narrow absolute path", name)
			}
			pathFD, err := unix.Open(resolved, unix.O_PATH|unix.O_CLOEXEC, 0)
			if err != nil {
				return fmt.Errorf("Landlock open %q: %w", name, err)
			}
			var stat unix.Stat_t
			err = unix.Fstat(pathFD, &stat)
			rights := group.rights
			if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
				rights &= unix.LANDLOCK_ACCESS_FS_EXECUTE | unix.LANDLOCK_ACCESS_FS_READ_FILE |
					unix.LANDLOCK_ACCESS_FS_WRITE_FILE | unix.LANDLOCK_ACCESS_FS_TRUNCATE
			}
			rule := unix.LandlockPathBeneathAttr{Allowed_access: rights, Parent_fd: int32(pathFD)}
			if err == nil {
				_, _, errno = unix.Syscall6(unix.SYS_LANDLOCK_ADD_RULE, fd, unix.LANDLOCK_RULE_PATH_BENEATH, uintptr(unsafe.Pointer(&rule)), 0, 0, 0)
				err = errno
				if errno == 0 {
					err = nil
				}
			}
			_ = unix.Close(pathFD)
			if err != nil {
				return fmt.Errorf("Landlock rule %q: %w", name, err)
			}
		}
	}
	if _, _, errno := unix.Syscall(unix.SYS_LANDLOCK_RESTRICT_SELF, fd, 0, 0); errno != 0 {
		return fmt.Errorf("Landlock restrict: %w", errno)
	}
	return nil
}

func buildSeccompFilter() []unix.SockFilter {
	stmt := func(code uint16, k uint32) unix.SockFilter { return unix.SockFilter{Code: code, K: k} }
	jump := func(k uint32, yes, no uint8) unix.SockFilter {
		return unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: k, Jt: yes, Jf: no}
	}
	load := func(offset uint32) unix.SockFilter { return stmt(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, offset) }
	allow := stmt(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_ALLOW)
	deny := stmt(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_ERRNO|uint32(unix.EPERM))
	filter := []unix.SockFilter{
		load(4), jump(unix.AUDIT_ARCH_X86_64, 1, 0), stmt(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_KILL_PROCESS),
		load(0), // syscall number; x32 numbers never match this allowlist.
	}
	rule := func(number uint32, checks ...unix.SockFilter) {
		filter = append(filter, jump(number, 0, uint8(len(checks))))
		filter = append(filter, checks...)
	}
	// Go uses clone for threads and fork/exec. New namespaces, alternate
	// parents, and unknown clone flags are refused. clone3 returns ENOSYS so
	// libc/Go can use the inspectable clone ABI instead.
	rule(unix.SYS_CLONE3, stmt(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_ERRNO|uint32(unix.ENOSYS)))
	cloneFlags := uint32(unix.CLONE_VM | unix.CLONE_FS | unix.CLONE_FILES | unix.CLONE_SIGHAND |
		unix.CLONE_THREAD | unix.CLONE_SYSVSEM | unix.CLONE_SETTLS | unix.CLONE_PARENT_SETTID |
		unix.CLONE_CHILD_CLEARTID | unix.CLONE_CHILD_SETTID | unix.CLONE_VFORK | unix.CLONE_PIDFD | int(unix.SIGCHLD))
	rule(unix.SYS_CLONE, load(20), jump(0, 1, 0), deny, load(16),
		stmt(unix.BPF_ALU|unix.BPF_AND|unix.BPF_K, ^cloneFlags), jump(0, 1, 0), deny, allow)
	// Resource ceilings cannot be raised. Allow only querying prlimit64.
	rule(unix.SYS_PRLIMIT64, load(36), jump(0, 1, 0), deny, load(32), jump(0, 1, 0), deny, allow)
	// Read-only prctl queries support diagnostics, without allowing process
	// attribute changes after installation.
	rule(unix.SYS_PRCTL, load(16), jump(unix.PR_GET_NO_NEW_PRIVS, 2, 0), jump(unix.PR_GET_SECCOMP, 1, 0), deny, allow)
	checks := []unix.SockFilter{load(24)} // fcntl command
	for _, cmd := range []uint32{unix.F_DUPFD, unix.F_DUPFD_CLOEXEC, unix.F_GETFD, unix.F_SETFD, unix.F_GETFL, unix.F_SETFL, unix.F_GETLK, unix.F_SETLK, unix.F_SETLKW} {
		checks = append(checks, jump(cmd, 0, 1), allow)
	}
	checks = append(checks, deny)
	rule(unix.SYS_FCNTL, checks...)
	for _, number := range []uint32{
		unix.SYS_READ, unix.SYS_WRITE, unix.SYS_READV, unix.SYS_WRITEV, unix.SYS_PREAD64, unix.SYS_PWRITE64,
		unix.SYS_CLOSE, unix.SYS_CLOSE_RANGE, unix.SYS_LSEEK, unix.SYS_GETDENTS64, unix.SYS_DUP, unix.SYS_DUP2, unix.SYS_DUP3,
		unix.SYS_PIPE, unix.SYS_PIPE2, unix.SYS_FLOCK, unix.SYS_OPEN, unix.SYS_OPENAT, unix.SYS_OPENAT2,
		unix.SYS_STAT, unix.SYS_FSTAT, unix.SYS_LSTAT, unix.SYS_NEWFSTATAT, unix.SYS_STATX,
		unix.SYS_ACCESS, unix.SYS_FACCESSAT, unix.SYS_FACCESSAT2, unix.SYS_READLINK, unix.SYS_READLINKAT,
		unix.SYS_GETCWD, unix.SYS_CHDIR, unix.SYS_FCHDIR, unix.SYS_MKDIR, unix.SYS_MKDIRAT,
		unix.SYS_UNLINK, unix.SYS_UNLINKAT, unix.SYS_RENAME, unix.SYS_RENAMEAT, unix.SYS_RENAMEAT2,
		unix.SYS_LINK, unix.SYS_LINKAT, unix.SYS_SYMLINK, unix.SYS_SYMLINKAT, unix.SYS_FTRUNCATE, unix.SYS_TRUNCATE,
		unix.SYS_FSYNC, unix.SYS_FDATASYNC, unix.SYS_SENDFILE, unix.SYS_COPY_FILE_RANGE, unix.SYS_FALLOCATE, unix.SYS_FADVISE64,
		unix.SYS_MMAP, unix.SYS_MPROTECT, unix.SYS_MUNMAP, unix.SYS_MREMAP, unix.SYS_MADVISE, unix.SYS_BRK, unix.SYS_MSYNC,
		unix.SYS_RT_SIGACTION, unix.SYS_RT_SIGPROCMASK, unix.SYS_RT_SIGRETURN, unix.SYS_SIGALTSTACK,
		unix.SYS_RT_SIGTIMEDWAIT, unix.SYS_RT_SIGPENDING, unix.SYS_RESTART_SYSCALL,
		unix.SYS_FUTEX, unix.SYS_SCHED_YIELD, unix.SYS_SCHED_GETAFFINITY, unix.SYS_GETCPU,
		unix.SYS_GETPID, unix.SYS_GETPPID, unix.SYS_GETTID, unix.SYS_GETUID, unix.SYS_GETGID, unix.SYS_GETEUID, unix.SYS_GETEGID, unix.SYS_GETGROUPS,
		unix.SYS_CLOCK_GETTIME, unix.SYS_CLOCK_GETRES, unix.SYS_CLOCK_NANOSLEEP, unix.SYS_NANOSLEEP, unix.SYS_GETTIMEOFDAY,
		unix.SYS_TIMES, unix.SYS_GETRUSAGE, unix.SYS_SYSINFO, unix.SYS_UNAME, unix.SYS_GETRANDOM, unix.SYS_UMASK, unix.SYS_GETRLIMIT,
		unix.SYS_EXECVE, unix.SYS_WAIT4, unix.SYS_WAITID, unix.SYS_EXIT, unix.SYS_EXIT_GROUP,
		unix.SYS_ARCH_PRCTL, unix.SYS_SET_TID_ADDRESS, unix.SYS_SET_ROBUST_LIST, unix.SYS_RSEQ,
		unix.SYS_EPOLL_CREATE, unix.SYS_EPOLL_CREATE1, unix.SYS_EPOLL_CTL, unix.SYS_EPOLL_WAIT, unix.SYS_EPOLL_PWAIT, unix.SYS_EPOLL_PWAIT2,
		unix.SYS_POLL, unix.SYS_PPOLL, unix.SYS_SELECT, unix.SYS_PSELECT6, unix.SYS_EVENTFD2,
	} {
		rule(number, allow)
	}
	return append(filter, deny)
}
