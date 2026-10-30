# Pinned source build and host checkpoint

`pluginbuild` builds an explicitly selected local Go checkout and loads the
result through the existing SDK and host. A `native` manifest requires
`Request.TrustedNative` and runs with process privileges. A `wasip1` manifest
builds a c-shared WASI module and runs in wazero's interpreter through the
fixed `plug_alloc`/`plug_handle` JSON ABI. The WASI host allows only
`wasi_snapshot_preview1` and the reserved `env.plug.call` import, provides no
preopened filesystem, environment, or network capability, and caps guest
memory at 64 MiB. Initialization has a one-second deadline. Outbound calls
through `env.plug.call` are explicitly refused in this bounded adapter; the
guest cannot use it as an alternate filesystem, egress, or write path.

`Build(ctx, Request)` uses the real `pluginmanifest.Load` to require a full
commit pin, matching HEAD and committed bytes/modes, a complete source
inventory, and the actual public manifest. `ExpectedDigest` can bind a
previously inspected source digest. The source is copied into disposable
staging with each copied file checked against that inventory. The original
checkout is verified again after compilation and before host admission.

The root `go.mod` is required. `Package` defaults to the root and otherwise
selects a directory inside the checkout. Native builds support only the
`PlugNatives` export; wasip1 builds use the fixed ABI above. `GoBinary` may
explicitly select an installed trusted toolchain. `ModuleRoot` may explicitly
select a local Plug SDK checkout; the Go tool adds its replacement to a
disposable module file. Other local replacements must stay inside the staged
source. Versioned dependencies must already be cached and satisfy the source
module's read-only dependency metadata.

The compiler invocation is fixed per runtime:

```text
# native (the host's GOOS/GOARCH, with CGO enabled)
go build -p=2 -trimpath -buildvcs=false -buildmode=plugin -mod=readonly \
  -modfile=.plug-pluginbuild.mod -o <disposable>/plugin.so ./<package>

# wasip1 (GOOS=wasip1 GOARCH=wasm CGO_ENABLED=0)
go build -p=2 -trimpath -buildvcs=false -buildmode=c-shared -mod=readonly \
  -modfile=.plug-pluginbuild.mod -o <disposable>/plugin.wasm ./<package>
```

Local mode-2 builds require the Linux/amd64 containment profile, an
unprivileged host UID, Landlock ABI 3 or newer (enabled by the kernel), and
seccomp BPF. The Go SDK must have `bin/go`, `src/runtime` and
`pkg/tool/linux_amd64`; the qualified SDK uses statically linked Go tools.
Other operating systems and architectures return typed `unsupported_platform`.
Missing kernel support, a denied installation operation, or failure to execute
within the installed profile returns typed `containment_unavailable`. Neither
case runs an uncontained compiler. The later container mode and explicit
unsandboxed-build flag from P68 are not implemented.

A private re-exec of the host installs `no_new_privs`, hard resource limits,
Landlock and a seccomp syscall allowlist before executing any Go command,
including module-file editing and parsing. Landlock permits source, SDK and
cached dependency reads; writes stay in disposable staging/output directories,
including a private home, temporary directory and Go build cache. The module
cache and explicit Plug SDK replacement are read-only. Only the trusted Go
binary and its tool directory can execute. No server environment, open server
descriptors or shared writable build cache reaches the compiler. `/dev/null`
and read-only `/dev/urandom` are the only device paths granted.

The syscall profile denies networking, namespace changes, process-group escape,
cross-process signals, tracing, IPC, kernel administration and filesystem
metadata changes that Landlock ABI 3 does not mediate. Go's optional
signal-based async preemption is disabled, and toolchain telemetry is off in
the private home. Ambient Go flags, workspace files, toolchain downloads,
module proxies, checksum service access and VCS resolution remain disabled.
This slice consumes already cached dependencies; it does not implement a
remote repository or checksum-verified dependency-fetch phase.

Each Go command has a three-minute wall deadline. Inherited hard limits can
only narrow these ceilings: 60 CPU seconds per process, 2 GiB address space per
process, 64 MiB per file, 256 open descriptors, no core dumps, and 16,384
processes/threads per real UID. The UID ceiling includes other work sharing
that account; these are not aggregate memory/disk or per-principal quotas.
Compiler parallelism remains two. Diagnostics retain at most 32 KiB. A
cancellation or completion kills remaining compiler process-group members.
Staging, private build directories and failed outputs are removed. Explicitly
trusted native builds retain their existing host privileges.

The source must export:

```go
func PlugNatives() ([]sdk.Native, error)
```

`Artifact.Host(ctx, config)` loads that symbol from the actual produced Go
plugin. Caller-supplied `config.Natives` refuse. `Bundle.Natives` requires
complete and exact source/compiled declarations; `pluginhost.New` then checks
actual constructor registrations and narrows explicit bindings. Existing
per-instance state, owned-field/read grants and callback restrictions apply.
No subscriber binding is added implicitly.

Go native plugins require the same Go toolchain, SDK package identities and
compatible compiler flags as the embedding host. Incompatible hosts refuse
at `plugin.Open`; there is no fallback implementation. The local Bazel test
compiles an external host with matching flags and runs real source callbacks
through that host and the memory write pipeline. This proves the connected
native checkpoint, not compatibility with every Bazel-built server binary.

An artifact retains its full source commit, source and manifest digests, and
the output file's size, executable bit and SHA-256. Accessors return detached
inventories. `Verify` refuses source/HEAD/output drift. `Close` removes the
output and prevents later admission; Go cannot unload code already loaded
into a process. The caller owns the lifetime of hosts already created.

`NewArtifactStore` preserves the process-local store used by existing callers.
`OpenArtifactStore(path)` opens a private file-backed repository with
`objects/<artifact-sha256>/{lock.json,artifact}` entries. A complete lock and
its bytes are staged, synced and published with one directory rename. Reopen
validates every digest, canonical lock, regular-file mode and expected entry;
missing, partial, corrupt, conflicting, traversal or symlink state refuses.
The lock and artifact are copied into detached restore state, and an optional
asynchronous rebuild remains a comparison only: a mismatch never replaces the
bytes admitted by digest.

Linux behavior tests cover the real containment installation and failed
admission when an outer seccomp filter blocks each required installation
operation. They exercise filesystem and symlink escapes, socket creation,
metadata and signal denial, resource ceilings, actual memory/file/descriptor
exhaustion, denied execution and cancellation. Supported-host tests also cover
compiled native and WASI method/mutator/validator
execution, callback restrictions, isolated host state, reproducible artifact
bytes, real compilation failure and cancellation, explicit trust and
containment, real guest filesystem/network/environment refusals, initialization
cancellation, static WASI import/export/memory checks, wrong source digest,
changed source/HEAD/output, closed output, digest restore, and asynchronous
rebuild match/mismatch comparison. Remote resolution, installation/approval, durable
external artifact storage, build quotas, and UI bundles remain separate work.
