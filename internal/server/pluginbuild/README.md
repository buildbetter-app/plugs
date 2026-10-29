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

Ambient Go build flags, workspace files, toolchain downloads, module proxies,
checksum service access and VCS resolution are disabled. This is an offline
invocation policy, not an OS sandbox. Compiler diagnostics retain at most
32 KiB. Context cancellation kills the Go compiler process group on supported
Unix hosts. Staging and failed outputs are removed.

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

Linux behavior tests cover compiled native and WASI method/mutator/validator
execution, callback restrictions, isolated host state, reproducible artifact
bytes, real compilation failure and cancellation, explicit trust and
containment, real guest filesystem/network/environment refusals, initialization
cancellation, static WASI import/export/memory checks, wrong source digest,
changed source/HEAD/output, closed output, digest restore, and asynchronous
rebuild match/mismatch comparison. Remote resolution, installation/approval, durable
external artifact storage, build quotas, and UI bundles remain separate work.
