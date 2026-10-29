# Pinned trusted native source checkpoint

`pluginbuild` builds an explicitly selected, trusted local Go checkout and
loads the resulting implementation through the existing SDK and native host.
`Request.TrustedNative` must be true. Native code executes with the host's
process privileges. This package is an internal checkpoint; the canonical
isolated mode-2 WASM runtime and `plugin.plan`/`plugin.apply` are unsupported.
It has no RPC binding, network listener, repository fetch or durable writes.

`Build(ctx, Request)` uses the real `pluginmanifest.Load` to require a full
commit pin, matching HEAD and committed bytes/modes, a complete source
inventory, and the actual public manifest. `ExpectedDigest` can bind a
previously inspected source digest. The source is copied into disposable
staging with each copied file checked against that inventory. The original
checkout is verified again after compilation and before host admission.

The root `go.mod` is required. `Package` defaults to the root and otherwise
selects a directory inside the checkout. `Export` supports only `PlugNatives`.
`GoBinary` may explicitly select an installed trusted toolchain. `ModuleRoot`
may explicitly select a local Plug SDK checkout; the Go tool adds its
replacement to a disposable module file. Other local replacements must stay
inside the staged source. Versioned dependencies must already be cached and
satisfy the source module's read-only dependency metadata.

The compiler invocation is fixed:

```text
go build -p=2 -trimpath -buildvcs=false -buildmode=plugin -mod=readonly \
  -modfile=.plug-pluginbuild.mod -o <disposable>/plugin.so ./<package>
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

Linux behavior tests cover compiled extension/mutator/validator execution,
callback restrictions, isolated host state, reproducible artifact bytes,
real compilation failure and cancellation, explicit trust and containment,
wrong source digest, changed source/HEAD/output, and closed output. Sandboxed
WASM, remote resolution, installation/approval, durable digest locks, build
quotas, restore and UI bundles remain separate work.
