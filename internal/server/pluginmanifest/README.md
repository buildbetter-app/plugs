# Local plugin source manifests

`Load(ctx, Checkout{Root: directory, Commit: fullCommit})` inspects an existing
local Git checkout. It runs only read-only Git builtins and reads source files.
It requires HEAD to equal the full lowercase SHA-1 or SHA-256 commit id and
matches every source file's contents and executable bit to the committed tree.
Missing, modified, untracked and ignored files refuse. Linked Git worktrees
are supported; only the root `.git` administration entry is excluded.

The manifest is the existing `pluginapi.Manifest`, decoded by
`pluginapi.DecodeManifest`. By default exactly one root `plug-plugin.json` or
`plug-plugin.yaml` must exist. An explicit `Checkout.ManifestFile` must also be
a root file. The input representation is strict JSON, including when the
filename is `.yaml`; general YAML is explicitly unsupported. Duplicate JSON
members, unknown fields, unsupported versions/runtimes/provisions, absolute or
parent paths, symlinks, submodules and nonregular source entries refuse. The
manifest is capped at 1 MiB. The current public schema supports native methods
and synchronous subscribers with API 1.0; it has no build, config-schema path,
UI bundle or secret-injection surface. This loader does not add one.

`Bundle.Files()` returns a detached, path-sorted inventory of every regular
source file, including hidden files, with byte size, lowercase SHA-256 and the
executable bit. `Bundle.Digest()` is SHA-256 of the compact Go JSON encoding of
`{"format":"plug-source/1","files":[...]}` in that order; each file has the
ordered members `path`, `size`, `sha256`, `executable`. It includes the raw
manifest file. `Bundle.ManifestDigest()` separately hashes the compact JSON
encoding of the validated public manifest, using its public struct field order
and sorted map keys. Array order is retained. Manifest whitespace/key-order
changes therefore change the source digest without changing the canonical
manifest digest. Roots and Git administration paths do not enter either hash.

`Bundle.Natives(ctx, compiledDescriptors)` verifies the checkout again, then
returns actual `sdk.Native` descriptors. Each source plugin requires at least
one explicit instance, and its compiled public manifest must match the source
declaration exactly, including all capability, field, method, read-budget and
egress grants. Call `ValidateNatives` when composing descriptors from several
bundles to check id/alias/instance/method collisions across the combined set.
`pluginhost.New` receives the results and checks actual constructor-returned
method and subscriber registrations. Ontology bindings may narrow the grants
through the existing host; they cannot widen them.

The loader provides validation, not source-build orchestration. It never fetches,
checks out, builds or executes source, installs a plugin, changes grants, or
binds `plugin.apply`. Native constructors are trusted compiled-in code. A later
builder must call `Bundle.Verify(ctx)` and supply its own immutable input/build
sandbox before using `Bundle.Root()`: this package does not freeze a mutable
filesystem against concurrent writers. WASM, general YAML, remote resolution,
secrets, durable lock/approval handling and global installation remain separate.

The Bazel test commits the actual compiled fixture source into disposable local
repositories, loads its two-plugin manifest, and runs its returned descriptors
through the real host, ontology, writer and memory storage. It also exercises
the host's field/read/API/egress boundaries, instance state isolation and actual
missing-registration refusals. No source build or remote code fetch is tested.
