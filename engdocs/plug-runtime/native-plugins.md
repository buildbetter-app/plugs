# Native SDK and host

Source: **`471d0199c89acf9e242f44978f2824120726de32`**;
[guide scope](README.md#snapshot-and-boundaries).

## Register compiled-in code

Plugins import [`pluginapi`](../../pluginapi/types.go) and
[`sdk/go`](../../sdk/go/native.go). These public packages do not import Plug's
internal model, storage or server packages. The SDK supplies `sdk.Native`
(manifest, instance and constructor), `Registration`, and callback helpers
`MutateFunc`, `ValidateFunc` and `AfterFunc`. It is a native plugin SDK, not a
general remote application client.

The [manifest](../../pluginapi/manifest.go) accepts manifest version 1,
`plugin_api: "1.0"` and `runtime: "native"`. Declare subscriber phases/read
grants/owned fields, and extension methods under exactly `x.<alias>.*` with
permission and resource extraction. Implement each declared method/callback.
Duplicate instances, missing implementations, wider effective grants,
overlapping field owners and mutator dependency cycles fail host construction.

The composition root creates [pluginhost.New](../../internal/server/pluginhost/load.go)
with explicit natives and bindings, then connects `host.Mutators()`,
`host.Validators()` and `host.Posts()` to the single Writer's pipeline options.
Bindings may narrow the manifest. Current writable ownership is concrete
`fields.<name>` only; scoped project/type routing and metadata ownership
refuse. The CLI does not load manifests or automatically attach these plugins.
Host extension registration is separate from the bounded RPC registry;
embedding code must supply authorization/schema/registry adapters before
exposing it as a network API.

## Capabilities and callback lifetime

Native plugins are trusted compiled-in code, **not runtime-isolated**. The host
checks the capabilities it supplies; code review and static restrictions must
also prevent direct filesystem/network access outside that Host.

| Phase | Supported host behavior |
| --- | --- |
| Mutate / validate | Masked proposal and declared, pinned `SnapshotReader` reads join the commit guard. No graph/state writes, mutable state reads or egress. RPC-shaped `Host.Call` reads refuse; use the reader. |
| Post | Committed-revision reader, instance state CAS and granted HTTPS egress. Graph writes and `Host.Call` RPC reads refuse. Failure is a warning on the committed result. |
| Extension method | Declared method/resource checks and optional caller authorization. Granted outbound core reads require the supplied Dispatcher; outbound graph writes refuse pending the field-grant adapter. |

[Readers](../../internal/server/pluginhost/reader.go) enforce field grants and
budgets of at most 32 point reads and four queries per binding call; omitted
manifest budgets use those defaults. Closure/Grants conversion is unsupported.
Values are detached/masked; retaining a callback map cannot widen its patch
targets. An undeclared patch refuses before graph publication.

[Calls](../../internal/server/pluginhost/call.go) serialize per native instance.
Default host budgets are 250 ms mutate, 500 ms validate/post and 1 s extension;
binding timeouts cannot exceed 1 s, and the Writer may impose a shorter budget.
Honor context and release retained work: a timed-out native callback retains
its instance slot until it exits. Supplied capabilities expire with the call;
the host cannot kill arbitrary native code. Pre failures fail closed by default;
an explicitly fail-open pre binding produces a visible `subscriber_skipped`
warning. Post failure produces `post_subscriber_failed` after commit.

[State](../../internal/server/pluginhost/state.go) is per-instance, process-local
CAS, not a persistent event checkpoint. [Egress](../../internal/server/pluginhost/egress.go)
requires exact declared host:port over HTTPS, public DNS results on every
request/redirect, and at most 10 MiB each request/response body; the supplied
client has a 1 s timeout. Secret declarations/injection refuse. WASI/wasm,
artifact building, plugin UI, durable plugin state and automatic network
extension exposure remain follow-ons.

## Run the real native fixture

The complete [fixture](../../sdk/go/conformance/fixture.go) and its
[manifest](../../sdk/go/conformance/manifest.json) demonstrate registration.
[Pipeline tests](../../internal/server/pluginhost/pipeline_test.go) connect it
to the real Writer and memory driver, including normalization, refusal,
no-op and replay without fresh post callbacks:

```sh
bazel --ignore_all_rc_files test //internal/server/pluginhost:pluginhost_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_arg=-test.v --test_timeout=900
```
