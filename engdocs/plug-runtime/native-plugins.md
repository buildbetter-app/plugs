# Native SDK and host

Source: **463881cec205e6cd3e5b85d1bac7d4018a153d5f**;
[guide scope](README.md#snapshot-and-package-boundaries).

## Register compiled-in code

Plugins import pluginapi and sdk/go. These are the only public Go packages for
plugins; they do not expose Plug's internal model, storage or server packages.
The SDK supplies sdk.Native (manifest, instance and constructor), Registration,
and callback helpers MutateFunc, ValidateFunc and AfterFunc. It is an embedding
SDK, not a general remote client.

The [manifest](../../pluginapi/manifest.go) accepts manifest version 1,
plugin_api: "1.0" and runtime: "native". Declare subscriber phases, read
grants, owned fields and extension methods under exactly x.<alias>.*. Host
construction refuses duplicate instances, missing implementations, wider
effective grants, overlapping field owners and mutator dependency cycles.
Bindings may narrow a manifest; current writable ownership supports concrete
fields.<name> only. Scoped project/type routing and metadata ownership refuse.

The composition root creates
[pluginhost.New](../../internal/server/pluginhost/load.go) with explicit native
instances and bindings, then connects the returned mutators, validators and
post callbacks to the existing Writer. Extension methods are separate from
the bounded RPC registry: an embedding application must supply the registry,
schema and authorization adapter before exposing one over a network.

## Compose callbacks into the configured server

`composition.Open` now owns the native callback host as well as storage and
transports. The trusted embedding application supplies `Config.NativePlugins`
and `Config.NativeBindings`; `Runtime.bind` constructs the host and attaches its
mutators, validators and post subscribers to the same Writer used by the
configured HTTP, WebSocket, stdio adapter and MCP requests. A caller cannot
install native code or change these grants through RPC or server YAML.

The selected store supplies durable native state only when it implements
`driver.PersistentCheckpointStore` and returns `Persistent() == true`. The host
receives that exact store and the runtime's existing active lease. A memory
checkpoint or a persistent-looking driver name cannot satisfy a plugin's
`requires.state: durable` declaration. Constructor or binding failures close
the acquired resources; normal shutdown drains transports, cancels the runtime
lifetime and joins the native host, then releases the lease and closes storage.

The standalone path remains `plug server` -> `LoadConfigFile` -> `Open` ->
`Listen`. Its YAML selects storage and listener settings, while compiled-in
callback registration belongs to the trusted embedding host. It does not
interpret YAML as executable plugin registration or create an authentication
provider from `auth.mode`. The merged workflow service adapter continues to
use its existing provider and standard-tool interfaces.

Standalone YAML may select an existing protected ontology identity through
`ontology.source`; `LoadConfigFile` passes that locator to `composition.Open`,
which validates the durable source at store head before admitting requests.
The file supplies no ontology body, executable binding or authorization.
An embedding host with native/plugin validators must provide its actual
validating-pre inventory directly to `composition.Open`.

Configured-runtime regressions open real SQLite through this composition,
exercise admitted native callbacks and durable checkpoints, and reopen the
same database after shutdown. This boundary does not qualify arbitrary native
code, source loading, WASI, PostgreSQL failover, browser login or production.

## Capabilities and callback lifetime

Native plugins are trusted compiled-in code, **not runtime-isolated**. The host
checks capabilities, while static restrictions and review must prevent direct
filesystem or network access outside the Host.

| Phase | Supported host behavior |
| --- | --- |
| Mutate / validate | Masked proposal and declared, pinned SnapshotReader reads join the commit guard. No graph/state writes, mutable state reads or egress. RPC-shaped Host.Call reads refuse; use the reader. |
| Post | Committed-revision reader, process-local instance-state CAS, configured durable state and granted HTTPS egress. Graph writes and RPC reads refuse. Failure is a warning on the committed result. |
| Extension method | Declared method/resource checks and optional caller authorization. Granted outbound core reads require the supplied Dispatcher; outbound graph writes refuse pending a field-grant adapter. |

Readers enforce field grants and budgets of at most 32 point reads and four
queries per binding call; omitted budgets use those defaults. Values are
detached and masked. A retained callback map cannot widen patch targets, and an
undeclared patch refuses before graph publication.

Calls serialize per native instance. Default host budgets are 250 ms for
mutate and 500 ms for validate/post, with 1 s for extensions; a binding cannot
raise the 1 s ceiling. The Writer may impose a shorter budget. Context
cancellation releases completed work, but a timed-out native callback retains
its instance slot until it exits. Pre failures fail closed by default; an
explicit fail-open pre binding produces a visible subscriber_skipped warning.
Post failure produces post_subscriber_failed after commit.

State is per instance and process-local CAS, not a durable event checkpoint.
DurableState uses the configured persistent checkpoint store and active lease.
A constructor may retain its durable facade, but every operation requires the
current host-issued callback context. Pre calls refuse mutable reads and
writes; completed, canceled, stale-context and closed-host operations refuse.
Detaching caller cancellation cannot extend the host's invocation lifetime.
Egress requires exact declared HTTPS host:port, public DNS checks on every
request/redirect and at most 10 MiB per body with a 1 s timeout. Secret
injection, WASI/WASM artifacts, plugin UI and automatic
network extension exposure are unsupported by this host snapshot.

## Run the real native fixture

The complete [fixture](../../sdk/go/conformance/fixture.go) and
[manifest](../../sdk/go/conformance/manifest.json) demonstrate registration.
[Pipeline tests](../../internal/server/pluginhost/pipeline_test.go) connect it
to the real Writer and memory driver, including normalization, refusal, no-op
and replay without re-running a post callback:

~~~~sh
bazel --ignore_all_rc_files test //internal/server/pluginhost:pluginhost_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900
~~~~

This is package behavior evidence only. It does not certify a sandbox, a
plugin ABI, source-built artifacts, deployment, performance or production
network policy.
