# Native title guard

This compiled Go plugin preserves `fields.title`, collapses Unicode whitespace
into a separate normalized title, and derives its word count. A validator
refuses normalized titles outside 1–80 Unicode code points. For example,
`"  Review\t the\n change "` derives `"Review the change"` and `3`.

The plugin imports only `pluginapi`, `sdk/go` and the standard library. It
performs no network, filesystem, secret, state or graph I/O. The embedded
[manifest](manifest.json) registers three subscribers; it declares no methods,
egress or secrets. An optional in-process observer receives detached values
read through the post callback's actual committed snapshot. Observer errors
produce `post_subscriber_failed` warnings while preserving the committed write.

## Fields, scope and grants

The current host and ontology support concrete, unqualified field keys. This
example represents its `x.nativeguard` namespace with the flat keys
`x_nativeguard_title` (string) and `x_nativeguard_words` (integer). Dotted field
keys, nested ownership and metadata ownership are unsupported; the example does
not broaden those rules. The [ontology overlay](ontology.json) requires both
derived fields and assigns them to the exact `nativeguard-enrich` binding ID.
Compile it over the default ontology with that owner in the plugin catalog.
The schema marks both fields read-only and checks their recorded setter.

| Binding ID / subscriber | Phase | Reads | Owns | Point/query ceilings | Deadline |
|---|---|---|---|---|---|
| `nativeguard-enrich` / `enrich` | mutate | `fields.title` | `fields.x_nativeguard_title`, `fields.x_nativeguard_words` | 1 / 1 | 250 ms |
| `nativeguard-title` / `title` | validate | `fields.x_nativeguard_title` | none | 1 / 1 | 500 ms |
| `nativeguard-observe` / `observe` | post | both derived fields | none | 16 / 1 | 500 ms |

All bindings use alias `nativeguard`, instance `default`, and fail closed. The
pre callbacks make no snapshot reads; post uses at most 16 point reads and no
queries. A zero read limit means the host default, so the manifest explicitly
sets the smallest positive ceilings for unused read categories.

Supported operations are bead `create`, `patch` and `delete`, on at most 16
distinct live beads per write. Deletes need no derived patch or post read.
Repeated operations on a bead use one final image and one patch, preserving the
host's overlap checks. Other operations or resource kinds fail explicitly.
The manifest API has no operation filter; this bound is enforced in the
callbacks. The current host refuses project/type routing, so these bindings are
global. Compose them only into a host whose bead types provide the declared
title and derived fields. A widened grant fails host initialization; a narrowed
ownership grant that omits an emitted field fails before commit.

## Go host composition

There is no installation command for this example. A Go host links
`github.com/buildbetter-app/plugs/examples/plugins/native-guard` and calls
`nativeguard.Native("default", observer)`. Inside this repository, compose the
returned `sdk.Native` with the existing host and Writer:

```go
native, err := nativeguard.Native("default", observer)
if err != nil { return err }
h, err := pluginhost.New(pluginhost.Config{
    Natives: []sdk.Native{native},
    Bindings: grants, // exact table above; no Project or Type routing
})
if err != nil { return err }
w := pipeline.New(store, activeLease,
    pipeline.WithOntology(schemaHook),
    pipeline.WithMutators(h.Mutators()...),
    pipeline.WithValidators(h.Validators()...),
    pipeline.WithPostSubscribers(h.Posts()...),
)
commit, err := w.Apply(ctx, principal, changeSet, writeMeta)
```

Here `store` and `activeLease` are the actual memory store and its acquired
lease, and `schemaHook` implements the existing `port.Ontology` interface using
the actual ontology compiler/evaluator. The complete compiled composition and
its small test-only schema adapter are in [integration_test.go](integration_test.go).
The host/service/Writer/driver packages used by these repo-local tests are
internal implementation details; they are not additional public plugin APIs.

A same-value write still runs pre admission. It creates a durable no-op receipt,
keeps graph/head/history unchanged and suppresses post. Replaying either receipt
through a fresh Writer returns the stored response without rerunning callbacks.
Post failures and their warnings also survive that replay. Native code is
trusted build-time code; the host checks its I/O and output grants but does not
isolate arbitrary native code.

## Run the real integration

```sh
bazel --ignore_all_rc_files test //examples/plugins/native-guard:native_guard_test \
  --jobs=2 --lockfile_mode=error --test_timeout=900 --test_output=all \
  --nocache_test_results --test_arg=-test.v
```

These tests use synthetic in-process data with the actual plugin host, ontology,
Writer and memory driver. They verify persisted derived values/provenance,
create/update and atomic batches, validator rollback, grant/target/overlap
refusals, committed post reads and warning replay, no-op receipts, copy
isolation, Unicode/resource bounds, unsupported scopes and cancellation. They
do not exercise `plugin.install`, plan/apply installation, WASI/WASM, dynamic
source builds, an installed CLI, external services, durable restart of the
memory process, or runtime/performance acceptance.
