# Ontology reads, plans, and protected apply over JSON-RPC

Claims on this page describe the current implementation, based on
**`9d1845ceef104e8847f2f45cfc7ef6b16072837f`**. Start with the
[domain operations](../../internal/core/ontologyops/README.md) and
the [registry binding](../../internal/server/ontologyrpc/README.md); the
binding uses the existing [canonical catalog](../../internal/api/wire/catalog.go)
and [registry](../../internal/server/rpc/registry.go).

## Bind only implemented methods

Call `ontologyrpc.Bind(registry, service)` before `rpc.New` snapshots the
registry. It registers the seven read/plan methods below, and adds
`ontology.apply` only when the supplied service exposes the canonical
protected Writer capability:

```text
ontology.get       ontology.describe    ontology.schema
ontology.check     ontology.history     ontology.diff
ontology.plan
```

Read-only composition roots therefore keep `ontology.apply` out of discovery;
an apply request there returns `method_not_found`. A protected composition root
advertises the additional method only after it has supplied the source locator,
active lease, validating-pre manifest and canonical Writer seam.

The server accepts the canonical named-object parameters and validates the
canonical result at the byte boundary. An explicit `project` overrides
`_meta.project`; an explicit empty optional project selects the base document.
History continues with its integer `after`; schema requires a nonempty
project. The composition root supplies the protected-source locator, active
lease, current read authorizer, and validating-pre manifest.

The following is the actual in-process request shape used by
[`integration_test.go`](../../internal/server/ontologyrpc/integration_test.go):

```json
{"jsonrpc":"2.0","id":"ontology-test","method":"ontology.get","params":{}}
```

Its successful result is a canonical ontology object with the stored source
revision. The same fixture sends `ontology.plan` with a complete document and
a revision precondition; it receives an immutable plan digest, one actual
stored row image, `ready: true`, and no violations. `ontology.history` projects
only changes to the protected source, and `ontology.diff` reconstructs the two
historical effective documents as an RFC 6902 root replacement or an empty
patch.

Run the actual byte-RPC fixture and print its named subcases:

```sh
bazel --ignore_all_rc_files test //internal/server/ontologyrpc:ontologyrpc_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no \
  --test_output=all --test_timeout=900 --test_arg=-test.v
```

The test crosses in-process JSON-RPC bytes, service writes, memory storage,
strict source decoding, compilation, migration planning, historical rows,
protected publication, complete row-image preservation, durable replay and
result-schema validation. It also checks discovery and malformed parameters;
the [domain tests](../../internal/core/ontologyops/service_test.go) cover row,
source, caller, and binding-manifest invalidation.

## Plans are read-only

`ontology.plan` replaces one base or project overlay, compiles it, enumerates
the complete stored bead inventory, and invokes the host's real validating-pre
manifest. It retains the candidate source, full row images, source hash and
revision, ontology digest, CAS, caller, store incarnation, and callback
manifest. A fresh read rechecks those guards. A changed source, row,
caller, or binding manifest returns `ontology_plan_stale`; current permission
denials also refuse reuse. A
process restart or bounded retention eviction requires replanning.

Missing or denied rows are refusals, not an incomplete successful plan.
Unknown document members, unsupported directives, missing installed-authorizer
transition facts, and projectless rows refuse. `NoValidatingPre()` is valid
only when the host explicitly has no validating-pre bindings.

On a read-only host, a request such as

```json
{"jsonrpc":"2.0","id":1,"method":"ontology.apply","params":{"planDigest":"anything"}}
```

returns `method_not_found`; a bearer credential or caller-supplied presence
object does not change that. When the protected Writer capability is bound,
`ontology.apply` accepts the retained digest and exact CAS, requires an
idempotency key, and commits the source and all planned row images in one
transaction. Anonymous callers must type the plan digest as a
`presence_check`; authenticated callers must be a live interactive `user`
session. API keys, services, agents and other-origin/iframe calls cannot
satisfy the proof. Replays are resolved from the Writer receipt and return the
original commit with response-only replay metadata. Policy revalidation,
projection updates, gate resolver facts, directive wire admission and
multi-source planning remain unsupported and return typed refusals.
