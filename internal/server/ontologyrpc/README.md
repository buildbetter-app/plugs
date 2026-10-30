# Ontology RPC bindings

Call `ontologyrpc.Bind(registry, ontologyService)` before `rpc.New` snapshots
the registry. This adds `ontology.get`, `ontology.describe`, `ontology.schema`,
`ontology.check`, `ontology.history`, `ontology.diff` and `ontology.plan` through
`Registry.Register`, using unchanged canonical descriptors, parameter admission
and result validation. When the service's composition root supplies a
`pipeline.ProtectedApplier`, it also binds `ontology.apply`; otherwise the
method is absent from discovery. It composes with the existing `rpc.BindService`
without editing that package or changing its runtime discovery snapshot.

An explicit `project` parameter takes precedence over `_meta.project` and the
server's default project. An explicit empty project selects the base for the
optional-project methods. `ontology.schema` requires a nonempty project as in
its canonical method shape. History continuation uses its existing integer
`after`; no new cursor parameter or invented descriptor is introduced.

Apply requires the retained `planDigest`, an exact `expect` array and the
transport idempotency key. Anonymous calls must send a `presence_check` whose
challenge and proof are both the plan digest. Authenticated calls require a
live interactive User session; services, API-key identities and agents receive
`human_presence_required`. The HTTP transport carries the opaque authn
identity into dispatch so `authn.RequireInteractiveUser` checks the live
session. Successful and replayed calls return the canonical Commit result,
with replay status in the response `_meta` member.

The composition root owns the actual protected source locator, active lease,
current read authorization and validating-pre manifest supplied to
`ontologyops.New`. The host must explicitly declare no bindings only when
none are installed. The command/transport composition roots are separate
owners and are not changed by this package.

The integration tests cross actual in-process JSON-RPC bytes, canonical
dispatch/schema validation, the domain write pipeline, memory storage, strict
source decoding, compilation, migration planning and historical reconstruction.
They verify discovery and unsupported apply, source invalidation, required and
unknown parameters, duplicate members, large integer refusal and real source
history projection. No fake RPC service or successful apply stub is used.

Run `bazel --ignore_all_rc_files test //internal/server/ontologyrpc:ontologyrpc_test
--jobs=2 --test_output=all --test_timeout=900`.
