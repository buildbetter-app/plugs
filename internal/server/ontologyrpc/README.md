# Ontology RPC bindings

Call `ontologyrpc.Bind(registry, ontologyService)` before `rpc.New` snapshots
the registry. This adds `ontology.get`, `ontology.describe`, `ontology.schema`,
`ontology.check`, `ontology.history`, `ontology.diff` and `ontology.plan` through
`Registry.Register`, using unchanged canonical descriptors, parameter admission
and result validation. It composes with the existing `rpc.BindService` without
editing that package or changing its runtime discovery snapshot.

An explicit `project` parameter takes precedence over `_meta.project` and the
server's default project. An explicit empty project selects the base for the
optional-project methods. `ontology.schema` requires a nonempty project as in
its canonical method shape. History continuation uses its existing integer
`after`; no new cursor parameter or invented descriptor is introduced.

`ontology.apply` has no handler and is absent from discovery. Calls receive
`method_not_found`; an API key or a client-supplied presence object cannot turn
it into a successful operation. Read-only planning returns `requiresHuman:true`
and an immutable report, with actual refusals in `violations`.

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
