# Option C workflow durable capability

The workflow plugin receives a narrow durable state capability from the host.
It does not receive a driver, transaction, SQL handle, filesystem path, or
raw database access. The host binds one capability to the plugin alias and
instance and stores it through the selected driver's unversioned
`CheckpointStore`.

The capability exposes JSON values by opaque key, per-key compare-and-set
generations, and a bounded prefix listing. The host serializes the complete
plugin map in one checkpoint CAS, so a restart observes either the old complete
map or the new complete map. A CAS conflict is a typed fencing conflict and is
never silently retried as a new operation. Returned bytes are copies.

Workflow keys are namespaced below `run/` and `receipt/`. A run value contains
the typed run identity, project, template, checklist item state, comments,
claim generation, and lifecycle state. No run value is written as a core
record, version, event body, lease row, dump record, or history version.

Writes carry the current principal, method, idempotency key, request digest,
and plugin-state generation. A matching receipt replays the original result;
the same key with a different digest returns `idempotency_key_reused`. Replay
is authorized again for the current principal. Replay after completion returns
the terminal receipt only and cannot resurrect the deleted run.

The host's checkpoint lease and the provider's run generation fence takeover.
Read and write paths fail closed when the store lease is lost or the current
principal is unavailable. Prefix queries are project-scoped, bounded, and
filtered before response construction, so denied rows do not affect cursors or
counts. There is no mixed-plane query: a request that combines a plugin run
reference with a durable bead reference returns `unsupported`.

GC is provider-owned. It removes ended-run bodies and stale receipts in one
state CAS. It never deletes an unfinished run, and it does not erase the core
durable audit/history retained by P45. Cleanup may leave a small terminal
receipt until the 24-hour replay window ends; it does not preserve the run body.

