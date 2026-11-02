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
never silently retried as a new operation. Returned bytes are copies. The
capability carries an explicit persistence marker: SQLite/PostgreSQL adapters
are restart-capable, while the memory adapter refuses ephemeral creation rather
than claiming that process-local checkpoints survive a process restart.

Workflow keys are namespaced below `run/` and `receipt/`. A run value contains
the typed run identity, project, template, checklist item state, comments,
claim generation, and lifecycle state. No run value is written as a core
record, version, event body, lease row, dump record, or history version.

Writes carry the current principal and principal digest, anonymous-agent label,
project, plugin alias/instance, store incarnation, method, idempotency key,
request digest, and plugin-state generation. The canonical receipt shape is:
`{scope, digest, runId?, itemId?, terminal, expiresAt, result}`. A matching
live receipt replays the original response; the same key with a different
digest or any changed scope returns `idempotency_key_reused`. Replay is
authorized again for the current principal. Completion atomically replaces
body-bearing invocation/claim results with identity-only terminal results and
deletes all run/item/comment/claim payloads. Replay after completion therefore
returns only the bounded terminal receipt and cannot resurrect the deleted run.

The host's checkpoint lease and the provider's run/item generations fence
takeover. Read and write paths fail closed when the store lease is lost or the
current principal is unavailable. Prefix queries are project-scoped, bounded,
filtered by owner before response construction, and sorted by immutable item
ID. The standard `bead.ready` adapter uses a signed cursor with a workflow
phase bit; its binding includes the method, full principal, anonymous agent,
project, filter, store incarnation, and authorization generation. There is no
mixed-plane query: a request that combines a plugin run reference with a
durable bead reference returns `unsupported`.

GC is provider-owned. Completion performs the deletion and terminal-receipt
sanitization in one state CAS, and repeated completion/replay is idempotent.
Later GC removes expired terminal receipts (and only runless receipts); it
never deletes an unfinished run, and it does not erase the core durable
audit/history retained by P45. Cleanup may leave a small terminal receipt until
the 24-hour replay window ends; it does not preserve the run body.
