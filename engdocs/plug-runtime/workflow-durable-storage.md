# Option C workflow durable capability

The workflow plugin receives a narrow durable state capability from the host.
It does not receive a driver, transaction, SQL handle, filesystem path, or
raw database access. The host binds one capability to the plugin alias and
instance and stores it through the selected driver's unversioned
`CheckpointStore`.

The capability exposes an atomic `Read(key) -> (value, revision)` snapshot,
JSON values by opaque key, per-key compare-and-set generations, and a bounded
prefix listing. The host serializes the complete plugin map in one checkpoint
CAS, so a restart observes either the old complete map or the new complete map;
providers never pair a value from one snapshot with a revision from another.
A CAS conflict is a typed fencing conflict and is never silently retried as a
new operation. Returned bytes are copies. The
capability carries an explicit persistence marker: SQLite/PostgreSQL adapters
are restart-capable. An absent or false marker grants no restart guarantee;
the memory adapter refuses every durable operation with
`plugin_durable_state_unavailable`. The composition keeps the registered
invocation surface available for permanent calls while the absent provider
refuses ephemeral calls with that typed capability error.

The provider stores one JSON document at its capability key `workflow/state`
inside the host's plugin/alias/instance checkpoint namespace. Its `runs`,
`items`, `receipts`, and identity-only `audit` maps/slices contain the typed run
identity, project, template, checklist item state, comments, claim generation,
and lifecycle state. Run and item IDs use the `workflow:run/` and
`workflow:item/` typed prefixes; receipt keys remain opaque scoped operation
keys. No value is written as a core record, version, event body, lease row,
dump record, or history version.

Writes carry the current principal key and canonical-principal SHA-256 digest,
anonymous-agent label (empty for authenticated principals), project, plugin
alias, instance, store incarnation, method, idempotency key, request digest,
and plugin-state generation. The canonical persisted receipt shape is:
`{principal, principalHash, agent, project, plugin, instance, incarnation,
method, key, digest, runId?, itemId?, terminal, expiresAt, result}`. A receipt
for a live run remains replayable even after its initial operation window; a
terminal receipt is replayable only until its completion-based 24-hour window
ends. The same key with a different digest or any changed scope returns
`idempotency_key_reused`. Replay is authorized again for the current principal.
The store incarnation is part of that scope. SQLite and PostgreSQL preserve
the incarnation on ordinary reopen, so response-loss retries, including
terminal receipts, replay across process restart. A retained receipt from a
different incarnation explicitly returns `idempotency_key_reused` instead of
being treated as a miss that could create another run. Restored live state
can continue under new operation keys in the current incarnation.
Completion atomically replaces body-bearing invocation/claim/demotion results with
identity-only terminal results and deletes all run/item/comment/claim payloads.
Replay after completion therefore returns only the bounded identity/commit
receipt (never title, fields, comments, claims or body) and cannot resurrect the
deleted run. The same checkpoint retains an identity-only audit ledger for
workflow lifecycle operations, completion, and receipt GC. Audit entries carry
full receipt scope and typed IDs but no work-item payload; they remain plugin
audit provenance and are not core versions, events, dumps, or permanent item
history.

Promotion erases the removed item's body from its operation receipts in the
same checkpoint transaction. If other checklist items remain, invocation
replay returns only those remaining items and the still-active run. Final
promotion returns terminal invocation replay with no items. Demotion replay
after completion retains only the completed run/item identity and revision;
the item projection's body fields are empty and no live run is recreated.

The ordinary service passes its canonical full `bead.claimNext` selector to
the provider separately from the selected item reference. Selected claims,
receipt-only retries, and empty selections share that selector digest and the
same `{found, item}` stored result. A replayed empty result remains terminal
even when another item becomes ready. Completion sanitizes both that canonical
result and older bare-item claim receipts while preserving each operation's
original item ID and revision.

The host's checkpoint lease and the provider's run/item generations fence
takeover. Read and write paths fail closed when the store lease is lost or the
current principal is unavailable. Prefix queries are project-scoped, bounded,
filtered by owner before response construction, and sorted by immutable item
ID. The standard `bead.ready` adapter uses a signed cursor with a workflow
phase bit; its binding includes the method, full principal, anonymous agent,
project, filter, store incarnation, and authorization generation. Mixed-plane
dependency reads are represented as typed references on the workflow
checkpoint. A workflow source may link to an ordinary target and an ordinary
source may link to a workflow target; the bridge never writes a core version
or lease row for the ephemeral endpoint.

Promotion/demotion are coordinated by the trusted service. The host prepares a
bound namespace image, without publishing it or exposing a SQL/transaction
handle. The ordinary Writer stages that data through an optional
`CheckpointWriteTx` participant. SQLite and PostgreSQL check namespace generation
and publish the checkpoint inside the same fenced SQL transaction as ordinary
body/auxiliary writes, unlinks or deletion. Receipt-only no-op publication uses
the same checkpoint guard after a replay miss, without consuming a Head or
allocator revision. A replay never reapplies the prepared namespace CAS. SQL or
authorization failure rolls back both planes; a separate provider CAS followed
by a core write is not an atomic move.

The ordinary source/target is read and authorized by the service/Writer; caller
parameters cannot supply a replacement demotion projection. Original comment
principal/time fields are included in the proposed ordinary image before
authorization. Claimed endpoints refuse before a move. Unsupported attachment,
imported-comment and mixed-hierarchy variants are typed refusals before any
publication, as documented in workflow-contract.md. An active workflow ID wins
over a colliding durable row only while live and included; ordinary point reads
fall back after active deletion.

GC is provider-owned. Completion performs the deletion and terminal-receipt
sanitization in one state CAS, and repeated completion/replay is idempotent.
Later GC removes expired receipts whose run is already ended or absent. It
retains receipts attached to an unfinished run so that the run remains
idempotent and durable regardless of age; it never deletes that run or its
current task, comment, or claim state. It does not erase the core durable
audit/history retained by P45; the plugin audit ledger records its own
completion and GC boundary in the same CAS. Completion starts a fresh bounded
24-hour terminal replay window and does not preserve the run body.

Direct ordinary claim/close decisions read the workflow dependency checkpoint
only after ordinary receipt replay and pre-image authorization. The trusted host
returns a read-only namespace generation expectation, not a transaction handle.
That expectation enters the existing Writer checkpoint read set and is checked
inside the ordinary/no-op SQL fence; a concurrent edge or child-state change
invalidates the attempt. Historical as-of ordinary reads do not append current
workflow links or blocker state. Completed-source demotion terminalizes and
sanitizes its staged one-item run before atomic publication.
