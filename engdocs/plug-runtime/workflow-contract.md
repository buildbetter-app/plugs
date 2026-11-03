# Workflow template invocation and ephemeral lifecycle

This document is the public B26 contract for the repository workflow plugin. It
keeps the historical B26 archive refusal as provenance while defining the
adopted Option C producer. A workflow run is a plugin resource, never a bead,
memory row, collaboration session, or wisp row.

## Invocation

`x.workflow.invoke` accepts a template reference, a project, a checklist, and
an explicit `mode` of `permanent` or `ephemeral`. Omitting `mode` is refused;
callers must choose the plane at the invocation boundary. A checklist item has
an opaque client label, title, and optional type. The transport authenticates
the current principal; `_meta` carries the project, anonymous-agent label, and
required idempotency key. Caller identity is never accepted from parameters.
The manifest grammar is the existing `manifest_version: 1` form with a
plugin provision `{kind: "methods", prefix: "x.workflow", methods: {invoke:
{permission: "write", resource: "params.project"}}}`; the canonical
OpenRPC/catalog descriptor has the same parameter/result schemas and
idempotency declaration. The host rejects a workflow instance that lacks the
durable capability rather than silently creating process-local state.
Native registration additionally requires a trusted invocation adapter that
resolves the current caller and metadata from the host context and routes
permanent invocation through the ordinary writer. Missing adapters refuse
registration; plugins do not synthesize principals or idempotency keys.

Permanent invocation lowers each checklist item through the ordinary
`bead.create` writer. The returned items are ordinary durable beads with the
existing revisions, leases, history, and permission behavior. A permanent run
has no workflow-state row.

Ephemeral invocation allocates an opaque typed run identity and one typed item
identity per checklist item. Workflow references are valid only as
`workflow:item/<opaque-id>` branch-zero ID references; they cannot be supplied
as keys, parents, links, history refs, or ordinary bead IDs. The run and items
are stored in the workflow plugin's durable state capability. Their bodies,
comments, claims, and receipts never enter the core record/version/history
tables. The result carries `plane: "ephemeral"`, `kind: "workflow.run"` or
`"workflow.item"`, the plugin alias, project, run identity, and lifecycle state
so a caller cannot mistake an item for an ordinary bead.

## Ordinary tool behavior while live

An active ephemeral item is visible in `bead.ready` for its project and is
eligible for `bead.claimNext`. The same current principal/project checks used
by ordinary service methods apply to `bead.get`, `bead.comment`, `bead.close`,
and claim operations. An item reference is routed by its typed workflow prefix;
it is never looked up in the durable bead record table. A claim is a workflow
lease in plugin state, not a core bead lease.

Comments and claim state are durable in the plugin plane and survive process
restart when the selected store is SQLite or PostgreSQL (or another
composition-admitted persistent `CheckpointStore`). The process-local memory
driver refuses new ephemeral invocations because it cannot make a process
restart guarantee. A state provider without an explicit positive persistence
marker is also refused. Memory hosts omit the ephemeral provider, preserving
ordinary ready lists and permanent invocation. A stale generation or lease
cannot overwrite a newer state.
A denied read or mutation fails closed without disclosing the item body,
comments, or run identity.

## Completion and history boundary

Closing the final checklist item atomically marks the run complete and deletes
the run, item, comment, and claim payloads in the same plugin-state CAS. A
canonical terminal receipt starts a fresh 24-hour completion-based replay
window: it contains only the scoped operation identity, digest, terminal
marker, item or run ID, expiry, and a response-safe identity/commit result. It
never retains titles, fields, comments, claims, or a run body. Replaying an
equal request returns that identity/commit-only terminal receipt and never
recreates a live run; a changed request or scope returns
`idempotency_key_reused`. Completed ephemeral items are absent from
`bead.ready`, `bead.get`, and all permanent history/as-of/export reads.
The plugin retains identity-only lifecycle audit entries for invocation,
completion, lifecycle operations, and receipt GC. These entries contain the
receipt scope and typed run/item IDs but never titles, fields, comments, claims,
or body; they are operational provenance and do not make an ephemeral item
part of permanent bead history.
There is no promotion, demotion, mixed-plane dependency, or relationship
bridge in this contract; those operations return typed `unsupported`.

## Restart, expiry, and fencing

The plugin state capability is backed by the selected driver's durable
checkpoint store. Restart restores live runs and their generation. Every
mutation compares the namespace generation and item generation, and the host
fences a lost storage lease. The provider does not apply an arbitrary active
run TTL: unfinished checklists remain live until explicitly completed. Receipt
retention is bounded by the existing 24-hour idempotency window; receipt GC may
remove expired receipts only when their run is already ended (or the receipt
does not reference a run), and never deletes an unfinished run. The workflow
provider's `workflow/state` document is the single checkpoint-CAS unit for
these maps and its identity-only audit ledger.

`bead.ready` uses a stable ID-ascending workflow phase after the core ready
phase. Its signed continuation cursor binds method, the complete effective
principal (including the anonymous-agent label when present), project, filter,
store incarnation, ontology and authorization generation; a workflow-phase bit
keeps the offset from being confused with a core offset. The workflow phase is
bounded by the standard page limit and ordered by immutable item ID, so a
continuation offset cannot reorder or skip an item within one authorization
scope. `bead.claimNext` uses that same order and binding. `bead.get`,
`bead.comment`, and `bead.close` accept the typed workflow ID through the
ordinary service dispatcher and apply the current principal/project ownership
check before exposing or mutating plugin state.

The store incarnation is part of every receipt and cursor binding. Ordinary
SQLite and PostgreSQL reopen preserves that persisted incarnation, so live and
terminal receipts replay across process restart within their retention window.
A restore into a different incarnation refuses a retained prior-incarnation
operation with `idempotency_key_reused`; it cannot turn a scoped receipt miss
into another run. Live operational state remains recoverable from the
persistent checkpoint.
