# Workflow template invocation and ephemeral lifecycle

This document is the public B26 contract for the repository workflow plugin. It
keeps the historical B26 archive refusal as provenance while defining the
adopted Option C producer. A workflow run is a plugin resource, never a bead,
memory row, or collaboration session.

## Invocation

`x.workflow.invoke` accepts a template reference, a project, a checklist, and
an explicit `mode` of `permanent` or `ephemeral`. Omitting `mode` is refused;
callers must choose the plane at the invocation boundary. A checklist item has
an opaque client label, title, optional type and fields, labels, metadata,
parent, typed auxiliary links and an explicit blocked flag. The transport authenticates
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
as keys or permanent history refs. Parents and links may use typed workflow
IDs while live; mixed-plane links are owned by the workflow checkpoint. The run and items
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
the active workflow item takes precedence over a colliding durable row.
After the active item is deleted, point reads fall back to the ordinary row.
A claim is operational state in the plugin checkpoint, surfaced by the standard
get/claim response, without a core durable lease or version row. Explicit
`filter.includeEphemeral=false` excludes workflow rows from ready and claimNext,
including terminal fallback, and leaves colliding durable rows visible.

Comments and claim state are durable in the plugin plane and survive process
restart when the selected store is SQLite or PostgreSQL (or another
composition-admitted persistent `CheckpointStore`). The process-local memory
driver refuses new ephemeral invocations because it cannot make a process
restart guarantee. A state provider without an explicit positive persistence
marker is also refused. Memory hosts keep the registered invocation method and
ordinary ready lists available for permanent calls, but do not install the
ephemeral provider; an ephemeral call returns
`plugin_durable_state_unavailable` before publishing workflow state. A stale
generation or lease cannot overwrite a newer state.
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
Demotion receipts follow the same rule: terminal replay returns the completed
run/item identity and revision with empty body fields. Promotion erases the
removed item's receipt projections in its atomic move. Invocation replay for
a partially promoted checklist contains only its remaining live items; final
promotion yields the completed run identity with no items.
The plugin retains identity-only lifecycle audit entries for invocation,
completion, lifecycle operations, and receipt GC. These entries contain the
receipt scope and typed run/item IDs but never titles, fields, comments, claims,
or body; they are operational provenance and do not make an ephemeral item
part of permanent bead history.
Promotion and demotion are typed plugin operations under
`x.workflow.run.promote` and `x.workflow.run.demote`. Promotion accepts a live
workflow item and an existing ordinary target with the same project and type.
The service reads the source and target, authorizes the complete ordinary
proposal, and materializes fields, labels, metadata, original comments and
permitted relationships. The source removal, inbound/outbound relationship
rewrites, core target write and checkpoint CAS share one lease-fenced storage
transaction. An invalid target or auxiliary SQL failure publishes neither plane.
Demotion accepts only a source reference, template and label. The service reads
the canonical source itself; unlinks, ordinary source deletion, and the new
workflow checkpoint image commit together. Public caller projections cannot
replace the stored source body. Neither move transfers an active claim; claimed
endpoints return `busy`. Ephemeral identities never receive core versions or
leases; the ordinary target write/source deletion retains ordinary history.

Current permitted moves exclude attachments, imported comment provenance,
demoting a durable parent with durable children, and promoting a workflow-parented
item. These variants return typed `unsupported` before publication. This explicit
boundary remains unqualified for any card obligation requiring those variants.
Workflow children of a promoted item keep its new ordinary parent identity;
inbound edges are rewritten once, metadata participates in relation identity,
and workflow child counters remain in the same atomic checkpoint image.

Mixed-plane `bead.link` and `bead.unlink` validate ordinary endpoint existence
and project before the provider CAS. Both workflow endpoints are owner-checked.
The checkpoint owns mixed edges, and ordinary reads append them to ordinary
links. Ready and direct claim resolve ordinary blocker status using the canonical
ontology. Durable get/ready, direct claim/claimNext/close and ordered `tx.apply`
close operations consult the same active workflow dependency predicate. Ordinary
write decisions run after receipt replay and pre-image authorization, and their
checkpoint generation is fenced in the core transaction. A concurrent mixed-edge
edit forces a fresh decision, including on receipt-only no-op commits. A closed ordinary
blocker releases its workflow dependent. In the reverse direction, workflow
claim, claimNext (including terminal null), and close resolve all durable
endpoints and waits-for children from one pinned core graph. The host checks
that snapshot's store, incarnation and head atomically with checkpoint CAS and
the lease fence; a concurrent core change rejects publication with
`write_conflict`. Drivers without this atomic capability return typed
`unsupported` before publication. This guarded metadata write creates no core
version, lease or receipt. Successful receipt replay precedes new blocker reads.
The stored explicit `blocked` flag is also a blocker for ordinary direct claim,
close and ordered transaction close after promotion; clearing it permits the
normal ontology checks. Completing a run removes its mixed edges and clears
other-run workflow parents that referred to deleted items.

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
Workflow state access outside ordinary-write guards performs eligible receipt GC and records the
identity-only deletion audit in the same lease-fenced CAS. An idle store may
retain expired identity-only receipts until its next workflow access; expiry
does not depend on physical cleanup to stop replay. A cleanup conflict fails
that access with `write_conflict` without publishing a partial map.
Ordinary-write blocker guards read without GC or checkpoint publication. On a
nonpersistent store they contribute no workflow facts and leave ordinary tools
available; ephemeral invocation still returns its typed capability refusal.
A workflow/global `write_conflict` from `bead.claimNext` is returned to the
caller. A fresh call may publish a newly fenced terminal null for an empty queue;
receipt replay returns that null without publishing again.

`bead.ready` uses a stable ID-ascending workflow phase after the core ready
phase. Its signed continuation cursor binds method, the complete effective
principal (including the anonymous-agent label when present), project, filter,
store incarnation, ontology and authorization generation; a workflow-phase bit
keeps the offset from being confused with a core offset. The workflow phase is
bounded by the standard page limit and ordered by immutable item ID. Workflow
continuations additionally require the provider checkpoint generation and the
earliest matching claim expiry: either can change ready membership within one
authorization scope. The provider's `ReadySnapshot` returns the sorted items,
same-read checkpoint revision (including any receipt cleanup), and a
`ValidUntil` deadline for the earliest future expiry of an otherwise matching
claimed item. No deadline is needed when no matching lease can change membership.
The service adapter must refuse a continuation when its snapshot binding has
changed, before applying the previous offset; it must also apply resource
filters before page slicing. Provider snapshot evidence alone does not prove
the service cursor binding. `bead.claimNext` uses the same ready order.
`bead.get`,
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

### Status and child-gate lowering

Workflow state uses `open`, `claimed`, and `closed`; concrete project ontology
status keys never become workflow state. Demotion maps unstarted and unleased
started work to unclaimed `open`. Completed/canceled sources are already complete
one-item checklists: the atomic move deletes the ordinary source and publishes
only completed replay identity, with no live workflow payload. Deferred/triage
sources return typed `unsupported` until a workflow scheduling bridge exists.
Promotion preserves the target's ontology status. Open or expired-claim sources
require an unstarted ordinary target; a closed source requires the target's
canonical close slot. Clients prepare a target with normal lifecycle verbs;
the move cannot bypass ontology transitions by patching a status field. Migration
copies only the stored explicit blocker flag and preserves dependency edges;
a computed ready/get blocker never becomes a permanent flag on the target.

Every `waits-for` edge evaluates its child gate, including absent/default metadata
and `also_blocks=false`. Durable spawners use the canonical ontology graph resolver.
Workflow spawners use Parent/legacy parent-child membership in the checkpoint.
Explicit all-children/default gates require no open child; any-children also allows
one terminal child; `also_blocks` additionally requires the spawner to be closed.
Absent metadata retains the terminal-spawner-or-no-open-children fallback. Ordinary
hierarchy operations do not admit durable children under workflow parents.
