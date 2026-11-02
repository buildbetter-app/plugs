# Workflow template invocation and ephemeral lifecycle

This document is the public B26 contract for the repository workflow plugin. It
keeps the historical B26 archive refusal as provenance while defining the
adopted Option C producer. A workflow run is a plugin resource, never a bead,
memory row, collaboration session, or wisp row.

## Invocation

`x.workflow.invoke` accepts a template reference, a project, a checklist, and
an explicit `mode` of `permanent` or `ephemeral`. Omitting `mode` is refused;
callers must choose the plane at the invocation boundary. A checklist item has
an opaque client label, title, and optional type. The request is authenticated
with the current principal and project in `_meta` and requires an idempotency
key.

Permanent invocation lowers each checklist item through the ordinary
`bead.create` writer. The returned items are ordinary durable beads with the
existing revisions, leases, history, and permission behavior. A permanent run
has no workflow-state row.

Ephemeral invocation allocates an opaque typed run identity and one typed item
identity per checklist item. The run and items are stored in the workflow
plugin's durable state capability. Their bodies, comments, claims, and
receipts never enter the core record/version/history tables. The result carries
`plane: "ephemeral"`, `kind: "workflow.run"` or `"workflow.item"`, the plugin
alias, project, run identity, and lifecycle state so a caller cannot mistake an
item for an ordinary bead.

## Ordinary tool behavior while live

An active ephemeral item is visible in `bead.ready` for its project and is
eligible for `bead.claimNext`. The same current principal/project checks used
by ordinary service methods apply to `bead.get`, `bead.comment`, `bead.close`,
and claim operations. An item reference is routed by its typed workflow prefix;
it is never looked up in the durable bead record table. A claim is a workflow
lease in plugin state, not a core bead lease.

Comments and claim state are durable in the plugin plane and survive process
restart. A stale generation or lease cannot overwrite a newer state. A denied
read or mutation fails closed without disclosing the item body, comments, or
run identity.

## Completion and history boundary

Closing the final checklist item atomically marks the run complete and deletes
the run, item, comment, claim, and receipt payloads from plugin state. A
replayed receipt may return the original terminal response for its retention
window, but it never recreates a live run. Completed ephemeral items are absent
from `bead.ready`, `bead.get`, and all permanent history/as-of/export reads.
There is no promotion, demotion, mixed-plane dependency, or relationship
bridge in this contract; those operations return typed `unsupported`.

## Restart, expiry, and fencing

The plugin state capability is backed by the selected driver's durable
checkpoint store. Restart restores live runs and their generation. Every
mutation compares the state generation and the item generation, and the host
fences a lost storage lease. The provider does not apply an arbitrary active
run TTL: unfinished checklists remain live until explicitly completed. Receipt
retention is bounded by the existing 24-hour idempotency window; receipt GC may
remove only receipts for ended runs and never deletes an unfinished run.

