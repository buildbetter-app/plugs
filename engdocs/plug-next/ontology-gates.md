# Ontology apply, gates, and projection status

The [wire catalog](../../internal/api/wire/catalog.json) is the authority for
method names, parameter shapes, effects, permissions, idempotency, and error
codes. The current main composition binds the seven read and plan methods
described in [Ontology RPC](ontology-rpc.md). A descriptor alone does not make
a handler available: `ontology.apply` and the gate methods are conditional on
their composition roots, and an unbound call returns `method_not_found`.

## Check and plan

`ontology.check` and `ontology.plan` remain the read-only operations described
in [Ontology RPC](ontology-rpc.md). Check returns `{ok, problems, drifted}`;
plan compiles one base or overlay replacement against the complete inventory
and the host's actual validating-pre manifest. The retained plan binds its
source, digest, complete row images, caller, incarnation, CAS, and callback
manifest. `Ready` is not an apply authorization. Changes to those guards,
retention, or process state return `ontology_plan_stale`; the existing guide
lists the other exact plan refusal codes.

## Protected apply

When a composition root supplies the canonical protected Writer, it may add
`ontology.apply` to discovery. Its catalog shape is:

- required `planDigest`, required `expect`, required `humanPresence`, and
  transport `_meta`;
- write effect, `admin` permission, and required idempotency;
- a canonical `Commit` result.

The `humanPresence` object is either
`{mode:"interactive_session",sessionId,challenge,proof}` or
`{mode:"presence_check",challenge,proof}`. The challenge is bound to the
plan digest and method. An anonymous caller must use `presence_check` with the
plan digest as both challenge and proof. An authenticated caller must be a live
interactive `user` session with `interactive_session`; API keys, services,
agents, iframe calls, and other-origin bearer calls do not satisfy this proof
and return `human_presence_required`.

Apply reloads the retained digest and requires exact precondition equality. It
rechecks the source, every planned row image, the candidate, caller context,
and installed validating-pre bindings before the Writer repeats those guards
under one transaction. The source record and all planned row images publish
together, with complete row bytes preserved. A failed compare-and-set returns
the catalog's `precondition_failed` or `ontology_plan_stale` refusal; no
partial source, row, history, event, or projection write is left behind.

The ontology source is a protected branch-zero `ontology` or `ontology/...`
record. Ordinary changes cannot target protected records or reserved core
metadata; they return `protected_record`. Apply is the dedicated path, rather
than `tx.apply`, `setMeta`, or a history revert. A durable idempotency receipt
replays the original commit with response-only `_meta.replayed:true`; changing
the request under the same key returns `idempotency_key_reused`.

Read-only roots retain the explicit behavior in [Ontology RPC](ontology-rpc.md):
`ontology.apply` is absent from discovery and a call returns
`method_not_found`. Policy revalidation, configured-authorizer transition
decisions, gate resolver facts, directive wire admission, and multi-source
planning remain unsupported by this seam.

## Gate approval and resolver reports

The conditional gate service adds these catalog methods:

| Method | Parameters | Effect and permission | Result |
| --- | --- | --- | --- |
| `gate.approve` | required `id`; optional `expect`, `_meta` | write, `approve`, idempotency required | `GateResult` |
| `gate.report` | required `id`, required `state`; optional `details`, `expect`, `_meta` | write, `write`, idempotency required | `GateResult` |

`GateResult` always contains `commit`, `state`, and `approvals`. `details` is
resolver-owned evidence. It is retained for `gate.report` but is not interpreted
by core. Both methods lower through the same Writer compare-and-set transaction
as the bead image and any workflow transition.

`gate.approve` is for a manual gate (`manual` or `builtin.manual`). The
`min_approvals` parameter is a positive integer and defaults to one. The
default count mode is `humans`: a `user` counts as itself, while an `agent` or
`service` counts as its sponsoring user. Two agents sponsored by the same user
count once, regardless of their tokens or labels. A retained anonymous
observation is not discarded, but it cannot satisfy a multi-human threshold;
an anonymous multi-approval attempt returns `forbidden`. A duplicate counted
identity is a no-op and does not add a second approval.

When the threshold is reached, the evaluator sets `state` to `resolved` and
lowers to the type's `close` slot. `gate.report` is for a non-manual resolver;
the implemented outcomes are `success` (resolved/`close`) and `failure`
(`failed`/`fail`). Reporting a manual gate is `forbidden`. A terminal
`resolved` or `failed` gate returns `precondition_failed`; a stale gate may be
replaced by a resolver report but cannot be approved. Missing workflow slots
or unmet ontology guards return `ontology_guard_failed`, before any partial
transition is published.

The Writer owns `gate_kind`, `gate_state`, `gate_approvals`,
`gate_approval_count`, and `gate_details`. Ordinary create/update requests
that forge or change these fields return `ontology_use_slot_verb`; a gate verb
must make the change. The durable approval array can contain anonymous or
duplicate observations, while `gate_approval_count` is the evaluator's
distinct counted value. Keyed retries return the byte-identical `GateResult`
with response-only replay metadata; changed parameters under that key return
`idempotency_key_reused`.

## Projection status is read-only metadata

Views and watches may carry a `projection` status object containing the
binding, durable checkpoint, failure, halted/stalled flags,
`lagAcceptedThrough`, and an operator-facing `resume` tuple. The status reader
loads the persisted projection checkpoint, current event-log incarnation, and
active lease. It never infers readiness from a process-local map, acknowledges
delivery, advances a checkpoint, registers a projection, or writes resume
state.

Unknown or non-projection bindings, malformed checkpoints, a lost lease, and
an incarnation mismatch refuse the read. A missing checkpoint is represented
at revision/index zero in the current incarnation; it is not silently reset
from another history. Status-only changes update response metadata and do not
invalidate a signed view cursor or force a view-watch reset. Actual history
gaps, visibility changes, or binding changes retain the reset behavior described
in [Events and delivery checkpoints](../plug-runtime/events.md).

## Migration and full-history limits

Ontology planning is a bounded migration-like check over the admitted stored
document model. It is not a general schema migration or a permission-policy
deployment. The wire method has no directives parameter; policy revalidation,
gate resolver decisions, and multi-source planning remain explicit refusals.
Above 5,000 affected rows, it returns `ontology_migration_too_large` before
applying callbacks. A missing or denied inventory row is a refusal, never a
silently dropped row or a partial success.

`ontology.history` reconstructs protected-source revisions from the changeset
log under current read authorization. An unavailable history range is
`history_gone`; an ontology history read does not certify a restore archive.
The [offline converter boundary](../../internal/core/dumpformat/README.md)
accepts only its `offline-head-and-source-archives` scope and is not a full A6
restore certificate. Full-history restore is a separate
`restore.full-history` capability, not an inference from an ordinary `history`
read. A target that cannot preserve that mode must refuse with
`restore_violation` before lease or transaction admission; a current-image
restore into a nonempty target remains `restore_requires_empty`.
