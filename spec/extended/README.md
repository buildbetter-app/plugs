# Extended connected acceptance

Done for this slice means runnable independent assertions through the actual
service, ontology adapter, Writer and memory driver, with JSON-RPC bytes for
bound methods, a focused PR, and clean exact-head local Bazel evidence. An
assertion that fails remains runnable and blocks readiness; a build analysis
failure is not a product test result. This package does not certify full cards,
recorded Beads parity, runtime performance, or cutover readiness.

Only `spec/extended/**` and the exact source-manifest additions belong to this
change. The original `spec` scenarios and `internal/acceptance` implementation
remain unchanged. Minimal visibility grants are separate owner dependencies.

Run the ordinary connected target:

```sh
bazel --ignore_all_rc_files test //spec/extended:acceptance \
  --jobs=2 --test_output=all --test_timeout=900 --lockfile_mode=error
```

Final evidence uses the shared `ci-local-v2.py` recorder with whole-tree tests
as the reverse-dependency superset. Actual XML must contain no skips. Historical
RED logs remain in the task report even after a dependency repair passes.

## Composition and observable proof

The fixture uses the existing `internal/acceptance` finite ID sequence, fixed
UTC clock, strict JSON-pointer projections, and real in-process wire trace.
That clock is installed in the real memory store, Writer, and service. A real
one-hour storage lease keeps explicit five-minute advances inside its lifetime;
bead claims retain the production five-minute TTL. No wall-clock sleep drives
a deadline assertion, and no test opens a `WriteTx`, seeds alternate state,
implements product decisions, or replaces a service/RPC handler.

All graph setup goes through the real Writer. Retained scenarios explicitly use
direct Writer calls for selected trusted slot-lowering assertions. Connected
B24 acceptance instead uses the production `bead.defer` and `bead.reopen` RPC
bindings through the real service, ontology adapter, Writer, and memory store.
No test replaces a service or RPC handler. RPC exchanges, including typed
refusals, are saved in Bazel undeclared outputs as `*.rpc.json`.

Refusals compare all committed record bodies and revisions, complete version
lists, the entire changeset log and gapless head, and the graph sentinel. Failed
write keys must have no durable receipt. No-op comparisons allow a new receipt
but forbid record, version, log, head, or sentinel changes. Positive batch checks
verify final stored bodies, operation-result revisions, and one version per
changed record. The concurrent opposite-edge scenario proves two real callers
cannot jointly publish a cycle; it is not a load/performance benchmark.

## Supported scenarios and full-card obligations

The authoritative cards are [work.md](../../engdocs/design/plug/spec/behaviors/work.md)
B03/B06/B08 and [surfaces.md](../../engdocs/design/plug/spec/behaviors/surfaces.md)
B24; A1/A3/A4 supply Plug's layering, category/slot, and atomic-write semantics.
Assertions are independently authored from those contracts, not recorded
reference goldens. Keep/deviate below is the source disposition, not a parity
certificate. No full card is complete in this package.

| Card | Implemented boundary exercised | Full-card obligations still open |
| --- | --- | --- |
| B03, deviate P10/P41/D24/A3/P22 | Compiled configured keys across all six categories and both completed outcomes; category-driven ready and claim behavior; custom claim/close/reopen slots; same-value, different-slot, and unknown status patch refusals with complete rollback; noninitial/unknown create refusals; blocker categories evaluated in the blocker's own project. | Real protected ontology plan/apply, immutable-key archival and whole-definition overlay lifecycle, malformed configuration through startup/apply, persistent status-catalog projections and SQL behavior, status-specific timestamps, importer presets, and recorded legacy active-status disagreement. No legacy 50-status cap is imposed on Plug without a Plug contract. |
| B06, keep with A4 | Acyclic ordinary/conditional/mixed graphs; self-edge and cycle refusal after earlier create/patch operations; no partial record/history/receipt publication; duplicate-edge no-op; nonblocking loops; final-graph validation when a batch adds then removes a cycle edge; concurrent opposite blocking edges with exactly one committed winner. | A bound deterministic diagnostic/report API, lowest-ID rotation, collapsed parallel report paths, honest unreadable members/counts, tracks expansion and waits-for distinctions, parent/ancestor scheduling rules, durable/ephemeral cross-plane graphs, and reference recording. `relates-to` is the supported nonblocking kind tested here; it does not certify tracks semantics. |
| B08, deviate P23b/A3/AZ9 | Template refusal, fixed required-field guard, guards over a same-target patch plus optional reason comment's final image, blocker transitions including own-project ordinary/conditional outcomes, reopen reblocking, stale-CAS close rollback, fresh-key already-closed no-op, and exact replay response metadata without durable mutation. | Parent/open-child observations with actual parented records, evidence attachment success, resolved gate success, authenticated transition/override permission, foreign-holder/force policy combinations, first-close/status timestamps, transitive and cross-plane derived settlement, and item-indexed multi-target close. The current slot envelope supports one target patch and optional same-target comment, not arbitrary multi-target slot batches. Genuine fresh-key close/replay failures remain blockers, not accepted deviations. |
| B24, intentional deviation with untimed defer and explicit reopen | Production RPC defer accepts `{id, reason?, expect?}` only, commits the category transition and optional same-target reason comment atomically, and honors revision expectations. A Beads-shaped target instant is refused without a write. Backlog stays outside ready as controlled time advances with no implicit wake; stale reopen expectations and direct status-patch wake both refuse without durable mutation; explicit RPC reopen restores readiness with exact revisions and history. The retained claim-expiry boundary assertion remains adjacent clock evidence. | The immutable Beads evidence retains exactly six runtime-clock outcomes with fixed past/future inputs and omitted timestamps: expired dated and ephemeral defers auto-wake and clear on ready, a dateless and a future defer stay deferred, an expired dated defer is claimable and becomes in-progress, and reopen clears defer into open-ready. Pinned Beads permits an omitted reason and rejects only a supplied blank reason; the recorded dateless case supplies a reason while the dated cases omit it. Controlled clock advances, an exact Go/SQL boundary, past-target warnings, history-off wake, and other full-card requirements remain unrecorded and unqualified. Stale-age views and invalid-age refusal remain a separate unqualified surface. This package does not claim reference parity, SQL-driver qualification, scheduling, performance, or cutover readiness. |

The additional paging scenario changes an unread ready bead and inserts a new
one between requests. Continuation must observe current ready membership and
report the current revision; `history.query` must instead retain the requested
historical rows, status and revision. The already-consumed prefix stays stable,
so this assertion does not invent keyset/seek behavior for the live offset
cursor. A changed filter must refuse with `cursor_invalid`, and reads must not
mutate storage. This discriminates live list behavior from the former false
snapshot assertion without changing the frozen original acceptance sources.

The Plug defer slot's supplied reason is an optional ordinary committed comment;
omission is valid. A parked bead remaining parked after controlled time advances
is the adopted untimed behavior, and only an explicit reopen restores a workable
category. The separately recorded Beads timed projection remains intentionally
different; no deadline field, wake scheduler, or SQL-clock twin is implied here.

Source-card command prose/envelopes remain dropped under P46. Missing runtime
surfaces remain explicit obligations; there are no skipped tests, fabricated
goldens, alternate stores, or successful unsupported stubs in this package.
