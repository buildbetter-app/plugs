# Parented ordinary create completion contract

Done requires flat parented create scaling on memory, SQLite, and PostgreSQL,
identical create outputs, two independent source PASSes, exact-head named focus
PASS, canonical whole PASS, hosted green, and landing by the sole train. Source,
baseline, focused, whole, hosted, and landing evidence remain separate. B04's
walk beyond 1000 remains held until landing; this package runs no B04 walk.

The supplied public fixture/retry helpers are copied from diagnostic
94ac7526f5dcf58137e52c8eb8ac95a39d833d72 into this independent product package.
The diagnosis package is untouched. Its independently verified allocator cause
is allocatorObservations repeatedly decoding its loaded bead set, rather than
only siblings. Existing materialization counters do not count those repeated
core DecodeDocument calls. The secondary guard collector selected existing
siblings even when their parent's current status had no child guard. No new
baseline or diagnosis is part of these regressions.

## Frozen quantitative oracle, version 2 (root 09:51/09:56 clarification)

Freeze this contract before executing changed code. For EACH engine, run the
unchanged original 2004-create workload in batches of 50 (last batch four), then
flat and common-parent-from-start controls of exactly 2500 measured creates in
50-create batches. Parented has one separately committed, untimed anchor.
Original final records/versions/changesets/head must be 2004/2004/41/41; flat
2500/2500/50/50; parented 2501/2501/51/51. All batches complete exactly once,
with zero busy errors and zero retries. Existing retry policy and the literal
2-second preCtx in pipeline/apply.go remain unchanged.

For N in {500,1000,1500,2000,2500}, F_N and P_N are medians of exactly the last
five completed 50-create batches ending at N. Retain ALL batch times and the
literal batch ending at each milestone. For EACH engine and EACH later
N in {1000,1500,2000,2500}, BOTH inequalities must hold:

    P_N / F_N <= 1.5 * (P_500 / F_500)
    P_N <= max(1.5 * P_500, P_500 + 1.5 * max(0, F_N - F_500))

Every parented milestone median must also be below 2000ms. Timing is the supplied
callSeedWithRetry wall time: request/response retention included, attempt
callbacks excluded. HTTP attempt durations are retained separately. The anchor,
receipt validation, durable snapshots, replays, and SQL reopen checks are outside
the create timer and create-counter delta, identically for both controls. Both
changed controls use the same HTTP observer and no CPU sampler. Baseline PG's
profile overhead and growing flat-control overhead remain explicit differences;
2500 baseline failure was projected, not observed.

## Fixed executed-work bounds, fixed before execution

For every parented 50-create batch, at every store size:

| Engine | numeric_child_id_candidates maximum | payload_records_materialized maximum | general Query calls |
| --- | ---: | ---: | ---: |
| memory | 3200 | 1000 (where exposed) | first batch 1; later 0 |
| SQLite | 1275 | 1000 | first batch 1; later 0 |
| PostgreSQL | 1275 | 1000 | first batch 1; later 0 |

The unchanged root-only anchor publication does not establish a semantic graph
certificate. The first parented batch therefore performs exactly one existing
initialization census; its actual Query call, payloads, and time remain inside
the measured first batch. Later batches must make zero actual general Query
calls. The table describes the unchanged anchor-only scaling workload, so its
first batch also retains the 1000-payload cap. Memory Query/payload observer
events are absent; the real-reader wrapper counts actual calls/returned rows
directly and reports materializations as NOT_EXPOSED, never as a proven zero.

TestParentedCreateInitializationCensus separately compares 0 and 2000 unrelated
ordinary flat creates before the same ordinary anchor and 50-child batches.
No bootstrap or certificate publication path is added. Before the first child
the certificate must be absent, and after real graph validation it must be
current. One initialization census makes exactly 1 actual Reader.Query call
at 0 unrelated items and exactly 3 at 2000 unrelated items plus the anchor:
documents() uses Limit=1000 and follows NextCursor. Every page's actual calls,
returned rows and exposed materializations are recorded separately; SQL events
must be positive and reach the enclosing HTTP observer. The first large-store
batch has an explicit payload-cap exemption: this one existing census can
materialize the uncertified store, and its size-dependent cost is stated plainly.
The later batch must have zero general Query calls and retain the steady work
caps. SQL reopen is followed by another real 50-child batch; census recurrence
is reported as observed, with complete page accounting if it recurs. No behavior
is changed to force that observation. These are additional regressions, not a
change to the original/flat/parented 2004/2500/2500 workloads or timing oracle.

The new numeric candidate event is emitted at the owning loop: every memory AVL
node visited (including navigation/rejected nodes), every SQL candidate returned
before eligibility filtering, and every driver/core staged-map entry visited
before kind/parent filtering. The request context must reach the real capability.
All engines must show positive candidate events; SQL engines must also show
positive payload events and positive selector events after the first batch. An
empty/absent observer earns no bounded-work proof. Other existing raw counters
are retained, with their original definitions. These are executed visits, not
distinct identities; they do not measure repeated core document decoding or SQL
executor visits.

Source arithmetic: the core staged overlay contributes at most 0+1+...+49=1225
candidate visits per 50 creates. Only genuinely created identities enter this
overlay; a read-only parent or previously existing edited record cannot add a
new identity. Existing live IDs still consume numbers when deleted in-batch,
and created-then-deleted IDs remain consumed. Current SQL lookup seeks at most one committed
candidate per allocation in these controls: 1225+50=1275. The immutable memory
AVL has at most 2500 indexed child identities; a conservative height bound 18
allows at most 36 navigation nodes per seek: 1225+50*36=3025 < 3200. No driver
staged-map entries exist during the preparation reader lookups in these controls.
The body bound is deliberately separate: a 50-operation, one-parent, no-links
proposal has at most 51 relevant images; preparation point reads, parent/label
reads, guard collection, graph validation and locked observations have fewer
than 20 materializations per operation (1000 per batch). The new identity lookup
materializes zero bodies; current-status-open guard collection selects zero
siblings. Source review and the real no-general-Query regression are conjunctive
with these numeric bounds, rather than inferring absence of a walk from timing.

SQL work additionally requires actual SQLite EXPLAIN QUERY PLAN and PostgreSQL
EXPLAIN ANALYZE/BUFFERS JSON at 2500 children, using each driver's exact
currentChildIDSQL constant and default planner settings. SQLite must SEARCH the
records_numeric_child ordered index without record SCAN or temporary sort;
PostgreSQL must use records_numeric_child_live below LIMIT, one actual candidate,
one loop, zero removed rows, and no sequential/bitmap scan or sort. Low Go
counters alone are insufficient. SQL plans are retained in named test logs/XML.

Index maintenance is also source-reviewed: memory path-copies only AVL search
paths; SQL engines maintain expression B-trees on existing tables' ordinary
writes/deletes/restores. Index construction at store open occurs outside create
requests. No table, deadline, retry, workload limit, permission, or history
representation is added or changed. Existing memory publication map cloning is
unchanged and is also represented in the paired flat timing control.

## Semantic and authoritative proof

TestParentedCreateFlatPublicRPC drives the actual anonymous HTTP RPC, real
engines, durable receipts, full version/history conservation at milestones and
completion, every-batch replay equality, and SQL close/reopen equality. Expected
IDs, parent, type, project, fields/status, fixed createdAt, SetBy, revision, and
history creates are independently authored from request inputs, not copied from
actual responses. All raw requests/responses, attempt readbacks/counters, batch
times, milestone medians, and final counts are undeclared outputs.

TestOrdinaryChildCreateUsesRealIdentityIndexWithoutQuery delegates to each real
engine's capability, refuses general Query, proves request-context propagation,
and checks stale-counter reconciliation plus unrelated-kind/tombstone occupancy.
A separate batch checks create-then-delete number consumption under the unchanged
general deletion guard path. TestNumericChildIdentitySemantics covers 9/10,
leading zeros, zero/nonnumeric/direct-child shapes, Unicode parent identity,
overflow/max-uint64, excluded/staged/deleted replacements, rollback and historical
visibility on all three engines. Storage query-plan tests, persistent AVL
snapshot/height tests, and open/guarded-old-parent collector tests complete the
source-specific regressions. Existing allocator/guard tests remain intact.

One author freezes SHA/tree/parents and sends this same oracle to both retained
reviewers. Source review, authorized focus, and zero-extra canonical whole run
in parallel after fresh Actions remaining>1500/no-outage precheck. One detached
collector owns each run; cancelled, incomplete, RED, skips and NOT_RUN are kept
literal. No unchanged rerun or duplicate collector is authorized. The author
publishes one App PR with Nikhil requested; only the sole train may land it.
