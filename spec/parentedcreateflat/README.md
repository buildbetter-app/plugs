# Parented ordinary create completion contract

Done = bounded allocator work and the v4 regression guards on memory, SQLite,
and PostgreSQL with identical create outputs and sole-train landing, proven by
two independent source/focus PASSes, own exact-head canonical whole SUCCESS,
and train integrated hosted/cumulative-whole landing gates; source approval or
an oracle edit alone does not count. B04's walk beyond 1000 stays held until
landing; this package runs no B04 walk.

The public fixture/retry helpers were copied from diagnostic
94ac7526f5dcf58137e52c8eb8ac95a39d833d72 into this independent package. The
verified cause was allocatorObservations repeatedly decoding its loaded bead
set, not only siblings. No new baseline or diagnosis is part of this package.

## Frozen quantitative oracle, version 4 (root 13:29 ruling)

The ontology guard's graph is outside this fix. hierarchy_indexed.go,
ontology.go, their existing tests and internal/core/ontology/BUILD.bazel are
byte-identical to main bfb571a6cf9e40631fd12d4a6accbe8a65180e1e. The spec label
is a test_suite selecting an internal test target using ontology's existing visibility;
there is no ontology visibility/dependency change. Both source reviewers verify
those bytes. The sibling-enumeration optimization is deferred, and no worker
analyzes claim interleaving.

For EACH engine run the unchanged original 2004-create workload in batches of
50 (last batch four), plus flat and common-parent-from-start controls of exactly
2500 measured creates in 50-create batches. Parented has one separately
committed, untimed anchor. Original records/versions/changesets/head must finish
2004/2004/41/41; flat 2500/2500/50/50; parented 2501/2501/51/51. The dedicated
focus sets `PARENTED_CREATE_FLAT_STRICT_FOCUS=1`. With that opt-in, every
measured batch commits once, every batch except the single original-workload
transition below requires zero busy errors/retries, and the wall-time guards
are asserted. Canonical whole, hosted and train runs leave the flag unset: any
batch may retry a typed retryable `busy` response at most five times, with every
attempt and elapsed time logged; wall-time guards are logged but not asserted.
Every committed batch still has the unconditional output, durable-state,
allocator, payload, census and conservation assertions below. The literal
2-second preparation and workload limits are unchanged.

B_N is the literal completed 50-item parented batch ending at N, without median
smoothing. For EACH engine both conditions must hold:

    B_2500 <= 5 * B_500
    B_2500 < 2000ms

ALL raw batch times, flat-control times and milestone last-five medians are
retained; the medians are descriptive and are not substituted into these v4
inequalities. Milestone medians also retain the earlier below-2000ms assertion
when strict focus is enabled. All five literal milestone batch times are logged
per engine in every mode. The inequalities are asserted only by strict focus;
whole and hosted runs retain the measurements without asserting them. These are
regression guards for the allocator fix alone, not constant whole-request time.
Remaining growth comes from the unchanged guard reading the parent's children
on each create. PostgreSQL takes about 0.8 to 1.3 seconds per 50 creates between
500 and 2500 children in the retained v3 measurement; that remedy is deferred.
If either v4 inequality fails, report measured numbers to root; never change the
guard to satisfy it. The v2 ratio/flat-difference and v3 3x/below-1000ms bounds
are superseded by the explicit 13:29 ruling; historical v3 RED stays RED.

Timing remains callSeedWithRetry wall time, including ordinary request/response
retention and excluding attempt callbacks. HTTP durations are separate. Anchor,
receipt validation, snapshots, replays and SQL reopen checks remain outside the
create timer/counter delta on both controls. All changed controls use the same
HTTP observer, without CPU sampling. Historical PG profiling overhead remains
a baseline difference; baseline failure at 2500 was projected, not observed.

Under whole-suite load, a PostgreSQL parented batch can cross the 2-second
preparation deadline and return a typed retryable `busy`; the allocator fix
reduces but does not remove that condition. The remedy is the deferred work to
reduce the unchanged ontology guard's parent-child reads.
On a dedicated runner, the original PostgreSQL workload measured about 1.8 s at
1,550 siblings.

Known measured cost: PostgreSQL plain creates are 5 to 33% slower in the
500-to-2,000-item range, under 1 ms per create, from the two expression indexes.

## Retry policy and original workload exception (root 21:36 ruling)

With strict focus enabled, the first batch containing children after the
original flat phase may retry only typed retryable busy, on any engine, for at
most five attempts. The PostgreSQL original workload's sibling-growth seed
batches have the same exception in both modes because the unchanged guard read
can cross the preparation deadline; fixed-path controls remain strict in focus.
Without strict focus, every batch may retry that same typed busy response at
most five times. Each attempt's elapsed milliseconds and status are retained and
logged. The existing retry helper keeps identical request bytes and idempotency
key. The transition remains reported separately and outside the parented
control's timing regression guards. The same refusal checks cover the
initialization census and ordinary child regression batches in this package.
Any error other than typed retryable `busy` fails immediately.

The three top-level creates in this mixed batch take their IDs from the committed
attempt, checked against the actual durable records. The fixture's external ID
generator advances during refused preparation and is not rewound, so those three
IDs can differ from their pre-request predictions. Independently authored fields,
ref shape/branch, attribution, revision, versions, history and exact replay still
apply. Later references follow those committed parent identities, preserving the
same 2004 operations and hierarchy shape. Each child's expected next suffix stays
independent: the original workload starts each parent at number 1. After every
refusal, in every run mode, assert unchanged durable counts, versions/history,
head, real child maxima and parent-counter state; the committed child IDs must
have no gaps or consumed
numbers from refused attempts. No other output expectation is relaxed.

## Constant allocator work and separately measured hierarchy work

Every parented 50-create batch, at every store size, requires exactly 50 actual
real-reader MaxChildNumber calls, exactly 50 core numeric identity lookups,
zero allocatorObservations bead DecodeDocument invocations and zero payload
materializations inside MaxChildNumber. The owning core fallback loop emits
every allocator decode; positive numeric candidate/lookup events, real-reader
wrappers and noQuery/context regressions establish observer liveness. This is
not a claim that all document decoding in the request is zero.

| Engine | numeric_child_id_candidates maximum | identity-scope payload maximum | actual general Query calls |
| --- | ---: | ---: | ---: |
| memory | 3200 | NOT_EXPOSED | first batch 1; later 0 |
| SQLite | 1275 | 0 | first batch 1; later 0 |
| PostgreSQL | 1275 | 0 | first batch 1; later 0 |

The numeric bounds are unchanged from v2. The core created-only overlay visits
0+1+...+49=1225 identities. Read-only parents and edits cannot add identities;
base-live and created-then-deleted identities still consume numbers. SQL seeks
at most one committed candidate per allocation: 1225+50=1275. Memory's immutable
AVL has at most 2500 child identities in the scaling control; conservative
height18 and at most36 navigation nodes per seek give1225+50*36=3025<3200.
Counts include rejected, staged and navigation candidates at their owning loops.
SQL actual plans below independently rule out a hidden SQL scan.

Root's 13:15 amendment removes the 1000-payload cap from parented batches:
restoring main's guard also restores its growing child reads. For EVERY parented
batch on EACH SQL engine, total exposed payload_records_materialized divided
by the number of creates must be at most 0.08 * children + 10, where children
is the completed child count at the batch end. Record that value and bound at
every milestone. This is the main-baseline envelope, not constant request work;
no hierarchy-scoped subtraction is made. The generic counter and dedicated SQL
hierarchy image counter remain distinct, as described below. Memory's absent
payload accounting is NOT_EXPOSED. Actual MaxChildNumber scope must still
materialize zero payloads, and SQL payload/selector events must be positive.
The 1000-payload batch cap remains for the flat control and wholly flat batches
in the original workload's flat phase. Its original transition batch retains
the same mixed operation order and full raw accounting.

The unchanged guard enumerates existing children. Record its real-reader calls,
returned records and elapsed time on EACH engine, for every batch. SQL owning
fetchHeaders/fetchImages loops additionally report actual header/image query
page counts, exact images materialized and elapsed nanoseconds (including query,
fetch, scan and close; these are client elapsed times, not server execution CPU).
Those SQL image counts are separate from the pre-existing generic payload
counter, which does not expose that path. These page/time observations overlap
the hierarchy-reader duration and must not be summed as disjoint CPU costs.
Memory has no SQL pages: report NOT_APPLICABLE, with actual reader calls/records/
time instead. Remaining hierarchy growth is expected and reported at
500/1000/1500/2000/2500; it is not credited as allocator work or optimized away.

## Existing initialization census, unchanged

Root-only anchor publication leaves the semantic graph certificate absent.
The first parented batch performs one existing initialization census; all its
time and counters stay inside the first measured batch. documents() retains
Limit1000/NextCursor: exactly1 actual Reader.Query call for anchor-only and
exactly3 for2000 unrelated flat items plus the anchor. Every later batch makes
zero actual general Query calls. No bootstrap/certificate path is added.

TestParentedCreateInitializationCensus separately exercises both0/2000 unrelated
items. Record each actual page's returned rows/materializations and forward
its observer to the enclosing HTTP request. SQL events must be positive. The
first large-store batch retains its named payload-envelope exemption: its one census
may materialize the entire uncertified store. Certificate absence before the
first child and freshness after real validation are asserted. After SQL reopen,
run another real50-child batch and report observed census recurrence, including
complete pages if it recurs. Do not force the behavior. The six-line FOLLOW-UP
states the large-store initialization risk without implementing bootstrap.

## Semantic and authoritative proof

The public HTTP test independently authors child IDs, parents, type/project,
fields, status, fixed createdAt, SetBy, revisions and history creates from
requests; only the three transition top-level IDs use the durable checks above.
Retain raw request/responses, attempts, durable milestone/final records/versions/
history, every-batch exact committed replay, and byte-identical SQL reopen
readbacks. Keep the genuine PostgreSQL direct-Read/plain/anchor/first-child
regression and conditional optional-capability wrappers.

Real-engine noQuery/context tests retain stale counters, occupancy/tombstones,
ordinary reparent certification and create-delete consumption. Numeric tests
retain9/10, leading zeros, zero/nonnumeric/direct-child shapes, Unicode parent,
overflow/maxuint64, exclusions/staged/deleted replacements, rollback and history.
SQL plan tests execute each exact currentChildIDSQL at2500 children with default
planner settings: SQLite ordered records_numeric_child SEARCH without record
SCAN/temp sort; PostgreSQL records_numeric_child_live under LIMIT, actual one
candidate/one loop/zero removed rows, no sequential/bitmap scan or sort. Index
maintenance remains path-copy AVL or B-trees on existing tables; no new table,
deadline, permission, result or history representation.

Both retained reviewers receive this identical frozen oracle/head before code
execution. Focus includes the parented spec, allocator/storage/core targets and
//internal/integration/postgresacceptance:postgresacceptance_test, selecting
TestB26PublicMixedParentReadinessSQLite/PostgreSQL and retaining literal PASS
for both roots and their required subtests. Protected tests are names/status
only. Focus and zero-extra canonical-whole run alongside source review after a
fresh Actions remaining>1500/no-outage check. Critical focus forces on-demand;
canonical-whole defaults on-demand. One detached collector owns each gate;
historical RED/BLOCK/SKIP/incomplete/cancelled evidence remains literal. No
manual hosted dispatch: train integrated hosted plus cumulative whole gate
landing. No self-approval or merge by the author.
