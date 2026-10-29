# Authorization spike results

Run date: 2026-10-28 UTC, branch `spike/authz`, commit recorded in the draft
PR. The implementation is the standalone module [`spikes/authz`](../../../spikes/authz)
with `cedar-go` v1.8.0, `modernc.org/sqlite` v1.46.1, Go 1.26.10, and
Postgres 16 in Docker container `plug-swarm-authz`. The container was removed
after the run. The synthetic database has 100,000 beads, maximum depth 10,
persisted `(sort_bucket, sort_rank, sort_created, id)` keys, the P50/P95
measurements below, three warm-ups, and five measured iterations. Durations are
milliseconds and include consuming the complete result set. Q4 is the ordered
page, Q5 is the locked candidate plus permission recheck, and Q6 is grouped
counts over authorized rows.

The agreed count target before running was p95 <250 ms. The page target from
"The spike" was p95 <50 ms. Plan A is the grant/closure scope-first CTE; Plan B
walks the stored bead-order index and probes the closure/grant semi-join.

## Item 1: SQL plans

### SQLite

| Plan | Query | Visible | p50 ms | p95 ms | rows touched |
|---|---|---:|---:|---:|---:|
| A | Q4 | 100% | 116.463 | 140.102 | 50 |
| A | Q5 | 100% | 119.552 | 122.535 | 50 |
| A | Q6 | 100% | 128.802 | 153.071 | 100000 |
| B | Q4 | 100% | 0.186 | 0.205 | 50 |
| B | Q5 | 100% | 0.103 | 0.121 | 50 |
| B | Q6 | 100% | 188.673 | 204.178 | 100000 |
| A | Q4 | 1% | 204.518 | 215.440 | 50 |
| A | Q5 | 1% | 204.859 | 212.131 | 50 |
| A | Q6 | 1% | 206.830 | 215.114 | 1000 |
| B | Q4 | 1% | 0.196 | 0.209 | 50 |
| B | Q5 | 1% | 0.113 | 0.127 | 50 |
| B | Q6 | 1% | 202.131 | 210.403 | 1000 |
| A | Q4 | 0.1% | 215.924 | 226.224 | 50 |
| A | Q5 | 0.1% | 206.130 | 245.251 | 50 |
| A | Q6 | 0.1% | 214.139 | 238.389 | 100 |
| B | Q4 | 0.1% | 0.199 | 0.208 | 50 |
| B | Q5 | 0.1% | 0.189 | 0.249 | 50 |
| B | Q6 | 0.1% | 193.017 | 209.813 | 100 |

### Postgres 16

| Plan | Query | Visible | p50 ms | p95 ms | rows touched |
|---|---|---:|---:|---:|---:|
| A | Q4 | 100% | 268.871 | 270.378 | 50 |
| A | Q5 | 100% | 327.313 | 333.048 | 50 |
| A | Q6 | 100% | 264.290 | 273.681 | 100000 |
| B | Q4 | 100% | 245.211 | 250.600 | 50 |
| B | Q5 | 100% | 0.773 | 0.852 | 50 |
| B | Q6 | 100% | 243.919 | 253.092 | 100000 |
| A | Q4 | 1% | 355.533 | 390.373 | 50 |
| A | Q5 | 1% | 403.319 | 418.971 | 50 |
| A | Q6 | 1% | 346.998 | 352.026 | 1000 |
| B | Q4 | 1% | 0.615 | 0.701 | 50 |
| B | Q5 | 1% | 0.417 | 0.489 | 50 |
| B | Q6 | 1% | 334.598 | 342.064 | 1000 |
| A | Q4 | 0.1% | 325.381 | 345.319 | 50 |
| A | Q5 | 0.1% | 394.570 | 407.415 | 50 |
| A | Q6 | 0.1% | 339.978 | 348.369 | 100 |
| B | Q4 | 0.1% | 0.626 | 0.627 | 50 |
| B | Q5 | 0.1% | 0.357 | 0.421 | 50 |
| B | Q6 | 0.1% | 355.017 | 378.957 | 100 |

The page criterion does not pass across both engines and plans. Plan B is
sub-millisecond for sparse SQLite pages and Postgres pages below 1 ms at 1%
and 0.1%, but its Postgres 100% Q4 p95 is 250.600 ms. Plan A is 116--419 ms
for pages. Counts pass the 250 ms target for SQLite (all measured p95 values
204.178 ms or lower for B and 238.389 ms or lower for A), but fail on Postgres
(253.092--378.957 ms for B and 264.290--352.026 ms for A).

## Item 2: closure writes

The epic's closure contains exactly 50,001 rows (the epic plus 50,000
descendants). A normal write first counts the closure and refuses with
`closure_too_large` at the 5,000-row limit. The explicit migration-class path
rewrites both ancestor and cut closures in the same transaction:

| Engine | Operation | rows | transaction time |
|---|---|---:|---:|
| SQLite | reparent | 50,001 | 8,570.1 ms |
| SQLite | set `restricted` | 50,001 | 8,463.0 ms |
| Postgres 16 | reparent | 50,001 | 5,539.8 ms |
| Postgres 16 | set `restricted` | 50,001 | 5,553.9 ms |

The cut closure includes the nearest restricted ancestor and then stops, while
the full closure remains transitive. No closure rewrite is deferred.

## Item 3: CP7 policy fit

The module contains both representations in `policy.go` and parses each policy
with pinned `cedar-go` v1.8.0. The relationship form is shorter and direct:

```text
relation User.admin -> importBulkHistory on History
relation User -> read on Bead
relation Agent -> modify on Project when project in agent.sponsor.involved_in
```

The Cedar form is explicit about typed principals, actions and resources:

```cedar
permit (principal is User, action == Action::"importBulkHistory", resource is History)
when { principal.admin };
permit (principal, action == Action::"read", resource is Bead);
permit (principal is Agent, action == Action::"modify", resource is Project)
when { resource in principal.sponsor.involvedIn };
```

All three parse and fit the spike's level-1/2 subset. The compiler accepts
permit rules with typed principal/action/resource scopes and the two bounded
contract conditions (`principal.admin` and sponsor project membership). It
binds the principal only when evaluating the request. Unsupported conditions
are refused. The read rule compiles to a list predicate; the other CP7 rules
are per-item write/import checks. Relationship lines are 2.2--2.5 times
shorter in this fixture, while Cedar makes entity/action/resource types and
conditions more explicit. The Cedar shape is readable enough for a core
compiler, provided the subset remains deliberately small.

The differential test covers four principals, three actions and five resources
(60 requests): the principal-bound compiled list equals the per-item
`cedar-go` decision for every request. No mismatch or Cedar diagnostic error
was observed.

## Pass criteria and decisions

| Criterion | Result |
|---|---|
| Q4--Q6 page p95 <50 ms at every fraction on both engines | **Fail**: Plan A fails everywhere; Postgres Plan B Q4 at 100% is 250.600 ms |
| Grouped-count target agreed before run, p95 <250 ms | **Partial**: SQLite passes; Postgres fails |
| 50,000-descendant rewrite measured in one migration-class transaction | **Pass**: both operations measured on both engines |
| Normal closure write above 5,000 rows refuses `closure_too_large` | **Pass** |
| CP7 relationship and Cedar representations plus subset/readability | **Pass** |
| Compiled list equals per-item `cedar-go` decisions | **Pass** (60-request differential sample) |

The overall spike therefore fails its latency gate. The policy and correctness
parts pass, but the SQL implementation needs a storage/index pass before it can
support the stated page/count budgets.

## Recommendations

- **AZ1 — model location: do not adopt the Cedar core evaluator yet.** The
  policy fit and differential evidence support the proposed (v) architecture,
  but the mandatory list latency gate failed on both engines. Revisit
  authorization as the spike directs; if a follow-up fixes the storage plans,
  (v) becomes reasonable.
- **AZ3 — engine: conditional `cedar-go` v1.8.0.** Parsing, evaluation and
  the small compiler subset are clear and differential-correct. Keep the pin and
  subset design as the candidate, contingent on a passing SQL follow-up.
- **AZ4 — refuse uncompileable read rules at plan.** The read policy compiles
  and the implementation refuses an unsupported condition. The measured failure
  argues against adding a capped post-filter to hide the plan cost.
- **AZ7 — plugin-declared grant records as principal attributes.** The
  sponsor/involved-project policy fits the data contract, uses a bounded join,
  and the differential test agrees. Keep grants as dumpable plugin records;
  bind them as request principal attributes.

The latency result is the blocker for AZ1, while the evidence supports the
candidate directions for AZ3, AZ4 and AZ7. Revoke-during-write and watches were
outside this narrowed run as specified by A4/A6.


## Repair addendum — 2026-10-29 UTC

The original readout above is preserved as the failed baseline. It must not be
used as current evidence: its sparse corpus exposed all 100,000 beads, its
`rows touched` values were labels rather than measurements, Q5 claims rolled
back, closure timing omitted the transaction prefix, and its run was not
reproducible from an exact head. This addendum records the bounded repair run;
no target was changed and no baseline number was rewritten.

### Exact run contract

- Source head: `69f28caaf71a673296abe3120c60be5cb3af5aec`; the runner recorded a
  clean worktree.
- Matrix: SQLite and PostgreSQL 16; Plan A and Plan B; Q4, Q5 and Q6; visibility
  fractions 100%, 1% and 0.1%; exactly three warmups and 20 measured samples.
- Corpus: 100,000 beads, graph depth 10, epic 2 with 50,000 descendants, and
  50,001 closure rows including the epic. The grant projection exposes exactly
  100,000, 1,000 and 100 rows at the three fractions.
- Targets remain page p95 <50 ms and grouped-count p95 <250 ms.
- Toolchain: Go 1.26.10, `modernc.org/sqlite` 1.46.1, pinned `cedar-go`
  1.8.0, PostgreSQL 16.

The checked-in manifest is `spikes/authz/benchmark-manifest.json` with SHA-256
`54c6fbb54e057705b02fbb411a818411b32273da0165910fe1b4869023fd652b`. The exact
commands, with the PostgreSQL DSN supplied only through the environment, were:

```sh
cd spikes/authz && AUTHZ_ENGINE=sqlite AUTHZ_ITERATIONS=20 \
  AUTHZ_ARTIFACT_DIR=/tmp/plug-swarm/work/t2-repair-artifacts-20261029-v6/sqlite \
  ./run-benchmark.sh

cd spikes/authz && AUTHZ_POSTGRES_DSN=<environment-only> AUTHZ_ENGINE=postgres \
  AUTHZ_ITERATIONS=20 \
  AUTHZ_ARTIFACT_DIR=/tmp/plug-swarm/work/t2-repair-artifacts-20261029-v6/postgres \
  ./run-benchmark.sh
```

The runner recorded exit status 0 for both engines. Its artifact digests are:

| engine | source-files.sha256 digest | result.json SHA-256 | stdout.log SHA-256 |
|---|---|---|---|
| SQLite | `d15d6f472d1131f904b2ad2991ebd3787e44f56db368e9d190057765c8cd8744` | `be32256af5d69a848dd6b39d287d122acaff2910c22eea186c9c97e9386b27a9` | `67e52faf6dbf80f62ccd9d1a41452d6272af01b8a16f00aff2c6c50e12061504` |
| PostgreSQL | `d15d6f472d1131f904b2ad2991ebd3787e44f56db368e9d190057765c8cd8744` | `d5c34ebcfbdcba64cfca627f0e637270c0410657c2e8952b0f6d912d765ff061` | `3c09af376a627a7aaa82d87639b1540ff50e20ab77dd9c9d773302e2a1259406` |

`CandidateRows` below is measured SQL candidate cardinality after the query,
with its method stated per row. It is not a fabricated executor counter: the
portable `database/sql` interface used by both drivers does not expose one.
The returned-page and qualified counts are measured independently.

### Corrected SQL measurements — SQLite

| plan | query | visibility | visible | p50 ms | p95 ms | qualified | returned | CandidateRows | method |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| A | Q4 | 100% | 100000 | 114.406 | 118.990 | 100000 | 50 | 100000 | visible ready rows |
| A | Q5 | 100% | 100000 | 116.555 | 129.418 | 100000 | 1 | 100000 | visible ready rows |
| A | Q6 | 100% | 100000 | 124.487 | 141.887 | 100000 | 100 | 100000 | visible ready rows |
| B | Q4 | 100% | 100000 | 0.184 | 0.195 | 100000 | 50 | 50 | ready rows through returned key |
| B | Q5 | 100% | 100000 | 0.100 | 0.152 | 100000 | 1 | 1 | ready rows through returned key |
| B | Q6 | 100% | 100000 | 192.000 | 222.600 | 100000 | 100 | 100000 | all ready rows |
| A | Q4 | 1% | 1000 | 1.758 | 3.580 | 1000 | 50 | 1000 | visible ready rows |
| A | Q5 | 1% | 1000 | 1.726 | 1.922 | 1000 | 1 | 1000 | visible ready rows |
| A | Q6 | 1% | 1000 | 1.641 | 1.821 | 1000 | 100 | 1000 | visible ready rows |
| B | Q4 | 1% | 1000 | 60.264 | 63.345 | 1000 | 50 | 16717 | ready rows through returned key |
| B | Q5 | 1% | 1000 | 62.089 | 70.997 | 1000 | 1 | 16668 | ready rows through returned key |
| B | Q6 | 1% | 1000 | 306.959 | 347.435 | 1000 | 100 | 100000 | all ready rows |
| A | Q4 | 0.1% | 100 | 0.200 | 0.332 | 100 | 50 | 100 | visible ready rows |
| A | Q5 | 0.1% | 100 | 0.335 | 0.427 | 100 | 1 | 100 | visible ready rows |
| A | Q6 | 0.1% | 100 | 0.213 | 0.261 | 100 | 100 | 100 | visible ready rows |
| B | Q4 | 0.1% | 100 | 128.574 | 139.656 | 100 | 50 | 50018 | ready rows through returned key |
| B | Q5 | 0.1% | 100 | 48.807 | 51.033 | 100 | 1 | 16668 | ready rows through returned key |
| B | Q6 | 0.1% | 100 | 252.759 | 312.261 | 100 | 100 | 100000 | all ready rows |


### Corrected SQL measurements — PostgreSQL 16

| plan | query | visibility | visible | p50 ms | p95 ms | qualified | returned | CandidateRows | method |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| A | Q4 | 100% | 100000 | 213.558 | 239.865 | 100000 | 50 | 100000 | visible ready rows |
| A | Q5 | 100% | 100000 | 276.049 | 307.805 | 100000 | 1 | 100000 | visible ready rows |
| A | Q6 | 100% | 100000 | 201.090 | 210.486 | 100000 | 100 | 100000 | visible ready rows |
| B | Q4 | 100% | 100000 | 190.446 | 200.134 | 100000 | 50 | 50 | ready rows through returned key |
| B | Q5 | 100% | 100000 | 282.511 | 293.302 | 100000 | 1 | 1 | ready rows through returned key |
| B | Q6 | 100% | 100000 | 180.622 | 195.628 | 100000 | 100 | 100000 | all ready rows |
| A | Q4 | 1% | 1000 | 2.622 | 2.881 | 1000 | 50 | 1000 | visible ready rows |
| A | Q5 | 1% | 1000 | 3.458 | 4.199 | 1000 | 1 | 1000 | visible ready rows |
| A | Q6 | 1% | 1000 | 2.421 | 2.720 | 1000 | 100 | 1000 | visible ready rows |
| B | Q4 | 1% | 1000 | 2.050 | 2.778 | 1000 | 50 | 16717 | ready rows through returned key |
| B | Q5 | 1% | 1000 | 3.486 | 4.262 | 1000 | 1 | 16668 | ready rows through returned key |
| B | Q6 | 1% | 1000 | 2.459 | 2.955 | 1000 | 100 | 100000 | all ready rows |
| A | Q4 | 0.1% | 100 | 0.431 | 0.519 | 100 | 50 | 100 | visible ready rows |
| A | Q5 | 0.1% | 100 | 1.088 | 1.436 | 100 | 1 | 100 | visible ready rows |
| A | Q6 | 0.1% | 100 | 0.470 | 0.596 | 100 | 100 | 100 | visible ready rows |
| B | Q4 | 0.1% | 100 | 0.309 | 0.554 | 100 | 50 | 50018 | ready rows through returned key |
| B | Q5 | 0.1% | 100 | 1.285 | 1.841 | 100 | 1 | 16668 | ready rows through returned key |
| B | Q6 | 0.1% | 100 | 0.418 | 0.490 | 100 | 100 | 100000 | all ready rows |


### Q5 claim and closure evidence

Every Q5 plan/fraction case reports 20 measured claims, 23 resets (three
warmups plus 20 measurements), 20 fresh candidates, 20 successful rechecks,
20 one-row updates, and 20 commits. SQLite uses `BEGIN IMMEDIATE` on a pinned
connection; PostgreSQL uses `FOR UPDATE`. The measured candidates are bead 3
at 100% and bead 50004 at 1% and 0.1%, so the claims are committed state changes
rather than a rolled-back timing loop.

The normal closure path refused with `closure_too_large: 50001 rows exceeds
normal-write limit 5000` on both engines. The explicit migration-class path
started its timer before `BEGIN`, included target selection, temporary-table
setup, closure rebuilds and commit, and rewrote 50,001 rows for each operation:

| engine | 50,000-descendant reparent | `restricted` toggle |
|---|---:|---:|
| SQLite | 8,172.579 ms | 7,264.931 ms |
| PostgreSQL 16 | 5,776.694 ms | 7,198.775 ms |

The independent CP7 differential fixture remains checked in at
`spikes/authz/testdata/cp7-differential.json` (SHA-256
`3616ceb02c08daa7553dc9b204ff18f44bdd32cc17fdd34cd8fdec68a6bca4fc`). The
compiled-list and Cedar paths load separate grant projections; Cedar does not
consume caller-supplied involved-project slices. The 60-request differential
sample remains exact, with the fixture loader and conflicting-caller-attribute
coverage passing.

### Corrected gate result

The repaired run still fails the unchanged page target. Plan A remains above
50 ms for dense SQLite pages and dense PostgreSQL pages; Plan B remains above
50 ms for sparse SQLite pages and dense PostgreSQL pages. Grouped counts pass
for Plan A on both engines and Plan B on PostgreSQL, but Plan B exceeds 250 ms
for SQLite at 1% (347.435 ms) and 0.1% (312.261 ms). Thus the overall latency
gate is **Fail**, while corpus cardinality, depth, Q5 transaction semantics,
closure behavior, policy fit, and independent differential checks are **Pass**.

The AZ1 recommendation therefore remains to revisit authorization rather than
adopt the core-hosted Cedar evaluator on this result, leaning toward the
simple Linear-style team-permissions direction recorded by the design. Cedar
1.8.0, the deliberately small compiler subset, plan refusal for uncompileable
read rules, and plugin-declared grants remain the conditional AZ3/AZ4/AZ7
recommendations. No target was weakened and no merge was performed.
