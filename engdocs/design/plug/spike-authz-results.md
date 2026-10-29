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
