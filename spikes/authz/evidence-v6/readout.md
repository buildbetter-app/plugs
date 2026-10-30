# Authorization spike v6 readout

The repaired v6 run is bound to measured source commit
`69f28caaf71a673296abe3120c60be5cb3af5aec`, with a clean worktree. The
evidence-only publication commit is later and is not the measured source.
The complete structured outputs are [`sqlite-result.json`](sqlite-result.json)
and [`postgres-result.json`](postgres-result.json); their hashes, redacted
commands, source-file hashes, and retained stdout digests are in the
[`provenance-manifest.json`](provenance-manifest.json).

## Decision

The unchanged latency gates **fail**. Page p95 must stay below 50 ms at
100%, 1%, and 0.1% visibility, and grouped-count p95 must stay below 250 ms.
The table reports p95 in the order `100% / 1% / 0.1%` (milliseconds):

| engine | plan | Q4 page | Q5 claim page | Q6 grouped count | gate result |
| --- | --- | ---: | ---: | ---: | --- |
| SQLite | A scope-first | 118.990 / 3.580 / 0.332 | 129.418 / 1.922 / 0.427 | 141.887 / 1.821 / 0.261 | **fail**: dense page |
| SQLite | B index-walk | 0.195 / 63.345 / 139.656 | 0.152 / 70.997 / 51.033 | 222.600 / 347.435 / 312.261 | **fail**: sparse page/count |
| PostgreSQL 16 | A scope-first | 239.865 / 2.881 / 0.519 | 307.805 / 4.199 / 1.436 | 210.486 / 2.720 / 0.596 | **fail**: dense page |
| PostgreSQL 16 | B index-walk | 200.134 / 2.778 / 0.554 | 293.302 / 4.262 / 1.841 | 195.628 / 2.955 / 0.490 | **fail**: dense page |

The accepted #15 branch therefore reopens authorization toward simple
Linear-style team permissions after this failed rerun. This evidence does not
select Cedar. Cedar-related policy parsing and differential checks remain
conditional evidence for later decisions, not an adoption result.

## What the repaired run establishes

- The corpus contains 100,000 beads at maximum graph depth 10. The three
  visibility fractions expose exactly 100,000, 1,000, and 100 visible and
  qualified rows. Q4 returns 50 rows, Q5 returns one candidate, and Q6 returns
  100 grouped rows. `CandidateRows` is measured SQL candidate cardinality, with
  its method recorded in each JSON case; it is not presented as a portable
  executor row-loop counter.
- Q5 has one committed transaction per sample. SQLite uses `BEGIN IMMEDIATE`;
  PostgreSQL uses `FOR UPDATE`. Across all 12 engine/plan/fraction Q5 cases,
  each has three warmups, 20 measured samples, 23 reset preparations, 20 fresh
  candidates, 20 authorization/unclaimed rechecks, 20 one-row updates, and 20
  commits.
- Each engine's migration-class closure operations rewrite 50,001 rows in one
  timed transaction. The SQLite durations are 8,172.579 ms (reparent) and
  7,264.931 ms (restricted toggle); PostgreSQL durations are 5,776.694 ms and
  7,198.775 ms. The normal write path still refuses closures above 5,000 rows
  with `closure_too_large`.
- The corrected policy differential uses independently loaded relationship
  projections and passes its 60-request sample. The fixture digest is recorded
  in the source-file manifest.

## Historical baseline

The 2026-10-28 result is preserved as **invalid historical evidence**. Its
sparse cases exposed all 100,000 rows, its `rows touched` values were labels
rather than measured cardinalities, Q5 claims rolled back, closure timing
started after transaction setup, and the output was not bound to an exact
reviewable source tree. Those historical numbers are not rewritten and are not
used for the v6 decision.

The committed JSON files intentionally omit raw stdout and PostgreSQL DSNs.
The provenance manifest retains SHA-256 digests for the omitted stdout and the
exact source tree, along with credential-free run metadata.
