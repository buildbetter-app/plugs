# Rejected annotation-only prototype: historical SOURCE_BLOCK

The sparse source described below is retained in commits 121b78a51/302ad3759,
not in the current observation path. The current child implements the shared
eager metadata design in `selection_source_history_projection_design.md`.
Its review/runtime disposition must be established independently.

Done = reduce duplicate derived-source history while preserving pinned
selection, later binding, invalidation and all refusals, proven by existing
and causal controls plus W7's matching product/census runs; source shape,
authored controls and row counts alone do not count. Scope retains every Ready
row, all four Selection rows, the 128-row cap, the 2-second budget and the
1000/1001 workloads.

**The annotation-only demand design is rejected.** Every valid `LockDerived`
is prospectively bindable, including an ordinary Ready value with no Index.
Thin source state has three consumers: sealed `SelectionBoundary.Sources`
on pure readers, staged selection before Commit, and committed fact envelopes.
The prototype discovers only annotations staged for Commit. It therefore
misses the first two consumers. Worse, an omitted thin row for a created and
deleted derived key can be accepted as a never-present revision-zero boundary.
An overlay cache cannot fix pure-reader boundaries. The correctness baseline
must retain eager metadata for every prospectively bindable derived state.

The retained prototype demonstrates a narrower mechanism: on first committed
annotation demand it reads the exact SQL `key_history` row at `rev <= t.at`,
verifies SQL/encoded revision, presence and value digest against the admitted
observation, then seeds at the annotation's commit revision. SQL key bytes are
the lossless identity; JSON key strings may replace invalid UTF-8. A staged
source uses its final value/tombstone and the existing rebound H+1 token.
Admission remains unchanged: an old mutation token after update, deletion or
Ready churn refuses at `t.at`; it never becomes an alternate historical pin.
The source's own Index is not transitive proof and adds no namespace, branch
or value-envelope admission rule.

Changed keys close existing intervals, including when no replacement is
demanded. Exact open observations are reused. Source closure, fact invalidation
and inserts remain in the fenced SQL transaction. Batches preserve close then
insert ordering, statement cleanup cannot mask prior errors, and expected
present sources without an exact witness refuse. None of these mechanisms
repairs the missing consumers or reconstructs pre-existing fact intervals that
lack thin history. Retained reconstruction would need actual fact boundaries
and source transitions, never an inferred current image.

The new test file includes valid public-API controls for cold/staged Select
before Commit, pure-reader present/missing/tombstone boundaries, opaque keys,
stale first binding after update/delete/Ready churn, retained fact pins and
rebinding. The boundary and precommit controls intentionally retain the
required behavior and expose the rejected design; they are not skipped.
Helper controls also cover exact witnesses, null/empty values, replacement
invalidation, overlap, native errors, cancellation and rollback.

`121b78a51` received independent SOURCE_BLOCK in
`/tmp/plug-swarm/reports/sqlite-demand-history-review-20261104/121b78a51-source-review.md`
(SHA256 `120167d42406750427dacb2de6cfbb5300bed61f18ca747b301db3f9544088d1`).
Follow-up raw-key/order fixes do not lift that ruling. Controls remain
AUTHORED_NOT_RUN; only formatting and diff checks were performed. The general
runtime throttle was lifted by root decision 7066; this blocked donor still
has no performance run or integration authorization. A separate finite
projection proposal lists the additional dependencies before any widening.
