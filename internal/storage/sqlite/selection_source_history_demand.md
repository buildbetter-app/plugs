# Demand-driven derived source history prototype

The SQLite publisher keeps record source history eager because records are the
public historical source image. It no longer writes a thin source-history row
for every changed derived key. A derived key becomes bindable when an admitted
selection annotation names that exact `LockDerived` key as its source. The four
selection rows and every Ready row still publish through their existing paths;
this change only removes unused thin observations.

The first binding is part of the same fenced Commit transaction as the
annotation. For a changed source, the staged `LockState` is the proof and the
source interval starts at the new commit revision. For an unchanged cold
source, the helper reads the latest exact `key_history` image at the
transaction's pinned current revision (`t.at`), checks the encoded key,
presence, last-mutation revision, and digest, then seeds one interval at the
new commit revision. The SQL history revision and the encoded state's
last-mutation revision must agree. A missing `key_history` row proves only
the never-present state and is represented as an explicit zero-revision,
`source_present=0` interval. A present row with a missing derived image,
malformed JSON, an identity mismatch, or a tombstone carrying an image
refuses. The existing admission validator remains authoritative: it observes
at `t.at` plus the same-token staged overlay, so an annotation carrying an
old token after update, delete, or Ready churn still refuses before this
helper runs. Staged source demands are checked against the same H+1 metadata
that `reboundSelectionSources` persists.

Every changed derived key closes its current thin interval, even when no
annotation currently demands a replacement. A demanded changed source then
opens the exact new image; a cold demand reuses an already-open identical
interval and otherwise opens one new interval. The close and seed batches use
the existing 128-row limit on Commit's fenced connection, so source history,
fact invalidation, selection rows, and Ready publication remain atomic. There
is no selector fallback to `key_history`, no current-value substitution, and
no branch or incarnation widening. Historical reads therefore remain exact for
intervals that were already bound, while an absent thin row outside a valid
first-binding demand remains an unavailable source rather than proof of no
source.

The authored controls cover the pinned-head read, exact open-row reuse,
changed-source tombstone closure, malformed and missing witnesses, overlap
refusal, and rollback. Existing admission controls in
`selection.go:validateSelectionRowsObserved` and
`selection_stage_work_test.go` retain stale-token refusal after update,
delete, and Ready churn. Existing `selection_publication.go` controls retain
source fact invalidation, four logical Selection rows, Ready publication,
Parent/completeness checks, incarnation checks, and SQLite's branch-zero
boundary. Those existing controls remain the authority for those semantics;
the new authored tests are source-only and were not run under the measurement
hold.

This prototype does not accept a stale historical `Source.Revision` as a read
pin. A future integration must add explicit interval reconstruction only when
retained selection/fact intervals require it; its starts must come from those
interval boundaries and retained source transitions, with missing retained
history refusing closed historical selection rather than silently inventing an
image.
