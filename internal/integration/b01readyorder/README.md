# Connected B01 ready ordering candidate

Status: **CONNECTED_SOURCE_CANDIDATE; AUTHORED_NOT_RUN**.

Done = a clean frozen B01 consumer plus lifecycle/rank producer candidate,
proven by exact input identity, preserved owner blobs, complete source bindings
and an exact-head gate plan; authored assertions, static checks and inherited
results do not count as runtime or full-B01 qualification.

This successor joins only the frozen B01 input
`81d35c2c8df7377429af5ea2f72b953c242b62cc` (tree
`e298316af357070fc8b19a14239f2ceb82dc4b78`) and lifecycle/rank input
`c236815aa598bdc8e2b0585190be03776473fb34` (tree
`77d138a9a110dcc9e87e2109d0974d26cae0f652`), retaining both as direct merge
parents. Their common base is
`9c925d6f5f8bc47e09845367ce2b391168b5f12f`. The original B01 packet's
`SOURCE_BINDING_NOT_RUNNABLE` status remains historical evidence for that
independent input. Both original checkouts, commits and packets are preserved.

Scope = B01 reads, its pure comparator, this integration package, the named
frozen lifecycle/rank dependency, and exact manifest entries for all new source
files. The successor changes only the B01 consumer/tests/documentation and
source inventory beyond the two-input union. Lifecycle-owned blobs remain
byte-identical to their frozen input. No tests, builds or gates were run.

## Ordering and membership

The canonical contract is `engdocs/design/plug/spec/behaviors/work.md` B01,
with adopted P74 and the live cursor contract in
`engdocs/design/plug/design.md`. The existing reference Git object
`9d6fff760b7a314b476539198504d77ce0959656`,
`internal/storage/sqlbuild/ready.go:BuildReadyWorkOrder`, supplies the ascending
tuple:

1. Bucket 0 at or after `now - 48h`, otherwise bucket 1.
2. Priority for bucket 0, constant 999 for bucket 1.
3. Actual immutable creation instant.
4. Bytewise ID.

Recent work precedes old work; old work ignores priority. The exact cutoff is
inclusive. Equal instants with different timezone offsets tie by ID. Complete
current membership, filters, blockers, ancestors and effective leases are
computed before sorting and pagination. Stored ranks never admit absent or
ineligible work. Queue spreading stays within equal bucket/rank/creation-time
groups. Public pagination, filter binding and revision contracts are preserved.

`pipeline.Document.CreatedAt *time.Time` and `CreationTime() (time.Time, bool)`
provide the real creation instant in detached UTC form. Ordinary creation uses
the existing Writer clock; later edits preserve it. Legacy absence stays
unknown. An eligible legacy row atomically refuses with existing
`internal_error`, `details.reason = ready_creation_time_unavailable`. Filters
and ready membership run first, so excluded legacy rows do not invalidate a
known-time answer. IDs, revisions and current time never supply a substitute.

## Connected persisted rank

Pipeline owns the exported `ReadySortKey` shape, `ReadySortKeyRowKey` and strict
`DecodeReadySortKey` codec. The consumer calls that exact codec for rows in
`driver.DerivedSortKey` at `ready/<id>`:

```json
{"bucket":0,"rank":2,"createdAt":"2026-11-03T12:00:00Z","rankedAt":"2026-11-03T12:00:00Z","revision":1}
```

Required members are non-null; integers are nonnegative, times are nonzero and
revision is positive. Unknown members and trailing data refuse. B01 additionally
validates bucket 0/1 and the canonical bucket/rank at `rankedAt`. The row's
creation instant and revision must match the current bead on the same pinned
Reader. Nonfuture rows are usable through exactly 60 seconds of age. Missing,
source-stale, future-clock or expired rows use a live tuple from actual creation
time. Reads never write repairs. A fresh source-bound semantic disagreement,
malformed row or storage error refuses without a partial page or publication.

The pure rule is
`service.ReadySortKey(pipeline.Document, time.Time) (bucket int, rank int, known bool, err error)`
and the same method on `*service.Ontology`. The optional ontology capability
keeps pipeline independent of service imports. Pipeline uses its existing
Writer clock, plans typed derived locks before Head, assigns the allocated
record revision, and publishes the row with body/history/events/receipt in one
transaction. Mutation updates rank; no-op and replay preserve it; delete removes
it. Unknown legacy creation is never backfilled.

Periodic refresh/job wiring remains absent. The consumer bounds stale order
through live recomputation, without a fabricated refresh, SQL-clock twin or
background-worker proof. Production clock defaults and budgets are preserved.

## Authored assertions

Every assertion is **AUTHORED_NOT_RUN**. B01 scenarios use real memory and
file-backed SQLite stores, the real service and pipeline, and public JSON-RPC
bindings over the in-process byte transport. Expected page contents and order
are literal independent scenario lists. Literal projection fixtures remain
explicitly bounded consumer fixtures.

| Assertion | Test |
| --- | --- |
| Hybrid reversal, creation/ID/offset ties, inclusive cutoff +/-1ns, every page | `TestB01PublicHybridOrderMembershipAndEveryPage` |
| Exact blockers, deferred ancestors, open parent, categories, leases, filters before limit | `TestB01ExactMembershipAndFiltersPrecedeThePage` |
| Stored order through 60s, expiration at 60s+1ns, recomposition and unchanged bytes | `TestB01StoredRankLagIsBoundedAtSixtySeconds` |
| Stored rank never delays current membership exclusion | `TestB01RankLagNeverDelaysMembership` |
| Ordinary priority mutation defeats an old projection immediately | `TestB01OrdinaryPriorityUpdateInvalidatesStoredRankImmediately` |
| Source revision/creation and future-clock mismatch fall back correctly | `TestB01StaleSourceAndFutureRanksCannotOverrideCurrentOrder` |
| Malformed and semantically corrupt projections refuse atomically | `TestB01MalformedStoredRanksRefuseWithoutPublication` |
| Missing creation time refuses; excluded legacy rows permit known-time reads | `TestB01LegacyMissingCreationTimeHasAnAtomicExplicitBoundary` |
| Qualifying rows beyond the first 1,000 stored records remain visible | `TestB01StoreScanningDoesNotDiscardQualifyingRowsAfterTheFirstThousand` |
| Cursor filter bindings and continuation over current revisions | `TestB01PublicCursorBindingsAndLiveRevisionContract` |
| Actual create time persists and stays immutable on edit | `TestB01CreationClockSeamFeedsOrderWithoutChangingCreationOnUpdate` |
| Queue spreading preserves hybrid boundaries | `TestB01QueueSeedDoesNotCrossHybridOrderBoundaries` |
| Graph and derived query failures never become empty or partial success | `TestB01QueryFailuresNeverBecomeEmptyOrPartialSuccess` |
| Public create, actual canonical producer row/receipt, cutoff update, observable stored consumption through 60s then live order at +1ns | `TestB01PublicCreateProducesTheStoredRankThatReadyActuallyConsumes` |
| Actual canonical producer mutation, quiet no-op, public replay, confirmed driver sort-row staging refusal, rollback and same-key retry | `TestB01CanonicalProducerMutationNoopReplayAndStagedFailure` |

The last two assertions use the actual `*service.Ontology` classifier and
ordinary Writer, without substitute classifiers or literal rank writes. Their
observed order differs from live recomputation within the allowed lag, making
stored consumption observable when the test is eventually run. Read/refusal
snapshots compare metadata, record bytes, versions, all derived tables, known
lock observations, relevant receipts, log/events and publication count.

Inherited lifecycle and claimsql assertions remain unchanged. They cover
immutable creation, start/close/reopen transitions, same-holder no-op, replay,
real SQL-trigger failure, rollback, store reopen, historical rank bytes and
delete. Their fixture classifier isolates the Writer/storage boundary; the new
connected B01 assertions supply canonical classifier/consumer causality. The
staged error injection records that the actual driver's sort-row staging was
reached before refusal.

## Separate exact-head gate routing

The sealed source packet records the combined full SHA/tree/parents/base,
complete base and causal patches, old/new blob/mode/hash table, owner blob
preservation, manifest completeness, source bindings and fully expanded future
argv. S1 routes distinct review and gates after receiving that packet.

The focused command uses `/tmp/plug-swarm/bin/remote-bazel` with the exact new
worktree, full frozen SHA and a unique label before `-- test`. Its targets are
`//tools/lexicon:lexicon_gate`, `//internal/core/pipeline:pipeline_test`,
`//internal/core/service:service_test`,
`//internal/integration/claimsql:claimsql_test`,
`//internal/integration/b01readyorder:b01readyorder_test`,
`//internal/server/rpc:rpc_test`, `//internal/server/claimrpc:claimrpc_test`,
`//spec/s1-b01-b04:replay` and `//spec/s1-b01-b04:claim_acceptance`.

Required flags are `--nocache_test_results`,
`--test_env=GO_TEST_WRAP_TESTV=1`, `--test_output=errors`, and CP7
`--test_timeout=900,900,900,9000`. Only complete focused receipts passing on the
same frozen SHA permit S1 to route conditional `-- test //...` with those flags.
Capture command, SHA/tree, receipt, exit, completion and evidence status; retain
all historical REDs. Missing-creation legacy fixtures may expose real refusals;
route their disposition instead of synthesizing timestamps or weakening gates.

Runtime, reference SQL clock twins, native producer/capture, PostgreSQL/HA,
refresh-worker and full-card qualification remain unproven. FullS1 is 0/28;
E3's inherited 5/5 remains bounded fixture evidence; NativeAdmission is None.
Publication, admission, merge to main and release remain controller-owned.
