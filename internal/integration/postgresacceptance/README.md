# PostgreSQL behavior acceptance

This package runs bounded additional slices of the canonical Plug behavior
cards on a real PostgreSQL engine. The test composition is loopback HTTP →
authentication admission → actual registry/Dispatch → service → ontology →
one Writer → admitted PostgreSQL driver. Graph setup, atomic batches and
reopen use that same real Writer directly because those RPC methods are not
bound. The tests label these calls; they install no replacement handlers,
stores, SQL adapters, ontology decisions or manually emitted events.

Run through Bazel:

```sh
bazel --ignore_all_rc_files test \
  //internal/integration/postgresacceptance:postgresacceptance_test \
  --jobs=2 --lockfile_mode=error --test_timeout=900 \
  --test_output=all --test_arg=-test.v
```

The harness always starts and removes its own
`plug-swarm-v3-accept-postgres-<random>` Docker container on the local socket,
bound only to `127.0.0.1`. It uses the admitted driver's pinned image
`postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3`
and prints the actual engine version. Every scenario gets a unique database.
Missing engine access fails; there is no external DSN, fallback or skip path.
Domain clock/IDs are controlled; storage lease expiry and receipts use the
real engine clock. Hosts renew their leases and close HTTP before SQL resources.

| Named scenario | Cards and exact bounded assertions |
| --- | --- |
| `TestB15PublicCommentsPostgresHistoryRefusalAndReopen` | B15: public HTTP comments keep exact Unicode body and verified actor, advance fixed-clock stamps, write one history/event per durable comment, preserve anchor fields and as-of snapshots, refuse empty/unknown/prefix requests without receipts, and replay identically after physical PostgreSQL reopen. This complements the retained memory/SQLite slice; public keyset pagination and the wisp plane remain unqualified. |
| `TestB28DurableWarningReceiptAcrossHandlesAndReopen` | B28: post failure returns one committed warning; an already-open independent SQL pool sees its finalized receipt. New service/Writer/lease on that pool and then full store reopen replay byte-identical results without another ID, graph write or post callback. Changed request under the old key refuses. |
| `TestB22CASNoopAndReplayRetainOriginalRevision` | B22/B18: current record revision and field hash accept; stale guards refuse without changing complete graph/history/predicates or storing a success receipt. A no-op at head 5 preserves record revision 4, creates no graph history, and replays the original receipt at head 6 and after reopen. |
| `TestB22ConcurrentCASHasOneWinner` | B22: two concurrent HTTP updates to disjoint fields with the same revision yield one winner and one typed refusal. Only the winning field/version/log/receipt exists; replay adds no work. |
| `TestB01B18LiveReadyAndPinnedHistoryContinuationSurviveReopen` | B01/B18: Ready continues in current ID order at revision 5 while history returns the old revision-3 images. Signed cursors survive a clean store/server reopen with stable test signing material. Method/filter/agent/asOf/tamper mismatches refuse; a live list shrinking below the offset terminates with an empty tail. |
| `TestB18DiffReplayAcrossStoredBaseBoundaryAndReopen` | B18: 34 HTTP writes persist 34 gapless changesets/versions; creation and version position 33 are full bases. As-of reads at 1/2/31/32/33/34 reconstruct exact fields after reopen; pre-creation returns not-found. |
| `TestB08CloseGuardRefusalAndReopenAreAtomic` | B08: a live ordinary blocker refuses close plus reason with no partial comment or history. Closing the blocker admits close/reason as one changed resource/revision. Fresh-key already-closed close is a no-op; stale CAS still refuses. Direct Writer reopen reblocks, and close refuses again. |
| `TestB21BatchRefusalAndOrderedBackwardReferencesAreAtomic` | B21: direct Writer final-item refusal rolls back an earlier patch/create. A backward create alias resolves patch/comment/link in order while another bead changes in the same revision, with one version per changed bead. Requests remain unchanged and durable replay is identical. |
| `TestB18B22LateSQLRefusalLeavesNoPartialStateAfterReopen` | B18/B22/B28: a local SQL trigger refuses log publication after records, versions and predicates were staged. A nontransactional sequence proves that exact fault point executed once. Independent and reopened driver handles see identical pre-failure state, no success receipt and no post call. Retrying the same key succeeds at the next gapless revision and replays once. |
| `TestUnsupportedCardRemainderRefusesExplicitly` | Seven real HTTP refusals cover unbound batch/reopen/versions RPC, saved views, query language, malformed named parents and direct status patch. Every refusal leaves the complete stored graph/history/predicates unchanged and creates no receipt. Canonical parented create is covered by batchsql hierarchy acceptance. |

Every HTTP exchange is retained as a JSONL request/response trace in Bazel
undeclared outputs. Graph comparisons cover complete records, every version
and changeset, incarnation/head and relevant record/graph/ontology/plugin/Ready
predicate observations. Receipt assertions use fresh actual driver readers.
The SQL failure fixture changes only its disposable database and does not
change production code or driver behavior. Existing B1 renewal/publication
locking assertions under `//internal/storage/postgres:postgres_test` remain
unchanged; final whole-tree verification includes that regression target.

These are executed Plug expectations for useful slices, not whole-card or
Beads-recorded goldens. B01's hybrid time buckets, parent/ephemeral/gate/defer rules,
full views/filter grammar and stored-rank lag remain unqualified. B08's full
guard/permission/force/claim-holder matrix is not covered. B18's redaction,
restore, current authorization and all canonical numeric/history-floor cases
are not covered. B21's complete 1,000-op boundary and every op kind are not
claimed here. B22's refusal-detail completeness and every G1–G5 fault are not
certified. B28 dead letters, alerts, plugin modes and crash recovery remain
separate obligations. Unsupported installed-surface remainders above actively
refuse rather than skip. B02 audited conditional-event composition remains
with the assigned integration owner; this package uses no audit fixture
decisions. It changes neither frozen `internal/acceptance/**` nor `spec/**`.
Isolated-engine tests make no HA, RPO/RTO, throughput, latency, production,
cutover or release claim.
