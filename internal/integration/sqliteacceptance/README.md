# SQLite connected acceptance

Done = the bounded card projections below execute through real SQLite, service,
ontology, Writer and Dispatch, proven by ordinary Bazel test results on the
published PR head; compilation or this matrix alone does not count.

The baseline is `471d0199c89acf9e242f44978f2824120726de32`. These are additional
connected tests derived from the [behavior cards](../../../engdocs/design/plug/spec/behaviors.md),
not a frozen reference golden or a claim that any entire card is complete.
No memory driver, replacement Writer, replacement service, invented ontology
decision, custom RPC handler or audit dependency is used.

Each fixture owns a unique disposable SQLite file and actual loopback listener.
An independent read-only SQL connection checks `sqlite_version()` and WAL mode;
the file header and a populated WAL are checked. Lifecycle calls go through
HTTP `/rpc/v1`, authentication admission, the real registry and `Dispatch`.
Batch and metadata operations use the actual Writer because their public RPC
methods are not bound at this baseline. State assertions use public driver
record/version/changeset/receipt reads, without SQL table-layout assumptions.
The host controls the domain clock and renews the real storage lease. Server,
lease, readers and SQL handles close before reopen; IDs are the actual returned
IDs. A fixture-local cursor signing key survives its server restart.

| Card slice | Executed test | Exact observable assertions |
| --- | --- | --- |
| B18/B22 durable writes, CAS, no-op, replay | `TestB18B22DurableCASNoopAndReceiptsAfterReopen` | HTTP create at revision 1; current revision CAS at 2; stale mixed patch refuses with no receipt/state change; unrelated write at 3; no-op returns head 3 and record revision 2 without history; independent field hash CAS lands at 4; pinned WAL reader still sees revision 1; identity, old/current state and three exact receipts survive close/reopen; changed keyed request refuses; only four gapless changesets and three versions of the target exist. |
| B18/B23 history snapshot and pagination | `TestB18B23PinnedHistoryPagesAcrossReopen` | Four one-row pages return each original ID/body/revision once in ID order at the original asOf, after every current row changes, a fifth is created and the server/store reopen; the late row is not found at that snapshot; changed filter, agent, asOf, method and malformed cursor refuse; a foreign incarnation refuses. |
| B21/B22 atomic batches | `TestB21B22AtomicBatchRefusalsAndOrderedRecovery` | Late missing-target, forward-alias and ontology failures preserve records, versions, log and head; failed key has no receipt and can be corrected; create/backward-patch/create resolves actual IDs in declaration order, commits one revision and one version per changed row, and replays unchanged after reopen. |
| B15/B25 comments and scalar preservation | `TestB15B25ScalarUpdatePreservesCommentsAndMetadata` | Two attributed comments have stable order and increasing timestamps; same-key HTTP comment replay returns identical result bytes; nested metadata and both comments survive a scalar HTTP update and reopen; replay adds no version/log row. |
| B22 concurrent revision CAS | `TestB22ConcurrentCompareAndSetHasOneWinner` | Two simultaneous HTTP patches to disjoint fields with the same current revision yield exactly one revision-2 winner and one `precondition_failed`; losing field and receipt are absent; exactly two versions/log entries survive reopen. |
| B22/G4 concurrent idempotency | `TestB22ConcurrentIdempotencyCreateReplaysOneDurableResult` | Simultaneous same-key HTTP creates return the same ID/revision/result bytes, exactly one transient replay, one record/version/changeset, and the identical receipt after reopen. |
| B20 changeset pagination only | `TestB20ChangesetPagesAreOrderedAndSinceExclusive` | Five actual HTTP writes survive reopen and return complete changesets in ascending, since-exclusive pages of 2/2/1; head stays 5 and incarnation is retained. |
| B04/B05 live ownership only | `TestB04B05LiveClaimOwnershipAcrossReopen` | Claim/get agree on holder, epoch, five-minute TTL and expiry; replay preserves exact bytes; fresh-key same-owner claim preserves the claim and record revision without history after reopen; foreign label claim/update and closed-host reads/writes refuse without durable changes or refusal receipts. |
| Unsupported remainder | `TestUnsupportedCardRemainderRefusesWithoutWrites` | Cataloged but unbound batch/claim-next/heartbeat/reopen/history/versions/revert/redact/restore/view methods return `method_not_found`; event.list, history.get/diff, and admin.health are bound read surfaces; parent create, attach and reparent refuse; direct status/unknown-field mixed patches refuse; all durable state stays unchanged. |

The B04/B05 slice does not cover claim-next, expiry/re-arm, heartbeat or reclaim.
B15 does not cover permission filtering. B18 does not cover time selectors,
redaction, history floors or the 32-version base boundary. B20 does not enable
audit event plans or test event delivery, pruning or truncation. B21 does not
qualify the 1,000-op boundary or partial-close results. B23 covers historical
ID pagination only, not general search, counts, views or live ordering. B25
covers comments/metadata preservation only, not labels, links or delete cascades.
B01-B03, B06-B14, B16-B17, B19, B24 and B26-B28 remain outside acceptance here;
B02 composition belongs to its separate owner. No full parity, performance,
crash recovery, production topology, restore or cutover claim is made.

```sh
bazel --ignore_all_rc_files test //internal/integration/sqliteacceptance:sqliteacceptance_test --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900
```

Final exact-head evidence is recorded by `/tmp/plug-swarm/tools/ci-local-v2.py`
under `/tmp/plug-swarm/ci-local/<pr>-<sha>/`; the task report identifies the run,
actual executed tests, cache results and retained iteration failures.
