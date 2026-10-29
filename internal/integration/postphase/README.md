# Post phase connected acceptance

`//internal/integration/postphase:postphase_test` connects a compiled-in SDK
native subscriber through the real plugin host and Writer to the real memory
driver. The Writer uses the service ontology and canonical audit adapter.
Fixtures supply plugin configuration, requests, an external idempotent consumer,
and transparent fault/observation wrappers. They do not seed receipts, warning
outcomes, log rows, event plans or delivery checkpoints.

| Named test (prefix `Test`) | Connected evidence |
| --- | --- |
| `NativePostReadsCommittedRevisionOutsideLaneAndIsolatesMutations` | Two native bindings block at revision 2 while another Writer advances the same record to revision 3; both read revision 2. Mutated callback images, operation bytes, provenance and reader results cannot alter a peer, graph, receipt or log. Read masks/query budgets, graph-write and undeclared-egress refusals, and retained capability revocation are checked. |
| `NativePostDeadlineBoundsUncooperativeBindingsAndRevokesIO` | Two callbacks receive positive deadlines no greater than one second. The response finalizes while both ignore cancellation; their retained I/O rejects the expired invocation. The current pipeline's per-binding limit is 500 ms; the elapsed response bound allows scheduling overhead. |
| `CallerCancellationAfterCommitStillFinalizesNativePostReceipt` | A callback cancels the caller after known commit, then reads its committed image and finishes; the returned response equals the final receipt. |
| `PartialNativePostFailurePersistsExactWarningWithoutRepeatingEffects` | One native fails while its peer succeeds. Only the failing binding produces a durable warning; final replay repeats neither callback. |
| `PendingDuplicatesWaitForFinalReceiptAndCancelledReplayReturnsCommittedBusy` | Real pending-receipt reads synchronize concurrent duplicates; cancellation returns retryable committed identity, changed parameters refuse, and original/waiting/final replay response bytes agree. An older reader remains pending. |
| `FinalizerTakeoverUsesCapturedNativeOutcomeAndFencesOldLease` | Four failed CAS attempts retain the actual callback outcome. A new epoch finalizes precisely that captured outcome, the old epoch refuses, and a retained reader stays pinned after a subsequent write. |
| `FinalizerLostAcknowledgementReloadsDurableNativeWarning` | The finalizer persists and loses its acknowledgement; Writer reloads the actual durable response before success. |
| `DispatcherCheckpointFaultReconstructsDeliveryFromNativeWriterLog` | Real native-created two-record log, event materializer and checkpoint adapter. Save rejection replays the identical key/event; a persisted save with lost acknowledgement resumes at the next index. Reconstructed adapters use a new lease; old checkpoint saves fence. |
| `DispatcherPostFailurePersistsDeadLetterAndEmitsNoticeBeforeLateRetry` | The actual event dispatcher persists dead-letter metadata, emits an observer notice, and reconstructs the same delivery marked late. |
| `NoopDryRunAndRejectedWriteEmitNoPostLogVersionOrDelivery` | Compares actual graph metadata, records, versions, log and checkpoint bytes. No-op creates only a final receipt; dry-run and ontology rejection create none. No extra native or event delivery occurs. |

## Boundaries

The native `Host.Posts()` adapter and the event `Dispatcher` are separate
existing execution paths. These tests consume the same real Writer-created
store history through each path; they add no queue composition. The public
native `CommittedChange` has no delivery key or `Late` field, and the direct
native path creates no binding checkpoint. Native crash redelivery, native
dead-letter/breaker/alert-sink composition, and runtime restart recovery remain
unsupported and unproved. The dispatcher-only notice does not prove those
native surfaces. Graph writes and undeclared egress explicitly refuse.

The driver is the real **memory** implementation. Checkpoints and receipts
survive adapter reconstruction and lease takeover within that store's lifetime;
this is not disk durability, a process restart, SQLite/Postgres acceptance or
production failover. Finalizer takeover preserves an outcome captured in the
test process; it does not reconstruct a lost callback outcome after a crash.

Run named cases with ordinary local Bazel:

```sh
bazel --ignore_all_rc_files test //internal/integration/postphase:postphase_test \
  --jobs=2 --test_timeout=900 --lockfile_mode=error --test_output=all \
  --test_arg=-test.v
```
