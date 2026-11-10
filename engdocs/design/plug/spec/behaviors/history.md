# History and transaction behavior cards

Source pin and evidence limits: [behavior catalog](../behaviors.md).

## B18 — versions, revisions, and as-of reads

**Behavior.** When versioned history is enabled, each accepted durable mutation
mints one version row per changed bead atomically with the mutation; no-op,
unversioned, or ephemeral writes mint none. Version state is canonicalized as
JCS-compatible JSON (including numeric admission limits), and as-of reads
select the latest version at or before an instant/version address, returning a
typed gone/unknown result rather than substituting a nearby state.

**Beads implementation.** `internal/storage/issueops/version_history.go:
RecordVersionInTx`, `RecordVersionForCreateInTx`, `CheckIssueVersionable`;
`internal/storage/issueops/version_list.go:ListVersionsInTx`; `internal/storage/issueops/asof_read.go:AsOfReadInTx`,
`resolveAsOfRevisionInTx`; `internal/storage/issueops/history.go:HistoryInTx`.

**Pinning tests.** conformance `RunDualWriteMintsOneVersionRowPerAcceptedMutation`,
`RunDualWriteNoOpMutationMintsNoRow`, `RunDualWriteAttributionIsRecordedWithTheMutation`,
`RunAsOfReadSelectorAcceptsEitherAnInstantOrAVersionAddress`,
`RunAsOfReadReturnsTheLatestVersionAtOrBeforeTStrictlyExcludingLater`,
`RunAsOfReadRefusesAsGoneOrUnknownNeverSubstitutingAnotherVersion`,
`RunExpectedRevisionAcceptsAWriteNamingTheCurrentVersion`,
`RunExpectedRevisionCoversFieldsOutsideAnyWatchedSubset`; `internal/storage/issueops/version_completeness_test.go:
TestEveryVersionedEntryPointMints, TestEveryBeadMutatorMintsOrIsExempt`;
`internal/storage/issueops/version_history_durable_state_test.go:
TestCanonicalDurableStateIsJCS`; `internal/storage/issueops/
version_history_integer_admission_test.go:TestCanonicalDurableStateRefusesIntegerPastBinary64`;
`internal/storage/issueops/version_history_number_admission_test.go:
TestNumberAdmissionIsClosedUnderCanonicalization`; `internal/storage/issueops/
version_history_switch_test.go:TestVersionedHistorySwitchOnlyEverTurnsOn`;
`internal/storage/issueops/asof_read_test.go:TestListVersionsAgreesWithTheAsOfReadOnWhyAVersionWasRemoved`;
`cmd/bd/versions_test.go:TestVersionsJSONIsOneObjectWhetherOrNotAnythingWasRecorded` and
`cmd/bd/protocol/versioning_contract_test.go:TestProtocol_V3_SchemaSkewRefusal`.

**Edge cases.** History may be switched on only once and must not create an
off window under concurrent writers; invalid JSON numbers, duplicate/unmarshal-
able state, and out-of-range integers refuse without a partial version. A
pre-enabled legacy row with no participation marker is skipped consistently by
scan and mint.

**Plug disposition.** **Deviate** Dolt commit/merge history is dropped (P6/P7),
while A6's driver log is canonical: one gapless `{incarnation,revision,index}`
changeset, one forward JSON Patch per changed record, and a full base at
creation/every 32 versions (P6/P30/P73). `history.get/query` uses current read
permission even for old versions (D26/P57).

## B19 — revert, redact, dump, and restore

**Behavior.** Beads as-of reads distinguish removed versions from unknown
addresses and preserve the recorded removal reason. The retention/epoch
contracts specify address stability, retained bounds, holds, and epoch changes;
their presence is not proof of a complete production redaction path.
`MarkLatestVersionRemovedInTx` is explicitly a test-support seam, not a
production removal operation. Beads `restore` reverses compaction (B12), and
its JSONL interchange preserves issue state (B27); neither implements the
Plug dump/restore format.

**Beads implementation.** `internal/storage/issueops/asof_read.go:
MarkLatestVersionRemovedInTx` (test support); `internal/storage/issueops/compaction.go:
RestoreFromSnapshotInTx`; `cmd/bd/export.go` and `cmd/bd/import.go`;
`internal/storage/issueops/version_history.go` canonical state and participation.

**Pinning tests.** conformance `RunAsOfReadRefusesAsGoneOrUnknownNeverSubstitutingAnotherVersion`,
`RunRemovalLeavesTheAddressAbleToAnswer`, `RunRemovalReasonIsRetentionErasureOrReorganizationDistinctly`,
`RunRemovalReportsTheSurvivingRetainedWindow`, `RunRemovalNeverReassignsASurvivingAddress`,
`RunGoneIsDistinguishableFromUnknown`, `RunAnAddressNeverResolvesToADifferentState`,
`RunADestructiveOperationEnumeratesAffectedAddressesFirst`, `RunAHoldPreventsRemovalAndReportsInRetainedBounds`,
`RunForcingAHeldRemovalRecordsWhoWhenWhy`, `RunAStoreThatRemovesStateStillAnswersGoneDurably`,
`RunAnEpochBumpIsTriggeredOnlyByRestoreReinitOrSchemeChange`,
`RunEpochBumpVoidsOnlyAddressesOfVersionsNoLongerServed`,
`RunTokenSchemeChangeCarriesLiveAddressesAndOnlyThose`,
`RunACarriedAddressKeepsResolvingAcrossLaterBumps`, and
`RunALostAddressStaysGoneAcrossASchemeChange`; `internal/storage/issueops/asof_read_test.go:
TestAsOfReadMapsAnyStoredRestrictionOutsideTheVocabularyToUnknown`,
`TestListVersionsAgreesWithTheAsOfReadOnWhyAVersionWasRemoved`;
`internal/storage/issueops/epoch_cas_test.go:TestCurrentAddressForInTxReturnsTypedErrorsForAddressesItCannotAnswer`,
`TestLoseVersionInTxMarksTheWholeLineageOnceAndOnlyWhereStillLive`,
`TestBumpEpochInTxWritesOnlyStoreEpoch`;
`cmd/bd/versions_test.go:TestVersionsOffButRecordedStillLists`,
`TestRestrictionLabelsAreDistinct`; `cmd/bd/restore_embedded_test.go:TestEmbeddedRestore`;
`cmd/bd/protocol/interchange_test.go:TestProtocol_Interchange_RoundTripEquivalence`,
`TestProtocol_Interchange_EpochSanitize`; `internal/storage/schema/schema_skew_test.go:
TestCheckSchemaSkew_EqualVersion_NoError`, `TestCheckSchemaSkew_OneAhead_ReturnsSchemaSkewError`,
`TestCheckSchemaSkew_EscapeHatch_ReturnsNilAndWarns`.

**Edge cases.** Plug redaction can affect old diffs, bases, blobs, dead letters,
checkpoints, and audit rows but not already-created backups or external plugin
destinations. Restore is strict by default: `full` refuses when source history
is unavailable, unverified, non-contiguous or unmappable; `partial` is an
explicit opt-in for a proven range, exact gaps and a floor, and currently
refuses as not yet supported until replay exists; `snapshot` is an explicit
head-only import that establishes a store-wide history floor at revision 1.
Snapshot starts a new incarnation, old incarnation tokens fail after restore,
and reads below the floor return typed truncation. Nothing fabricates versions
or changesets.

**Plug disposition.** **Deviate**: path-scoped revert refuses a head/base
conflict or a protected record (A6); `history.redact` is the only removal
(P45) and reports unreachable external copies as warnings. Dumps default to
full history with a head-only option (P20),
restore is empty-store-only (P44), and restore loads plugin artifacts only
after lock-digest verification (P75). Tracker `sync.import` is an ordinary
head-state write that mints new IDs (P77), not a restore. Plug restore starts a
new incarnation and refuses bad checksums and unknown record kinds (A6).

## B20 — events journal and delivery order

**Behavior.** Beads records one structured journal row per committed mutation
inside the same transaction, with a gapless sequence in commit order; pages
are ascending and since-exclusive, limits cap rows rather than the head, and a
stale cursor returns a typed truncation error without a partial page. The head
remains observable after pruning and every mutation kind has a classified row.

**Beads implementation.** `internal/storage/issueops/journal.go:
RecordEventInTx`, `RecordDepEventInTx`, `RecordCommentEventInTx`,
`nextEventSeq`; `internal/storage/issueops/events.go:EventsSinceInTx`; journal prune/truncation/autoprune
helpers; `internal/eventsjournal` activation.

**Pinning tests.** Conformance `RunJournalPagesAreSeqAscendingAndSinceExclusive`,
`RunJournalHeadArrivesWithItsRowsAndDetectsCaughtUp`,
`RunJournalLimitCapsRowsNotHead`, `RunJournalTruncationIsTypedAndNamesTheWindow`,
`RunJournalHeadSurvivesAFullPrune`, `RunJournalEveryMutationKindLandsARow`;
`internal/storage/issueops/journal_vocabulary_test.go:
TestEventOpVocabularyIsFrozen`, `TestEveryDeclaredEventOpIsClassified`;
`internal/storage/issueops/journal_completeness_test.go:TestEveryMutationFunctionJournals`;
`internal/storage/issueops/journal_truncation_test.go:TestComputeEventsTruncation`,
`internal/storage/issueops/journal_prune_test.go:TestComputeEventsPruneWhereResolvesToOnePrefixBound`,
`internal/storage/issueops/journal_autoprune_test.go:TestComputeEventsAutoPruneBoundStopsAtTheNarrowestFloor`;
`internal/storage/dolt/journal_coverage_test.go:TestEventsJournal_SeamEntryPoints` and
`cmd/bd/events_journal_construction_test.go:TestEveryStoreConstructionActivatesTheEventsJournal`.

**Edge cases.** Rollback burns no sequence; concurrent writers serialize the
counter; full prune makes the next sequence `head+1`; interior holes refuse.
At the pinned native implementation, `events tail`/`export` limit `0` means
unbounded: the query adds a limit only when it is positive. Beads activation
is opt-in/configurable and local ignored state does not replicate.

**Plug disposition.** **Deviate** preserve atomic, ordered cursor/truncation
semantics, but A4/P6 makes the changeset log always on in every driver and
combines journal/outbox/audit. Script hooks and their autoprune path are
removed (P29b); projections consume total-order changesets and listeners stall
at their lag limits (P29a).

**Limit vector and caller parity (adopted P6/P20 deviation).** Native limit
`0` returns the complete since-exclusive sequence. Plug `event.list` requires
positive bounded limits and typed refusals for nonpositive limits;
[commands](../commands.md#history-events-audit-and-provenance) folds export
into paged `event.list` or `admin.dump`, with no unbounded one-shot wire
response. `plug events --limit 0` / `--limit all`, `events all`, and
`events export` use the real Plug client to page to completion in bounded
batches.

**Completion Contract.** Done = the actual CLI list/export callers return the
same complete ordered, gapless, since-exclusive set as pinned native unbounded
reads on memory, SQLite and PostgreSQL, proven by two fresh public native
recordings, actual client/wire/store observations, independent exact-source
review, and original focused plus canonical whole evidence; source-only or
positive-limit-only observations do not count.

**Prune design difference.** Native prefix pruning is recorded literally,
including its unchanged sequence head, audit and issues, and typed truncation
below the retained window. Plug folds prune into operator retention/driver
policy (commands.md:228) and retains audit forever (events.md:299-305); there
is no client prune RPC or row deletion. An operator retention-floor mechanism
is not implemented. Below-floor `cursor_expired` must be exercised when that
operator feature exists; its absence is not a B20 failure. No compaction
operation substitutes for retention.

## B21 — batch apply and batch close

**Behavior.** `ApplyBatch` applies create/update/close/dependency operations in
declaration order, resolves named references only backward (with
metadata-reference exceptions), enforces expected version/status/assignee
preconditions against in-request state, and rolls back every item if any item
refuses. The separate `BatchCloser` reports indexed per-item refusals while
surviving closes commit; both preserve caller request values, and a landed
`ApplyBatch` is bounded at 1,000 items with one durable history entry when the
transaction changes a durable row, even if an item reports `Changed=false`;
history cardinality follows the durable row delta, not item landing flags.

**Beads implementation.** `internal/storage/issueops/batch_apply.go:ApplyBatchInTx`
and `applyRun.applyItem`, `spliceMetadataRefs`, `runEndGate`; public
`issueops/batchapplier.go`; `internal/storage/issueops/close_batch.go:ExecuteCloseBatchWithPolicy`.

**Pinning tests.** conformance `RunBatchApplyAppliesEveryItemInDeclarationOrder`,
`RunBatchApplyBindsEachNamedKeyToItsMintedID`,
`RunBatchApplyResolvesABackwardKeyRef`, `RunBatchApplyRefusesAKeyDeclaredLater`,
`RunBatchApplyRollsBackEverythingWhenTheLastItemRefuses`,
`RunBatchApplyRecordsExactlyOneHistoryEntry`,
`RunBatchApplyRecordsOneEntryForAWriteThatLandedNothing`,
`RunBatchApplyBoundsTheItemCount`,
`RunBatchCloserPerItemRefusalIsAResultAndSurvivorsCommit`,
`RunBatchCloserAllIdempotentBatchLandsNothing`; `internal/storage/batch_apply_test.go:
TestPlanApplyBatchRefusesAnUnusableRequest`, `TestPlanApplyBatchAcceptsTheBoundary`,
`TestPlanApplyBatchResolvesKeysBackwardOnly`, `TestPlanApplyBatchRefusesAVersionGuardOnARowThisRequestWrote`;
`cmd/bd/create_atomic_test.go:TestCreateIssueWithDepsRunsOneTransaction`,
`TestCreateIssueWithDepsRollsBackOnDepFailure`; backend
`batch_closer_contract.go:RunBatchCloserOutcomesMirrorItemsIndexForIndex`,
`RunBatchCloserPerItemRefusalIsAResultAndSurvivorsCommit`,
`RunBatchCloserOutcomeSnapshotIsTheDocumentedShape`,
`RunBatchCloserRequestValidationReturnsZeroResultAndChangesNothing`,
`RunBatchCloserBackendFailureReturnsNoOutcomes`,
`RunBatchCloserAllIdempotentBatchLandsNothing`,
`RunBatchCloserRecordsOneHistoryEntryForWhatLanded`, and
`RunBatchCloserAllRefusedBatchRecordsNoHistory`; `cmd/bd/batch_parity_test.go` wrappers
`TestBatchCloseReportsItemRefusalsInsideTheBody` and `TestBatchCloseMapsTheRolesValidationToA400`.

**Edge cases.** A later named key is a typed `DeclaredLater` refusal; a row
created earlier may be updated/closed in the same batch; self-dependencies and
cross-plane new edges are refused; external targets and metadata forward/self
references follow their documented exceptions. An all-idempotent or no-op
`ApplyBatch` has no history when it produces no durable row delta; a batch
containing only wisps has no durable history. A same-type dependency re-add
that rewrites durable waits-for metadata is the counterexample: its item may
report `Changed=false`, but the stored row changes and exactly one history entry
named `bd: apply batch` is required. `BatchCloser` preserves item indexes,
commits survivors, and records no history when every item is refused or already
idempotent.

**Plug disposition.** **Deviate** retain `ApplyBatch` item semantics and the
1,000 cap, but `tx.apply` is one A4 typed ChangeSet and one atomic revision
(P2). The Beads partial-close shape is folded into per-op results in that
ChangeSet rather than allowing a hidden second transaction; subscriber
failures are `result.warnings` (D28). Idempotency replay returns the stored
Commit and runs no post subscriber.

## B22 — compare-and-set revision and transaction guarantees

**Behavior.** Expected revision/field guards accept the current token and reject
stale or mismatched status/assignee with a typed refusal that identifies the
holder/current revision; disjoint concurrent updates have one deterministic
winner. A successful mutation atomically commits row state, relational data,
versions, events, and derived projections, while rollback leaves every part
unchanged.

**Beads implementation.** `internal/storage/issueops/version.go:CheckVersionInTx`,
`internal/storage/issueops/version_commit.go`, lifecycle update/close transaction bodies, and backend
transaction runners.

**Pinning tests.** conformance `RunExpectedRevisionAcceptsAWriteNamingTheCurrentVersion`,
`RunExpectedRevisionAcceptsAWriteNamingNoVersion`,
`RunDualWriteCurrentRevisionMatchesTheNewVersionRow`;
`backend/conformance/lifecycle_update_contract.go:
RunLifecycleUpdateExpectedVersionSingleWinnerWithDisjointColumnsUnderConcurrency,
RunLifecycleCloseExpectedVersionSingleWinnerUnderConcurrency,
RunLifecycleUpdateExpectedVersionSingleWinnerAcrossUpdateAndCloseUnderConcurrency`;
`internal/storage/dolt/transaction_test.go:
TestRunInTransactionSerializationConflictInvokesCallbacksOnce`,
`TestRunInTransactionAlreadyClosedDoesNotCommitUnrelatedEvent`;
`internal/storage/embeddeddolt/transaction_indeterminate_test.go:
TestRunInTransactionPostCommitStageFailureIsIndeterminate`,
`internal/storage/uow/tx_test.go:TestRunTx_RetriesOnSerializationError,
TestRunTxResultWithin_ExhaustedBudgetReturnsSerializationError`; expected-revision
contract `backend/conformance/expected_revision_contract.go:RunExpectedRevisionAcceptsAWriteNamingTheCurrentVersion`,
`RunExpectedRevisionAcceptsAWriteNamingNoVersion`, `RunExpectedRevisionCoversFieldsOutsideAnyWatchedSubset`,
`RunRefusalReportsTheRefusingVersionAddress`, `RunRefusalIsATypedOutcomeNotAGenericError`.

**Edge cases.** A same-value update is a no-op; expected-version is checked
before an otherwise idempotent close; a failed count/hydration read rolls back
the enclosing claim; retry-safe serialization errors re-run a callback only
when no external side effect occurred.

**Plug disposition.** **Deviate** replace backend-specific Dolt retries/merge
semantics with A6 guarantees G1–G5: latest-state locks, one transaction,
24-hour idempotency key, and lease-epoch fencing (P6/P49). A4 commit guard
records sentinels and retries stale snapshots up to three times before
`write_conflict` (P3).
