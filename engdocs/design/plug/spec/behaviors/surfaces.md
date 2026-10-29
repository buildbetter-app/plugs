# Surface and infrastructure behavior cards

## B23 — search, list, counts, and statistics

**Behavior.** Search/list filters are composable and plane-aware: text matches
specified fields, labels support all/any/exclude and exact collation, status
and type filters follow workspace vocabulary, and pagination/keyset order does
not lose or repeat rows. Counts answer the full predicate (without page
limits), while ready counts use the blocker-aware ready predicate.

**Beads implementation.** `internal/storage/issueops/search.go:SearchIssuesInTx`,
`internal/storage/issueops/search_counts.go:SearchIssuesWithCountsInTx`; `internal/storage/issueops/count.go`; SQL builders in
`internal/storage/sqlbuild/filter.go`, `internal/storage/issueops/labels.go`, and `internal/storage/sqlbuild/ready.go`.

**Pinning tests.** `internal/storage/issueops/search_test.go:TestSearchIssues_MaxRows_Exceeded_ReturnsErrTooManyRows`,
`internal/storage/issueops/search_counts_test.go:TestSearchIssuesWithCountsPushesSortIntoSQL`,
`TestFinishSearchIssuesWithCountsTruncatesByRequestedSort`;
`internal/storage/dolt/queries_test.go:TestSearchIssues_ByTitle,
TestSearchIssues_LabelFilter, TestSearchIssues_StatusFilter,
TestSearchIssues_StableOrdering`; reader contract
`RunReaderReadyPageIsThePrefixOfTheUnboundedAnswerCountsIncluded`,
`RunReaderReadyParentScopesToItsTransitiveDescendants`; ready-counter
`RunReadyCounterEqualsTheUnboundedPage`, `RunReadyCounterCountsTheBlockerAwareSet`,
`RunReadyCounterEmptyFrontIsZeroAndNil`; `tests/regression/scenarios_test.go:
TestSearchByTitleSubstring, TestListCountParity, TestQueryDSLParity`.

**Edge cases.** A max-row cap returns a typed too-many-rows error rather than a
truncated count; a missing optional wisp table is tolerated only where the
role says so. Empty query, unknown status/type, mixed durable/wisp duplicate IDs,
natural numeric sort, and keyset positions are separate projections.

**Plug disposition.** **Keep** semantic filters/counts as core views (P31b),
with category-based statuses and stored ready bucket/rank (P74). Drop Beads
CLI formatting and public Go role API (P46/P47).

## B24 — stale, defer, wake, and due-time semantics

**Behavior.** Stale work is selected by age and status without mutating rows;
`defer` requires a nonempty reason, appends that reason to notes, and excludes
future-deferred work from ready until the controlled clock passes the target.
Expired defers wake through the same status/derived-state path.

**Beads implementation.** `internal/storage/issueops/stale.go`,
`internal/storage/issueops/wake_defers.go:WakeExpiredDefersInTx`, `internal/storage/issueops/ready_work.go` defer predicates;
`cmd/bd/defer.go` and stale role adapters.

**Pinning tests.** `cmd/bd/protocol/stale_test.go:TestProtocol_StaleRejectsNonPositiveDays`;
`cmd/bd/protocol/defer_test.go:TestProtocol_DeferPastDateWarns`,
`TestProtocol_DeferReasonAppendsNotes`, `TestProtocol_DeferRejectsEmptyReason`;
`internal/storage/issueops/wake_defers_test.go:TestWakeExpiredDefersWithHistoryOffLoadsNothingBeforeWaking`;
`tests/regression/scenarios_test.go:TestDueAndDeferRoundTrip`,
`TestDeferredExcludedFromReady`; `backend/conformance/reader_contract.go:
RunReaderReadyDeferredAndEphemeralGates`.

**Edge cases.** Past defer targets warn but remain a deliberate write; nonpositive
stale age is validation; wake with history off must not load/mint version rows;
SQL and Go clocks must agree at the exact boundary.

**Plug disposition.** **Keep** the semantic slots as `defer`/stale views with
A3 category transitions and controlled clock. The old CLI text/warning envelope
is dropped (P46), and all accepted writes use the A4 ChangeSet.

## B25 — relational preservation and delete cascades

**Behavior.** Scalar updates preserve labels, dependencies, comments, metadata,
parent edges, and close provenance unless the request explicitly patches that
relation; deleting a bead removes its owned auxiliary rows and handles
cross-plane dependents according to cascade/force policy. A failed auxiliary
write rolls back the entire lifecycle transaction.

**Beads implementation.** lifecycle update/create/delete paths in
`internal/storage/issueops/update.go`, `internal/storage/issueops/create.go`, `internal/storage/issueops/delete.go`,
`internal/storage/issueops/delete_role.go`, and `internal/storage/issueops/delete_cascade_tables_test.go`.

**Pinning tests.** `cmd/bd/protocol/preservation_test.go:
TestProtocol_LabelsPreservedAcrossUpdate`, `TestProtocol_DepsPreservedAcrossUpdate`,
`TestProtocol_CommentsPreservedAcrossUpdate`, `TestProtocol_ScalarUpdatePreservesRelationalData`,
`TestProtocol_DeleteCleansUpDeps`; `tests/regression/scenarios_test.go:
TestUpdateDoesNotClobberRelationalData`, `TestDeleteCascade`,
`TestDeleteWithDependencies`; `internal/storage/issueops/delete_cascade_tables_test.go:
TestDeleteCascadeTablesCoversSchema`, `TestDeleteCascadeTablesWispCoversSchema`;
conformance `RunLifecycleCreateWritesEveryScalarField`,
`RunLifecycleUpdatePersistsThePatchAndHydratesTheResult`,
`RunLifecycleUpdateRefusalWritesNoMemberOfThePatch`.

**Edge cases.** Durable/wisp auxiliary tables route by source plane; cross-plane
inbound edges survive a demotion while copied outbound edges move; deleting a
missing row is typed/not-found rather than an unrelated event. Relational
preservation and explicit replacement must be distinguished in history.

**Plug disposition.** **Keep** preservation/cascade meaning, represented by
typed ChangeSet ops and derived reference indexes. Beads-specific table names,
Dolt cascades, and CLI delete output are not Plug contracts (P1/P6/P46).

## B26 — ephemeral/wisp plane and promotion

**Behavior.** Wisps/ephemeral rows are queryable when explicitly included,
participate in blockers, labels, comments, and mixed-plane dependency reads,
but are excluded from durable history/leases as configured. Promotion/demotion
moves the row and only the permitted auxiliary relationships atomically.

**Beads implementation.** `internal/types/types.go:NormalizePersistenceMode`,
`EffectiveStorageClass`; `internal/storage/dolt` mixed-table lifecycle;
`internal/storage/issueops` plane routing and `internal/storage/issueops/child_id.go`.

**Pinning tests.** `internal/storage/dolt/mixed_table_lifecycle_test.go:
TestGetReadyWorkIncludesNoHistoryWispsByDefault`,
`TestGetReadyWorkExcludesBlockedNoHistoryWisp`,
`TestGetBlockingInfoForIssuesHandlesCrossClassDeps`,
`TestDemoteToWispPreservesInboundDependencies`,
`TestDemoteToWispRemovesCopiedOutboundDependencies`,
`TestDemoteToWispRollsBackWhenAuxiliaryCopyFails`; `internal/storage/issueops/wisp_routing_test.go:TestPartitionByWispSet`;
`cmd/bd/protocol/lease_claim_test.go:TestProtocol_ClaimIsUnassignedOnly`;
mixed-plane conformance `RunReaderReadyDeferredAndEphemeralGates`,
`RunDependencyEditorMixedBatchWritesBothPlanes`,
`RunCommenterCommentOnAWispLandsOnTheWispThread`,
`RunIssueOperationsUpdateLabelPatchOrdering`.

**Edge cases.** A wisp ID collision prefers the active wisp for reads; wisps
never get leases or durable version rows; no-history durable work stored in the
wisp tier can still be ready. Promotion must reconcile child counters and
preserve inbound edges without copying outbound rows twice.

**Plug disposition.** **Deviate** retain the semantic distinction as typed
record kinds and unversioned ephemeral runs (P45), rather than Beads' ignored
Dolt tables. Workflow molecule runs are a plugin (P48); the converter gives
each wisp issue type/storage class an explicit census disposition.

## B27 — import/export, configuration, and auxiliary records

**Behavior.** Interchange round trips preserve fields, metadata, labels,
comments, dependencies, parent-child edges, close reason, and spec IDs;
imports do not delete existing rows and report unresolved dependency targets.
Custom statuses/types are the union of stored config and `.beads/config.yaml`;
routes, interactions, snapshots, provenance, and event rows are independent
store sources that must be inventoried rather than inferred from issue rows.

**Beads implementation.** `cmd/bd/export.go:runExportFromSource` and
`cmd/bd/import.go:runImportFromReader`; `cmd/bd/import_shared.go:
importIssuesCore`, `orderImportIssuesForChunking`, and
`filterStaleImportIssues`; `internal/storage/dolt/transaction.go:
CreateIssueImport`; `internal/storage/issueops/lease.go:RestoreLeaseOnImportInTx`;
`internal/storage/issueops/config_helpers.go:ResolveCustomConfigInTx`; and the
independent record tables in
`internal/storage/schema/migrations/0009_create_issue_snapshots.up.sql`,
`internal/storage/schema/migrations/0012_create_routes.up.sql`,
`internal/storage/schema/migrations/0014_create_interactions.up.sql`, and
`internal/storage/schema/migrations/0063_create_provenance_events.up.sql`.

**Pinning tests.** `cmd/bd/protocol/roundtrip_test.go:TestProtocol_ImportPreservesRelationalData,
TestProtocol_FieldsRoundTrip, TestProtocol_MetadataRoundTrip,
TestProtocol_SpecIDRoundTrip, TestProtocol_CloseReasonRoundTrip,
TestProtocol_ParentChildDepShowRoundTrip, TestProtocol_LabelAddRemoveRoundTrip;
`cmd/bd/protocol/interchange_test.go:TestProtocol_Interchange_RoundTripEquivalence`,
`TestProtocol_Interchange_ImportNeverDeletes`,
`TestProtocol_Interchange_MissingDepTargetSkippedAndReported`;
`internal/storage/issueops/config_helpers_test.go:TestParseStatusFallback,
TestReopenCategoryInTx`; `internal/storage/issueops/delete_cascade_tables_test.go:
TestDeleteCascadeTablesCoversSchema, TestDeleteCascadeTablesWispCoversSchema`;
`tests/regression/scenarios_test.go:TestExportImportRoundTrip`.

**Edge cases.** Import keeps explicit IDs/prefixes and reconciles counters;
lease snapshots never clobber a live local lease; missing/cross-prefix targets
require an explicit mapping or remain reported. Multiple Beads stores merge
offline one project per prefix before restore (P76).

**Plug disposition.** **Deviate** `plug-dump/1` is the sole restore boundary
(P5c/P44), with full history/config and a kind registry; Plug imports Beads
semantics, not its JSONL/CLI wire contract (P22/P46). Every census entry gets
`mapped`, `drop:<decision>`, or `n/a` before G2.

## B28 — write pipeline, subscribers, and warnings

**Behavior.** Beads post-commit hooks observe successful mutations and cannot
veto them; transaction rollback emits no hook. Hook/decorator wiring preserves
operation order, changed-row snapshots, and no-op suppression.

**Beads implementation.** `internal/storage/hook_decorator.go:HookFiringStore`,
`fireHook`, and `createHookEvents`; `internal/hooks/hooks.go:Runner.RunSync`;
`internal/storage/uow/notifying.go:notifyingUOW.Commit` and `CommitScoped`;
`internal/storage/dolt/event_commit_boundary_test.go:
TestAddCommentSQLCommitResponseLossAccountsCircuitOnce`.

**Pinning tests.** `internal/storage/uow/notifying_test.go:
TestNotifyingUOWFiresNothingUntilCommit`, `TestNotifyingUOWRollbackFiresNothing`,
`TestNotifyingUOWDropsAWorkThatCommitsNothing`, `TestNotifyingUOWOpToHookEventMapping`,
`TestNotifyingUOWBatchApplierAnnouncesOnlyLandedCloses`;
`internal/storage/hook_role_census_test.go:TestRoleFiresHooksKnowsEveryHookFiringRole`;
`internal/storage/hook_issue_operations_test.go:TestHookFiringStoreCompleteIssueOperationsSnapshotIssue`,
`internal/storage/hook_batch_closer_test.go:TestHookBatchCloserFiresOncePerLandedItem`,
`internal/storage/hook_heartbeater_test.go:TestHookHeartbeaterFiresOnlyForARearm`,
`internal/storage/hook_decorator_internal_test.go:TestHookFiringStoreUpdateIssueCheckedFiresOnSuccessOnly`.

**Edge cases.** A response-loss/retry must not double-fire a terminal hook;
comments/dependencies carry structured payloads while delete snapshots have a
null issue. Hook errors and circuit breakers are distinct from transaction
failure.

**Plug disposition.** **Deviate** replace script hooks with A4 pre mutators,
validators, post subscribers, projections, and listeners (P3/P29b). Pre is
fail-closed by default with per-binding fail-open warnings (P24); post failure
returns the committed result with `post_subscriber_failed`, dead-letters, and
alerts (P66/D28). Plugin failures and permission checks are represented in
`result.warnings`, not an error beside a result.
