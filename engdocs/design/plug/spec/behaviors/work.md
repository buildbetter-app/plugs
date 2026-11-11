# Work and graph behavior cards

Source pin and evidence limits: [behavior catalog](../behaviors.md).

## B01 — readiness membership and ordering

**Behavior.** `ready` admits unblocked, non-deferred, ready-category work and
excludes pinned/closed, hidden infrastructure types, and children of deferred
parents; filters and both durable/wisp planes are applied before the page. The
default order is the hybrid 48-hour bucket/priority/created/ID order, with
priority and deterministic ID tie-breaks.

**Beads implementation.** `internal/storage/issueops/ready_work.go:GetReadyWorkInTx`,
`buildReadyWorkPredicates`, `sortReadyIssues`; `internal/storage/sqlbuild/ready.go:BuildReadyWorkWhere`
and `BuildReadyWorkOrder`.

**Pinning tests.** `internal/storage/issueops/ready_work_test.go:TestGetReadyWorkInTxOpenIncludesCustomActive`,
`TestGetChildrenOfDeferredParentsInTx_ReturnsChildrenFromBothDependencyTables`,
`TestGetReadyWorkInTxStatusesSingleQuery`; `cmd/bd/protocol/ready_front_test.go:
TestProtocol_OpenParentDoesNotBlockChildren`, `TestProtocol_ReadyParentFilterIsTransitive`,
`TestProtocol_ReadyDefaultOrdering`; `cmd/bd/protocol/blocking_semantics_test.go:
TestProtocol_ReadyOrderingIsPriorityAsc`, `TestProtocol_DeferredExcludedFromReady`;
conformance `RunReadyCounterCountsTheBlockerAwareSet` and
`RunReadyCounterEqualsTheUnboundedPage`.

**Edge cases.** An open parent alone does not block children, but a blocked
parent does; deferred parents suppress descendants across both dependency
planes. A missing optional wisp table is tolerated while unrelated query errors
propagate; a page limit must not discard a qualifying row after blocked rows.

**Plug disposition.** **Deviate** membership stays exact, but stored bucket/rank
order may lag by one minute (P74); categories replace literal status predicates
(A3/D3). The card records the old/default and Plug order separately.

## B02 — blocking and status categories

**Behavior.** `blocks`, `conditional-blocks`, parent-child, and waits-for edges
make a bead blocked when their live conditions hold; closing a blocker makes a
dependent ready and transitive/diamond graphs are evaluated across durable and
wisp rows. Closed and pinned subjects are not reported as blocked in Beads.

**Beads implementation.** `internal/storage/issueops/blocked_state.go:RecomputeIsBlockedInTx`
and `RecomputeAllIsBlockedInTx`; `internal/storage/issueops/blocked.go:GetBlockedIssuesInTx`;
`internal/storage/issueops/blocked_consistency.go`;
`internal/storage/issueops/blocked_recheck.go`.

**Pinning tests.** `cmd/bd/protocol/blocking_semantics_test.go:
TestProtocol_ClosedBlockerNotShownAsBlocking`, `TestProtocol_ClosingBlockerMakesDepReady`,
`TestProtocol_DiamondDepBlockingSemantics`, `TestProtocol_TransitiveBlockingChain`,
`TestProtocol_DepRmUnblocksIssue`; `cmd/bd/protocol/blocked_test.go:
TestProtocol_WaitsForAppearsInBlocked`; `internal/storage/issueops/blocked_recheck_test.go:
TestBlockedRecheck_RecordsEveryUnblockingWrite`, `TestBlockedRecheck_CountsOnlyTheWritesItDidNotName`;
conformance `RunBlockedLister`, `RunBlockingAnnotatorReportsOpenBlockersOnly`,
`RunBlockingAnnotatorReadsBothPlanes`, and `RunBlockingAnnotatorReportsTheTransitiveBlockedFlag`.

**Edge cases.** `waits-for` supports all/any child gates, childless-any, and
`also_blocks`; parent fallback and direct blockers are distinct reasons. The
stored flag is a projection, so a stale flag is recomputed before an answer;
recompute preserves `updated_at` and must not recurse through its own recheck.

**Plug disposition.** **Deviate** the predicate is category-driven (A3):
`completed`/`canceled` satisfy `blocks`; `frozen` and `pinned` map to backlog
and no longer satisfy it (CP19/P22). `conditional-blocks` is outcome-aware:
only a completed `failure` satisfies it; other terminal outcomes emit
`dependency.conditionUnmet` (A3).

## B03 — custom status catalog and lifecycle slots

**Behavior.** Built-in and configured custom statuses are validated by the
workspace catalog; only open and custom `active` statuses are claimable, while
custom wip/done/frozen statuses remain valid but are not ready. Reopen/close
preserve status-specific timestamps and reject malformed status configuration.

**Beads implementation.** `internal/types/types.go:ParseCustomStatusConfig`,
`BuiltInStatusCategory`, `Status.IsValidWithCustomStatuses`;
`internal/storage/issueops/config_helpers.go:ReopenCategoryInTx` and status
loading; `internal/storage/issueops/claim.go:ClaimableSourceStatusesInTx`.

**Pinning tests.** `internal/types/types_test.go:TestParseCustomStatusConfig,
TestParseCustomStatusConfigMaxLimit, TestBuiltInStatusCategory,
TestIsValidWithCustomStatuses`; `internal/storage/issueops/config_helpers_test.go:
TestReopenCategoryInTx, TestParseStatusFallback`; `cmd/bd/protocol/status_validation_test.go:
TestProtocol_UpdateRejectsGarbageStatus, TestProtocol_UpdateAcceptsBuiltinStatuses,
TestProtocol_UpdateAcceptsCustomStatus`; conformance
`RunWorkspaceConfigRefusesAnUnparseableCustomStatus`,
`RunWorkspaceConfigProjectsCustomStatuses`,
`RunClaimerAcceptsAConfiguredActiveStatusAndRefusesAConfiguredWipOne`,
`RunLifecycleCloseAndReopenSpanTheConfiguredDoneCategory`.

**Edge cases.** At most 50 custom statuses are accepted; names collide with
built-ins case-insensitively; malformed fallback config can silently yield no
custom rows today. The current ready listing and claim path disagree about
whether custom active is included by default, which the card must expose.

**Plug disposition.** **Deviate** `status_catalog` categories are canonical,
keys are immutable, overlays replace definitions by key (P10/P41/D24), and
workflow slots replace literal close/reopen transitions (A3). Import accepts
Plug semantics (P22).

## B04 — claim-next and direct claims

**Behavior.** A direct claim succeeds only for an unassigned open/custom-active
bead (or the same actor's already-owned in-progress bead), atomically sets
assignee/status/started time, and grants a replica-local lease. `claimNext`
walks the full ready set, skips races/ineligible rows, returns the winning hydrated row,
and returns a nil success when the committed ready front is empty.

**Beads implementation.** `internal/storage/issueops/claim.go:ClaimIssueInTx`,
`ClaimReadyIssueInTx`; `internal/storage/issueops/claim_next.go:ValidateClaimNextRequest`,
`ExecuteClaimNext`, `HydrateReadyRowInTx`.

**Pinning tests.** `internal/storage/issueops/public_claim_test.go:
TestExecuteClaimRefusesWispIDsBeforeAnyWork`, `TestExecuteClaimReportsTheStateThatLostTheCAS`,
`TestExecuteClaimIdempotentReclaimStagesNothing`, `TestExecuteClaimIdempotentReclaimAcrossSpellingStagesNothing`,
`TestExecuteClaimRefusesGenuinelyDifferentIdentityDespiteSimilarSpelling`,
`TestExecuteClaimStagesTheRowAndEventItWrote`; `cmd/bd/protocol/lease_claim_test.go:
TestProtocol_ClaimIsUnassignedOnly`, `TestProtocol_ReadyClaimEmptyFrontIsSuccess`,
`TestProtocol_ReadyClaimComposesWithFilters`; conformance `RunClaimerClaimsAnUnassignedOpenIssueAndAnswersTheBareRow`,
`RunClaimerReclaimsItsOwnInProgressIssueWithoutWriting`,
`RunClaimerAcceptsAConfiguredActiveStatusAndRefusesAConfiguredWipOne`,
`RunReadyClaimerEmptyFrontIsNormal`, `RunReadyClaimerSkipsIneligibleFrontRows`,
`RunReadyClaimerRecordsOneHistoryEntryForAWin`.

**Edge cases.** Actor spelling with separators is equivalent for ownership;
foreign holders receive a typed conflict; pool aliases can claim only their
pool work; wisps are never leased. Validation rejects actor-missing,
limit/offset/brief claim-next requests before SQL.

Public claim and claim-next refusals use the canonical error catalog's
`retryable` value and transport-generated non-empty `traceId`. Domain errors
may carry an empty trace ID before transport encoding; the public HTTP
boundary must validate the closed error schema and add the trace ID without
replaying or publishing a claim.

**Plug disposition.** **Keep** semantic claim behavior, expose `bead.claimNext`
and a 300,000 ms default lease, and add the A4 lazy walk/skip-locked behavior
and 200-agent contention benchmark. Anonymous agent labels are unverified
lease scope (P60).

## B05 — leases, heartbeat, and reclaim

**Behavior.** A claim's five-minute lease lives in a replica-local, ignored
`leases` table; granting or refreshing it mints no Dolt commit or version row.
Beads JSONL export/import carries lease expiry and heartbeat timestamps, but
import never overwrites an unexpired local lease. Heartbeat refreshes only
that row, while a missing lease for a live claim can
be re-armed as a recorded mutation. Reclaim atomically reopens stale claims,
clears ownership/lease, records the recovery actor/event, and never reclaims a
live or grace-window lease.

**Beads implementation.** `internal/storage/issueops/lease.go:UpsertLeaseInTx`,
`DeleteLeaseInTx`, `RenewLeaseInTx`, `HeartbeatIssueInTx`,
`ReclaimExpiredLeasesInTx`; `internal/storage/issueops/heartbeat_role.go:ExecuteHeartbeatInTx`;
`internal/storage/issueops/lease_reclaimer.go:ValidateReclaimRequest`, `ExecuteReclaimInTx`.

**Pinning tests.** conformance `RunHeartbeaterRefreshesTheHoldersLease`,
`RunHeartbeaterASameSecondHeartbeatIsNotARearm`,
`RunHeartbeaterChangesNothingOnTheRow`, `RunHeartbeaterRefusesAWisp`,
`RunHeartbeaterRearmsAHandDoledClaim`; and
`RunLeaseReclaimerRevertsAStaleLease`, `RunLeaseReclaimerHonorsTheGraceWindow`,
`RunLeaseReclaimerScopesToAssigneesAndLabels`,
`RunLeaseReclaimerAttributesTheRecoveryEvent`;
`internal/storage/issueops/lease_reclaimer_test.go:
TestValidateReclaimRequestRefusesInTheDocumentedOrder`,
`TestExecuteReclaimInTxRefusesBeforeAnyQuery`, `TestReclaimVersionCommitComposesNothingForANoOp`;
`internal/storage/issueops/lease_replica_test.go:TestReclaimReplicaSQL`,
`TestFormatForeignSkipSummary`; `cmd/bd/protocol/lease_claim_test.go:
TestProtocol_LeaseFieldsExportToJSONL`, `TestProtocol_LeaseFieldsRoundTripJSONL`,
`TestProtocol_ImportNeverClobbersLiveLocalLease`;
`cmd/bd/reclaim_embedded_test.go:TestEmbeddedReclaimRunsTheLeaseReclaimerRole`.

**Edge cases.** Reclaim uses strict expiry-before-cutoff and rechecks each row
to win a heartbeat race; filters are capped and fail closed on degenerate
scope. Foreign granting replicas are skipped unless explicitly scoped. A
heartbeat on a wisp, released/closed claim, missing holder, or foreign actor
is a typed refusal; orphan cleanup may commit without a successful heartbeat.

**Plug disposition.** **Deviate** retain quiet refresh and reclaim; heartbeat
may recreate only the replica-local overlay for a valid existing base
`ClaimLease` held by the caller. A truly missing base lease returns the typed
`lease_not_held` refusal and publishes no durable change or new overlay, even
for a leaseless started/hooked bead. Unlike Beads' recorded missing-lease re-arm,
heartbeat never creates a base lease; any future durable re-arm requires a
separate protected operation and event. Plug leases are unversioned-but-logged
and excluded from `plug-dump/1` (A6),
unlike Beads JSONL lease snapshots. Lease commit fencing follows the
storage-plugin epoch (A6/P49/P71); lease loss stops
reads and writes. Restore preserves leaseless started/hooked beads and reports
warnings rather than silently moving them (CP19). Heartbeat remains a quiet
lease-only call with no revision or event; only reclaim is a durable
`bead.leaseReclaimed` change.

## B06 — cycles and graph diagnostics

**Behavior.** Add-time checks reject self edges and scheduling cycles across both
planes; blocking-hierarchy checks reject a blocker between an ancestor and its
descendant. The cycle report is deterministic: paths rotate to the lowest ID,
parallel edges collapse, partially unreadable members remain represented, and
optional tracks expand the report without treating a pure tracks loop as a
scheduling cycle.

**Beads implementation.** `internal/storage/issueops/dependencies.go:
CheckDependencyCycleInTx`, `WouldCreateSchedulingCycleInTx`,
`CheckBlockingHierarchyInTx`; `internal/storage/issueops/cycles.go:DetectCyclesInTx` and graph builders;
`internal/storage/issueops/cycle_report.go:CanonicalCyclePaths`,
`CanonicalMixedCyclePaths`, `BuildCycles`; public `issueops/cycledetector.go:DetectCycles`.

**Pinning tests.** `internal/storage/issueops/dependencies_test.go:
TestCheckDependencyCycleInTxSelfDependencyIsWrapPreserving,
TestCheckDependencyCycleInTxCycleIsWrapPreserving,
TestCycleDetectionTablesUseBothTablesByDefault,
TestCycleReachabilityQueryMultipleTablesTraversesUniqueNodes`; `internal/storage/issueops/
cycle_report_test.go:TestCanonicalCyclePathsRotatesEachCycleToItsLowestID,
TestCanonicalCyclePathsCollapsesParallelEdges, TestBuildCyclesReportsAnHonestPartial,
TestCanonicalMixedCyclePathsIgnoresAPureTracksLoop`; conformance
`RunCycleDetectorReportsNoCycleForAnAcyclicSubgraph`,
`RunCycleDetectorFindsADurableCycleRotatedToItsLowestID`,
`RunCycleDetectorMergesTheDurableAndEphemeralPlanes`,
`RunCycleDetectorFollowsOnlyBlockingEdges`,
`RunCycleDetectorReportsAnHonestPartial`; `cmd/bd/dep_cycles_test.go:
TestDepCyclesNamesTheMembersItCannotDescribe, TestDepCyclesTruncatesTheRenderedListButNotTheCount,
TestRunDepCyclesThreadsIncludeTracks`.

**Edge cases.** `waits-for` affects readiness but is excluded from the default
cycle report; parent-child participates in add-time scheduling checks. Cross-
plane edges and unreadable nodes must not disappear from a report; bulk callers
may explicitly skip checks.

**Plug disposition.** **Keep** refusal and deterministic report semantics;
A4 adds the sorted `graph/blocking` sentinel lock so concurrent writes cannot
close a cycle together. Plug's cycle/error result is JSON-RPC meaning, not
Beads CLI text (P46).

## B07 — dependency vocabulary and edge effects

**Behavior.** Dependency types have a fixed set of well-known workflow,
association, graph, entity, and tracker values, while nonempty custom values
within the column limit are accepted. Only blocks/parent-child/conditional-
blocks/waits-for affect ready membership; parent-child is scheduling but not a
blocking edge, and re-adding the same edge/type is idempotent while changing
its type is refused.

**Beads implementation.** `internal/types/types.go:DependencyType.IsValid`,
`IsWellKnown`, `AffectsReadyWork`, `IsBlockingEdge`, `IsSchedulingEdge`;
`internal/storage/issueops/dependencies.go:AddDependencyInTx`,
`RemoveDependencyInTx`; `internal/storage/issueops/dependency_editor.go` validation and execution.

**Pinning tests.** `internal/types/types_test.go:TestDependencyTypeIsValid,
TestDependencyTypeIsWellKnown, TestDependencyTypeAffectsReadyWork,
TestParseWaitsForGateMetadata`; `cmd/bd/protocol/dep_identity_test.go:
TestProtocol_DepReAddSameTypeIsIdempotent`, `TestProtocol_DepReAddDifferentTypeIsAnError`;
`internal/storage/issueops/dependencies_test.go:TestDependencyMetadataEqualComparesValuesNotBytes,
TestReAddedDependencyThreadKeepsTheStoredThreadUnlessANewOneIsNamed`; conformance `RunDependencyEditorMixedBatchWritesBothPlanes`,
`RunDependencyEditorMixedBatchRefusalRollsBackBothPlanes`,
`RunDependencyEditorSameTypeReAddIsIdempotent`,
`RunDependencyEditorRetypeRefusalLeavesTheOriginalEdge`,
`RunDependencyEditorAcceptsAnExternalTarget`,
`RunDependencyEditorRefusesSelfDependencyWithTheProbeSkipped`,
`RunDependencyEditorMaintainsBlockedStateAcrossPlanes`.

**Edge cases.** `relates-to` and other non-blocking edges never block or close-
guard a bead; waits-for supports gate metadata and `also_blocks`; external
and cross-prefix targets can be unresolved but must remain represented.

**Plug disposition.** **Keep** edge identity, routing, and graph effects. The
well-known v1 dependency kinds are fixed and cannot be redefined (D25); opaque
custom kinds remain importable where the Beads store contains them (P22), but
no custom kind changes kernel readiness or guards. Conditional outcome semantics
change per B02. Formulas/molecules that create these graphs become a workflow
plugin (P48), not core behavior.

Public parent-child writes to an explicit `external:` capability target are
refused without state changes, matching Beads; bare cross-prefix references
remain accepted as unresolved, and imported-edge reads are unchanged. Parent
defer is a named difference: Plug's deferred category suppresses descendants
without an `until`, compared with Beads's future-`until` suppression. Beads's
indefinite defer leaves children ready and is recorded as a separate native
control (orchestrator ruling 2026-11-11 18:46Z).

## B08 — close guards and lifecycle settlement

**Behavior.** Closing is transactional and refuses templates, pinned beads,
foreign assignees, open children, live blockers, failed gate conditions, or
stale expected revisions according to the applicable guard; force bypasses
policy guards but never templates or CAS. A successful close settles dependent
readiness across both planes, is idempotent for an already-closed row, and
reopen reblocks it while retaining the claim holder where Beads does.

**Beads implementation.** `internal/storage/issueops/close_guard.go:CheckClosable`,
`enforceCloseGuardsInTx`; `internal/storage/issueops/close.go:CloseIssueCheckedInTx`,
`EnforceClosePolicyInTx`, `countOpenChildrenForTargetInTx`.

**Pinning tests.** `internal/storage/issueops/close_guard_test.go:TestCheckClosable`,
`TestRefusedMoleculeAutoCloseReportsTheRefusalsOwnSentence`; `cmd/bd/protocol/close_guard_test.go:
TestProtocol_CyclePreventionBlocks`, `TestProtocol_SelfDependencyPrevention`,
`TestProtocol_CloseGuardRespectsDepType`, `TestProtocol_CloseGuardBlocksForBlocksDep`,
`TestProtocol_CloseForceOverridesGuard`; conformance `RunLifecycleCloseRefusalsCarryTheirTypesAndWriteNothing`,
`RunLifecycleCloseCountsOpenChildrenInBothPlanes`,
`RunLifecycleCloseIsIdempotentAndKeepsTheFirstClose`,
`RunLifecycleCloseAndReopenKeepTheClaimHolder`,
`RunLifecycleCloseSettlesItsTransitiveAndCrossPlaneDependers`,
`RunLifecycleCloseEnforcesTheCloseGuards`.

**Edge cases.** A stale `is_blocked` flag without a live direct blocker does
not itself prevent close; pinned children count as open under current literals;
force does not bypass a template. Batch close reports item-indexed refusals.

**Plug disposition.** **Deviate** fixed A3 guards are
`fields_required`, `evidence_required`, `no_open_children`,
`no_open_blockers`, `not_template`, `gates_resolved`, and `permission` (P23b);
workflow slots and categories replace status literals, and permission asks the
`transition` action (AZ9). Conditional/custom rules move to validating pre
subscribers.
