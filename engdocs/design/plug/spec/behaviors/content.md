# Identity and content behavior cards

Source pin and evidence limits: [behavior catalog](../behaviors.md).

## B09 — hash IDs and adaptive collision handling

**Behavior.** The shared hash allocator emits `prefix-<base36>` from the
SHA-256 input `title|description|creator|timestamp.UnixNano|nonce`.
`internal/idgen.GenerateHashID` has explicit digest byte widths for requested
lengths 3 through 8; any other requested length uses its three-byte fallback,
while `EncodeBase36` still pads or truncates to the exact requested width. The
allocator contract is path-specific: the legacy `issueops` table allocator
hard-caps its maximum tried length at 8 (the documented 3-through-8 range),
while the domain `mintTopLevelID` path reads the configured
`max_hash_length` and tries through that maximum (default 8;
`docs/core-concepts/adaptive-ids.md` pins an example of 10).
`max_collision_prob` chooses the birthday-paradox starting length; it is not a
retry budget. At each tried
length, the paths make exactly ten nonce attempts (`0` through `9`) before
advancing to the next length, and refuse after the final length is exhausted.
The separate `internal/types.GenerateHashID` helper returns a full hexadecimal
SHA-256 over title, description, `RFC3339Nano` creation time, and workspace ID;
it is not the allocator's hash contract.

**Beads implementation.** Allocator: `internal/idgen/hash.go:EncodeBase36`,
`GenerateHashID`; legacy path `internal/storage/issueops/helpers.go:
GenerateIssueIDInTable`, `GetAdaptiveIDLengthTx`, `GetAdaptiveConfigTx`; domain
path `internal/storage/domain/issue.go:mintTopLevelID`,
`internal/storage/domain/db/config.go:GetAdaptiveIDConfig`. Separate helper:
`internal/types/id_generator.go:GenerateHashID`.

**Pinning tests.** Allocator: `internal/idgen/hash_test.go:
TestGenerateHashIDMatchesJiraVector`; `internal/storage/dolt/store_unit_test.go:
TestGetAdaptiveIDLength`, `internal/storage/dolt/create_issue_commit_test.go:
TestCreateIssueCommitsInitialRelationalData`, and
`internal/storage/issueops/create_batch_cache_test.go:TestFlushAuxEventsMintsThePerRowIDs`.
Separate helper: `internal/types/id_generator_test.go:
TestGenerateHashID_Deterministic`, `TestGenerateHashID_DifferentInputs`.

**Edge cases.** For the allocator, unsupported lengths use the three-byte
fallback but still produce the requested base36 width. Thus the domain path
can try lengths 9 and 10 when `max_hash_length=10`, with candidates derived
from only those three digest bytes; the legacy path stops at 8 even when its
configuration is higher. Existing imported IDs, explicit prefixes, and
collisions between rows in one batch must be considered; a creator, timestamp,
or nonce change changes its digest. The separate helper has no creator or nonce
and is classified independently.

**Plug disposition.** **Keep the native domain allocator:** Plug's single
allocator contract follows that path's configured maximum, including the
pinned `max_hash_length=10` example, and preserves that adaptive setting across
restore (A1/P44). It does not promise byte-identical behavior with the legacy
issueops/Dolt allocator above length 8; for lengths 3 through 8 it keeps the
copied `idgen` encoding and the fixed ten-nonce retry semantics. A card
additionally proves no generated hash collides with a restored ID. The
`internal/types.GenerateHashID` helper is a separate legacy utility, not
allocator evidence; its own golden is deferred until a caller requires that
helper's behavior.

## B10 — hierarchical child IDs and counters

**Behavior.** A child ID is `parent.N`; the allocator never reuses a number,
scans only direct children, repairs stale counters from observed IDs, and
routes active-wisp parents to the wisp counter/table. Hierarchy depth uses the
`hierarchy.max-depth` setting: the default maximum is 3, positive configured
values are honored, and breadth is unbounded.

**Beads implementation.** `internal/types/id_generator.go:GenerateChildID`,
`ParseHierarchicalID`, `CheckHierarchyDepth`; `internal/storage/issueops/child_id.go:GetNextChildIDTx`;
`internal/storage/issueops/create.go:ReconcileChildCounters`.

**Pinning tests.** `internal/types/id_generator_test.go:TestGenerateChildID`,
`TestParseHierarchicalID`, `TestCheckHierarchyDepth`; `cmd/bd/protocol/child_id_test.go:
TestProtocol_ChildIDCounterNeverReusesNumbers`; `internal/storage/domain/db/
child_counter_test.go:TestChildCounterSQLRepository` (the FirstChildReturnsDotOne,
MonotonicallyIncrements, RecoversFromStaleCounter, GrandchildrenDoNotInfluenceCounter,
and Wisp subtests); `internal/storage/embeddeddolt/child_id_test.go:TestGetNextChildID`;
conformance `RunIssueOperationsCreateUnderAParentMintsTheNextChildID`.

**Edge cases.** Deleted/closed children do not free a number; explicit `.8`
advances a stale counter; grandchildren do not influence a parent's next
number. With `maxDepth=5`, depth 4 is accepted and depth 5 is refused;
nonpositive `hierarchy.max-depth` configuration values are refused, while the
unset/default path uses 3. Missing parents may be skipped during import
reconciliation, while an empty/missing parent at allocation is an error.

**Plug disposition.** **Keep** the dotted ID and counter semantics. Restore
seeds durable, child, and wisp counters from stored counters plus observed IDs;
acceptance covers continuation after restore (P44/A1).

## B11 — dependency IDs and edge identity

**Behavior.** A dependency row ID is deterministic UUIDv5 over `(issueID,
target)` and therefore re-adding the same endpoints addresses the same edge;
dependency type and metadata are separate mutable identity fields. Renaming or
moving an endpoint rekeys related rows without creating duplicates.

**Beads implementation.** `internal/storage/depid/depid.go:New`;
`internal/storage/issueops/dependencies.go` target replacement and metadata
comparison.

**Pinning tests.** `internal/storage/depid/depid_test.go:TestNewGolden`,
`TestNewDeterministic`, `TestNewNoCollision`; dependency rename/merge/conflict
and `internal/storage/issueops/dependencies_test.go:TestReplaceDependencyTargetRekeysCascadedRows`,
`TestDependencyMetadataEqualComparesValuesNotBytes`.

**Edge cases.** The UUID namespace omits dependency type, so same endpoints
with a different type are a typed conflict; metadata equality is semantic JSON,
not byte spelling. Cross-plane and external targets may be unresolved but keep
their canonical edge identity.

**Plug disposition.** **Keep** copied depid behavior and endpoint identity;
represent type/metadata changes as typed `link`/`unlink` changes in one
ChangeSet (P1/P2/D25).

## B12 — compaction and reversible snapshots

**Behavior.** Tier 1 compacts closed issues at least 30 days old and tier 2
compacts level-1 issues at least 90 days old; eligibility requires closed_at
and the expected prior level. The original text is snapshotted before overwrite,
a summary must be smaller, and restore reverses the text/level while retaining
the snapshot audit row.

**Beads implementation.** `internal/storage/issueops/compaction.go:CheckEligibilityInTx`,
`SnapshotIssueInTx`, `ApplyCompactionInTx`, `RestoreFromSnapshotInTx`,
`GetTier1CandidatesInTx`, `GetTier2CandidatesInTx`; `internal/compact/compactor.go:CompactTier1`.

**Pinning tests.** `internal/storage/dolt/compact_test.go:TestCheckEligibility_Tier1EligibleOldClosed`,
`TestCheckEligibility_Tier2Eligible`, `TestGetTier1Candidates_ReturnsEligible`,
`TestApplyCompaction`, and `TestSnapshotRestoreRoundTrip`;
`internal/compact/compactor_unit_test.go:TestCompactTier1_Success`,
`TestCompactTier1_SnapshotBeforeOverwrite`, `TestCompactTier1_SnapshotError`,
`TestCompactTier1_Ineligible`, `TestCompactTier1_SummaryNotSmaller`,
`TestCompactTier1Batch_MixedResults`, and `TestNew_NoAPIKeyFallsToDryRun`;
`cmd/bd/restore_embedded_test.go:TestEmbeddedRestore`.

**Edge cases.** Open/recent/closed-without-timestamp, already-compacted, bad
tier, and invalid/nonpositive config thresholds are ineligible. Dry-run and
missing API key do not mutate; snapshot/update failure must not lose original
text; batch compaction returns per-item outcomes and is not atomic.

**Plug disposition.** **Deviate** eligibility, snapshot-before-overwrite, and
restore stay; the old text is an ordinary permanent diff version (`history.get`,
P45/P73) rather than only a separate snapshot. Core accepts a supplied summary;
a plugin may call an LLM, but no outbound API key path is enabled by default
(CP6). Controlled Go/SQL clocks pin 30/90-day boundaries. Each `admin.compact`
call compacts one record, the next eligible one in (closed time, ID) order;
repeating calls is the non-atomic batch. A refusal writes nothing. Once a
record is selected, every refusal or failure names it (`details.id`), and a
not-smaller summary is refused as not retryable. The optional exclusive
`afterId` cursor resumes after a named closed record's latest close position,
so one record whose summary is never smaller does not block the rest (P47).
The cursor joins only the idempotency request hash; compacted records keep the
`plug-compaction/1` metadata unchanged.

## B13 — memories: key derivation and plane isolation

**Behavior.** `remember` stores content verbatim under `kv.memory.<key>`; an
absent key is derived by lowercasing and slugging content with an eight-segment,
60-byte cap. Recall distinguishes a missing key from an existing empty value,
forget removes only the exact memory row, and list/search never leaks generic
settings rows.

**Beads implementation.** `internal/memoryapi/memoryapi.go:DeriveKey`,
`ValidateContent`, `ResolveKey`, `FilterMemories`; `internal/storage/memoryops/memories.go:
RememberInTx`, `RecallInTx`, `ForgetInTx`, `ListInTx`; `cmd/bd/prime.go` memory
rendering.

**Pinning tests.** `internal/memoryapi/memoryapi_test.go:TestDeriveKeyGoldenTable`,
`TestDeriveKeyNeverExceedsTheCap`, `TestValidateContentRefusesNothingToRemember`,
`TestResolveKeyPrefersTheExplicitKey`, `TestFilterMemoriesFoldsBothSides`;
`internal/storage/memoryops/memories_test.go:TestStorageKeyEncodesTheFullPrefix`,
`TestMemoriesFromConfigNarrowsToTheMemoryPlane`, `TestMemoriesFromConfigAnswersAnEmptyMap`;
conformance `RunMemoriesRememberStoresContentVerbatim`,
`RunMemoriesRememberDerivesTheKeyWhenAbsent`,
`RunMemoriesRememberReplacesAndReportsIt`,
`RunMemoriesRecallReportsAMissAsNotFoundNotAnError`,
`RunMemoriesEmptyRowIsPresentUntilForgotten`,
`RunMemoriesForgetNeverTouchesTheSettingsPlane`,
`RunMemoriesListSearchMatchesKeyOrValueCaseInsensitively`,
`RunMemoriesARefusedWriteRecordsNoHistory`; `cmd/bd/protocol/memory_contract_test.go:
TestProtocol_MemoryCRUDSurface`, `TestProtocol_MemoryReservedNamespace`.

**Edge cases.** Whitespace-only content or explicit keys and punctuation-only
underivable content are refused; explicit Unicode/spaced keys are preserved
when nonblank. Replacement reports whether a row existed; list search is
case-insensitive over key and value and returns a nonnil empty map.

**Plug disposition.** **Deviate** semantic key/content/refusal behavior is
kept, but memories become a core `memory.*` record kind returned in
`agent.brief`, rather than config rows injected only by `bd prime` (P85). The
parity set includes this card if the store inventory finds `remember`/`prime` in
use.

## B14 — gates and waits-for members

**Behavior.** Gate beads are graph nodes with typed await fields and waiters;
waits-for all/any semantics and `also_blocks` determine readiness, while gate
field validation bounds IDs, timeout, and waiter JSON. Gate listing applies
explicit template/closed filters and drops missing rows rather than turning a
query failure into an empty answer.

**Beads implementation.** `internal/storage/issueops/gate_fields.go:
ValidateGateFields`, `ValidateGatePatch`; `internal/types/gated.go:GateIsHolding`,
`GatesHolding`; `issueops/gates.go:ListGates`; waits-for SQL in
`internal/storage/issueops/blocked_state.go` and constructors in
`internal/types/types.go`.

**Pinning tests.** `internal/storage/issueops/gate_fields_test.go:
TestValidateGateFieldsBoundsEachMemberAtItsColumn,
TestValidateGatePatchChecksOnlySetMembers, TestUpdateFieldsMapsWaitersAsAWholeValue`;
`internal/types/gated_test.go:TestGatesHoldingSelectsOnlyLiveGates`,
`TestGatesHoldingSubjectStatus`, `TestSubjectCanBeGated`; `cmd/bd/gate_test.go:
TestFilterIssueGates, TestGateCheckSkipsTemplateGates,
TestCreateGateRefusalWritesNothing, TestAddGateWaiterIsAGuardedWholeListUpdate,
TestFindPendingGatesListsThroughListGates`; conformance
`RunLifecycleCreateWritesTheGateMembers`, `RunLifecycleCreateBoundsTheGateMembers`,
`RunLifecycleUpdatePatchesTheGateMembers`, `RunLifecycleUpdateBoundsTheGateMembers`,
`RunBatchApplyCarriesTheGateMembers`; `cmd/bd/gate_embedded_test.go:
TestEmbeddedGateWaiterCheckLifecycle`; `backend/http/e2e_gates_test.go:TestE2E_GatesOverHTTP`.

**Edge cases.** Any-child with no children, all-child closure, duplicated
waiters, timeout zero, and waits-for edges that do not decorate ordinary gate
subjects are distinct. Gate fields are whole-list replacements, not set merges;
invalid one-item batch input rolls back the whole batch.

**Plug disposition.** **Deviate** retain graph-node blocking and slot settlement,
but use A3 `gate_kind` resolvers (`poll`, `push`, `manual`), CAS approvals,
resolved/fail slots, timeout escalation, and typed `gates_resolved` guards.
Anonymous mode has one principal; unauthenticated multi-approval plans are
refused (P64). Formula-created gates move to the workflow plugin (P48).

## B15 — comments and comment ordering

**Behavior.** A comment preserves exact nonblank text and author, routes by the
anchor's durable/wisp plane, and orders a thread by `(created_at,id)` with a
keyset cursor. Live timestamps use the clock unless the newest comment is at or
after now, in which case they advance one second; a durable comment records one
history/event without mutating the issue row, while a wisp comment records no
durable history.

**Beads implementation.** `internal/storage/issueops/commenter.go:
ValidateAddCommentRequest`, `ExecuteAddComment`; `internal/storage/issueops/comments.go:GetIssueCommentsPageInTx`,
`AddIssueCommentInTx`, `ImportIssueCommentInTx`.

**Pinning tests.** conformance `RunCommenterStoresTextVerbatim`,
`RunCommenterAdvancesALiveStampPastTheThreadsNewestComment`,
`RunCommenterCommentOnAWispLandsOnTheWispThread`,
`RunCommenterRefusesAnIDOnNeitherPlane`,
`RunCommenterDoesNotResolvePrefixes`,
`RunCommenterRecordsExactlyOneHistoryEntry`,
`RunCommenterLeavesTheAnchorIssueUntouched`; `internal/storage/dolt/commenter_contract_test.go:
TestCommenterContract`, `internal/storage/dolt/commenter_persistence_test.go:TestDoltStoreCommenterNamesTheIssueInHistoryAndCommitsTheTable`,
`TestDoltStoreCommenterOnAWispSweepsNoPendingRow`,
`internal/storage/dolt/comments_page_test.go:TestGetIssueCommentsPageWalkEqualsFullRead`,
`TestGetIssueCommentsPageEmptyThread`, `TestGetIssueCommentsPageWisp`;
`cmd/bd/protocol/comments_test.go:TestProtocol_CommentRejectsEmptyText`.

**Edge cases.** Empty IDs are validation errors; unknown nonempty IDs are not
found; prefixes are never resolved. Pages default to 100 and cap at 500;
request objects remain unmodified and missing threads return an empty page.

**Plug disposition.** **Keep** verbatim/validation/order/plane semantics in
`bead.comment` (P2). Plug adds Markdown/reference indexing in the view layer;
wire prose and CLI envelopes remain dropped under P46.

## B16 — labels and label filters

**Behavior.** Labels are sorted, exact values with bounded length, and add/remove
are idempotent row operations; label patching replaces the whole set in the
specified order (remove wins), and rename merges rows while reporting renamed
and merged counts across durable and wisp planes.

**Beads implementation.** `internal/storage/issueops/labels.go:GetLabelsInTx`,
`AddLabelInTx`, `RemoveLabelInTx`, `RenameLabelInTx`; SQL predicates in
`internal/storage/sqlbuild/labels.go`.

**Pinning tests.** `internal/storage/dolt/labels_test.go:TestGetLabelsForIssues`,
`TestGetLabelsForIssues_MixedWispAndPermanent`, `TestAddAndRemoveLabel`,
`TestAddLabel_Duplicate`; `internal/storage/dolt/label_rename_test.go:
TestRenameLabel_Basic, TestRenameLabel_Merge, TestRenameLabel_WispSwept,
TestRenameLabel_OverLongNewLabelRefused`; conformance
`RunIssueOperationsUpdateLabelPatchOrdering`, `RunIssueOperationsUpdateLabelPatchValueRules`,
`RunLifecycleCreateInheritsParentLabels`, `RunLifecycleUpdateReplacesTheLabelSet`;
`cmd/bd/protocol/preservation_test.go:
TestProtocol_LabelsPreservedAcrossUpdate`, `TestProtocol_StatusTransitionsPreserveLabels`.

**Edge cases.** Empty label entries are dropped in patches; duplicate entries
apply once; overlong labels refuse without VARCHAR truncation. Rename is
case-sensitive, same-name-after-trim is refused, absent old labels are a
no-op, and a concurrent add is counted in the merge result.

**Plug disposition.** **Keep** label state/filter semantics through typed
`patch`/views (P23a/P31b), with each actual durable change in the A4 ChangeSet;
no-op behavior and wisp isolation remain cards.

## B17 — metadata compare-and-set

**Behavior.** A metadata CAS compares one key canonically, distinguishing absent
from JSON null, preserves sibling keys, and either swaps/removes the key or
returns the current value with no write. Equal target and absent-to-absent
operations are no-ops; durable swaps mint one history entry while wisp swaps
remain ephemeral.

**Beads implementation.** `internal/storage/metadata_cas.go:PlanCompareAndSetKey`,
`CanonicalMetadataValue`, `MetadataValuesEqual`; `internal/storage/issueops/metadata_cas.go:
CompareAndSetMetadataKeyInTx`.

**Pinning tests.** `internal/storage/metadata_cas_test.go:TestCanonicalMetadataValueIgnoresFormatting`,
`TestCanonicalMetadataValueKeepsNumbersLiteral`, `TestMetadataValuesEqualTreatsNilAsAbsent`,
`TestCanonicalMetadataValueRefusesMalformedInput`, `TestPlanCompareAndSetKeyRefusesUnusableRequests`,
`TestPlanCompareAndSetKeyCanonicalizesWithoutTouchingTheRequest`, and
`TestPlanCompareAndSetKeyKeepsAbsence`;
`backend/conformance/metadata_cas_contract.go`: `RunMetadataCASCreatesAKeyThatWasAbsent`,
`RunMetadataCASRefusesASecondCreateAndReportsTheHolder`,
`RunMetadataCASSwapsOnAMatchAndReportsTheNewValue`,
`RunMetadataCASRefusalReportsTheCurrentValueAndWritesNothing`,
`RunMetadataCASDistinguishesAnAbsentKeyFromAStoredNull`,
`RunMetadataCASPreservesSiblingKeys`, `RunMetadataCASNoOpSwapWritesNothing`,
`RunMetadataCASAWispSwapRecordsNoDurableHistory`,
`RunMetadataCASRecordsExactlyOneHistoryEntry`; backend
`internal/storage/dolt/metadata_cas_contract_test.go:TestMetadataCASContract`,
`internal/storage/embeddeddolt/metadata_cas_contract_test.go:TestMetadataCASContract`, and
`internal/storage/uow/metadata_cas_contract_test.go:TestMetadataCASContract`.

**Edge cases.** Canonicalization sorts object keys and preserves numeric
literal spelling (`1` differs from `1.0`); malformed/trailing JSON is refused.
The holder/current answer on a lost race is a nil-error refusal today, and the
request must not be mutated.

**Plug disposition.** **Keep** canonical and plane semantics, but expose
field-scoped expected revision/hash through `setMeta` (P31a/A1) and the
standard typed `precondition_failed` result; protected ontology/plugin/lock
records refuse mutation.
