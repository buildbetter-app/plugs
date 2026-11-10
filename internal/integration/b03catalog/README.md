# B03 installed catalog acceptance

Done = custom catalogs and same-key project overlays are accepted through
public `ontology.plan` / `ontology.apply`, used by ordinary bead operations,
and preserved with their revisions, history and refusal state after SQL
close/open. The configured production composition now selects and validates
the installed protected source at store head, shares its compiled snapshot
with the ordinary Service and Writer, binds the ontology catalog, and emits
durable ontology-pinned audit events. The real `composition.Open` path is
covered separately across memory, SQLite and PostgreSQL; compiler tests,
fixture presence and inherited/base PASS do not count as runtime or full-card
qualification.

Full B03 additionally requires this author's main-based accepted-artifact
preparation, four matching physical canonical exports with complete command
and SQL evidence, actual memory/SQLite/PostgreSQL replay, and independent
different-ID review. The native producer dependency recorded below keeps that
qualification blocked.

Scope = B03's installed catalog boundary, configured startup/source selection,
catalog RPC wiring and the routed key-continuity guard in
`internal/core/ontologyops/plan.go`, plus the direct lifecycle timestamp
implementation adopted from `2db4afa54b6166385954871877ed7c7371a957b7`.
Creation is immutable, claim stamps the started entry, terminal slots stamp
the closed entry, repeated close preserves that first close, and reopen clears
the terminal entry while retaining the started entry. These are core-owned
application-clock instants, separate from driver-generated commit times.

Evidence status: **AUTHORED; qualification remains exact-source and receipt-bound**.
Focused, whole, hosted, native and full-card claims remain separate; no native
producer or full-card qualification is claimed by this file.

## Adopted behavior

`engdocs/design/plug/spec/behaviors/work.md`, B03, adopts Plug semantics.
`engdocs/design/plug/design.md`, A3, states that keys are immutable, names are
free text, and elements are archived, never reused. P10 stores versioned
server ontology; P41 provides a base plus project overlays; D24 replaces each
same-key definition whole. P65 permits overlays to archive base elements only.
No historical Beads parser limit is added.

The production change preserves candidate-local compilation. Before planning
migration rows, it compares the installed and candidate base and effective
projects. It refuses missing old keys and archived-to-active keys for statuses,
workflows, types and gate kinds with `invalid_params` and an element path.
Removing a project tombstone cannot reveal an active base definition again.
Active definition replacement and free-text display-name edits remain valid.

## Connected assertions

| Test | Observable assertions |
| --- | --- |
| `TestInstalledCatalogPlanApplyLifecycleAndDurableReopen` | A public catalog containing more than 50 custom statuses is accepted; complete existing rows migrate with source in one attributed revision; historical serialized bytes remain exact; ordinary create/ready/claim/close/reopen/defer/fail/cancel use installed slots; source history and diff reflect accepted definitions; actual SQLite/PostgreSQL close/open preserves rows, versions, receipts and ready membership. |
| `TestInstalledCatalogLifecycleTimestampsPreserveStatusSpecificEntries` | Public custom-slot create/claim/close/repeated-close/reopen on all three engines preserves the immutable creation and last started entries, retains the first close instant for repeated settlement, and clears the closed entry on reopen. |
| `TestSameKeyProjectOverlaysReplaceWholeDefinitionsAndOlderReplay` | The same key is completed/failure in base, unstarted in west and backlog in east. Omitted outcomes are cleared by whole replacement. Public creates, readiness and claim refusal follow each project's category. Older accepted apply replay refreshes from current durable source. Overlay behavior persists after SQL close/open. |
| `TestInstalledBaseKeysArchiveWithoutDeletionRenameOrResurrection` | Public plans refuse deletion of an active unused key; accepted archival retains status category/outcome; old plans cannot apply over the new source; all four definition kinds refuse deletion or resurrection; key rename refuses; archived display-name edits remain accepted; permanence survives SQL reopen. |
| `TestProjectArchivedKeysCannotBeReactivatedByOverlayRemovalOrReplacement` | Minimal tombstones preserve base definitions whole, without changing other projects. Removing or replacing tombstones cannot reactivate any definition kind. Unknown-base archival refuses. Removing an unused project-local key also refuses. Tombstones survive SQL reopen. |
| `TestMalformedCatalogAndMixedBatchRefuseAtomically` | Unknown categories/members, duplicate definitions, null configuration and unsupported directives refuse. A valid update/create/comment batch prefix cannot survive an invalid status suffix. A malformed protected source refuses reads, plan, retained apply and connected reload without replacing durable bytes or the prior runtime. |
| `TestCatalogApplySQLPublicationRollbackAndRetry` | Database-side publication faults exercise actual SQLite/PostgreSQL transactions. Failed public apply preserves source, rows, versions, log/head, derived rows, observations, allocators and every receipt. The same key succeeds after fault removal and remains durable after close/open. |
| `TestPublicCatalogApplyDryRunEventLimitParity` | Real public batches create 999 rows; dry-run and real apply accept exactly 1,000 source-plus-row descriptors. A no-op dry-run publishes nothing. With 1,000 rows plus changed source, both dry-run and real apply refuse `scope_too_large` and conserve complete paginated records, histories, events, derived state, receipts and views on all three engines. |
| `TestPublicNativeCategoryCatalogLifecycleAndSQLReopen` | Public plan/apply retains exact `awaiting_review` / `qa_testing` / `archived` / `on_hold` keys, native `active` / `wip` / `done` / `frozen` categories and a custom type. Ordinary public lifecycle, readiness, first-close and reopen timestamps correspond on all three engines; actual SQL reopen preserves the complete durable snapshot and create receipt. |
| `TestPublicNativeCategoryOverlaysUseProjectCategoryAndKeepExactKeys` | The same exact native status key is ready in base and non-ready with project-local `wip` or `frozen` categories. Public claim refusal and reopen follow each project's installed category and slots; exact definitions and rows survive SQL reopen. |
| `TestPublicMalformedNativeCategoryCatalogRefusesWithoutMutation` | Unknown/case-changed native categories, incompatible native outcomes, a claim slot pointing at `active` and an absent close-slot status refuse through public planning. A valid batch prefix followed by a status patch refuses atomically, conserving all existing durable snapshot planes. |
| `TestConfiguredRuntimeLoadsCatalogAndAuditsOnEveryEngine` | Real `LoadConfigFile` -> `composition.Open` and public byte RPC on memory, SQLite and PostgreSQL load the installed source (SQL uses production `openStore`; preseeded memory uses StoreFactory), bind catalog methods, refresh the shared ontology after `ontology.apply`, apply the new workflow to an ordinary create, and pin the refreshed revision and digest in the durable audit/event stream. |

The bootstrap inserts only the built-in ontology. Custom catalogs, project
overlays, archival and ordinary bead writes all use real byte RPC with canonical
result validation, the canonical protected Writer and the shared
`OntologyRefresher`. Configured startup loads the protected source from the
store head before admitting Service, Writer or RPCs; missing, wrong-kind,
decoder-malformed, validly decoded but uncompilable, and unprotected sources
close the store and fail closed. `TestServerCommandConfiguredOntologyUsesInstalledCatalogOverHTTP`
separately starts the standalone CLI command from `.plug/config.yaml` on real
SQLite and verifies the installed custom catalog and ordinary create over HTTP. A
source-corruption fixture is explicitly fault injection, not producer
acceptance. The SQL publication fault does not replace the Writer. A configured authorization
chain must supply current ontology read authority; unsupported protected write
authorization keeps `ontology.apply` unadvertised. Event delivery requires the
trusted `EventAuthorization` port for both ordinary images and every condition
resource whenever policy is configured. The anonymous/no-policy fallback is
rechecked on each read; installing policy cannot retain `AllowAll`. Composition
regressions prove ordinary event denial, condition-resource denial, missing
capability refusal and installed/later-installed ontology authority refusal.
These trusted fixtures do not establish positive native/AZ9 authorization.

Refusal snapshots require complete bounded record/version/log inventories,
all derived tables, known record/predicate/allocator observations, allocator
presence, all issued-key receipt/changeset lookups, and ordinary current/ready
views. Missing inventory or engine capabilities fail the test. PostgreSQL is an
owned disposable pinned image; unavailable Docker/PostgreSQL fails the target.
The target's `exclusive` tag isolates this engine-owning proof only.

The native-category public tests retain reversible, exact request/response byte
strings and lifecycle checkpoint documents in `*.public-catalog.json` test
outputs, plus a named memory/SQLite/PostgreSQL correspondence vector. These
inputs are public catalog fixtures. They do not replace the native recorder's
accepted executable, physical stores, canonical exports or importer evidence;
native producer and full-card qualification remain separate.

## Exact-source verification

After committing and sealing build/vet evidence, start one focused gate and one
literal `//...` gate on the same commit through `/tmp/plug-swarm/bin/remote-bazel`.
The wrapper receives the worktree, commit and label before `-- test`. Preserve
`--cache_test_results=no --flaky_test_attempts=1 --test_env=GO_TEST_WRAP_TESTV=1
--test_output=all --test_timeout=900,900,900,9000 --jobs=2
--local_resources=memory=8192` in both commands. Focus covers ontologyops,
ontologyrpc, pipeline, claimsql, composition, CLI and this target. Source review
runs independently and is not a dispatch hold.

Preserve every original failure and historical candidate. Preserve the complete
nonpoll terminal output before parsing it, excluding `poll.log` by name before
metadata, reads, hashes or copies. Raw named cases, XML, output inventory and
source/dispatch/artifact identities must be recorded separately from target
summaries. A focused PASS does not qualify whole-tree, hosted, native or full
B03/S1 behavior. Native/public producer evidence and full-card qualification
remain unclaimed until their required real receipts exist.

## Native catalog recording boundary

The separate manual `b03catalog_native_test` consumes the committed
`native_catalog_inputs.json` and accepted R418 executable through the committed
main preparer. Ordinary `b03catalog_test` and literal `//...` exclude this
recorder. The input preserves the accepted producer/control/readiness tuple;
the historical B01 input is unchanged. Each recording owns four Dolt databases
with different prefixes and root IDs, and retains every physical command,
stdout/stderr hash, inventory sidecar, complete 33-table SQL census, concurrent
catalog read, canonical export refusal and validated archive metadata.

The canonical gate requires exact custom status/type values and provenance
rows, the production strict-union import migration, and a nonempty validated
archive from each physical store. Empty exports, synthetic archives and
input-derived custom ontology seeds cannot qualify. The recorder bootstraps
only a built-in protected source, then publicly plans/applies the migration
document derived by the validator from that store's actual native archive.
Custom lifecycle slot edits are a subsequent public action, distinct from the
producer's original catalog. Replay acts on the imported root/project/type on
memory, SQLite and PostgreSQL. It checks active readiness, claim, terminal and
frozen refusals, close/repeated-close/reopen timestamps, full refusal
conservation, and exact durable state across SQL close/open plus public checks
after reopen. Memory has no durable reopen claim. Raw RPC streams, imported
metadata and timestamp bytes remain available; correspondence compares actual
semantic images across all four different IDs and three engines, normalizing
only physical root/project identity and wall instants. Source and compilation
alone do not establish any of these runtime outcomes.

At `a813392398e2ed7be9453a8ef7c193c437c6fdb4` (tree
`b6fdb8fd649c99413a7aa5212c7ce869e18da9b2`), native RunsOn
`41948679662` completed with evidence status `ok`, exit 3, raw
**1 RUN / 0 PASS / 1 FAIL / 0 SKIP**, and XML **1 test / 1 failure / 0 errors /
0 skipped**. Its immutable receipt is
`/tmp/plug-swarm/ci-local/remote/b03-main7a43-native-0005-a813392398e2-20261105T012739Z-99174fa1f331`;
the archived testlogs SHA256 is
`e1b6ba2c61a74eda174ad2467c60e23ffab7a7e1d67fccffb19454080726d7d5`.
The independent reviewer confirmed all four real roots retained
`awaiting_review` / `delivery`, all before/after SQL bytes and rows matched,
and all 316 commands / 632 stdout+stderr streams were hash-bound. The recorder's compact-JSON
substring check was a false negative; typed decoding replaces it without
changing this RED receipt.

The actual producer dependency remains literal: native-a/b/d refused canonical
export with `Error: plug-dump/1 refused: unsupported nonempty family
custom_statuses (live claims must be resolved)`; native-c refused the same way
for `custom_types`. There are **no canonical archives and no paired replay**.
The existing producer-owned capability route is controller bundle `2f2/564`.
No further native run is authorized against the unchanged c8eb producer.
Qualification remains **BLOCKED; paired replay AUTHORED_NOT_RUN/NOT_RUN**.

The native0005 workflow preparation files were retained under its existing
`.artifact/` directory; the wrapper copied only selected evidence files to the
receipt root. `public-bundle-helper-binding.txt` has SHA256
`edf10d53aca4e853c11132172543733574c7c3d91f61f0b6fb870a3e8a018b5f`
(251 bytes), and `public-bundle.json` has SHA256
`0c6d19f3eec0bb1044a8f4765ed77edbfd18ef891ff652edbfab6e29f914f3b1`
(1,431 bytes). They bind the trusted helper, accepted R418 tuple, ZIP,
manifest, canonical bundle and executable to this same native attempt. This
local discovery required no API, collector change, new preparation or native
execution, and does not alter the canonical-export RED. Future authorized runs
also retain these exact files inside the test's own evidence directory and
verify the helper and actual manifest/executable/ZIP tuple.

Preserve native0004 `41946520562` (faff72ad prefix/setup and census-path defects
plus producer refusal), historical ordinary whole RED `41901144448`
(154/163), and historical native **17 RUN / 14 PASS / 3 FAIL** with downstream
capability/replay NOT_RUN. Pinned-main focus `41937629687` and whole
`41937629903` are bounded ordinary PASS only; the latter's three unrelated
optional E5 skips remain literal. No source, ordinary gate or compile result
promotes native, hosted, full B03 or FullS1 qualification.
