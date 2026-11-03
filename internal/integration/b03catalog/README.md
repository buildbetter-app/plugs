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

Scope = B03's installed catalog boundary, configured startup/source selection,
catalog RPC wiring and the routed key-continuity guard in
`internal/core/ontologyops/plan.go`. Lifecycle timestamp representation remains
a separate owner boundary.

Evidence status: **SOURCE_COMPLETE; qualification remains receipt-bound**.
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
| `TestSameKeyProjectOverlaysReplaceWholeDefinitionsAndOlderReplay` | The same key is completed/failure in base, unstarted in west and backlog in east. Omitted outcomes are cleared by whole replacement. Public creates, readiness and claim refusal follow each project's category. Older accepted apply replay refreshes from current durable source. Overlay behavior persists after SQL close/open. |
| `TestInstalledBaseKeysArchiveWithoutDeletionRenameOrResurrection` | Public plans refuse deletion of an active unused key; accepted archival retains status category/outcome; old plans cannot apply over the new source; all four definition kinds refuse deletion or resurrection; key rename refuses; archived display-name edits remain accepted; permanence survives SQL reopen. |
| `TestProjectArchivedKeysCannotBeReactivatedByOverlayRemovalOrReplacement` | Minimal tombstones preserve base definitions whole, without changing other projects. Removing or replacing tombstones cannot reactivate any definition kind. Unknown-base archival refuses. Removing an unused project-local key also refuses. Tombstones survive SQL reopen. |
| `TestMalformedCatalogAndMixedBatchRefuseAtomically` | Unknown categories/members, duplicate definitions, null configuration and unsupported directives refuse. A valid update/create/comment batch prefix cannot survive an invalid status suffix. A malformed protected source refuses reads, plan, retained apply and connected reload without replacing durable bytes or the prior runtime. |
| `TestCatalogApplySQLPublicationRollbackAndRetry` | Database-side publication faults exercise actual SQLite/PostgreSQL transactions. Failed public apply preserves source, rows, versions, log/head, derived rows, observations, allocators and every receipt. The same key succeeds after fault removal and remains durable after close/open. |
| `TestConfiguredRuntimeLoadsCatalogAndAuditsOnEveryEngine` | Real `composition.Open` and public byte RPC on memory, SQLite and PostgreSQL load the installed source, bind catalog methods, refresh the shared ontology after `ontology.apply`, apply the new workflow to an ordinary create, and pin the refreshed revision and digest in the durable audit/event stream. |

The bootstrap inserts only the built-in ontology. Custom catalogs, project
overlays, archival and ordinary bead writes all use real byte RPC with canonical
result validation, the canonical protected Writer and the shared
`OntologyRefresher`. Configured startup loads the protected source from the
store head before admitting Service, Writer or RPCs; missing, wrong-kind,
malformed and unprotected sources close the store and fail closed. A
source-corruption fixture is explicitly fault injection, not producer
acceptance. The SQL publication fault does not replace the Writer.

Refusal snapshots require complete bounded record/version/log inventories,
all derived tables, known record/predicate/allocator observations, allocator
presence, all issued-key receipt/changeset lookups, and ordinary current/ready
views. Missing inventory or engine capabilities fail the test. PostgreSQL is an
owned disposable pinned image; unavailable Docker/PostgreSQL fails the target.
The target's `exclusive` tag isolates this engine-owning proof only.

## Exact-head gate proposal

S1 owns routing and execution. On the frozen candidate, record full SHA/tree,
parent/base, command, receipt identity, exit code and completed evidence status.
Proposed ordinary remote-Bazel focus:

```sh
/tmp/plug-swarm/bin/remote-bazel test \
  //internal/core/ontologyops:ontologyops_test \
  //internal/server/ontologyrpc:ontologyrpc_test \
  //internal/integration/b03catalog:b03catalog_test \
  --test_output=errors
```

Only after the exact focus succeeds, and only after S1 routes the whole run:

```sh
/tmp/plug-swarm/bin/remote-bazel test //... --test_output=errors
```

Preserve every original failure and historical candidate. A focused runtime
PASS remains bounded to the explicit connected host composition. The configured
path is now implemented and tested, while full B03/S1 qualification still
requires its exact routed evidence. No native Beads producer, native admission,
timestamp proof, merge or release is claimed here.
