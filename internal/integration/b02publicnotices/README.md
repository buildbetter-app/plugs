# B02 public category and notice proof

Done = the retained PR333 missing-endpoint and PR334 persisted-notice
acceptance survives in a successor integrated with frozen main, proven by new
exact-head focused, literal whole-tree and hosted results plus independent
review; merged PR377, authored checks and inherited PASS do not count.

Scope = this public notice package, the separable public missing-endpoint
regression in `../batchsql`, their minimal fixture helpers and additive
registration/inventory. Protected B05 claim interleaving and authorization
overlap remain paused. This package's event.list/ACL-generation overlap is a
separate B02 refusal case. Native producer, preparer and capture work remain
outside this unit.

## Adopted behavior and assertions

The authoritative behavior is B02 in
`engdocs/design/plug/spec/behaviors/work.md`, A3 in
`engdocs/design/plug/design.md`, the adopted CP19 mapping in the behavior
specification section of `engdocs/design/plug/clean-room-plan.md`, and P22 in
`engdocs/design/plug/review-2026-10-27.md`.

`TestB02PublicCategoriesPersistNoticesAndReopen` authors these five cases on
each of real file-backed SQLite WAL and disposable real PostgreSQL:

| Blocker category/outcome | Public slot | Ordinary dependent ready | Conditional dependents ready | Ordered condition notices |
| --- | --- | --- | --- | --- |
| completed/success | `bead.close` | yes | no | a, z |
| completed/failure | `bead.fail` | yes | yes | none |
| canceled | `bead.cancel` | yes | no | a, z |
| backlog, key `frozen` | `bead.defer` | no | no | none |
| backlog, key `pinned` | `bead.defer` | no | no | none |

Each project catalog uses custom lifecycle keys, installed through public
`ontology.plan` and `ontology.apply`. The dependent project gives the same
blocker keys contradictory category/outcome definitions: success becomes
backlog, failure becomes success, canceled becomes backlog, frozen becomes
failure and pinned becomes success. Its cancel slot targets a separate active
`b02-dependent-canceled` status; `b02-canceled` remains backlog. Public
`ontology.get` checks the installed statuses and complete workflow, including
slot targets and membership; subsequent ordinary `bead.create` proves the
shared runtime refresher uses its custom initial status. Default ontology keys cannot supply that
observation.

All graph records and links are created through public `bead.create` and
`bead.link`. Conditional dependents are inserted z then a. After the named
blocker transition, public `bead.ready` checks the exact dependent set and
excludes the terminal/backlog blocker. Public `event.list`, persisted
changesets, original receipt and retained record/version bytes bind the result
to the real Writer output. The ordinary lifecycle event is index 0, followed
only when required by notices for a and z at indexes 1 and 2. Notices name the
actual dependent/edge revisions, blocker before/after revisions and installed
blocker-project source revision/digest; dependent records and history stay
unchanged.

A stale revision refusal checks the head/metadata, all source/graph records and
versions, complete persisted log and event page, SQL payload-family counts and
absence of a request receipt.
Successful one-event pagination is stopped after index 0, all HTTP/service/store
handles are closed, and new handles acquire a later lease epoch from the same
SQL database. The next public page resumes at the first notice. Same-key public
replay returns the original result bytes with the replay flag, unchanged
receipt, head, complete log, payload-family counts and histories. A second
store reopen retains the event page and receipt. Finally, public `bead.reopen`
reblocks every dependent without creating
terminal notices or mutating dependent history.

`frozen`/`pinned` are installed backlog definitions for the adopted post-import
semantics. This does not claim importer/native producer execution or new
Beads-recorded behavior.

## Fixed-policy authority concurrency refusal

The frozen-main integration adds
`TestB02FixedPolicyEventListRefusesConcurrentAuthorityChange` with
`sqlite_wal`/`postgres` and `revoke_grants`/`restore_grants` leaves. It first
creates the real notices through the original public ontology and lifecycle
flow. A separate actual API-key-authenticated fixed-policy HTTP request is
held at ingress while the bound `MutableFacts` source commits a new ACL
generation. The original request must return `method_not_found` with no result,
event cursor or replay. The real Checker must reject its pre-change authority
binding and accept a fresh binding. Public readback then checks the original
persisted notice ordering, category, receipt and complete retained histories.

The finite CP7 policy does not permit `event.list`, and its guarded composition
has no EventReader adapter. This regression preserves that refusal; it does
not invent an authorized installed event stream. CP7 User bead reads remain
allowed independently of involvement. The barrier precedes authentication and
does not alter its 50ms provider deadline. Original category/reopen tests stay
unchanged. All fourteen named leaves require new exact-head runtime evidence;
the old `22ab79f9` results do not qualify this integration candidate.

## Connected source bindings

| Boundary | Existing production path used by this package |
| --- | --- |
| HTTP admission and transport | `internal/server/authn`, `internal/server/transport/httpjson` on actual loopback HTTP, anonymous/no authorization plugin |
| Canonical RPC lowering/result and refusal validation | `internal/server/rpc.BindService`, registry catalog, `internal/api/wire` |
| Project catalog installation | `internal/server/ontologyrpc.Bind`, `internal/core/ontologyops.Service.Plan/Apply`, canonical Writer `ApplyProtected` and shared `service.Ontology.Refresh` |
| Named lifecycle and ready membership | `internal/core/service`, its actual per-project ontology normalizer and graph traversal |
| Transactional graph/receipt/event plan | `internal/core/pipeline.Writer` with `WithOntology` and `WithOntologyAudit` |
| Conditional decision and source pin | `service.NewTrackedOntologyAudit(service.StaticOntologySource(sourceRef))`; its actual protected snapshot reads and ontology compiler |
| Persisted event reads and cursors | `internal/server/events.Log`, `HistoryMaterializer`, `Service`, `CursorCodec` and `internal/core/events.Reader` |
| Durable storage and reopen | `internal/storage/sqlite.Driver`, `internal/storage/postgres.Driver`, driver records/versions/changesets/receipts and active leases |

The only event-provider glue forwards the domain port to the production event
service. It creates no event or decision. The only conformance seed bootstraps
the default protected ontology record with explicit system attribution. Engine
lifecycle helpers reuse the existing `auditsql` disposable PostgreSQL pattern
and pinned image digest. Engine absence fails the target; no skip, memory
substitute or external DSN exists. The matrix owns an isolated database per
case and keeps production clocks, TTLs and budgets unchanged.

## Current-main composition and verification

The current continuation preserves PR334's four Go sources and local BUILD
from `0c98cb7577710bf32c89f7f5978bc0424dda8a46`. It also carries PR333's
`TestB02PublicDependencyMissingEndpointsAcrossDrivers` from `94eb9168`, with
only five helper names adapted to extracted local B02 fixtures. The existing
current-main public HTTP host supplies the remaining B01 fixture interfaces.
The missing-endpoint matrix retains the positive cross-project link and
unrelated claim, two public write methods, two dependency kinds, both missing
endpoints, repeated refusals, staged-prefix rollback, full conservation,
no failed receipt/publication and both SQL reopens.

Original credit belongs to Nikhil Shinday and the contributors of PR333
`6cd771a9`/`94eb9168` and PR334 `b7910b78`/`179d7092`/`22ab79f9`, with its
`6afffe08` integration and `b45aa62b` correction/publication lineage. The
canonical `5918230d` refusal correction and its original attribution remain
recorded in the preserved source history. No stale inherited hierarchy
implementation or native recorder is imported into this B02 continuation;
that separately owned lineage remains on the original branches and in the
sealed source/credit matrix.

The author owns new source, focused, whole and hosted evidence on the exact
published head, concurrently with the distinct reviewer. Focus includes
`//internal/integration/b02publicnotices:b02publicnotices_test`,
`//internal/integration/batchsql:batchsql_test` and
`//tools/lexicon:lexicon_gate`. Both focused and literal `test //...` use
`--nocache_test_results --flaky_test_attempts=1
--test_env=GO_TEST_WRAP_TESTV=1 --test_output=all
--test_timeout=900,900,900,9000 --jobs=2 --local_resources=memory=8192`
through the RunsOn remote-bazel wrapper. Complete first failures, raw/XML
outcomes, mandatory skips and source/run/artifact identities are preserved.
No old runtime result transfers to the new composition.

Positive installed event-list delivery, waits-for, transitive/diamond,
ephemeral providers, parent settlement, database/process restart/failover,
independent Beads recording and native admission remain outside this bounded
package proof. FullS1=0/28; NativeAdmission=None. Train639 alone admits and
merges.
