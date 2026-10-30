# Events and delivery checkpoints

Source: **127ac22e1d003cce4babe6cbcbc8367ad264e649**;
[guide scope](README.md#snapshot-and-package-boundaries).

## Read committed history

The [event contract](../design/plug/contracts/events.md) defines durable types
and control markers. The implemented [delivery ports](../../internal/core/events/ports.go)
read committed changesets, materialize historical images and apply **current**
read authorization. Plugins cannot submit arbitrary event payloads. A rollback
or durable no-op produces no new graph revision or changeset event.

An event position is (incarnation, revision, index). The
[storage adapter](../../internal/server/events/storage.go) rereads the complete
changeset at a partially acknowledged revision and discards only indexes
already acknowledged. A non-nil ordered audit plan uses logical event indexes;
a nil plan is the legacy one-change ordering and cannot infer slot, lease,
mutator or nested causal provenance. An invalid non-nil plan refuses.

The current [HistoryMaterializer](../../internal/server/events/audit.go) uses
retained, revision-pinned source facts rather than current ontology. Conditional
notices carry typed pinned details and no before/after images; the dependent is
not rewritten. Every required source must still exist and be readable with
unredacted audit facts. Denial suppresses the whole notice, while unavailable
authorization capability refuses. Ordinary images are masked by current policy;
materialization errors never become a successful empty page.

The transport-independent [Service](../../internal/server/events/service.go)
offers List and Poll. Its opaque cursor binds method, filter, effective
principal/caveats, ontology, policy generation and incarnation. The event
reader's filter supports event types and resources; the broader catalog
query/view filter is not installed here. Pull reports cursor_expired or
incarnation_changed, and polling emits event.gap across a restore or failover.
Reopen from a fresh authorized snapshot after a gap.

## Ordered audit plans and delivery

The [Writer producer](../../internal/core/pipeline/audit.go) can append ordered
conditional notices through WithOntologyAudit and trusted event context. The
tracked [service adapter](../../internal/core/service/audit.go) reads a
protected ontology source from the same guarded snapshot and evaluates the
blocker's project. The complete connected fixture uses real memory storage,
Writer, adapter, materializer and fenced checkpoints:
[internal/integration/audit](../../internal/integration/audit/README.md).
It proves ordinary event ordering, pinned source/decision details, refusal and
masking paths, lost-ack retry and replay suppression. The fixture uses direct
Writer calls for slot operations that are not named service methods; it is not
a claim that every behavior card is complete.

Memory, SQLite and PostgreSQL validate and retain a complete non-nil plan in
the changeset and receipt transaction. SQL persistence is covered by each
driver's own tests; the connected service-to-delivery composition remains
qualified against memory at this snapshot. See
[storage](storage.md#audit-plan-and-sql-boundaries).

The [Dispatcher](../../internal/core/events/dispatcher.go) uses driver-backed
checkpoints with an active-lease check and generation CAS on every save.
Missing checkpoint capability refuses; there is no fallback map. Memory
progress lasts only for that store, while SQLite and PostgreSQL checkpoints
survive close/reopen. Native SDK state is separate and process-local.

Callbacks are **at least once**: a failed acknowledgement can replay the
identical DeliveryKey (position plus binding). Deduplicate external effects
with that key. Listeners are partitioned by resource; projections receive
whole changesets in order and halt on failure. Post delivery returns warnings
for an already committed result, is finalized durably, and is not rerun on
receipt replay. Context cancellation and lease loss prevent acknowledgement.
Host resume/skip controls are seams, not public administration RPCs.

## Exercise the merged boundaries

Run the connected audit and delivery tests with ambient settings disabled:

~~~~sh
bazel --ignore_all_rc_files test \
  //internal/integration/audit:audit_test \
  //internal/server/events:events_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900
~~~~

The SQL plan tests are:

~~~~sh
bazel --ignore_all_rc_files test \
  //internal/storage/memory:memory_test \
  //internal/storage/sqlite:sqlite_test \
  //internal/storage/postgres:postgres_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900
~~~~

These targets use synthetic stores and, for PostgreSQL, the package's disposable
loopback-only Docker harness. They do not certify network subscriptions,
background watch scheduling, runtime/performance targets, managed failover,
backup, production recovery or a public release. The baseline registry does
not bind event.list/subscribe/unsubscribe, so HTTP supplies no push channel.
