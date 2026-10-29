# Events and delivery checkpoints

Source: **`471d0199c89acf9e242f44978f2824120726de32`**;
[guide scope](README.md#snapshot-and-boundaries).

## Read committed history

The [event contract](../design/plug/contracts/events.md) defines durable types
and control markers. Implemented [delivery ports](../../internal/core/events/ports.go)
read committed changesets, materialize historical images and check **current**
read authorization. Plugins cannot submit arbitrary event payloads. A rollback
or durable no-op produces no new graph revision or changeset event.

An event position is `(incarnation, revision, index)`. The
[storage adapter](../../internal/server/events/storage.go) rereads the complete
changeset at a partially acknowledged revision; the delivery reader discards
only indexes already acknowledged. A non-nil ordered descriptor plan uses
logical event indexes, which can outnumber changed records. Nil alone selects
legacy one-change ordering; an invalid non-nil plan refuses.

[HistoryMaterializer](../../internal/server/events/audit.go) uses retained,
revision-pinned sources rather than current ontology to reconstruct a
descriptor. Conditional notices have typed pinned details and no before/after
images; the dependent is not rewritten. Every required source needs current
existence and readable, unredacted audit facts. Denial suppresses the whole
notice; unavailable authorization capability fails explicitly. Ordinary images
are masked by current policy. Materialization/history errors must not become a
successful empty page.

Legacy nil plans cannot supply slot/lease, mutator or nested causal provenance
by inference. Without a trusted `Describer`, unsupported provenance explicitly
refuses materialization; baseline CLI writes alone do not install a complete
slot-event stream.

The transport-independent [Service](../../internal/server/events/service.go)
offers `List` and `Poll`; its [opaque cursor](../../internal/server/events/cursor.go)
binds method, filter, effective principal/caveats, ontology, policy generation
and incarnation. This event reader's filter supports event types and resources;
the broader catalog query/view filter is not installed here. Pull reports
`cursor_expired` or `incarnation_changed`; polling emits an obligatory
`event.gap` even when filters exclude it. Reopen from a fresh authorized
snapshot after a gap. Policy/ontology/view boundaries can require a watch reset.

## Deliver and recover

The [Dispatcher](../../internal/core/events/dispatcher.go) uses driver-backed
[Checkpoints](../../internal/server/events/storage.go) with an active-lease
check and generation CAS on every save. Missing checkpoint capability refuses;
there is no fallback map. Memory retains progress only in the same live store;
SQLite/PostgreSQL baseline checkpoints survive reopen. Native SDK state is
separate and process-local.

Callbacks are **at least once**: a crash or failed acknowledgement after the
side effect replays the identical `DeliveryKey` (position plus binding).
Deduplicate external effects with that key. Listeners expose resource partition
keys; this implementation serializes their delivery. Projections receive whole
changesets in order and halt on failure; they never skip poison events.

Post delivery uses a one-second aggregate ceiling and returns warnings for an
already committed result. A failed post is dead-lettered for late recovery.
The caller invokes `RunPost` only for a fresh commit, suppresses it on receipt
replay, and durably finalizes the response warnings. A listener halts after its
attempt limit (default three); projection failure halts immediately. Lag stalls
listeners after seven days and projections after 30. Authorized `Resume`
retries the same checkpoint; listener/post `Skip` requires an authorized reason
and persists an audit. These are host seams, not bound administration RPCs.
Callbacks must honor context; loss of the lease prevents acknowledgement.

## Included boundary and local example

The optional [Writer producer](../../internal/core/pipeline/audit.go) can append
ordered conditional notices through `WithOntologyAudit` and trusted event
context. Memory validates/persists these plans atomically. The complete
protected-source service adapter, actual ontology-to-Writer-to-delivery B02
composition and SQL audit persistence remain separate follow-ons at this pin;
baseline CLI composition does not enable the producer. See the
[storage audit boundary](storage.md#audit-plan-boundary).

Run the existing disposable memory-backed examples:

```sh
bazel --ignore_all_rc_files test //internal/server/events:events_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_arg=-test.v --test_timeout=900
```

[Writer/delivery tests](../../internal/server/events/audit_writer_test.go) use
explicit fixture ontology decisions; they do not certify the missing adapter.
[Recovery tests](../../internal/server/events/dispatcher_test.go) cover identical
lost-ack replay, warning/dead-letter behavior, halt/resume and fencing. Network
subscribe/watch registration, snapshot coordination, coalescing and background
scheduling remain separate; `event.list/subscribe/unsubscribe` are unbound in
the baseline RPC service, and HTTP supplies no push channel.
