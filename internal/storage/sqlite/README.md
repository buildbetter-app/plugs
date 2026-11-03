# SQLite storage

`sqlite.Driver{}.Open(ctx, driver.Config{DSN: "/local/path/store.db"})`
opens a persistent local file. Local `file:` URIs without query parameters also
work. In-memory databases, custom options, nonzero branches, unknown query
filters, and migrations other than the current schema explicitly refuse.

The driver uses modernc.org/sqlite, WAL, and FULL synchronous publication.
Readers pin real SQLite read transactions. Close every reader and transaction;
Close is idempotent and unfinished transactions roll back. Store.Close also
releases outstanding local resources and its leases.

Sorted guard sets use 1,024 driver-owned advisory lock stripes in
`<database>.plug-locks`. Canonical keys hash into fixed `guard-stripe-NNNN`
files; batches acquire each distinct stripe once, in numeric order, exclusively
if any key needs exclusivity and shared otherwise. Collisions can add contention
but cannot weaken exclusion. Failed acquisitions release the entire partial set
before waiting; a busy stripe containing only skipLocked requests is omitted.
Linux retains one mapping per held stripe and closes its descriptor, so even a
large restore uses at most 1,024 guard mappings, not one per key. Other supported
systems retain at most 1,024 guard descriptors. All processes must use the same
stripe layout; it replaces the unreleased per-key lock-file layout.

Head is the last file lock. Records, tombstones, forward-patch versions, blobs,
derived observations, predicates, replay receipts and log rows
publish together under BEGIN IMMEDIATE. Lease acquisition, renewal, release,
checkpoint CAS and response finalization use the same publication lock and SQL
transaction. Epoch checks occur inside that transaction, including immediately
before COMMIT. Staging holds no SQLite write transaction, so lease release and
expiry takeover can fence unfinished writers.

Keyed writes with post work atomically publish ResponsePending. Fresh readers
observe finalization; existing snapshots retain their original pending state.
Only the fenced finalizer clears pending, including for an empty post outcome.
Final receipts are immutable; no-op receipts are immediately final. Schema 2
adds this flag through a transactional additive upgrade from checkpoint schema
1, treating older receipts as final and preserving identity and graph history.

Revisions, epochs and checkpoint generations use lossless unsigned decimal
TEXT. The current implementation checks the uint64 boundary and refuses larger
revisions or exhausted counters before publication. JSON bodies retain exact
bytes in current rows and bases; other history entries use RFC 6902 root replace
patches, with a base every 32 versions. Query implements only exact equality on
kind, id, key, type, project and status. Core owns semantic graph rules and exact
sentinel selection for opaque derived rows.

Use one stable database path on a local filesystem with working flock and WAL
shared-memory locks. All handles/processes must use this driver and retain the
lock directory; do not remove or replace it while handles are open. Hard-linked
files and existing non-Plug databases refuse. SQLite has
no standby, replication or failover capability. Automated backups and scheduled
engine-neutral dumps are not implemented here; health leaves the backup
checkpoint unset. No production cutover or data movement is provided.
