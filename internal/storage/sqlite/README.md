# SQLite storage

`sqlite.Driver{}.Open(ctx, driver.Config{DSN: "/local/path/store.db"})`
opens a persistent local file. Local `file:` URIs without query parameters also
work. In-memory databases, custom options, nonzero branches, unknown query
filters, and migrations other than the current schema explicitly refuse.

The driver uses modernc.org/sqlite, WAL, and FULL synchronous publication.
Readers pin real SQLite read transactions. Close every reader and transaction;
Close is idempotent and unfinished transactions roll back. Store.Close also
releases outstanding local resources and its leases.

Sorted guard sets use driver-owned advisory file locks in `<database>.plug-locks`.
Failed acquisitions release the entire partial set before waiting; skipLocked
omits conflicts. Head is the last file lock. Records, tombstones, forward-patch
versions, blobs, derived observations, predicates, replay receipts and log rows
publish together under BEGIN IMMEDIATE. Lease acquisition, release, checkpoint
CAS and response finalization use the same publication lock and SQL transaction.
Epoch/token and expiry checks occur inside that transaction, including immediately
before COMMIT. Staging holds no SQLite write transaction, so lease release and
expiry takeover can fence unfinished writers.

A long publication does not block its own holder's lease heartbeats. After
BEGIN IMMEDIATE validates the fresh durable lease, the driver registers that
exact lease as the publication owner. Timely Renew calls can then advance a
synchronized private deadline without taking the SQLite writer or publication
lock again. Both wall-clock expiry and the real monotonic deadline are checked
on each heartbeat and fence; a delayed expiry callback cannot resurrect a
paused or expired holder. A publication without timely heartbeats still rolls
back at the final fence. The TTL is unchanged.

This bridge is safe across processes because the publication gate excludes all
lease acquisition/release and other publications until handoff; the native
writer is also held when ownership is registered. Matching the epoch/token is
still mandatory, including when a WAL reader sees the older durable expiry.
Accepted heartbeats are persisted with the graph. Heartbeats racing COMMIT/fsync
are flushed in lease-only transactions while retaining the same gate, until the
durable heartbeat and private heartbeat agree atomically. If rollback,
cancellation or an uncertain outcome prevents that handoff, the driver closes
Lost before releasing exclusion rather than advertising an unpersisted live
extension. In particular, an aborted publication that accepted a private
heartbeat may require acquiring a new lease. No second connection or process
can take the lease based on the older expiry while its publication gate is held;
after genuine expiry, takeover advances the epoch and fences the old holder.

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
