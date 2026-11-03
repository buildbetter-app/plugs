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
publish together under BEGIN IMMEDIATE. Lease acquisition, release, checkpoint
CAS and response finalization use the same publication lock and SQL transaction.
Epoch/token and expiry checks occur inside that transaction, including immediately
before COMMIT. Staging holds no SQLite write transaction, so lease release and
expiry takeover can fence unfinished writers.

A long publication does not block its own holder's lease heartbeats. After
BEGIN IMMEDIATE validates the fresh durable lease, the driver registers that
exact lease as the publication owner. Timely Renew calls then persist their
expiry in `<database>.plug-locks/lease-heartbeat`, independently of SQLite's
writer and native COMMIT/fsync. Each record binds the expiry to the durable
SQLite epoch and a digest of its owner token. An atomic replacement, file sync
and directory sync finish before a renewal is acknowledged. Independent handles
and processes combine only a matching journal record with the SQLite expiry;
old epochs, released tokens and other owners' records cannot extend a lease.
SQLite remains authoritative for identity, and checkpoints/graph state stay in
one SQLite transaction. The journal is one fixed-size record, not another database.

Renewals remain serviceable through the entire native publication, including
COMMIT's return. The driver unregisters the owner after exactly one COMMIT;
there is no post-COMMIT lease-only flush, admission cutoff, moving target or
loop. Both the final SQL identity fence and the publication flock remain in
place. A contender cannot take over during publication, and after it returns
the contender sees all acknowledged journal extensions even if SQLite's last
expiry is older. Normal renewals outside a publication still use the same
registration and a single lease-only SQLite transaction.

Both wall-clock expiry and the real monotonic deadline are checked on every
heartbeat and fence. Journal I/O is serialized by the lease mutex, but no
SQLite statement or database fsync holds that mutex. A delayed expiry callback
or process paused beyond its *previous* real deadline cannot renew on resuming:
the deadline is explicitly rechecked after journal I/O, before acceptance. A
publication without timely heartbeats rolls back at its final fence. Lost is
irreversible; expiry/takeover still fence the old holder, and the 10-second TTL
is unchanged. Rollback/cancellation after an accepted heartbeat and uncertain
native COMMIT outcomes retain the fail-closed lease-loss policy.

A confirmed graph, checkpoint or response-finalization COMMIT returns success,
even if cancellation or genuine expiry is observed after that COMMIT. Success
reports the durable result, not a promise that its lease remains live. There is
no fallible follow-up transaction that could turn a published result into a
fencing/cancellation rejection. Pre-publication errors still leave that operation
unpublished; an uncertain primary COMMIT is not converted into success.

Native COMMIT can publish and still return a cancellation error: modernc checks
the context again after the native step. Graph commits, both checkpoint APIs and
response finalization expose such uncertain outcomes as public
`driver.ErrIndeterminate`, **not** an error unwrapping to cancellation or fencing.
The owner fails closed, but uncertainty is neither proof of rollback nor proof
of success. Recovery uses a fresh context and durable readback, not the canceled
context or an in-memory staged result:

- For `SaveCheckpoint` / `SaveCheckpointIfRecord`, the error's idempotency key is
  empty because checkpoints have no replay receipt. Call `LoadCheckpoint` with
  the original checkpoint key and compare generation and payload. A later
  generation cannot identify this attempt. Any retry must retain the **original
  expected generation**; never load a new generation and blindly apply again.
- For `FinalizeResponse`, the error retains the original idempotency key. Use
  `IdempotencyByKey` on a fresh reader for the same full scope and revision.
  Retry only the same known post outcome under an active lease. This metadata
  uncertainty does not make the original graph write uncertain or authorize
  rerunning that write or its callbacks.

**Limits.** This removes SQLite writer/COMMIT latency from an already registered
owner's renewal path; it does not make leases immune to scheduling pauses or
I/O failure. The heartbeat journal's own file/directory sync and any wait for
registration must fit within the previous lease budget. A slow or failed journal
write fails closed rather than increasing the TTL or reviving an expired owner.
Native SQLite COMMIT itself remains non-preemptible: a process stopped after the
last fence, inside COMMIT, may have an uncertain or already-published outcome on
resuming. The driver cannot roll back a confirmed native COMMIT. All processes
must use this journal-aware driver and retain the stable lock directory; mixing
older binaries or deleting/replacing the journal while an owner is live is not
supported. An unacknowledged journal write interrupted by a crash can at most
delay takeover until its timestamp expires; it cannot authorize a stale owner.

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
