# Storage drivers and lifecycle

Source: **463881cec205e6cd3e5b85d1bac7d4018a153d5f**;
[guide scope](README.md#snapshot-and-package-boundaries). These are internal
embedding APIs. `plug server` selects one of these drivers from its strict
`.plug/config.yaml` form; ordinary `plug` local mode uses SQLite by default
and does not open PostgreSQL directly.

| Driver | Open configuration | Lifetime and limits |
| --- | --- | --- |
| [Memory](../../internal/storage/memory/store.go) | memory.Driver{}.Open(ctx, driver.Config{}); empty DSN or :memory:, no options. | Independent copy-on-write store with history, receipts, checkpoints and ordered plans. State ends at Store.Close. |
| [SQLite](../../internal/storage/sqlite/README.md) | A stable local path or file: URI without query parameters; no custom options, nonzero branches or in-memory DSN. | Persistent WAL/FULL publication, pinned SQL readers, driver-owned file locks, history, receipts, checkpoints and ordered plans. No standby, replication or automated backup. |
| [PostgreSQL](../../internal/storage/postgres/README.md) | A pgx DSN for one dedicated Plug database; the host supplies credentials. | Persistent repeatable-read snapshots and transactional publication, including receipts, checkpoints and ordered plans. Replication, backup, promotion and recovery belong to managed deployment integration. |

All three engines publish records, tombstones, versions, blobs, derived rows,
predicate updates, changesets and keyed replay receipts atomically. SQLite and
PostgreSQL support only equality queries on kind, id, key, type, project and
status, and refuse unknown operators/options and nonzero branches. History
replays JSON Patch replacement diffs from full boundaries at most every 32
versions. Revision, epoch and checkpoint conversions are checked against the
engine's uint64 domain; values are never silently narrowed.

## Ownership, leases and failure

The composition root opens the store, acquires and renews the active lease and
owns shutdown. [Driver interfaces](../../internal/storage/driver/driver.go)
expose transactions only to drivers and the single core write pipeline. Plugin
code receives a read/capability surface, never an SQL connection or WriteTx.

- Close every Reader; it pins an immutable committed snapshot. Use a fresh
  reader to observe later writes or response finalization.
- Finish each transaction with commit or rollback. Closing an unfinished
  transaction rolls it back. Close readers and transactions before Store.Close;
  store close is idempotent and never publishes staged work.
- The standard active-store lease is 10 seconds, renewed every 3 seconds. It is
  distinct from a bead work-claim lease. On Lost, stop admitting reads and
  writes until a new lease exists. Expired holders cannot renew themselves.
- Writes, checkpoint saves, no-op receipts and response finalization are
  lease-epoch fenced. A stale holder receives ErrFenced.
- A keyed write with post work publishes a pending receipt with its graph
  change. Only the fenced finalizer clears pending, including when warnings
  are empty. No-op receipts are final immediately and preserve prior record
  revisions and head.
- An indeterminate commit or lost answer requires lookup/replay with the
  original idempotency key. A mismatched request conflicts; a pending receipt
  never licenses a second graph write. Final responses are immutable.

SQLite retains state across close/reopen and keeps its lock directory beside the
database while any handle is open. PostgreSQL retains identity across reopen;
the caller must not admit traffic after an uncertain restore or promotion
without a recovery path that rotates the store incarnation. The managed design
target is a five-minute RPO and one-hour RTO, not a result of these local tests.
Dump/restore, cutover and production data movement are not installed CLI/RPC
operations. The [shared-server guide](server.md) describes which state survives
a server restart and which process-local session state does not.

## Audit-plan and SQL boundaries

The current [changeset header](../../internal/storage/driver/driver.go) carries
a complete ordered Events plan. Memory, SQLite and PostgreSQL validate, clone
and persist a non-nil plan with the records, changeset and receipt in one
publication transaction. A nil plan retains legacy one-change ordering; an
invalid non-nil plan refuses. A no-op cannot contain a plan.

This persistence does not by itself prove the full connected audit flow. The
[Writer producer](../../internal/core/pipeline/audit.go), tracked
[ontology adapter](../../internal/core/service/audit.go), event materializer
and delivery fixture are separately composed and qualified in memory; SQL
drivers have engine-specific plan persistence tests. Do not infer network event
delivery, policy management or production recovery from a durable plan.

## Exercise the merged engines

Run the focused driver suites with ambient settings disabled:

~~~~sh
bazel --ignore_all_rc_files test \
  //internal/storage/driver:driver_test \
  //internal/storage/memory:memory_test \
  //internal/storage/sqlite:sqlite_test \
  //internal/storage/postgres:postgres_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900
~~~~

The PostgreSQL target creates and removes its own loopback-only PostgreSQL
container and fails if the pinned engine is unavailable; it never substitutes
memory. Shared conformance covers lifetime cleanup, locks, revisions, atomic
publication, replay, no-op/finalizer, fencing, checkpoints, history and
pagination. These checks do not certify managed topology, backups, failover,
performance, migration or a public release.
