# Baseline storage drivers

Source: **`471d0199c89acf9e242f44978f2824120726de32`**;
[guide scope](README.md#snapshot-and-boundaries). These are internal embedding
APIs. The executable selects only disposable memory at this snapshot.

| Driver | Open configuration | Lifetime and limits |
| --- | --- | --- |
| [Memory](../../internal/storage/memory/store.go) | `memory.Driver{}.Open(ctx, driver.Config{})`; empty DSN or `:memory:`, no options. | Each open is independent. Copy-on-write readers, history, receipts and checkpoint CAS live only in that store. Reopening creates a new identity and empty state. |
| [SQLite](../../internal/storage/sqlite/README.md) | File path or local `file:` URI without parameters; no options or in-memory DSN. | Persistent WAL/FULL publication, pinned SQL readers and driver-owned file locks. Use a stable local path and retain its `.plug-locks` directory while open. Hard-linked files and non-Plug databases refuse. No standby/replication/automated backup. |
| [PostgreSQL](../../internal/storage/postgres/README.md) | pgx DSN for a dedicated Plug database; host supplies credentials privately. | Persistent repeatable-read snapshots and transactional publication. Initial role needs schema-creation permission. Replication, backups, promotion and recovery orchestration belong to a separately qualified deployment integration. |

Baseline records, tombstones, versions, blobs, derived rows, predicate updates,
changesets and keyed replay receipts publish atomically within each driver.
SQLite/PostgreSQL support exact string equality on kind/id/key/type/project/status;
unknown operators/options and nonzero branches explicitly refuse. The reserved
branch field is not a graph-branch feature. Revision/epoch/checkpoint conversion
is checked against each engine's supported integer range rather than narrowed.
History uses JSON Patch replacement diffs with a full replay boundary at most
every 32 versions; deletion/recreation retain bounded reconstruction.

## Ownership and failure

The composition root opens the store, acquires/renews the active lease and owns
shutdown. [Driver interfaces](../../internal/storage/driver/driver.go) expose
transactions only to drivers and the single core write pipeline. Plugin code
receives a read/capability surface, never an SQL connection or `WriteTx`.

- Close every `Reader`; it pins an immutable committed snapshot. A fresh reader
  is needed to observe later writes or pending-response finalization.
- Finish each transaction with commit or rollback. Closing an unfinished
  transaction rolls it back. Close readers/transactions before `Store.Close`;
  store close is idempotent and never publishes staged work.
- Use bounded contexts. The standard active-store lease is 10 s, renewed every
  3 s; it is distinct from a bead's work-claim lease. On `Lost()`, stop admitting
  reads/writes until a new active lease exists. Expired holders cannot renew
  themselves back to life. Writes/checkpoint saves are epoch-fenced atomically.
- A keyed write with post work publishes a pending receipt with its graph
  change. Only fenced response finalization clears pending, even for no
  warnings. Await finalization before reporting successful replay. No-op
  receipts are final immediately and preserve prior record revisions/head.
- An indeterminate commit or lost answer requires lookup/replay with the
  **original** idempotency key. A pending receipt never licenses a second graph
  write. Mismatched params under a used key conflict; post warnings survive
  finalization/reopen according to the driver lifetime.

Stable database continuation preserves incarnation. After an uncertain restore
or promotion, admission needs a qualified recovery path that rotates it; this
package does not provide that operational path. PostgreSQL's design RPO of five
minutes/RTO of one hour is a target, not a result of local transactional tests.
Dump/restore, cutover and production data movement are not installed CLI/RPC
operations in this snapshot.

## Audit-plan boundary

The merged [header contract](../../internal/storage/driver/driver.go) can carry
ordered `Events`, and [memory publication](../../internal/storage/memory/transaction.go)
validates, copies and atomically persists them with the changeset/receipt.
The SQLite [commit path](../../internal/storage/sqlite/transaction.go) and
PostgreSQL [commit path](../../internal/storage/postgres/commit.go) at this pin
do not carry `header.Events` into their committed changesets. Do not enable
ordered-plan production on those drivers or expect conditional notices to
survive SQL publication. Baseline history/checkpoints/receipts do not imply
this capability. The separately authored SQL audit follow-ons are not
incorporated or tested by these guides.

## Exercise disposable baseline stores

Run the existing tests through Bazel; they create synthetic stores and clean up
their resources. PostgreSQL requires Docker's local socket and the pinned image
documented in its guide, and fails rather than skipping if unavailable.

```sh
bazel --ignore_all_rc_files test \
  //internal/storage/memory:memory_test \
  //internal/storage/sqlite:sqlite_test \
  //internal/storage/postgres:postgres_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_arg=-test.v --test_timeout=900
```

The [shared conformance cases](../../internal/storage/conformance/conformance.go)
exercise lifetime, locks, fenced publication, history, receipts and checkpoints.
[SQLite process tests](../../internal/storage/sqlite/process_test.go) and
[PostgreSQL reopen tests](../../internal/storage/postgres/persistence_test.go)
cover persistent engine boundaries. Those cases do not certify the complete
runtime's behavior cards, performance or managed recovery targets.
