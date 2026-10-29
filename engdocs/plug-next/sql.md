# SQL event-plan persistence

This page is pinned to **`9d03323f423cf28d202b328ed7c116e45db8406f`**. The
engine guides are [SQLite](../../internal/storage/sqlite/README.md) and
[PostgreSQL](../../internal/storage/postgres/README.md); the shared contract
is in [the driver header](../../internal/storage/driver/driver.go).

## What changed since the baseline storage guide

Every write's `ChangesetHeader.Events` is a complete ordered event plan. Both
SQL drivers clone and validate that plan against the actual changes and
allocated revision before touching records, versions, the changeset log, or a
receipt. The plan therefore commits or rolls back with the graph write.

| Store | Current event-plan behavior | Evidence |
| --- | --- | --- |
| Memory | Validates and retains plans for the process lifetime. | [`RunAudit`](../../internal/storage/memory/audit_test.go) |
| SQLite | Stores plans in the changeset and replay rows; a close/reopen returns the same event descriptor. | [`TestAuditPlanSurvivesFileReopen`](../../internal/storage/sqlite/sqlite_test.go) |
| PostgreSQL | Stores plans in the transactional changeset and receipt rows, with the shared audit conformance suite. | [`TestAuditConformance`](../../internal/storage/postgres/conformance_test.go) and [`persistence_test.go`](../../internal/storage/postgres/persistence_test.go) |

The SQL implementation does not infer events from changed rows. A nil plan
retains the older history discriminator; an invalid non-nil plan refuses. A
no-op cannot contain an event plan. `ChangesetByKey` returns the same plan on a
matching replay, and a request-hash mismatch remains a conflict.

## SQLite file lifecycle

Open a stable local path with
`sqlite.Driver{}.Open(ctx, driver.Config{DSN: "/local/path/store.db"})`.
Readers hold real SQLite snapshots. Publication uses WAL and `BEGIN IMMEDIATE`
with the complete sorted lock set, the lease epoch check, graph data, event
plan, and receipt in one transaction. A file reopen preserves the event plan,
history, and replay identity. The lock directory beside the database must stay
in place while any handle is open. Branches, custom options, unknown filters,
and schema migrations outside the admitted schema refuse.

The focused persistence proof is:

```sh
bazel --ignore_all_rc_files test //internal/storage/sqlite:sqlite_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no \
  --test_output=all --test_timeout=900 --test_arg=-test.v
```

SQLite is local persistent storage. It does not provide standby, replication,
failover, automated backups, or production cutover.

## PostgreSQL transaction boundary

Open one Plug store in the caller's PostgreSQL database with
`postgres.Driver{}.Open(ctx, driver.Config{DSN: dsn})`. The driver creates its
private schema transactionally. Repeatable-read readers, bytewise sorted lock
rows, the lease row held through SQL `COMMIT`, and the changeset/event-plan
insert share one publication boundary. Fencing and response finalization use
the same lease lock; an uncertain commit returns `ErrIndeterminate` with the
original idempotency key.

Run the real engine target when Docker is available:

```sh
bazel --ignore_all_rc_files test //internal/storage/postgres:postgres_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no \
  --test_output=all --test_timeout=900 --test_arg=-test.v
```

The test harness creates a fresh loopback-only PostgreSQL 17.11 container and
removes it afterward. If it cannot start the pinned engine, the target fails;
it never substitutes memory. Managed replication, backup, promotion, RPO/RTO,
and recovery incarnation changes remain deployment work.

The [older storage guide](../plug-runtime/storage.md) remains pinned to
`471d0199` and correctly describes that snapshot's audit-plan limitation. This
page is the current SQL delta; it does not rewrite that historical evidence.

