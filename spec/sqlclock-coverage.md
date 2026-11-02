# S1 runtime clock boundaries

This slice controls the clocks used by the current Plug production drivers.
It does not qualify a complete behavior card or replace Beads twin evidence.
The retained reference packets still honestly say `sql-driver-clock-unreached`.

| Engine | Actual time source | Runtime control | Precision |
| --- | --- | --- | --- |
| Memory | `Store.now` | Existing `memory.Options.Clock` | 1 ns |
| SQLite | Go `time.Now` | Optional `sqlite.Driver.Clock`; nil keeps the exact production default | 1 ns |
| PostgreSQL | SQL `clock_timestamp()` | Test-only SQL function selected by the disposable database connection's explicit `search_path` | 1 us |

SQLite has no production `CURRENT_TIMESTAMP`, `strftime`, or other SQL clock
call in its schema, receipt, commit, health, or writer-lease paths. Native
SQLite SQL-clock coverage is **not applicable/unreached**. No SQL time path was
added. The optional Go seam controls `saveReplay`, `readReplay`, the zero-Time
`transaction.Commit` fallback, `Store.Health`, `Store.fence`, and the durable
expiry timestamps in `Store.Lease` and `activeLease.Renew`.

SQLite's real lease timer still starts from the original `time.Now` sample;
its deadline, `time.Until`, callbacks, cancellation and TTL budgets are unchanged.
With a nil optional clock, that same sample supplies both durable expiry and
local deadline, exactly as before. Clock injection is not exposed through
server or driver configuration options. Publication locks, connection lifecycle,
transaction cleanup, and authorization are outside this change.

PostgreSQL production SQL is unchanged. A test-only library creates a SQL
sequence and a VOLATILE `plug_test_clock.clock_timestamp()` in an owned,
disposable database. The driver's existing SQL performs timestamp generation,
interval addition, expiry predicates, fencing and renewal. The clock function
uses integer seconds plus microseconds, not a floating-point Unix timestamp.
Sequence state remains visible inside pinned MVCC snapshots, matching a wall
clock. `pg_catalog.clock_timestamp()` keeps the real production clock. Every
pool and reserved connection gets the explicit test search path and a non-UTC
session timezone. Sub-microsecond PostgreSQL seeds/advances are refused rather
than silently rounded. No driver response is mocked, no receipt timestamps are
rewritten, and no sleeps drive boundary outcomes.

The named runtime cases cover:

- `TestS1PostgresClockControlsGeneratedCommitAndReceiptTime` and
  `TestS1SQLiteClockControlsGeneratedCommitAndReceiptTime`: generated commit time
  and receipt creation/expiry match the controlled instant, including fractional
  seconds and a seed with a non-UTC offset.
- `TestS1{Memory,SQLite,Postgres}*ReceiptPendingFinalAndExpiryBoundaries`:
  pending/final lookup and finalization before, exactly at and after the 24-hour
  window; pinned/fresh snapshot behavior; finalization preserves receipt age.
- `TestS1{Memory,SQLite,Postgres}*LeaseExpiryAndRenewalBoundaries`: live renewal,
  fencing and expired-renewal refusal at the unchanged ten-second writer lease.
- `TestS1SQLiteDefaultClockUsesWallTimeAndUnchangedLeaseBudget`: nil clock uses
  wall time and identical durable/local lease deadlines.
- `TestS1B04ApplicationClaimAndDriverReceiptBoundaries`: the real S1 RPC fixture
  crosses five-minute application claim expiry and 24-hour driver receipt expiry
  on all three stores, with refused writes leaving the durable image unchanged.
- Existing `TestSQLClaimRecoveryAtTheRPCBoundary`: actual SQLite and PostgreSQL
  driver clocks advance with application time; strict reclaim grace is tested
  before, exactly at and after the cutoff, with ready membership and attributed
  history/event/replay checks retained. PostgreSQL uses a representable 1 us tick.
- `TestS1B21BatchReceiptWindowAcrossStores`: a batch replays before expiry and
  executes fresh exactly at and after expiry on all three stores.
- Existing `TestS1B24UntimedDeferRequiresExplicitReopen`: all three engines keep
  deferred work parked after clock advance and require explicit reopen.

Tests that advance beyond a writer lease explicitly release it and acquire a
new epoch before subsequent application work. They do not extend or disable
production leases. PR251's heartbeat branch and `lease_overlay.go` are untouched.
Its absent B05 implementation and clock-qualified Beads twins remain separate
requirements. B01 hierarchy/hybrid-order changes remain with their owner.
Full S1 remains 0/28; E3 remains 4/5.

All build/test execution is through `remote-bazel` on RunsOn. Exact committed
SHA/tree, commands, raw named RUN/PASS, XML, REDs, and skip limits belong in the
immutable author packet; these source descriptions are not runtime receipts.
