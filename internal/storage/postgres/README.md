# PostgreSQL storage driver

`postgres.Driver{}.Open(ctx, driver.Config{DSN: dsn})` opens one dedicated
PostgreSQL database containing one Plug store. The caller supplies a pgx DSN and
database credentials through its existing configuration. SQL, schema objects and
connection handles remain private to this package. The database role needs
permission to create the private `plug_postgres` schema on first open. Schema 1
initialization is transactional and serialized across independent handles.

Readers hold read-only repeatable-read transactions. Close readers promptly;
their MVCC snapshots remain stable across writes, response finalization and
other pool closures. As-of reads reconstruct records from full bases and forward
JSON Patch replacements, with at most 31 patches. Creation, recreation and
every 32nd version position create replay boundaries; tombstones retain their
revision. Current records preserve the caller's JSON bytes. Batched point reads
resolve missing keys from the current-record lookup without probing version
history for each absence. Only a physical row newer than the requested snapshot
requires historical reconstruction; current tombstones are retained.

Writers declare the complete, bytewise sorted lock set once. Exact bytea lock
rows cover missing records, read facts, sentinels and derived rows without hash
collisions. Share locks permit shared reads; update and skipLocked locks permit
writes. Failed NOWAIT attempts roll back their entire prefix before waiting.
Placeholder lock rows carry no graph state. Head is acquired after the complete
data lock set; successful publication alone advances it. One SQL transaction
publishes records, tombstones, versions, blobs, derived rows, predicate bumps,
the complete changeset and any replay receipt.

Every publication holds the lease ownership key FOR KEY SHARE until SQL COMMIT.
The unique ownership index makes acquisition and release conflict with this
fence, while expiry-only renewal uses its reserved connection independently.
Contenders use NOWAIT so their stronger lock cannot queue ahead of renewal.
Graph writes, no-op receipts,
checkpoint CAS and response finalization all check epoch/owner/expiry under the
publication lock. Expiry closes Lost; stale or foreign handles cannot publish
or renew. Close rolls back unfinished work, releases snapshots and leases, and
closes the pool. Callers should close readers and finish transactions before
closing their store and should use bounded operation contexts.

Idempotency uses the full principal/anonymous-label/method/key scope for 24 hours
on the database clock, independently of attribution time. Authenticated labels
are ignored. Request-digest mismatches conflict. Keyed graph writes
can publish a pending response atomically with their data. Readers retain that
state across pool closure and reopen; core must await durable finalization
before returning success. The fenced finalizer appends its outcome once and
clears pending even for no warnings, preserving receipt age and graph state.
Final receipts are immutable; identical post outcomes replay, different ones
conflict. No-op replay refuses pending receipts. No-op receipts retain the current
head and each result's original revision and create no graph or history entry.
Unknown COMMIT outcomes return ErrIndeterminate with the original key.

Supported queries are string equality on kind, id, key, type, project and status,
ordered by canonical record key. Pages bind incarnation, snapshot, method and
filter. This implementation scans candidate keys; it makes no query-performance
claim. Branches other than zero, unknown query operators/configuration options,
and migrations beyond schema 1 return explicit errors. Revisions, epochs and
checkpoint generations cover uint64, including values above MaxInt64, with
checked conversion and overflow refusal. Indexed opaque keys/scopes are limited
to 2048 bytes explicitly. Blob writes are content addressed and transactional.

Allocator continuation is durable in `allocator_states` and participates in
the existing complete lock set, read-fact history and fenced SQL publication.
Sequential top-level and direct-child counters continue across reconnects.
The table is an additive schema-1 initialization step, so reopening an older
schema-1 database creates it without changing existing records or identity.
Hash-mode evidence is retained, but hash allocation remains fail-closed until
a collision/nonce contract is admitted; this driver never falls back to a
random ID. Duplicate or non-canonical restore prefixes refuse before staging.

Replication, physical backups, HA promotion and uncertain-recovery incarnation
rotation require the deployment's managed PostgreSQL recovery integration; this
package does not orchestrate them. The managed-HA design targets RPO 5 minutes
and RTO 1 hour. Local transactional tests do not certify either target. Stable
database continuation preserves incarnation; callers must not admit traffic
after an uncertain restore/promotion without a separately qualified recovery
path that changes incarnation. No production cutover is provided here.

Run the real-engine tests through Bazel:

```sh
bazel --ignore_all_rc_files test //internal/storage/postgres:postgres_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_arg=-test.v \
  --test_timeout=900
```

The test harness always creates and removes its own loopback-only Docker
container and fresh synthetic databases. Docker's local socket and the pinned
image must be available; absence fails the test, never skips or substitutes a
memory store. The image is
`postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3`
(observed PostgreSQL 17.11). Shared conformance runs directly on this driver.
Additional tests cover reopen persistence, independent SQL sessions, a late
engine error rolling back every data family, 24 concurrent snapshots, numeric
boundaries, expiry, pending receipt recovery after an SQL finalizer error, and
takeover during graph/no-op/checkpoint/finalizer writes.
