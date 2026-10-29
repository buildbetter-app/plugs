# Post subscribers and delivery on SQL

This acceptance package composes the actual SDK native plugin host, pipeline
Writer, default ontology and tracked ontology audit with file-backed SQLite/WAL
and disposable PostgreSQL. Only the protected ontology source uses
`conformance.Seed`, with explicit system attribution. Beads, versions, event
plans, responses and delivery checkpoints use production operations.

Both engines are mandatory. PostgreSQL runs in an owned loopback-only Docker
container pinned by digest; every scenario owns a fresh database or SQLite
file. An unavailable engine fails the target. There is no engine skip, external
DSN or memory substitute. Containers and databases are removed after the run.

The six tests cover seven scenarios on each engine:

- A native post reads its committed SQL revision after an independent Writer
  advances the head. The pending response, final response, record versions and
  event plan agree. Reopen preserves them, and exact replay adds no post effect.
- A failed native binding returns a durable warning while its peer succeeds.
  Replay, no-op, dry-run and rejected requests add no post effects. Non-committed
  requests have no receipt; changed parameters cannot reuse the same key.
- A real SQL trigger rejects all four response finalization attempts. Pending
  duplicates return committed/retryable `busy`; pending state survives reopen.
  Explicit finalization uses only the actual captured Writer warning. An old
  epoch is fenced, retained readers stay pending, and final bytes survive a
  second reopen without another callback or graph write.
- A finalizer acknowledgement is lost after a successful SQL CAS. The Writer
  reloads the real durable warning before returning success; reopen replay is
  identical and does not repeat the external effect.
- The dispatcher consumes the native Writer's two-event log through the real
  history materializer. SQL checkpoint rejection and a persisted-save/lost-ack
  boundary retain their distinct positions. Old adapters are lease-fenced;
  fresh adapters resume after handle reopen with stable keys/payloads and late
  markers, even after later graph edits. Checkpoints contain metadata only.
- A dispatcher post failure persists dead-letter metadata and emits its actual
  observer notice. Reopen retains that failure; a late retry repeats the same
  delivery key and historical payload, then advances normally.

Callback effects are disposable synced files. Dispatcher delivery is explicitly
at least once: the rejection/failure cases intentionally record duplicate effects
with the same key. Consumer idempotency remains the consumer's responsibility.

The native `Host.Posts()` path and event `Dispatcher` remain separate existing
APIs. Native payloads have no delivery key/late marker and native callbacks do
not create event checkpoints. This suite does not claim automatic native crash
redelivery, native dead-letter/breaker/alert composition, an automatic recovery
worker, OS-process/database restart, replication/failover, RPC coverage,
performance or full B20/B21/B28 acceptance. Known-outcome finalization is an
explicit capability call using captured arguments, not unknown-outcome recovery.
Dispatcher observer notices prove the notification port, not an installed alert
sink or breaker implementation.

Run through local Bazel with the shared cache:

```sh
bazel --ignore_all_rc_files test \
  --disk_cache=/tmp/plug-swarm/bazel-disk-cache \
  --local_resources=memory=8192 --jobs=2 --test_timeout=900 \
  --lockfile_mode=error --test_output=all --test_arg=-test.v \
  //internal/integration/postsql:postsql_test
```
