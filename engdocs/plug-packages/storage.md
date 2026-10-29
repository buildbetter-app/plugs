# Storage contract and lifecycle

This page is derived from merged source snapshot
`3577f0387caf4b96a63dd4bee12a249c0e6d133b`. The authoritative boundary is
[`internal/storage/driver/driver.go`](../../internal/storage/driver/driver.go);
the merged implementations are
[`memory`](../../internal/storage/memory/store.go) and
[`sqlite`](../../internal/storage/sqlite/README.md). Receipt durability follows
the engine: memory is disposable, while SQLite persists it in the store file.

## Ownership and handles

| Handle | Owner | Close/finish behavior |
| --- | --- | --- |
| `Store` | Composition root, implemented by the driver | Close every reader and commit or roll back every transaction before `Store.Close`. Store close is idempotent, releases its resources/active lease and never commits staged writes. |
| `ActiveLease` | Store | `Lease` increments the epoch and returns a fenced holder. `Release` ends that holder; `Lost()` closes on release, expiry or fencing. The hosting server must stop admitting reads and writes until a new lease is acquired. The contract defines a 10-second `LeaseTTL` and 3-second `LeaseRenewEvery`; callers supply the TTL to `Lease`. |
| `Reader` | Caller that opens it | `Read` captures an immutable committed snapshot. `Reader.Close` releases the snapshot resources; memory drops its state pin and SQLite rolls back its read transaction. Returned records, blobs, lock states, pages and receipts are caller-owned copies. |
| `WriteTx` | Store, bound to an active lease | It inherits `Reader`. Acquire the complete sorted lock set once, call `Head` last, stage changes, then `Commit` or `Rollback`. `Close` is cleanup equivalent to rollback and never commits. A successful commit finishes the transaction and releases its locks. |

`Read` does not wait for writers. A view must carry the store incarnation; a
missing or mismatched incarnation fails before reading, and a revision below
the history floor is rejected. A transaction reader sees its own staged values
and the latest committed values protected by its locks, never another
transaction's staged values.

[`model.Record`](../../internal/core/model/runtime.go) is the core read value
with a decoded body; `driver.Record` holds raw body bytes and storage revision.
`model.ChangeSet` is an input proposal, while `model.Changeset` is the committed
history entry. The [service constructor](../../internal/core/service/service.go)
receives an existing store, active lease and Writer; it does not create another
store or own a parallel transaction path.

## G1–G5 boundaries

| Guarantee | Driver boundary |
| --- | --- |
| **G1: revisions** | `Head` serializes allocation after every other lock. Successful graph commits advance the head by one; rollback, validation failure and a no-op consume no graph revision. |
| **G2: locks and reads** | `Lock` accepts one globally sorted, duplicate-free set in record, read-fact, sentinel, derived order. It returns the latest committed state for each acquired key; `skipLocked` omits unavailable keys. A failed call acquires no partial set. Share locks never authorize writes. `Observe` reads a snapshot without acquiring locks. |
| **G3: atomic publication** | `Commit` checks the exact acquired `LockKeys` and publishes records, tombstones, version rows, blobs, derived rows, predicate/read-fact bumps and the changeset together. A failed validation or rollback publishes none of them. Core chooses the semantic predicate scopes; the driver checks structural lock/bump coverage. |
| **G4: replay** | A keyed operation is unique for 24 hours by canonical principal, anonymous agent label, method and idempotency key; authenticated labels do not change the scope. A different request digest is a conflict. Ordinary responses are stored with the graph commit; the key remains in the changeset log even after the deduplication window. |
| **G5: fencing** | The active lease epoch is checked on every write and at publication, including checkpoints, no-op receipts, response finalization and renewal. A stale holder receives `ErrFenced`; it cannot publish after takeover. |

Core must select every exact affected scope, including absent facts and deleted
keys. A matching sentinel family alone cannot prove semantic coverage of opaque
derived rows. Both merged engines support branch zero and equality queries on
kind/id/key/type/project/status, and explicitly reject unsupported queries and
branches. The model's lossless natural-number domain is wider than the engines'
checked uint64 revision domain; an overflow is refused, never narrowed.

The shared lock-key encoding is in [`keys.go`](../../internal/storage/driver/keys.go),
and typed fence, indeterminate, incarnation and truncation errors are mapped in
[`errors.go`](../../internal/storage/driver/errors.go).

## Durable response lifecycle

The receipt state is independent of graph history but is published atomically
with a keyed graph commit:

1. A keyed `Commit` with `ResponsePending=true` publishes the graph, changeset,
   initial response and pending receipt together. A pending response is not a
   successful replay result; it is visible to a fresh reader for digest and
   revision checks. A pending header without an idempotency key is invalid.
2. A caller that has a known post outcome invokes
   [`ResponseFinalizer.FinalizeResponse`](../../internal/storage/driver/driver.go)
   with the same full scope and committed revision. The operation is a
   compare-and-set: it appends the warnings once and clears pending without
   changing graph data, versions, log or head. Nil and empty warning lists are
   the same outcome.
3. A final receipt is immutable. Repeating the same outcome succeeds; a
   different outcome conflicts. Fencing, cancellation, validation failure or
   an expired scope leaves the pending receipt intact. A fresh reader is
   required to observe pending-to-final; an already pinned reader keeps its
   old snapshot.

`NoopCommitter.CommitNoop` is a separate final path. It requires the complete
lock set and `Head`, rejects all staged mutations and pending headers, and
stores only an optional final receipt at the current head, preserving each
per-result revision. It creates no record, version, blob, derived row, predicate
bump, changeset or head advance; the caller never invokes post work for it. A
matching pending receipt is a conflict. Ordinary `Commit` cannot replay a no-op
as a changeset; resolve it through `IdempotencyByKey`. See
[`noop.go`](../../internal/storage/memory/noop.go) and shared cases in
[`noops.go`](../../internal/storage/conformance/noops.go) and
[`responses.go`](../../internal/storage/conformance/responses.go).

[`pipeline/response.go`](../../internal/core/pipeline/response.go) implements
bounded polling through fresh readers and retries only a known post outcome.
It reloads the final durable response before success. An unresolved committed
receipt produces retryable `busy` with the committed revision, key and pending
state; replay does not rerun callbacks. `ErrIndeterminate` instead denotes an
uncertain publication and must be resolved using the original key.

`NoopCommitter`, `ResponseFinalizer` and `CheckpointStore` are optional driver
capabilities: callers must refuse with `ErrUnsupported` when absent, rather
than substitute process-local persistence. Both merged stores implement them.
[`metadata.go`](../../internal/storage/memory/metadata.go) shows checkpoint
generation CAS and finalization without graph advancement. The
[event storage adapter](../../internal/server/events/storage.go) uses that
checkpoint capability, not a graph transaction or a delivery-owned map.
Audit descriptor persistence and Writer emission remain separate follow-ons.

## Shared conformance and local commands

`conformance.Run` is the common suite. It covers lifetime cleanup, concurrent
gapless revisions, latest-state and atomic lock sets, atomic publication and
derived predicates, keyed replay, no-op receipts, pending/final responses,
finalizer CAS, lease fencing and takeover, checkpoint CAS, history replay and
pagination. The merged memory target calls it from
[`checkpoint_test.go`](../../internal/storage/memory/checkpoint_test.go), and
SQLite calls it from [`sqlite_test.go`](../../internal/storage/sqlite/sqlite_test.go).
The [SQLite engine tests](../../internal/storage/sqlite/engine_test.go) add
persistence and SQL-specific boundaries; shared memory results alone do not
prove another engine's persistence or runtime behavior.

Run the focused checks with ambient Bazel settings disabled:

```sh
bazel --ignore_all_rc_files test //internal/storage/driver:driver_test \
  //internal/storage/memory:memory_test //internal/storage/sqlite:sqlite_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no --test_output=all
bazel --ignore_all_rc_files test //:lexicon_gate \
  --jobs=2 --lockfile_mode=error --color=no --curses=no --test_output=all
bazel --ignore_all_rc_files build //... \
  --jobs=2 --lockfile_mode=error --color=no --curses=no
```

The docs enter the existing tree gate through their exact sorted
[`source_manifest.txt`](../../tools/lexicon/source_manifest.txt) entries; no
new BUILD target is required.

Memory remains disposable. SQLite is persistent local storage with no standby,
replication/failover or automated backup. [Postgres PR #33](https://github.com/buildbetter-app/plugs/pull/33)
is still a follow-on at the pinned snapshot. None of these package checks
certifies deployment topology, recovery targets or production cutover.
