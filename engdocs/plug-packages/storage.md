# Storage contract and lifecycle

This page is derived from merged source snapshot
`18f7883b6b7d8f9edf8a6bb9f1effaa4e88c10ce`. The authoritative boundary is
[`internal/storage/driver/driver.go`](../../internal/storage/driver/driver.go);
the merged executable implementation is
[`internal/storage/memory`](../../internal/storage/memory/store.go).

## Ownership and handles

| Handle | Owner | Close/finish behavior |
| --- | --- | --- |
| `Store` | Driver implementation | `Store.Close` is idempotent. It ends the active lease, finishes unfinished transactions as rollback, releases lock/state handles and never commits staged data. |
| `ActiveLease` | Store | `Lease` increments the epoch and returns a fenced holder. `Release` ends that holder; `Lost()` closes on release, expiry or fencing. The contract defines a 10-second `LeaseTTL` and 3-second `LeaseRenewEvery`; callers supply the TTL to the memory store. |
| `Reader` | Store or transaction | `Read` captures an immutable committed snapshot. `Reader.Close` drops the pinned state after in-flight reads finish; snapshot identity metadata remains available, while data methods fail. Returned records, blobs, lock states, pages and receipts are caller-owned copies. |
| `WriteTx` | Store, bound to an active lease | It inherits `Reader`. Acquire the complete sorted lock set once, call `Head` last, stage changes, then `Commit` or `Rollback`. `Close` is cleanup equivalent to rollback and never commits. A successful commit finishes the transaction and releases its locks. |

`Read` does not wait for writers. A view must carry the store incarnation; a
missing or mismatched incarnation fails before reading, and a revision below
the history floor is rejected. A transaction reader sees its own staged values
and the latest committed values protected by its locks, never another
transaction's staged values.

## G1–G5 boundaries

| Guarantee | Driver boundary |
| --- | --- |
| **G1: revisions** | `Head` serializes allocation after every other lock. Successful graph commits advance the head by one; rollback, validation failure and a no-op consume no graph revision. |
| **G2: locks and reads** | `Lock` accepts one globally sorted, duplicate-free set. It returns the latest committed state for each acquired key; `skipLocked` omits unavailable keys without retaining a partial set. Share locks never authorize writes. `Observe` reads a snapshot without acquiring locks. |
| **G3: atomic publication** | `Commit` checks the exact acquired `LockKeys` and publishes records, tombstones, version rows, blobs, derived rows, predicate/read-fact bumps and the changeset together. A failed validation or rollback publishes none of them. Core chooses the semantic predicate scopes; the driver checks structural lock/bump coverage. |
| **G4: replay** | A keyed operation is unique for 24 hours by canonical principal, anonymous agent label, method and idempotency key. A different request digest is a conflict. Ordinary responses are stored with the graph commit; the key remains in the changeset log even after the deduplication window. |
| **G5: fencing** | The active lease epoch is checked on every write and at publication, including checkpoints, no-op receipts, response finalization and renewal. A stale holder receives `ErrFenced`; it cannot publish after takeover. |

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
   stores only an optional final receipt at the current head. It creates no
   record, version, blob, derived row, predicate bump, changeset or head
   advance; it never invokes post work. A matching pending receipt is a
   conflict, never permission to return unfinished success. See
   [`noop.go`](../../internal/storage/memory/noop.go) and the shared cases in
   [`noops.go`](../../internal/storage/conformance/noops.go) and
   [`responses.go`](../../internal/storage/conformance/responses.go).

The driver contract deliberately stops at durable state transitions. This
snapshot has no merged subscriber dispatcher or audit emission package, so it
does not claim end-to-end callback delivery or audit behavior. A future caller
must poll with fresh readers, retry only a known post outcome within its
bounded policy, and never turn an unresolved pending receipt into fabricated
success.

## Shared conformance and local commands

`conformance.Run` is the common suite. It covers lifetime cleanup, concurrent
gapless revisions, latest-state and atomic lock sets, atomic publication and
derived predicates, keyed replay, no-op receipts, pending/final responses,
finalizer CAS, lease fencing and takeover, checkpoint CAS, history replay and
pagination. The merged memory target calls it from
[`checkpoint_test.go`](../../internal/storage/memory/checkpoint_test.go); a
persistent driver must call the same suite after it is merged.

Run the focused checks with ambient Bazel settings disabled:

```sh
bazel --ignore_all_rc_files test //internal/storage/memory:memory_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no --test_output=all
bazel --ignore_all_rc_files test //:lexicon_gate \
  --jobs=2 --lockfile_mode=error --color=no --curses=no --test_output=all
bazel --ignore_all_rc_files build //... \
  --jobs=2 --lockfile_mode=error --color=no --curses=no
```

For an exact committed head, use the shared recorder with `--all --mode
build`; it records the Bazel version, expanded targets, command, log and clean
head under `/tmp/plug-swarm/ci-local/<pr>-<sha>/`.

The memory store is the only merged storage implementation at the pinned
snapshot. SQLite and Postgres remain open PRs (#31 and #33), so persistence,
reopen, multi-handle and engine-specific conformance are follow-on evidence,
not claims of this page.
