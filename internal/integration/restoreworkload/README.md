# Restored in-process workload acceptance

This package proves the admitted native head-image restore can carry real work
after a same-process SQLite close/reopen. The fixture creates records through
the bound local RPC client, dumps them through `admin.dump`, restores the
archive into an empty SQLite file through `admin.restore.*`, closes that
runtime, and opens a fresh bound local service/RPC composition over the same
file. No test writes through a driver after restore.

The reopened runtime must preserve the source store identity and record IDs,
publish a new incarnation at restore revision 1, and expose the restored head
through history. A real `bead.claimNext` claims one restored record while its
keyed replay returns identical bytes without claiming the other. One atomic
`tx.apply` then updates the claimed record and creates a distinct ordinary
`pl-<32 lowercase hex>` ID at a single revision; its replay is also byte-stable
and write-free. Populated-target restore and stale-precondition refusals leave
the head unchanged, and a refused request does not consume its idempotency key.

Run the focused target with:

```sh
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD i2-local-vertical -- \
  test //internal/integration/restoreworkload:restoreworkload_test \
  //internal/cli:cli_test --test_output=all --test_timeout=900
```

`TestRestoredThinSliceLifecycleAcrossMemoryAndSQLite` also runs one complete
I2 chain on each fresh memory and SQLite restore: ready, claim, update, comment,
close, history, dump, and restore. It preserves the D1 close-guard, atomic
`tx.apply`, warning, replay, and generated-registry assertions. Its final dump
comes from the mutated restored runtime at revision 7. Each resulting archive
is restored into new memory and SQLite targets, so all four driver pairs check
the completed state, exact IDs, updated fields, comment body/actor/time, and
dependency link. Readback checks the record census, store identity, fresh
incarnation, empty ready set, and new revision-1 history floor; old-incarnation
and below-floor history requests must refuse. The archive explicitly requests
`history: head`; it does not claim that pre-restore revisions are replayed.

Every operation uses `cli.OpenLocal` and its in-process JSON-RPC byte transport,
which starts no listener and serves no `/ui/`. Local runtimes are closed with
their lease-renewal workers joined before the test completes. Admin dump and
restore use the explicitly injected `restore-workload` **test presence
verifier** for data-path qualification; this is not a live human-presence test
or production admission. The named verifier appears in each I2 completion log.

This is a native, head-only memory/SQLite acceptance slice. Native restore does not
carry allocator state, so the asserted allocator contract is the admitted
ordinary random-ID fallback and collision refusal, not sequential continuation.
The test does not qualify the Beads exporter, converter inputs, full-history
restore, PostgreSQL, migration parity, authorization, production recovery, or
cutover readiness.
