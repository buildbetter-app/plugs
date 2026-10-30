# Restored SQLite workload acceptance

This package proves the admitted native head-image restore can carry real
work after a process boundary. The fixture creates records through the bound
local RPC client, dumps them through `admin.dump`, restores the archive into an
empty SQLite file through `admin.restore.*`, closes that runtime, and opens a
new local service/RPC composition over the same file. No test writes through a
driver after restore.

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
bazel --ignore_all_rc_files test \
  //internal/integration/restoreworkload:restoreworkload_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900
```

This is a native, head-only SQLite acceptance slice. Native restore does not
carry allocator state, so the asserted allocator contract is the admitted
ordinary random-ID fallback and collision refusal, not sequential continuation.
The test does not qualify the Beads exporter, converter inputs, full-history
restore, PostgreSQL, migration parity, authorization, production recovery, or
cutover readiness.
