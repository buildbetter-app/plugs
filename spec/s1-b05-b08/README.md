# S1 B05–B08 runtime qualification slice

This package is the focused Plug runtime lane for:

- B05 lease heartbeat, quiet replay, expiry, and trusted reclaim;
- B06 scheduling-cycle refusal;
- B07 dependency vocabulary, custom-edge retention, and same-endpoint type
  conflict;
- B08 linked-child close guards and lifecycle settlement.

The tests construct the real Writer, service, RPC registry, claim binding, and
memory or file-backed SQLite driver with fixed UTC clocks, finite IDs, fixed
project/agent metadata, and deterministic idempotency keys. Heartbeat refreshes
are checked against graph head, versions, changesets, and persisted bead bytes;
SQLite is reopened before replay to prove checkpoint receipt durability.

The historical B07 reference bytes under `spec/recorded/extension/` remain
unchanged and are not relabeled as full-card evidence. The reference twin
recordings in this package are deliberately typed as unavailable until the
pinned executable has been run twice in disposable public stores on RunsOn.
`record_reference.py` fails closed unless the source checkout is the exact
clean pin and a generated proof binds the executable to controlled Go and SQL
clocks; each operation retains its actual JSON or byte output, executable
version digest, and probe projection. The Plug runtime tests therefore record
the executable behavior and their stable semantic projection without inventing
Beads receipts or private-store observations.

Run the focused target through the swarm wrapper:

```sh
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD s1-b05-b08-focused -- test //spec/s1-b05-b08:acceptance --test_output=all
```
