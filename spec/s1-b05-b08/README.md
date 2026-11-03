# S1 B05–B08 runtime qualification slice

This package is the focused Plug runtime lane for:

- B05 lease heartbeat, quiet replay, expiry, and trusted reclaim;
- B06 scheduling-cycle refusal;
- B07 dependency vocabulary, custom-edge retention, and same-endpoint type
  conflict;
- B08 linked-child close guards and lifecycle settlement.

The tests construct the real Writer, service, RPC registry, claim binding, and
memory, file-backed SQLite, or disposable PostgreSQL driver with fixed UTC clocks, finite IDs, fixed
project/agent metadata, and deterministic idempotency keys. Heartbeat refreshes
are checked against graph head, versions, changesets, and persisted bead bytes;
Persistent stores are reopened before replay to prove checkpoint receipt durability.

The heartbeat/reclaim regressions pause actual producer calls before forwarding
to real checkpoint saves and graph commits. Both publication orders are tested:
a completed heartbeat invalidates an older recovery observation, while reclaim
or close prevents a late heartbeat from publishing an orphan overlay. Cancelled
publication retains only its recoverable reservation. Direct driver checks cover
absent/existing checkpoint generations, stale records, incarnation mismatch,
lease fencing, rollback, missing capabilities, and reopen. The original PR251
authorization, replay, interruption, and same-key CAS assertions also run on
PostgreSQL. No request-phase or test timeout is increased.

B06 additionally refuses ancestor blockers in either direction over explicit
parents or parent-child links. A reparent cannot introduce such a blocker, and
refusals preserve earlier batch images, history, graph sentinel, and receipts.
Association links remain legal. These tests do not certify the missing public
cycle-report producer or B14's waits-for metadata/fanout behavior.

The refill assertions additionally check plain waits-for child settlement and
reopen, direct/transitive dependency readiness, ordered multi-target close,
atomic refusal of a parent-first batch, durable close replay, and stale close
CAS without history or receipt writes. These are Plug runtime assertions, not
fresh reference observations.

B07 opaque-kind regressions exercise public link and unlink with exact custom
bytes, including whitespace-only values and the 32-byte UTF-8 boundary, across
all three engines. Replay, duplicate edges, retype refusal, reopen, and history
are checked alongside empty/over-limit batch rollback. Custom loops remain inert
for ready membership, cycle checks, and close guards. This bounded admission
repair does not qualify the full B07 card or controlled-clock reference twins.

The historical B07 reference bytes under `spec/recorded/extension/` remain
unchanged and are not relabeled as full-card evidence. The reference twin
recordings in this package are deliberately typed as unavailable until the
pinned executable has been run twice in disposable public stores on RunsOn.
`record_reference.py` fails closed unless the source checkout is the exact
clean pin and a generated proof binds the executable to controlled Go and SQL
clocks; each operation retains its actual JSON or byte output, executable
version digest, and probe projection. Stdout and stderr always retain lossless
base64 bytes alongside parsed JSON and stream digests. The independent projector
checks both raw and projected twin digests. The Plug runtime tests therefore record
the executable behavior and their stable semantic projection without inventing
Beads receipts or private-store observations.

`reference_admission` tests disposable unit fixtures for fail-closed clock and
profile admission, dirty/hidden/raw source bytes, private input copies, credential
isolation, and lossless streams. Fixture success is not paired-store evidence.
`prepared_reference_route` is manual and consumes the trusted workflow's fixed
public descriptor `41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2`.
That profile pins the original Go-built executable (not canonical Bazel) at
SHA256 `3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4`.
It copies and validates the fixed ZIP, manifest, source and proof before granting
execute permission. Missing generated Go+SQL proof fails before output creation.
Trusted preparation alone provides no proof, so this target must not be dispatched
to repeat the known missing-clock boundary. No reference capture is claimed here.

Run the focused target through the swarm wrapper:

```sh
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD s1-b05-b08-focused -- test //spec/s1-b05-b08:acceptance --test_output=all
```
