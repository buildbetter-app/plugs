# S1 relational preservation and subscriber integration

Done = B25 declared-label preservation and replacement history, auxiliary
refusal/retry, and B28 timed-post warning/replay behavior execute through the
public byte RPC on memory, SQLite, and real PostgreSQL, proven by this target
and an exact-head whole-tree RunsOn gate plus independent review; changing
card statuses or passing a smaller store matrix does not count.

This target replays the original, hash-pinned B25 recording twice on fresh
instances of every required store. It preserves the recorder bytes and
compares their declared projection of labels, dependencies, comments, and
each title/priority/description edit. All mutations use the real RPC, service,
Writer, ontology, and driver. A local declared `strings` field supplies labels,
matching the original accepted fixture. Both Writer and service share that
resolver. Dependency images are checked through the real reader because the
public detail DTO does not expose them.

Additional cases check parent/metadata/comment/close-reason preservation,
explicit label replacement versus scalar-only historical diffs, replay of an
older scalar request after replacement, equal replacement as a no-op, and an
atomic auxiliary refusal followed by same-key retry. B28 exercises the timed
callback path against its committed snapshot, sanitized warnings and durable
dead letters, fail-closed pre refusal, and callback-free replay. SQL cases
close/reopen both store and server; memory cases reconstruct the server and
Writer against the same store. PostgreSQL is a mandatory pinned disposable
container with an independent database per fixture; absence fails the target.

Deletion follows the existing explicit-unlink policy. A bare delete with an
outbound edge refuses without changes; one `tx.apply` then unlinks and deletes
the owner atomically, removes its reference rows and retains its versions.
B27 uses the unchanged hash-pinned exporter archive to verify empty-target
unverified-history refusal and populated-target refusal on all three stores.
The populated case contains the same labels, parent, metadata, comments,
close provenance, history and final receipt exercised by the other cases.

SQL engine time remains the driver's real time. The reference projection is
non-temporal and makes no clock, lease-expiry, or performance claim. Existing
PR #245 assertions, reference data, and attribution remain unchanged. B26
public ephemeral producers await the recorded product decision; B27 public
complete-source converter repairs remain with their existing owner.

Run through `/tmp/plug-swarm/bin/remote-bazel`, with target
`//internal/integration/s1relations:s1relations_test`. No local build or test
execution is required.
