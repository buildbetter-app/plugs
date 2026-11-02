# S1 B25-B28 and I4 acceptance slice

This package is the dedicated four-card recorder, projection, and acceptance
slice for B25 relational preservation/delete cascades, B26 ephemeral-plane
promotion, B27 import/export/configuration, and B28 write pipeline/subscribers.
It also exercises the already-admitted I4 transaction boundaries: auxiliary
invariants are atomic, a late event-log failure rolls back the write/version/
receipt, committed post failures become sanitized durable warnings and dead
letters, and sponsor-resolved approvals count distinct humans before resolving.

The B25 and B28 tests use real memory and file-backed SQLite stores with a fixed
UTC clock, explicit IDs, a disposable lease, and the production Writer. The
SQLite cases close and reopen the store before replay/readback. The I4 event
case installs a real SQLite trigger that fails only at changeset publication;
the retry proves that no version, receipt, event, or callback leaked from the
refused transaction. The timed post case deliberately panics in
`AfterWithTimeout`; the Writer returns the committed result, persists only
sanitized failure metadata, and does not call the subscriber again on replay.

B27 retains the two independent pinned Beads reference recordings already
admitted by the converter slice. `record_reference.py` records actual public
init/import/export commands in disposable stores under `/tmp/plug-swarm/work`;
`provenance.py` independently recomputes the raw-export projection and all
fixture digests. The Go replay checks the pinned source commit/tree, executable,
recorder, command receipts, and both run outputs, then refuses JSONL at the
public `plug-dump/1` converter boundary. It also preserves a real current Plug
write and every SQLite table when archive conversion is refused on a nonempty
target. The retained B25 extension bytes and existing B28 reference bytes stay
outside this package and are not rewritten.

The manual `public_recording_test` target closes the executable-side B27 gap
with two fresh, independent RunsOn recordings. It consumes the verified
`plug-public-companion/1` bundle through `PLUG_PUBLIC_BUNDLE`, checks the
sealed Beads commit/tree, executable digest, manifest, and `go version -m`
build information, then runs real commands in disposable stores. Each run
records `init`, configuration, `remember`, relational issue creation with
labels/metadata/parent/dependency/comment/close data, JSONL export, import
into a non-empty target, and target export. It then captures the actual
`beads-inventory/1` census and `plug-dump/1` archive for both stores. Raw
stdout/stderr, argv, stdin/output hashes, inventories, archives, build info,
and a provenance file remain under `/tmp/plug-swarm/work`; the test also
compares an independent JSONL projection with an independent archive
projection and requires the two recordings to match. Store identifiers and
timestamps are excluded only by the declared projection fields in
`testdata/public-recording-fixture.json`.

Run it only on RunsOn after preparing the public bundle descriptor:

```sh
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD s1-b25-b28-public-recording -- \
  test --test_env=PLUG_PUBLIC_BUNDLE=RUN_ID:ARTIFACT_ID:MANIFEST_SHA256 \
  //spec/s1-b25-b28:public_recording_test --test_output=all
```

The recorder is evidence for the pinned public head-state/configuration and
relational import/export boundary. It does not turn the explicit B26
unqualified boundary, Postgres replay, production operation, or release into
a passing claim.

B26 is a typed unqualified boundary, not a successful substitute. No admitted
public Plug producer currently creates an ephemeral-plane record, so this slice
does not use a memory row or a successful durable bead as a stand-in for
promotion. Instead, a source archive whose real `storage_class` is
`"ephemeral"` reaches `dumpformat.Read` and is explicitly refused with
`unsupported storage class`. The decision and limits are retained in
`b26-deviation.json`; promotion, mixed-plane dependencies, and promotion
rollback remain dependent on an adopted producer contract.

The package is an acceptance slice, not a full-card parity certificate. It does
not claim B26 positive producer evidence, complete B27 configuration/auxiliary
source coverage, Postgres replay, or production deployment. Run it through the
RunsOn wrapper, never local Bazel:

```sh
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD s1-b25-b28-focused -- test \\
  //spec/s1-b25-b28:acceptance_test \\
  //internal/core/pipeline:pipeline_test \\
  //internal/integration/postsql:postsql_test \\
  --test_output=errors --test_arg=-test.v
```
