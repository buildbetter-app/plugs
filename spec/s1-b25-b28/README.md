# S1 B25-B28 and I4 acceptance slice

This package is the dedicated four-card recorder, projection, and acceptance
slice for B25 relational preservation/delete cascades, B26 ephemeral-plane
promotion, B27 import/export/configuration, and B28 write pipeline/subscribers.
It also exercises the already-admitted I4 transaction boundaries: auxiliary
invariants are atomic, a late event-log failure rolls back the write/version/
receipt, committed post failures become sanitized durable warnings and dead
letters, and sponsor-resolved approvals count distinct humans before resolving.

The direct B25 and B28 tests use real memory, file-backed SQLite and PostgreSQL
stores with fixed Go and driver/SQL clocks, explicit IDs, a disposable lease,
and the production Writer. PostgreSQL uses the test-only VOLATILE SQL clock
through the unchanged driver queries. The tests read actual persisted changeset
and receipt timestamps; production clocks and budgets are unchanged. Both SQL
stores close and reopen before replay/readback. The I4 event
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

The manual `public_recording_test` target requires two fresh, independent
RunsOn recordings to close its executable-side B27 slice. Its native profile is
`plug-public-native/1`, Beads source `305fec3afac83224f46c54f982959fd99db136fa`,
tree `48e3545b7c5c6a5cdc0119e34dfa379354456a6e`. The separately named
`native-public-profile.json` and `native-public-recording-fixture.json` preserve
the old companion runfiles and the PR295/roundtrip consumer contract.
The target consumes only the trusted prepared artifact through
`PLUG_PUBLIC_BUNDLE`, binds producer run `41610284344`, the exact manifest digest
and workflow source, then checks executable bytes and `go version -m` main-package
identity. The module pin hashes the actual source module path, not `go.mod` bytes.
Each separately dispatched recording uses `-public-recording-id=recording1` or
`recording2`; its individual receipt explicitly requires the independent second
run and semantic comparison before pair qualification. Each run
records `init`, configuration, `remember`, relational issue creation with
labels/metadata/parent/dependency/comment/close data, JSONL export, import
into a non-empty target, and target export. It then captures the actual
`beads-inventory/1 --plug-capture-public` census and `plug-dump/1` archive for
both stores. Both exports are `--readonly`; explicit `dolt commit` commands
precede capture so dirty source state cannot be silently declared clean. Raw
stdout/stderr, full executable/argv, cwd, isolated public environment, stdin/output hashes, inventories, archives, build info,
and projections go to `TEST_UNDECLARED_OUTPUTS_DIR` (or a disposable directory
under `/tmp/plug-swarm/work` when the variable is absent). Command receipts
are flushed after each command, including failures. Executable copies and
stores stay in separate disposable directories and are removed at test end.
The test also compares an independent JSONL projection with an independent archive
projection. The two independent RunsOn packets must additionally match. Store identifiers and
timestamps are excluded only by the declared projection fields in
`testdata/native-public-recording-fixture.json`. Dependency endpoints/types, the
parent edge, and comment contents/multiplicity must survive import exactly;
equal row counts do not establish preservation. The separate manual
`recorder_validation_test` contains authored corruption controls for those
assertions and does not invoke Beads or qualify a behavior card. This control
target is now registered in ordinary `//...` and includes real three-engine
restore/service/subscriber measurement using an explicitly synthetic archive.

When native capture is available, each actual archive is replayed through the
real Plug restore and reverse layers in fresh memory, SQLite and PostgreSQL
stores, including both SQL reopens. PostgreSQL is an owned disposable loopback
engine; an external DSN or missing-engine skip is not accepted. Ordinary JSONL import keeps its original user-memory
expectation. Reverse output must contain those exact user memories plus exactly
the independently expected `plug.config/<sourceKey>` string memories, with
matching `ConfigReferences` and `ConfigAsMemories`. No reserved prefix is
discarded: an unrelated user memory with that prefix still has to survive, and
a key collision refuses the expectation. The replay is an additional connected
check; it does not convert a producer refusal or a missing hosted raw-output
upload into a pass.

After each replay, real public `service.UpdateWithOutcome` requests patch the
native-origin root and child, then explicitly replace labels. A production
Writer post subscriber records the committed revision, before/after changes,
and exact pinned-snapshot readback. Assertions preserve every unpatched field,
metadata, parent, dependency, comment and close provenance. Request replay and
equal replacement must not publish another post or change state. These are
subscribers on the subsequent public writes; offline restore itself makes no
subscriber claim. The authored measurement control exercises the same observer
on all three stores without invoking or impersonating a native producer.

The recorder retains two additional genuine `config list --json` readbacks,
after setup/export for each store, and writes separate config expectation
artifacts. The public corpus declares public settings; the source's setup key
must still match the earlier set/list observation. Every returned stored
setting and ordinary JSONL user memory contributes an exact source-table/key,
body/value/classification and policy assertion, independently of archive and
reverse output. Missing, extra, duplicate or substituted archive records fail,
as do policy discrepancies and duplicate JSONL memory keys. Config list excludes
the KV plane; the user-memory records come from JSONL. Any other filtered KV or
metadata entry needs an independent admitted observation and fails otherwise.
The synthetic controls explicitly enumerate a structured secret reference to
check its canonical string and source identity; no secret is resolved.

The `TestPublicProjection` controls include same-count substitutions, missing
or extra config, duplicate archive IDs/memory keys, reserved collisions,
classification/source/value changes, missing config source with unchanged
relational output, malformed readbacks, and incorrect reverse report entries.
Their authored observations and output rows are measurement inputs, not public
recordings. These controls add no native delivery or genuine public capture.

The historical `682ca798` companion has JSONL and `plug-dump/1` export but no
native `beads-inventory/1` capture. Historical focused r8/r9 remain RED at
inventory capture; two complete new recordings and paired archive projections
have not been produced. A separately delivered, reviewed native producer is
required before that boundary can execute. No fixture inventory substitutes
for it. The existing remote workflow retains undeclared outputs and original
output archives, but actual new recording bytes and receipts are still required.

The consumer composes the producer-owned `internal/publicnative` helper from
source `4daecb4457c92e9d2f6b08629bbd4073c6c01b22` without modifying its bytes.
`StartDolt` returns the owned loopback port, PID and real data root in process;
`ChildEnv` binds every Beads child to that root, and `RequireDatabaseDir`
checks the actual database directory created by init. The target declares the
pinned Dolt runfile, and the helper retains its startup and cleanup receipts.
There is no outer endpoint argument or stand-in database directory.

Native recording remains **BLOCKED** pending independent helper review and
actual lifecycle proof, trusted preparer/admission/control adoption, actual
RunsOn preparation, and the B01 pair. Source composition supplies none of those
runtime receipts. The original `1a52615d` checkpoint's pending-helper refusal
and RED synthetic-input gate remain immutable. Its causal control repair changes
only the detached synthetic manifest's allocator mode/width; all source record
bytes and the original fixture remain intact. That input carries no native credit.

Run it only on RunsOn after preparing the public bundle descriptor:

```sh
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
/tmp/plug-swarm/bin/remote-bazel "$PWD" SEALED_CONSUMER_SHA s1-b25-b27-native-recording1 -- \
  test //spec/s1-b25-b28:public_recording_test \
  --test_env=PLUG_PUBLIC_BUNDLE=41610284344:13218208758:4aa497bb1dfb70bf85371daa9e388b5b0ae84130f83f45a49816559d4698c0db \
  --test_arg=-public-recording-id=recording1 \
  --cache_test_results=no --flaky_test_attempts=1 --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_output=all --test_timeout=900,900,900,9000 --jobs=2 --local_resources=memory=8192
```

The second independent RunsOn invocation uses the same sealed source and flags,
label `s1-b25-b27-native-recording2`, and `-public-recording-id=recording2`.
Both invocations remain planned until the preparation, helper-runtime and B01
prerequisites are met. The manual target selects only `TestPublicNativeRecording`;
synthetic controls run in `recorder_validation_test`. Individual successful runs
still require full raw/XML/undeclared outputs and independent cross-run comparison.

The recorder is evidence for the pinned public head-state/configuration and
relational import/export boundary. It does not turn the explicit B26
unqualified boundary, production operation, or release into
a passing claim.

B26 is a typed unqualified boundary, not a successful substitute. No admitted
public Plug producer currently creates an ephemeral-plane record, so this slice
does not use a memory row or a successful durable bead as a stand-in for
promotion. Instead, a source archive whose real `storage_class` is
`"ephemeral"` reaches `dumpformat.Read` and is explicitly refused with
`unsupported storage class`. The decision and limits are retained in
`b26-deviation.json`; promotion, mixed-plane dependencies, and promotion
rollback remain dependent on an adopted producer contract.

The package is an acceptance slice, not a full-card parity certificate. Direct
B25/B28 three-store cases and B27 hierarchy regressions do not replace genuine
pinned reference pairs. B26 positive producer evidence and complete B27
configuration/auxiliary source coverage remain outside this proof. Run it through the
RunsOn wrapper, never local Bazel:

```sh
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD s1-b25-b28-focused -- test \\
  //spec/s1-b25-b28:acceptance_test \\
  //internal/core/pipeline:pipeline_test \\
  //internal/integration/postsql:postsql_test \\
  --test_output=errors --test_arg=-test.v
```

The B27 converter now lowers an admitted source parent-child edge into the
core Parent field, retains the source edge mirror, and constructs canonical
ancestor pair rows used by ordinary writes. Its independent regression inputs
exercise a three-level parent tree, SQL reopen, a real reparent, reverse
refusal after a parent changes, and prewrite rejection of multiple parents,
missing parents and scheduling conflicts on memory, SQLite and PostgreSQL.
Those authored inputs are not Beads recordings. The legacy entry point keeps
its existing parent-edge refusal; the admitted import and inspection paths
carry the new lowering. Prior raw captures and all RED receipts are retained.

`TestB27ConfigUnionStoredOntologyAndReopenAcrossDrivers` authors a detached
regression archive containing independent stored, YAML and normalized-table
custom status/type declarations. It enumerates the expected installed union,
checks complete raw source bodies and retained member/tar bytes, and checks
actual stored ontology and derived readiness across memory, SQLite and
PostgreSQL, including both SQL reopens. Its mapped-conflict and mapped-missing-
category cases retain the original declarations. These are source assertions;
they have not been executed on this candidate and are not native/public capture.

`TestB27ConfigUnionRefusalHasNoWriteHistoryReceiptOrPublicationAcrossDrivers`
authors conflicting, incomplete, malformed and ambiguous inputs. It requires
an exact named typed refusal before `Store.Write`, unchanged records, versions,
changesets/events and derived rows, no receipt or archive blob, and the same
absence after SQL reopen. This covers the durable publication boundary; it
does not observe an external subscriber or transport. Core parser controls are
`TestStrictConfigUnion*` in `internal/core/dumpformat`.

`TestB27ConfigUnionHistoryRequiresExplicitSnapshotAcrossDrivers` requires
default/full and partial imports of the strict archive's unavailable history
to refuse before `Store.Write`, with unchanged durable state and no receipt,
log/events or archive blob, including after SQL reopen. Successful cases above
explicitly select snapshot. These assertions have not been executed; no head
configuration is substituted into source-history frames.

The strict migration uses a distinct `beads-compat-union-head/1` derived image
profile and installs one compiled document for aliases and derived predicates.
The existing reverse profile guard refuses it, with no partial output or store
mutation. Custom strict reverse/history, multi-project ownership, authentic
producer parsing/delivery, and production ontology resolver refresh require
separate composition/review. Companion DB-first runtime, native restore and
allocator profiles, pins/preparer, B26 and auxiliary writers remain outside
this source change. Ordinary/control gates and a fresh independent source
review require S1's separate candidate binding and release.
