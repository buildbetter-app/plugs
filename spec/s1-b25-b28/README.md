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
`plug-public-native/1`, corrected Beads source `c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724`,
tree `930405e863284a3955cbd35e73a6097b606b91a5`. The separately named
`native-public-profile.json` and `native-public-recording-fixture.json` preserve
the old companion runfiles and the PR295/roundtrip consumer contract.
The target consumes only the trusted prepared artifact through
`PLUG_PUBLIC_BUNDLE`, binds producer run `41800330346`, the exact manifest digest
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

### Additive generated-comment comparison of the frozen nested vector

Done = the two genuine exact57e recordings satisfy a declared semantic pair
comparison with verified generated-comment identity correspondence and exact
within-run preservation, proven by retained265's source and cached-evidence
review; a causal explanation or two individually passing runs does not count.

Scope = the explicit nested workspace with `export.auto=false`, source
`57e9e7f85d03e6dac807a69909bfaae08c4b3401`, tree
`af13af10c7e0aee9d346d1b8e81ede4e2c18fe01`, runs `41798347942` and
`41799011690`, attempt 1, control `fcd03ab3fee9abe0911f651bc258c884e73a7767`,
evidence artifacts `13306336629` and `13306836780`. The strict57e pair
comparison above remains BLOCK because its projected comment IDs differ. This
successor performs a separately attributed offline comparison of retained
copies; those native recordings did not execute this comparison child.

`compare_native_pair.py` requires a SHA-256-bound input manifest with each
original source/tree/run/attempt/control/artifact identity and every consumed
cached asset's path, byte length and digest. It never executes a public binary.
The frozen accepted305 `comments.go`, `derivedid.go` and `rowid.go` authorities
are pinned in the comparer. Each original command return must have the exact
workload owner, author and text, one comment, and a canonical UTC whole-second
timestamp. The comparer independently derives its original ID using the
accepted length-prefix SHA-256 digest and table/ordinal UUIDv5 algorithm. The
one-to-one correspondence retains both full original IDs and times. Neither
value is stripped or replaced by a made-up common identifier.

Each original identity and full content must match both native dumps and all
16 source/target JSONL, archive, reverse, replay and SQL-reopen projections.
All three-engine public-write operation snapshots and post images must retain
their own complete comment content, principal and source timestamp. Model
comments have no native-ID field; exact native-ID preservation is checked in
the dumps and reverse projections, while the unchanged recorder checks whole
subscriber images and replay/no-op invariants. All recorder guards remain
unchanged. Every other declared issue/edge/user-memory projection must match
across runs. Full config source/body/classification, key census, policy and
reverse memories must equal their own independent pre-capture readbacks. The
only permitted cross-run config value differences are the explicitly listed
fresh `_project_id`, `clone_id`, `last_import_time` and `repo_id` metadata;
every actual difference and its two original values is reported. This is no
general timestamp exemption or reserved-prefix discard.
Comparisons preserve JSON types recursively, so a boolean, integer and decimal
cannot substitute for one another. Duplicate keys and nonfinite JSON numbers
refuse before comparison.

`native_pair_comparison_test` checks generated-ID golden vectors and refusals
for wrong IDs/times, changed owner/author/text, missing/extra/duplicate or
ambiguous comments, per-stage substitutions, native source/body changes,
subscriber substitutions, issue/edge/memory differences, missing observations,
wrong policy/stage/run identity and duplicate JSON keys. These ordinary controls
are synthetic and provide no native runtime credit. The actual cached-input
comparison is a separate receipt bound to the comparison child and both57e
runs. Canonical root-layout capture still needs a corrected accepted producer
and its own preparation/recordings; B26, full-card admission and FullS1 remain
unqualified, with FullS1 `0/28` and B05 paused.

### Corrected canonical root-layout continuation

Done = two fresh canonical root-layout recordings preserve the complete B25/B27
workload through actual corrected public capture, replay, reopens and public
writes, proven by their own exact-source/control/preparation/lifecycle/content
packets and retained265 review; the nested57e pair does not count.

The accepted corrected artifact is producer run `41800330346`, artifact
`13307042737`, manifest digest
`ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a`.
The consumer composes exact preparer-author source
`70230ccd92597a96000f90223c5d89b103e25426` admission bytes, with no new
preparer logic. Dc8 retains trusted helper/control adoption and genuine fresh
preparation. Source composition and ordinary gates do not supply that runtime
binding. Every recording still needs its own four receipt-equal exports and
same-run bundle/helper proof; no outer port or fifth field is introduced.

The new input is a canonical Git-root workspace, with explicit supported
`export.auto=false`. The corrected producer alone deduplicates exact repeated
census paths. Distinct paths, strict reader duplicate refusal, full capture,
metadata readbacks, optional-field typing, declared reverse provenance, every
negative/control, workload and all import/reverse/reopen/update guards remain.
No default/comments-only or historical root failure is retrospectively passed.

Before any new physical execution, the canonical pair rule declares exact
agreement on every existing projected issue/edge/user-memory field and complete
config source/body/classification/policy census. Each live comment must retain
its exact original ID/time/content through both stores and every reverse,
replay, reopen and public-write preservation guard. Across two independent live
creations, require unique correspondence by exact owner, author and body with
multiplicity one; independently derive each full original ID from its own
command-return timestamp using the accepted c8eb algorithm. Its three identity
source files are byte-identical to the frozen305 authorities above. Only the
explicit four fresh metadata identity/time values listed above may differ,
each matched to its own independent pre-capture observation. Report every
literal difference and refuse any unexplained, ambiguous or typed substitution.
The fixed57e offline comparison remains its own historical evidence unit and
is not an executable or proof of this future canonical pair. Own new physical
packets, their source-bound pair comparison and retained review remain NOT_RUN.

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
source `43c0f2410ccb24062b8bd1bd073aeb29ef21288f` without modifying its bytes.
`StartDolt` returns the owned loopback port, PID and real data root in process.
Every Beads child consumes returned `ChildEnv` unchanged: `BEADS_DOLT_DATA_DIR`
is the physical data root, while `DOLT_ROOT_PATH` is the separate private
configuration root. `RequireDatabaseDir` checks the actual database directory
created by init. The target declares and expands the same direct pinned Dolt
runfile label. The helper retains startup, version, config and cleanup evidence,
including early startup failures; its ordinary unit tests provide no native
execution credit. There is no outer endpoint argument or stand-in database
directory. The earlier e949 composition of producer4da and both ba387 source
blockers remain immutable historical findings; child dependency bytes and gates
cannot upgrade them.

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
  --test_env=PLUG_PUBLIC_BUNDLE=41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a \
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
