This trusted offline integration harness imports a validated Beads archive into
an empty disposable Plug store, exercises claim/release and an idempotent live
edit, and reverses one pinned reader snapshot. It reuses the narrow live reverse,
exact-envelope retention and privacy tests from source
`e4f0aa3831f0cd16c2eb4e316eb95b8fb227303b`; that old review does not certify this
adaptation. No source from its full ancestor bundle is required.

Build/test/runtime qualification uses RunsOn only. The executable target is
`//tools/roundtrip:roundtrip`. Input flags are `--input`, `--output-dir` (a new
resolved directory under `/tmp`), `--history-mode full|partial|snapshot` and
`--engine memory|sqlite|postgres`. PostgreSQL additionally requires
`--postgres-dsn` naming an empty loopback database `plug_roundtrip_*`; destination
overrides refuse. The harness creates SQLite only inside its fresh output
folder. The caller owns creation and disposal of the PostgreSQL database.

Full remains the strict default. Partial requires a verified suffix, baseline,
exact gaps and floor; snapshot is explicit. Source-event identity order is not
interpreted as chronology. Neither a synthetic full fixture nor a genuine
public partial fixture proves complete-root history. The distinct unchanged
990eb800 public complete-root archive proves only its paired public synthetic
SQL source: seven actual Dolt commits, empty root and four real audit events.
It has no portable comment row; a comment audit event does not create one.
The sibling partial archive retains exact gaps 1 not-retained, 2 unmappable,
3 not-retained and never inherits the complete archive's status.
Actual JSON payload.n is 9007199254740992; BIGINT timeout_ns is 9007199254740993.
The converter accepts only exact SQL pinned encodings 0/1 (numeric or textual)
and booleans; textual source encoding is also retained in source metadata.
The output records source range/gaps, target floor/incarnation and the reverse
reader's as-of revision.
`--exercise=false` checks the unmodified imported head.

Reverse alone ends in `WAIT` with `beads_reimport=WAIT` and
`beads_readback=WAIT`, exit 2. Real reimport additionally requires
`--beads-binary`, `--beads-sha256` and `--beads-build-run`.
`--beads-inventory auto` asks that same verified executable to produce
`beads-inventory/1` after the fresh target completes `version`, `init`, and
`import`; the generated census and its sidecars are retained as private output
evidence. An explicit inventory path remains supported for independently
prepared captures. Omitting the flag still ends at `beads_inventory_required`
with `beads_reimport=PASS` and `beads_readback=WAIT`.
The artifact's hosted build receipt and digest must be independently verified
before invoking the harness; a supplied run ID is provenance to record, not
independent attestation. The supported executable pin is Beads
`c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724`, tree
`930405e863284a3955cbd35e73a6097b606b91a5`. Its reported runtime commit and
actual binary hash must match. The archive's `beads_reference` is the schema
reference, not executable provenance. No build, download, workflow dispatch,
retry or PATH-selected `bd` is performed.

After the verified executable reports its expected version, the harness runs
`git init --quiet --template=` in the owned target with its private home and
the same sanitized environment used by Beads. Native inventory therefore
resolves a real Git hooks directory without inheriting user templates or Git
configuration. The initialization command, exit and raw stream hashes are
retained separately from the five Beads command receipts; failure blocks the
roundtrip before Beads initialization.

The real executable runs `version --json`, then `init --prefix ... --skip-hooks
--skip-agents --non-interactive --quiet` in a fresh isolated Beads workspace,
then `import reverse-live.jsonl --json`. With `--beads-inventory auto`, a
fourth real subprocess runs `--readonly export --format beads-inventory/1
--plug-capture-public --output ...` against that new target. After successful
import, missing `--beads-inventory` returns `WAIT` at `beads_inventory`, reason
`beads_inventory_required`, with `beads_reimport=PASS` and
`beads_readback=WAIT`. Invalid executable provenance and bytes are checked
even when inventory is absent. A separate process runs the supported
raw exporter: `--readonly export --format plug-dump/1 --plug-inventory ...
--output beads-raw-readback.tar`. The inventory must truthfully describe that
new target's actual config/external census; missing schema, source families or
policies refuse through the exporter. No native-head export adapter is added.
Embedded `bd sql` is unsupported at this pin and is not used as a fallback.

Independent readback compares raw stored issue fields, metadata, labels,
timestamps, dependency envelopes, comments and memories against the submitted
JSONL. It also checks every original opaque source column separately. Keeping
`beads.<field>` metadata does not prove restoration of the native Beads column;
that difference returns `beads_native_opaque_fields_changed`, with exact field
names in `source_field_losses`. The final PASS, when actually observed, covers
bounded head-state reimport only. JSONL cannot replay source or post-switch
history; `reverse-report.json` retains the exact loss reasons.

Standard output uses bounded field names, counters, digests and fixed codes.
Output directories are 0700 and evidence files are 0600. Subprocess command
arguments, exit codes, stdout/stderr and hashes are retained in the new output
directory. The raw readback tar contains actual stored fields; a converter
projection, original archive or command construction never substitutes for it.
Only public disposable data is authorized for this lane. No executable artifact
is bundled here, and tests never substitute a successful Beads factory. Without
real pinned executable evidence, actual Beads reimport/readback remains WAIT.

The explicit `//tools/roundtrip:public_beads_runtime_test` target requires a
delivered `plug-public-native/1` artifact. Its `-public-beads-runtime` flag
is set by the target; missing artifact environment or identity fails the test.
It checks `PLUG_PUBLIC_BEADS_BINARY`, `PLUG_PUBLIC_BEADS_SHA256`,
`PLUG_PUBLIC_BEADS_BUILD_RUN` and `PLUG_PUBLIC_BEADS_MANIFEST` against the
manifest's source pin, canonical exporter target, build identity, executable
size/mode and digest. The production path then verifies the copied bytes and
reported runtime commit before any init/import. No artifact is built by tests.

The declared native profile and 26-family protocol files in `testdata/` are
unchanged copies from `e44a1edbe1c939b755a1e5e88e33085d084c0414` under
`spec/s1-b25-b28/testdata/`, authored in the separately owned native consumer
lane. Their complete SHA256 pins are checked through Bazel runfiles. The
profile identifies accepted producer run `41800330346`; its manifest digest
is `ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a`.
The old companion profile and historical source fixtures remain unchanged.
The consumer still rejects altered source/version pins and unsupported
profiles, including an explicit companion-profile refusal.

After the trusted preparation workflow has landed, select the runtime target
explicitly through `remote-bazel` and provide exactly one
`--test_env=PLUG_PUBLIC_BUNDLE=RUN_ID:ARTIFACT_ID:MANIFEST_SHA256`. The trusted
workflow independently verifies and extracts the immutable same-repository
artifact before forwarding the four runner-local values. Ordinary `//...`
does not select this manual target; whole-tree qualification that claims
artifact-backed runtime coverage must also name it explicitly.

The runtime target imports the unchanged public complete-root and partial
fixtures in full/partial/snapshot modes across memory, SQLite and disposable
PostgreSQL, with the normal claim/release and live-edit workload. It uses
explicit `--all` export for the partial fixture's three ephemeral beads; the
complete fixture uses default export. The default-omission controls stay separate.
It requires
five actual successful Beads subprocesses for each admitted case when using
`--beads-inventory auto`, including the native census, and retains their raw
output in the test log. The raw exporter then reads that truthful target census
and the runtime compares its archive against the submitted reverse JSONL plus
every native opaque field. A final `PASS` remains bounded to public head-state
reimport/readback; it does not certify source-history replay, private stores,
or production migration.
