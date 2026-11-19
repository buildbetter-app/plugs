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
The artifact's hosted build receipt and digest must be independently verified
before invoking the harness; a supplied run ID is provenance to record, not
independent attestation. The supported executable pin is Beads
`682ca798dab448803ea9b74f7da3715c21d8c3ae`, tree
`9d026de307ee915953b247cf42758d0c53303574`. Its reported runtime commit and
actual binary hash must match. The archive's `beads_reference` is the schema
reference, not executable provenance. No build, download, workflow dispatch,
retry or PATH-selected `bd` is performed.

The real executable runs `version --json`, then `init --prefix ... --skip-hooks
--skip-agents --non-interactive --quiet` in a fresh isolated Beads workspace,
then `import reverse-live.jsonl --json`. After successful import, missing
`--beads-inventory` returns `WAIT` at `beads_inventory`, reason
`beads_inventory_required`, with `beads_reimport=PASS` and
`beads_readback=WAIT`. Invalid executable provenance and bytes are checked
even when inventory is absent. A separate process runs the supported
raw exporter: `--readonly export --format plug-dump/1 --plug-inventory ...
--output beads-raw-readback.tar`. The inventory must truthfully describe that
new target's actual config/external census; missing schema, source families or
policies refuse through the exporter. No native-head export adapter is added.
Embedded `bd sql` is unsupported at this pin and is not used as a fallback.

A distinct disposable public fixture uses `--beads-capture-public` together
with `--beads-capture-binary`, `--beads-capture-sha256` and
`--beads-capture-build-run`. It preserves the admitted importer above and uses
the separately declared capture-only source
`446dbbdaf978385099aeb8625e08016889fffccf`, tree
`64f0d22c8622c2a20da2bfe8c3ad49458fd12b06`. This component verifies its own
version and captures the actual fresh imported target with `--readonly export
--format beads-inventory/1 --plug-capture-public --output
beads-generated-inventory.json`, then raw-exports that same target. Both
executable identities, hashes, build receipts and command streams remain
separate. This fixture does not supply the admitted importer's missing capture
capability or native admission. It cannot be combined with a supplied
`--beads-inventory`. The default without either option keeps the original
inventory-required WAIT boundary.

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
directory. A rejected raw archive also retains the actual decoder refusal in
`beads-readback-validation-error.txt`; it receives no validated readback digest
or native comparison credit. The raw readback tar contains actual stored fields; a converter
projection, original archive or command construction never substitutes for it.
Only public disposable data is authorized for this lane. No executable artifact
is bundled here, and tests never substitute a successful Beads factory. Without
real pinned executable evidence, actual Beads reimport/readback remains WAIT.

The explicit `//tools/roundtrip:public_beads_runtime_test` target requires a
delivered `plug-public-companion/1` artifact. Its `-public-beads-runtime` flag
is set by the target; missing artifact environment or identity fails the test.
It checks `PLUG_PUBLIC_BEADS_BINARY`, `PLUG_PUBLIC_BEADS_SHA256`,
`PLUG_PUBLIC_BEADS_BUILD_RUN` and `PLUG_PUBLIC_BEADS_MANIFEST` against the
manifest's source pin, canonical exporter target, build identity, executable
size/mode and digest. The production path then verifies the copied bytes and
reported runtime commit before any init/import. The importer is never built by
tests. The distinct capture fixture reuses the single canonical B09 public
source builder and its unchanged declared archive/complete manifest. Its actual
build, validations, executable, toolchain and source provenance are retained
under the current test's output directory.

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
It requires six actual successful subprocesses for each case: importer version,
init and import, then separate capture version, fresh-target public inventory
capture and raw readback export. Every command records its executable digest,
working directory and isolated environment. Each case invokes the independent
raw capture verifier and requires the
existing native field comparison to pass. Original live-workload, provenance,
history and import-count assertions remain. Raw streams, both inventory files,
all captured sidecars, the raw archive and submitted live JSONL are retained in
the test outputs, including on failure. Physical directory observations bind
the inventory store ID and raw archive to the actual imported embedded target;
the configuration policy census must cover every raw stored key. The capture's
literal filesystem atomicity limitation remains unchanged. An additional real missing-inventory
control retains the three-command reimport-PASS/readback-WAIT boundary. These
public fixtures provide no strict two-source history, private-store or official
native-admission qualification.

The separate manual `fresh_same_run_native_test` authenticates the committed
public capture, physical workspace, request templates and independent oracle,
then records a fresh native continuation and invokes the composed same-process
ID consumer. The input archive and provenance sidecar have exact SHA-256
bindings; all 284 original inner paths and bytes are retained. A disposable
extracted copy preserves the unchanged repository-relative request paths for
the existing capture-set reader. The packet remains below 20 MiB. Set
`PLUG_ID_STYLE_A_FRESH_RUN=1` for the three Full cases. The other three Partial
and six Snapshot cases retain named WAIT/SKIP by default. The merged PR481
receiver and shared lifecycle are present. After qualification of the exact
case inputs and actual consumer composition, the additional
`PLUG_ID_STYLE_A_FRESH_MODES=1` selects all twelve in their original modes.
This flag is a test selection input and supplies no landing or equality proof.

Each Partial case explicitly selects source ordinal 2. The independent oracle
authenticates that image and its predecessor while retaining the complete
1..N plus HEAD/current ledger. Full and Snapshot supply no floor. The consumer
binds its native case, mode and ordinal to the actual imported profile, then
uses the shared writer-once and post-write reopen assertion phases. Expected
IDs remain assertions; they never become allocator inputs. Every case needs
its own recipe, retained physical source/archive, capture, native tuple/clock
inputs, actual consumer ELF and readback. Previous case outputs and synthetic
controls supply no fresh equality credit.
