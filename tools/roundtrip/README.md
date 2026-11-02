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
interpreted as chronology. Neither a synthetic full fixture nor the genuine
public partial fixture proves complete-root history. The output records source
range/gaps, target floor/incarnation and the reverse reader's as-of revision.
`--exercise=false` checks the unmodified imported head.

Reverse alone ends in `WAIT` with `beads_reimport=WAIT` and
`beads_readback=WAIT`, exit 2. Real reimport additionally requires
`--beads-binary`, `--beads-sha256`, `--beads-build-run` and `--beads-inventory`.
The artifact's hosted build receipt and digest must be independently verified
before invoking the harness; a supplied run ID is provenance to record, not
independent attestation. The supported executable pin is Beads
`a8a2a629e6f774928b190f3b9751d6bf95f21ea2`, tree
`8471e1477993f2a9c9de7e3b7b5cbe195afc765d`. Its reported runtime commit and
actual binary hash must match. The archive's `beads_reference` is the schema
reference, not executable provenance. No build, download, workflow dispatch,
retry or PATH-selected `bd` is performed.

The real executable runs `version --json`, then `init --prefix ... --skip-hooks
--skip-agents --non-interactive --quiet` in a fresh isolated Beads workspace,
then `import reverse-live.jsonl --json`. A separate process runs the supported
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
