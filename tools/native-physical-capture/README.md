# Disposable native physical capture

This private capture companion reads every retained immutable Dolt commit through
the storage driver's physical-image operation. It preserves each image's own
tables, columns, row multiplicity, NULLs and binary values without resolving
current nonlocal-table aliases. Current working-set rows, inventory, commit
metadata, sidecars and portable history still use the existing capture code.

Build `//tools/native-physical-capture:native-physical-capture` with the repository's
Bazel toolchain. Run the resulting binary only on a quiesced disposable copy:

```sh
cd /absolute/disposable/workspace
BEADS_DIR=/absolute/disposable/workspace/.beads \
  /absolute/path/native-physical-capture \
  --disposable \
  --data-dir=/absolute/disposable/dolt-data \
  --database=exact_database \
  --beads-dir=/absolute/disposable/workspace/.beads \
  --output-dir=/absolute/disposable/output
```

The data, Beads and output directories must already exist, have owner-only
permissions and contain no symlink components. The working directory must be the
copied workspace containing the selected `.beads` directory. Set HOME and XDG
configuration paths to an isolated disposable home before invocation; inventory
captures environment and configuration from those locations. The output directory
must contain none of `inventory.json`, `inventory.json.sources` `capture.tar` or `physical-evidence.json`.
The default deadline is 20 minutes; `--timeout` accepts a Go duration.

On success, the tool publishes the validated `capture.tar`, `inventory.json` and
`inventory.json.sources/` sidecars and `physical-evidence.json`, then reports current-family row counts. Retain
the actual manifest and history ranges: complete native physical history does not
upgrade portable history that could not be mapped. Errors retain their typed
driver cause; they do not establish physical-table absence. A failed invocation
may leave sidecars; discard its disposable output rather than treating it as a
completed capture.

Capture includes source contents and is not a redaction mechanism. Keep private
inputs and outputs local, report only counts/categories, and clean up disposable
copies under the owning capture procedure. This executable changes no stock
`bd` command or Beads behavior.

The source-bound successor reports failures as `beads-capture-diagnostic/1`
JSON. It emits only a fixed stage (`open`, `schema-probe`, `census`,
`history-walk`, `cell-read`), Go error type names, numeric SQL/Dolt codes,
numeric found/expected schema versions and a migration-row count class. Table
and column names pass through the explicit public Beads schema registry.
Unknown identifiers, error messages, cell values, paths and commit references
are omitted. Argument parser errors use this same report.

Before inventory or history, the reader observes the restored database's actual
`schema_migrations` cursor. Add `--schema-only` to the invocation above to emit
that observation on stdout and stop without reading inventory/history or
publishing files. A missing cursor is a typed error, never inferred version zero.
Full capture accepts only authenticated main/ignored prefixes 72/28 or 66/26.
All migration content hashes and the entire current table/column census must
match the selected profile; the main cursor alone never proves wisp absences.
Other versions are observed and typed-refused without migration. The physical image
operation itself reads older immutable schemas independently of that projection.
Any future migration fallback must be explicitly labeled because it adds commits.

The supported public route controls are selected by the canonical whole gate:

```text
//internal/plugdump:physical_root_control_test
//cmd/plug-capture-sql-server:native_capture_test
```

The native target retains the original default full-history comparisons using
standalone physical capture for both default and configured tracked fixtures.
Portable suffixes keep the unchanged SQL image mapper and their earlier gaps.
The clock fixture also verifies the stock command's named capability refusal,
empty output and unchanged source graph. The target rereads every native image
through a separate physical connection and keeps every source/current/status/type
assertion. The external
`@com_github_dolthub_driver_v2//:physical_commit_test` is selected explicitly in
the focus gate. The former combined native label remains an alias.

Stock SQL refuses native physical-root capability before querying; it cannot
turn a redirected declaration census into immutable evidence. Failed AS OF and
revision-database experiments retain their original assertions in explicit manual
`sql_native_reference_test` and `revision_database_reference_test` targets. Their
old RED receipts remain unchanged and do not qualify the supported route.

Run all gates through the authorized remote Bazel wrapper. Public controls prove
their fixture route only; source, focused, whole, real-store delivery and official
qualification remain separate.

For v66, `beads-native-tables/1` includes `schema_profile` with actual
`main_version`, `ignored_version` and ordered `absences` entries. Each entry
names `family`, optional `column`, `introduced_in` and the exact reason
`absent-by-source-schema`. The source-pinned delta comprises three absent tables
(`epoch_minted_addresses`, `issue_versions`, `store_epoch`) and four absent
columns (`current_revision` and `participation_generation` on both `issues` and
`wisps`). No empty table, NULL cell or default is synthesized. Immutable images
retain their original schemas independently. Legacy v72 omits this profile.

The separate `beads-physical-evidence/1` sidecar binds the exact archive SHA256,
store/project tuple, complete typed working-root identity and every retained
immutable commit image. Columns carry their actual SQL type strings; detached
cells carry explicit kind tags and string payloads, preserving SQL NULL versus
JSON-column `"null"`, integer widths, binary bytes and float bits. Native JSON
text is retained exactly as read from the unchanged engine. This artifact is
source evidence, not a names-only type inference or a restore certificate.
`sql_defaults: "not-captured"` explicitly leaves SQL default/index/constraint
measurements unproved. It contains source values and obeys the same private
output handling as the archive.
