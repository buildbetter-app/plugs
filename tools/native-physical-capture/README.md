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

Historical images produced by the physical reader additionally carry optional
`images[].schema_profile` with format `beads-commit-schema/1` and scope
`immutable-commit-root`. The numeric main and ignored versions come from that
exact immutable root. A null main version requires a complete authenticated
empty root or an initial root containing only `dolt_ignore` with columns
`ignored` and `pattern`, no Beads tables and no ignored cursor. No version zero
is inferred. A null ignored version means the cursor table is physically absent.
Every present cursor authenticates its entire pinned migration-hash prefix.
Supported historical cursors are the pinned main prefixes 0–72 and ignored
prefixes 0–28, independently when physically present. No main/ignored pairing
or historical working-set schema is inferred from HEAD.

The profile's ordered `absences` name missing public tables or columns in that
root's complete physical census, with reason `absent-by-source-schema`. The
comparison census includes pinned current surfaces and explicitly named removed
columns. It does not impose today's schema on old images: every observed old,
dropped or reshaped table/column/cell remains present exactly as read. Ignored
families absent from immutable history are not promoted into versioned history.
All images use the annotation together, or all retain the legacy wire. The
physical-evidence/1 sidecar binds the identical profile beside each image's
original SQL types and tagged cells. SQL defaults, indexes and constraints
remain not captured.

Failures additionally report `innermost_go_error_type` (including unfamiliar
code-defined type names), observed `commit_schema_version` and one-based
`commit_ordinal_from_head` when known. The ordinal is the reverse of the
canonical parent-before-child native commit order, with HEAD at 1; merge
ancestry uses that same deterministic order. The innermost type is the deepest
error-tree leaf, with first unwrap child breaking equal-depth ties. Existing
numeric `sql_dolt_codes` and safe fields remain unchanged. Missing context is
omitted, never filled from the current schema observation. Unknown prefixes
produce a typed historical-schema refusal before any archive publication.

`TestPublicPerCommitOwnVersionAcrossPinnedMigrationHistory` constructs a real
pinned-reference history at main 39/40/41/43/54/55/61/62/63/64/66, including
migration-internal commits, dependency reshaping, removed lease columns and
the versioned-to-ignored events transition. It compares every original typed
image and current root after real reopen, and proves an actual historical73
refusal with HEAD still66. Existing same-version66/26 and72/28 assertions
remain separate and unchanged.
