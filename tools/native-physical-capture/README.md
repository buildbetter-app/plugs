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

Add `--native-delta` to select the explicit `beads-native-delta/2` format.
It writes `capture-set/parts/`, `capture-set/head.tar`, inventory sidecars and
the sibling `physical-evidence.json`, then publishes
`capture-set/capture-set.json` after closing all
source handles and rechecking the working root, graph and ignore policy.
The output directory must also have no existing `head.tar`, `capture-set.json`
or `parts/`, or a preexisting `capture-set/` directory. The root binds the head
attachment, chained parts and stream hash. The matching receiver takes
`--capture-set /absolute/disposable/output/capture-set`. That directory has
exactly `capture-set.json`, `head.tar` and `parts/`; inventory and evidence are
siblings of the set directory.

Durable dotted children may have parents in the active plane. Capture resolves
their allocation counters across both planes, retaining the original child ID
and the parent's recorded counter even when it exceeds retained child numbers.
A parent absent from both planes refuses with the closed diagnostic
`child_counter_parent_unresolved`; no partial capture is published.

The delta stream retains actual parent order and primary-key order, exact
typed cells, keyless row multiplicity and schemas. Roots and CP11 frontiers
have snapshots; every other immutable commit uses its retained first parent
as the delta base. Independent HEAD and current snapshots close the stream.
Every commit/table has reconstructed row counts and content hashes. There is
no total native wire cap. Part payloads are at most 64 MiB, individual JSON
records including newline at most 128 MiB, and the root at most 1 MiB.
Legacy capture remains the default and keeps its existing archive limits.
The bounded `head.tar` is a current-state attachment; full native verification
requires the authenticated stream and receiver reconstruction. Capture success
does not establish receiver import success.

For the current receiving phase, the receiver verifies reconstruction before
lowering through the existing aggregate history interface. Its configurable
decoded accounting ceiling defaults to 2 GiB and is separate from wire size
and process RSS. A streaming core receiving interface remains required before
the switch; this capture tool does not change core importer APIs.

On success, the tool publishes the validated `capture.tar`, `inventory.json` and
`inventory.json.sources/` sidecars and `physical-evidence.json`, then reports current-family row counts. Retain
the actual manifest and history ranges: complete native physical history does not
upgrade portable history that could not be mapped. Errors retain their typed
driver cause; they do not establish physical-table absence. A refused invocation removes its own unpublished output files and sidecars; any cleanup error remains a failure.

Capture includes source contents and is not a redaction mechanism. Keep private
inputs and outputs local, report only counts/categories, and clean up disposable
copies under the owning capture procedure. This executable changes no stock
`bd` command or Beads behavior.

The source-bound successor reports failures as `beads-capture-diagnostic/1`
JSON. It emits only a fixed stage (`archive-read`, `schema`, `table-scan`,
`history-walk`, `row-decode`, `report`), allowlisted Go error type names, numeric SQL/Dolt codes,
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

Native source-family capture retains populated `federation_peers`, `routes` and
`interactions` as opaque source evidence. It does not activate federation, routes
or recorded interactions. A peer's absent credential stays SQL NULL; an empty
credential stays empty, including its exact empty BLOB representation. A present
credential or digest still refuses the whole capture. The portable export path
keeps its existing refusal for these nonempty families.

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

Failures additionally report an allowlisted `innermost_go_error_type` when known,
the count of omitted unfamiliar error classes, observed `commit_schema_version` and one-based
`commit_ordinal_from_head` when known. The ordinal is the reverse of the
canonical parent-before-child native commit order, with HEAD at 1; merge
ancestry uses that same deterministic order. The innermost type is the deepest
error-tree leaf when allowlisted, with first unwrap child breaking equal-depth ties. Existing
numeric `sql_dolt_codes` come from the original error, before driver translation;
a generic translated fallback is not a source number. Unknown commit schema,
ordinal and verified count are explicit JSON nulls, never filled from the current
schema observation. Error type and numeric-code fields are always arrays, including
empty arrays. Every unavailable-history diagnostic has a closed `reason_subcode`;
unlocalized failures use `other` without admitting a history boundary. Unknown prefixes
produce a typed historical-schema refusal before any archive publication.

`TestPublicPerCommitOwnVersionAcrossPinnedMigrationHistory` constructs a real
pinned-reference history at main 39/40/41/43/54/55/61/62/63/64/66, including
migration-internal commits, dependency reshaping, removed lease columns and
the versioned-to-ignored events transition. It compares every original typed
image and current root after real reopen, and proves an actual historical73
refusal with HEAD still66. Existing same-version66/26 and72/28 assertions
remain separate and unchanged.


A genuine unavailable-history frontier is retained with optional
`history_coverage` in both `history/native-tables.json` and the typed physical
evidence file. The object has format `beads-native-history-coverage/1`, status
`verified-to-boundary`, `verified_commit_count`, `boundaries` and
`events_attachment`. Existing complete captures omit it. A boundary capture
keeps `complete:false`, an empty `gaps` array, readable HEAD and at least one
fully verified immutable image. Every present image, cursor, table and cell
is still verified; corruption, contradictory edges, unsupported schemas and
falsely complete or missing-image claims refuse. Original named-gap captures
retain their earlier semantics and do not acquire boundary admission.

Each boundary retains its actual `commit_id`, sorted unique
`child_commit_ids`, `commit_ordinal_from_head`, optional observed
`commit_schema_version`, closed `reason`, and optional sorted unique nonzero
`sql_dolt_codes` (uint16). Reasons are `ghost_parent`, `missing_root_chunk`,
`missing_table_chunk`, `missing_archive_chunk`, `unreadable_schema` and `other`; arbitrary error text
does not establish any boundary. Each external parent edge is covered exactly
once. Multiple DAG frontiers are supported. Ordinal is one-based reverse
canonical parent-before-child DFS, extending the graph only with frontier IDs
as terminal leaves, with sorted parents. HEAD is 1. No frontier metadata,
parents or image is invented. A linear frontier follows its N verified
commits at ordinal N+1. Missing HEAD still refuses.

`events_attachment` names `history/source.json`, hashes its exact member bytes
with SHA256, and counts the original current events. Portable events, native
current events and archive source rows must agree. Portable history remains
unverified with reason `native-history-boundary`; current events are never
assigned to immutable commits. Historical event tables that physically existed
remain in their own images. SQL defaults, indexes and constraints are still
not captured.

Successful publication emits the original schema observation, then one safe
`beads-capture-diagnostic/1` history-walk record per boundary with
`history_boundary_reason`, `verified_commit_count`, boundary ordinal, schema
where readable and actual numeric codes. The final `published-validated`
summary adds `verified_commit_count` and `history_boundaries`, whose entries
contain only the same safe reason/ordinal/optional schema/code fields. These
records contain no commit IDs, hashes, values, raw error text or paths. The
caller must validate the entire stream and published archive/sidecar together.

The public control uses the pinned `DOLT_CLONE('--depth','3',file_url,name)`
procedure and authenticates the actual ghost parent independently. It checks
every retained physical cell, current events, graph, count, ordinal, exact
events-member hash, safe diagnostic stream and source conservation after
close/reopen. The existing populated multiversion fixture and its historical
73 refusal remain separate required controls. Public fixture PASS alone does
not complete the separately owned two-store policyv2 capture/import unit.

Run `--history-probe-only` with the same disposable/path/environment arguments
as capture, before `--schema-only` or full capture. The modes are mutually
exclusive. It publishes no files and emits exactly one stdout object with
format `beads-history-completeness/1`. Valid genuine absence returns exit 0;
corrupt/inconsistent observations, timeouts, permission and cleanup failures
return nonzero with safe stderr diagnostics. A zero exit does not admit an
archive or prove that every user table/row is readable.

The required probe fields are `reachable_commit_count`,
`reachable_count_is_lower_bound`, `readable_root_count`,
`chunk_complete_commit_count`, `first_unreadable_ordinal`,
`first_unreadable_schema_version`, `shallow_ghost_marker_count`,
`archive_chunk_count`, `loose_chunk_count`, `archive_file_count`,
`loose_file_count`, `reason_subcode`, `go_error_types` and `sql_dolt_codes`.
The graph census includes observed frontier nodes and walks stored parents
independently of root readability. Ghosts or unavailable parent commits make
the reachable count a lower bound; a missing root does not hide its commit's
readable parent edges. Ordinals use reverse canonical DFS on that observed
graph. The first unreadable ordinal is null if no absent root, descendant or
frontier is observed. Its reason and schema version are null in that case;
type/code arrays are empty. Missing roots have no inferred schema version.

A readable root is a present addressed commit root value that decodes. The
probe walks every immutable root's backing reference closure, including table,
primary/secondary index and out-of-line value chunks. Only roots whose entire
closure is present contribute to `chunk_complete_commit_count`. On an incomplete
root, present-sibling validation still refuses malformed data and fixed migration
cursors are read directly to locate its schema version. No `PartitionRows` call
is used at that boundary. Complete probes remain presence observations; full
capture separately verifies row semantics. Storage counts come from the engine's active
`Sources` table-file census: `.darc` entries versus other indexed table-file
entries, with chunk counts summed from `NumChunks`. Appendix entries already
included there are counted once. These are indexed entries, not a count of
unique reachable chunks or every on-disk file. Unsupported/unmeasured layout
counts are null; measured absence is zero. File counts never establish the
cause of missing history or a claim of completeness.

Full immutable reads confirm the authenticated table and schema objects, then
walk the primary-index references against the backing store before row
iteration, including references to
out-of-line values. An empty chunk confirmed absent by the store can establish
`missing_table_chunk`; present empty, mismatched or malformed bytes refuse.
This check reads stored bytes directly, so a shared engine node cache cannot
hide an omitted chunk. Present primary and blob nodes must also have consistent
vector cardinalities, bounded offsets and tuples, subtree totals and child levels;
a different absent sibling cannot hide a malformed present node. Present primary
tuples are also decoded against the immutable map's actual key and value
descriptors before absence is admitted. Fixed-width fields must fit their
authenticated encoding; nullable and truncated nullable suffixes remain valid.
Deferred values read only the already authenticated backing bytes, and present
field errors still refuse when another field or sibling is actually absent.
Present fields also use their immutable SQL column definitions and the existing
reader conversion before an absence boundary is admitted. This validates ENUM
ordinals and SET bits against the actual member dictionary, including stored
zero values; nullable and truncated nullable fields remain supported. Keyless
synthetic fields and virtual columns retain their storage positions, and extended
decoders use the authenticated byte store. Row and
partition cleanup failures are retained with
the original read error and prevent boundary admission. Ordinary SQL driver
conversion and its error surface remain unchanged.

The history reader establishes the first absent ordinal before reading images
and walks only the newer verified suffix. It never calls `PartitionRows` at or
beyond that boundary. A probe-complete root that fails during reading refuses as
typed `PhysicalStoreInconsistent`, with safe subcode `inconsistent_store`, and
stops the walk. Reader-boundary panic recovery retains only the panic's Go type
and preserves cleanup failures. It yields no image or history-boundary credit.
The pinned Dolt implementation and ordinary SQL path are unchanged.

`TestPublicCP11HistoricalPrimaryRowLeafLossRetainsVerifiedSuffix` additionally
removes an authenticated historical primary-map leaf from loose and archive
stores while retaining every root and all newer images. It requires the exact
schema-39 boundary, verified suffix and current events. Its guarded missing-leaf
read must return the original typed absence without an escaped panic. Historical
raw observations from earlier heads remain separate; an eager `PartitionRows`
failure does not prove that raw row `Next` observed missing data.

`TestPublicCP11CompleteChunkProbeReadFailureRefusesWithoutBoundary` persists an
out-of-domain ENUM tuple through the pinned serializers, then restores the valid
current root with a real child commit. Every reachable chunk is present. The
historical read must return the typed inconsistency, and capture must stop with
no history boundary or published files while preserving current data.

`TestPublicCP11ActualChunkRemovalAndDoltArchiveRepack` constructs actual public
multi-commit 39/40/41/61/62/66 history and records its original graph, roots,
typed cells, events and chunk bytes before making disposable variants. It
repackages all original chunks into an actual sole `.darc`, and separately
omits one authenticated historical root from loose and `.darc` stores. It
checks every other chunk byte, current working state and retained immutable
image, the measured probe, exact verified suffix and current events attachment.
Missing HEAD, corrupt present archive bytes and a falsely complete boundary
archive remain refusals. These public controls do not qualify private stores.

## Structured capture diagnostics

The capture executable writes `beads-capture-diagnostic/1` records. It never
prints error messages, source paths, commit hashes or cell contents in these
records. A failed capture retains its original error and exits unsuccessfully.

Every record contains:

| Field | Contract |
| --- | --- |
| `stage` | `history-walk`, `schema`, `table-scan`, `row-decode`, `archive-read` or `report` |
| `reason_subcode` | Null on schema success; otherwise `ghost_parent`, `missing_root_chunk`, `missing_table_chunk`, `missing_archive_chunk`, `unreadable_schema`, `other` or `inconsistent_store` |
| `table` | A table accepted by `plugdump.PublicCaptureIdentifiers`, or null when unobserved or inapplicable |
| `commit_ordinal_from_head` | Observed one-based uint64 ordinal, or null |
| `commit_schema_version` | Observed uint64 immutable-root schema version, or null |
| `verified_commit_count` | Observed uint64 boundary coverage count, or null |
| `go_error_types` | Unique names accepted by `safeGoErrorType` in `diagnostic_contract.go`; always an array |
| `unrecognized_go_error_type_count` | Number of distinct omitted error-class names in the traversed evidence; uint64, including zero |
| `sql_dolt_codes` | Original positive uint16 codes, sorted and deduplicated; always an array |
| `schema_migrations` | Public expected schema version, measured current version when available, and `unmeasured`, `empty`, `single` or `series` row-count class |

`format` is always `beads-capture-diagnostic/1`. Optional `column` is emitted
only with its allowlisted table and column pair. Optional
`innermost_go_error_type` is the actual deepest original class if allowlisted;
an unknown deepest class is omitted, never replaced by a shallower class.
Optional `history_boundary_reason` retains the existing six history-boundary
reasons. `inconsistent_store` is a failure classification and cannot establish
a missing-history boundary.

Source opening and transaction acquisition use `archive-read`; immutable
ancestry uses `history-walk`; schema queries use `schema`; census and table
reads use `table-scan` and `row-decode`; artifact validation uses `archive-read`;
argument, serialization and output failures use `report`. Typed physical
failures refine the stage and table without parsing their messages. Unknown
coordinates remain null, and probe counts never become capture coverage.

The reader omits unknown type names and counts them. The caller independently
owns any `UNRECOGNIZED_CODE` projection for future enum values and must reject
unknown or substituted evidence when qualifying a successful publication.
The fifteen-field history-completeness probe remains a separate report.
