# Restore-only physical snapshot seam

This Plug composition carries the writer from checkpoint `164004ee` onto main
`2902006be`. The immutable capture is the original `347f269f` implementation;
the distinct complete working-root capture is the producer's `f7919ad5` port.
The embedded driver and engine versions retain the checkpoint's exact pins.

The executable is `//tools/native-physical-restore:native-physical-restore`.
Native tests declare the pinned Dolt 2.2.0 archive as their own data. The
version check is `//tools/bazel:dolt_version_test` and fails if the declared
binary is missing or reports another version.

The tests use narrow `testonly` native schema and normalized read-oracle
packages. Their numbered SQL corpus and fresh CLI substitutions retain the
checkpoint bytes. The schema fixture applies those inputs only to disposable
empty databases; it provides no production schema upgrades or repair behavior.
The normalized reference is used only for independent test comparisons. Both
production writer inputs retain the original SQL types and owned driver cells.
The native root, working-root and writer assertions are retained in full.
The legacy exporter CLI and its separate capture suite are not part of this
composition or its runtime proof.

Targets linking the native driver compile with `gms_pure_go`. Their library
closure uses `manual` so wildcard builds compile those libraries through the
tagged tests and binaries; all tests, binaries and nogo checks remain enabled.

`physicalrestore.Restore(ctx, dedicatedIdleConn, manifest)` inserts a complete
physical row set into existing empty tables through normal native Dolt SQL.
It does not call `types.Issue`, create/update/import commands, migrations or
logical reverse conversion. No existing command semantics change.

The `beads-physical-restore/1` envelope requires `history_mode: snapshot`, exact
table/column names and SQL types, and a cell for every column in every row.
Cells have an explicit `type` and textual `data`: null, string, bytes (base64),
bool, time (RFC3339Nano), signed/unsigned integer widths, and finite float32/64.
Integer data uses decimal strings solely as the transport for typed scalars;
metadata JSON remains JSON in the destination, with the same keys and numbers.
Byte buffers are owned before entering the driver transaction. Unknown kinds,
missing columns, duplicate names, incompatible schemas and occupied targets
refuse. SQL constraints remain active. No DDL or UPSERT is performed.

`FromImage` accepts the original `physicalcapture.Image` with SQL types and
owned detached cells. The existing `NativeTable` archive does not carry enough
type information to construct this envelope; callers must bind an independently
captured same-source schema. Do not infer types from JSON shapes or metadata.
The reverse converter's interim physical guards remain mandatory until its
owner supplies and verifies this separately bound route.

The driver validates all expressions before the first insert, uses one SQL
transaction, reads the actual physical working root independently of expected
values, verifies every requested cell/type/multiplicity and every nonrequested
table, and only then commits. A mismatch, insertion error or cancellation rolls
back. A commit error is named `commit-indeterminate` and a failed rollback is
`rollback-indeterminate`; both retain the complete error tree and require
destination inspection before any retry. A cell mismatch names its physical
column and zero-based requested row. Native cell conversion errors name the
actual physical field and readback row; row/partition failures use `*` for the
unavailable field instead of inventing one. The restore-only reader reuses the
unchanged driver cell conversion. Ordinary JSON string binding is unchanged.

The optional `tools/native-physical-restore` executable requires explicit
`--disposable-target`, owner-only canonical paths, a manifest and a fresh
`--readback-output`. It creates a new snapshot commit and reads its immutable
physical root through the existing capture port. Its output contains actual
observed cells. It does not replay or claim source history.

## Completion contract and native oracle

The native GMS dependency, its JSON semantics, driver reader and capture oracle
remain unchanged under binding amendment 8376f857. Every requested cell is
compared with actual physical readback before acceptance. Beads-origin cells
roundtrip against the values actually captured from that engine. For a newly
created value that native cannot retain exactly, the required native result is
a typed `physical-readback` refusal naming table, field and requested row, with
all writes rolled back. A refusal never counts as a successful exact write.

`ExactMetadataBoundary` records ordinary native SQL binding, native SQL JSON
constructors and immutable physical-root output for the same >2^53 fixture. The
recorded native result determines the expected writer outcome. It separately
requires a successful native-origin roundtrip and either an exact requested
readback or the matching typed refusal/rollback. No hand-written rounded number
serves as the engine oracle, and no GMS patch/replacement is used.

The original typed capture port distinguishes nil SQL NULL from string `null`
JSON null. The writer preserves them with distinct tagged cells, as well as
nested JSON null and empty strings. Only the later normalized `NativeTable`
representation collapses this distinction and omits SQL types. It cannot by
itself supply a writer manifest. Actual native public controls prove both NULL
cases; no original reader or exporter assertion changes.

Nonlocal declarations matching a destination, reserved Dolt tables, schema DDL,
nondefault schemas, native source-history replay, and exact schema reconstruction
from the names-only native archive are unsupported. Full/strict history requests
refuse; the tool never silently selects snapshot mode for them. Commit ancestry
and source history cannot be claimed from the tool's new snapshot commit.

Public fixture proof earns no real private-store reversal, cutover, official
producer, trial/night, FullS1 or portable history qualification.

## Undo on a preserved-store copy

The separate `beads-physical-undo/1` request requires `history_mode: switch-back`.
`BindWorkingSource` binds both the original immutable347 capture and the distinct
complete typed working-root capture. The latter includes ignored current tables;
neither port may be inferred from normalized `NativeTable` rows. Every physical
user table needs explicit replacement rows, including empty tables. The driver
authenticates the original database, HEAD, working root, schemas and cells before
deletion, retains reserved tables, and uses one transaction for child-first
deletion, parent-first insertion and every-cell readback. One native switch-back
commit advances the active branch; original commit metadata, parents, other
branches and tags must remain intact. Strict history replay is refused.

The command's `--undo-preserved-copy` mode requires a closed preserved source and
an absent disposable target. It hashes and copies the original files, performs
the undo, closes and reopens the target, then verifies the actual graph, immutable
versioned cells and complete working cells. Any failure after commit is explicitly
indeterminate and retains all causes, including close and publication failures.
Precommit refusal or cancellation rolls back all user-table changes.
Every target, readback, loss and receipt path must be disjoint from the preserved
source; overlap refuses before copying or opening the destination engine.

`reverse_report` carries the existing converter Report without reclassification.
Optional `reverse_report_bytes` carries its original file bytes, including file
whitespace, through JSON transport; when both are present their compact JSON must
agree exactly. This byte field is only for the loss file, never metadata cells.
`UndoReportBytes` returns owned original bytes. The loss output preserves them and
the undo receipt binds their SHA256. Native public fixtures test this transport;
an actual Plug snapshot/project/source-plane binding and measured post-switch
changeset/checkpoint census must still come from their owners. Explanation
strings and historical fixture counts cannot supply that missing census.

## Native row/meta readback and undo

`ReadNativeRowTables` consumes a caller-owned fixed `driver.Reader` snapshot.
It enumerates unfiltered `Query` pages through the terminal cursor, selects both
native Ref namespaces before validating Kind, and rejects missing, unexpected,
duplicate or malformed records. `NativeTableBinding` supplies the independently
bound complete group/Ref census, identity kind and original table-envelope
SHA256; D3 groups also require the original typed table commitment. The landed
`dumpformat` codec reconstructs each full table and checks its meta, canonical
ordering and duplicate occurrences. `TableBodies` returns detached original
table-envelope bytes. Caller authentication of the archive, mapping and source
revision remains separate from these structural checks.

Both `v1-content-bag` and `d3-physical` reconstruct. V1 does not require a typed
witness and never infers primary keys. `PrepareNativeRowUndo` is a separate
physical-write boundary: it retains the original immutable347 and complete
workingf791 bindings and a privately minted `ReadNativeRowSchema` witness. That
reader authenticates the original database/HEAD/working root and complete typed
binding before and after collecting native PRIMARY index metadata in an explicit
read-only transaction. It includes ignored and reserved tables. Preparation
compares the full D3 SQL-column/type/PRIMARY-order descriptor before projecting
to writer columns; absent or differently bound witnesses refuse. It requires D3
typed current cells, the original table census/families and unchanged reserved
view-schema evidence. Valid re-encoded PRIMARY addition/removal/reorder controls
exercise this boundary separately from codec corruption controls.
Missing typed values, schema/DDL/view intent and unavailable secret references
refuse before the normal `Undo` call. SQL NULL, JSON `null`, integer widths,
float bits and binary cells retain explicit tags. No CLI input format changes.
The consumer passes the resulting request to existing `Undo`, then closes and
reopens the copied store and calls existing `VerifyUndo`.

`CanonicalPhysicalBytes` implements contract R2's SQL payload domain: original
manifest struct fields; tables ordered by exact name; columns in SQL ordinal
order with original type text; rows ordered by exact marshaled tagged-cell bytes,
including duplicates. It does not parse or normalize `Cell.Data`. New commit
metadata and nondeterministic raw Dolt container files are separate domains.

`TestNativeRowUndoIndependentPublicNativeBytes` prepares the actual native Beads
schema, nonempty public issues and ignored wisps, binary/NULL/numeric/empty/keyless
controls and two retained source commits. For edited and no-edit cases it first
copies the closed source three times, authors the expected copy with ordinary
native DML, and freezes independently tagged expected user-request, complete
immutable/working and reserved/view bytes before either adapter or writer runs.
The old typed path and the new row/meta path each invoke normal Undo on their own
preserved copy. Both are independently compared to those expected files after a
real close/reopen; original rooted tables, source file inventories, one-commit
receipts and original loss-report bytes remain separate assertions. Expected
serialization does not invoke the writer or reconstruction helpers.

This is a small disposable public-fixture qualification. The shared pure codec
currently supplies no complete reservation API for reconstruction transients and
outputs. Its normalized/D3/envelope allocations have no complete simultaneous
ledger in this adapter; recorded process RSS/HWM is an observation, not proof of
the unchanged 2 GiB decoded-held ceiling. This layer grants no large-fixture,
private-store, full-history, production or admission credit.
