# Restore-only physical snapshot seam

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
column and zero-based requested row. Ordinary JSON string binding is unchanged.

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
