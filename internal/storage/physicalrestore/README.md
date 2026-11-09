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
back. A commit error is named `commit-indeterminate` and requires destination
inspection before any retry. Ordinary JSON string binding is unchanged.

The optional `tools/native-physical-restore` executable requires explicit
`--disposable-target`, owner-only canonical paths, a manifest and a fresh
`--readback-output`. It creates a new snapshot commit and reads its immutable
physical root through the existing capture port. Its output contains actual
observed cells. It does not replay or claim source history.

## Boundaries requiring evidence

The pinned native JSON reader calls `ValueContext`/`ToInterface`, which decodes
JSON using float64 before the producer's `UseNumber`. SQL JSON constructors are
used by this writer to retain integer arguments, but they count as successful
exact metadata restoration only if actual native/root readback retains the
original integer. A typed physical-readback refusal is a blocker, not exact
metadata success. The public `ExactMetadataBoundary` control records either the
successful readback or this explicit blocker and verifies rollback.

The existing capture API conflates SQL NULL and top-level JSON null for JSON
columns. Both ambiguous nil JSON cells and top-level JSON `null` are refused;
nested JSON null values are supported. No existing capture oracle is changed.
Nonlocal declarations matching a destination, reserved Dolt tables, schema DDL,
nondefault schemas, native source-history replay, and exact schema reconstruction
from the names-only native archive are unsupported. Full/strict history requests
refuse; the tool never silently selects snapshot mode for them.

Public fixture proof earns no real private-store reversal, cutover, official
producer, trial/night, FullS1 or portable history qualification.
