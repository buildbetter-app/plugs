# Typed physical source ports

`Image` / `ReadCommit` retain the original immutable reader347 contract. An
immutable image does not contain current-only tables that Beads deliberately
leaves unversioned. It cannot establish a complete current-state undo input.

`ReadWorkingRoot(ctx, dedicatedIdleConnection, expectedHead)` returns
`WorkingImage`, format `beads-physical-working-root/1`, from one authenticated
read-only driver transaction. It verifies the exact expected head against the
actual source, records the observed database and working-root hash, enumerates
every stored physical table, and reads those tables locked to that same root.
SQL nonlocal aliases are bypassed. No caller-provided table census or expected
cells influence the read.

`Complete` and `ReadOnlyScope` are usable only on a successful return. Any error,
including rollback failure, returns a zero image. Nondefault database schemas
refuse because this version does not carry qualified table names. Physical
absence means absence from the complete returned census; read errors never
become empty tables.

Each `Table` contains actual `Column.Name` and `Column.DatabaseTypeName` SQL
type strings and detached owned driver cells. SQL NULL is `nil`; JSON null is
the JSON column's string `"null"`; binary buffers are copied. Scalar widths,
keyless duplicate multiplicity and native JSON storage behavior are preserved.
No normalized archive `NativeTable`, name-based type inference, migration,
current-to-history graft, lease filtering or optional-family exclusion occurs.

The database/head/root evidence must be bound by the caller to its original
store/project capture receipt. A root hash identifies captured content; it does
not invent an external project identity or authenticate an unrelated receipt.
The complete working census supplies required current-only families only when
they actually exist. A writer must validate its required table/type set and
same-source binding before mutation, and retain a named refusal for absent input.
History completeness remains an independent contract.

Public remote controls run at
`//internal/storage/physicalcapture:working_root_test`: all thirteen current-only
family names with explicit synthetic schemas and owned SQL NULL / JSON null /
binary / keyless duplicate rows, independently recorded native SQL JSON,
uncommitted working rows, self-alias declarations, missing-head and occupied-scope
refusal, cancellation, source-database identity, plus actual schema66 and current
schema72 with its real ignored-source migrations. Original immutable and native
archive controls remain unchanged and run separately.
