# S1 B09–B12 focused slice

This package owns the narrow allocator and dependency-identity implementation
for B09–B11 and the evidence boundary for B12. The source pin is
`9d6fff760b7a314b476539198504d77ce0959656` with tree
`cde121d6e4119e454857ad1c3ca293e2791be840`.

`replay_test.go` runs the Plug allocator and link paths through real memory and
SQLite drivers. It fixes the Go clock, principal, project, allocator state,
collision inputs, direct-child history and hierarchy depth, then reopens the
SQLite store. The projection helper preserves ordered outcomes and decision
IDs independently from the raw fixture. B11 also carries the pinned UUIDv5
endpoint vectors in `internal/storage/depid`.

The JSON files intentionally do not claim full-card qualification. The
source-vector cases are copied from the pinned source tests; they are not a
Beads executable recording. `record_reference.py` is a fail-closed recorder
wrapper: it requires an exact source-built executable and exact source
commit/tree, runs independent disposable stores twice, and emits nothing when
those prerequisites are absent. No private or production store is accessed.

B12 remains explicitly `full-card-unqualified`: this head has no admitted
public Plug compaction producer or reversible-snapshot replay. Its accepted
contract still requires eligibility, snapshot-before-overwrite, permanent
history, restore and controlled Go/SQL clock boundaries before qualification.

The package does not alter the seven retained reference slices or their
goldens. It is a focused implementation/replay harness, not a census or a
substitute for source-built Beads evidence.
