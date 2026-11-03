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

The retained `vectors.json` and root `provenance.json` are the earlier
synthetic/replay boundary and remain unchanged. The `reference/` directory is
the separate historical pinned-Beads recording. Its version 1 files are retained
byte for byte. Their `rawDigest` / `rawSha256` values hash normalized decoded
transcripts; exact stdout/stderr bytes, clean-checkout enforcement and credential
isolation were not retained or proven. The version 1 recorder is preserved in Git
at `aedeca725:spec/s1-b09-b12/record_reference.py` (SHA-256
`3a979c3b1bb5e1e45643adf1e13ebf675d56c5549fdc9b6fa0962f5594251936`).
The old B09–B11 disposition links to `work.md` are historical errors; all four
canonical cards are in `engdocs/design/plug/spec/behaviors/content.md`.

The repaired `record_reference.py` emits version 2 only. It rejects tracked or
untracked source changes before evidence hashing, reads pinned Git blobs, and
runs in fresh disposable Git-backed stores with private HOME/XDG directories,
fixed Git identity/configuration and an explicit environment. Ambient tokens,
credential/config paths and SSH agents are excluded. No private or production
store is accessed.

Each version 2 command retains exact stdout/stderr bytes as base64 and per-stream
SHA-256 values. `rawStreamsSha256` hashes the canonical lossless stream envelope;
`normalizedTranscriptSha256` separately hashes normalized decoded commands and
observations, and `projectionSha256` hashes the semantic projection. The same
names are used in each run and its summaries. `equality` reports raw byte
equality per command and overall independently from normalized transcript and
projection equality. Runtime timestamps and generated IDs may make raw bytes
differ even when the normalized transcripts agree; that result is retained.

Fresh version 2 recordings remain pending: this repair is source-only under the
controller's retained-collector/no-new-gate instruction. The Go tests retain the
version 1 evidence boundary and validate exact streams when version 2 artifacts
are admitted. `TestS1ReferenceRecorderContract` runs recorder-policy regressions;
its temporary Git repositories and mocked command streams are unit fixtures,
not source-engine or paired-store acceptance evidence.

The recordings are evidence for the selected B09–B12 cases, not full-card
qualification. B12 records the embedded reference's honest `--apply` and
restore refusals; Plug's adopted permanent diff history and `history.revert`
contract remains a documented deviation until a producer and replay proof are
admitted.

B12 remains explicitly `full-card-unqualified`: this head has no admitted
public Plug compaction producer or reversible-snapshot replay. Its accepted
contract still requires eligibility, snapshot-before-overwrite, permanent
history, restore and controlled Go/SQL clock boundaries before qualification.

The package does not alter the seven retained reference slices or their
goldens. It is a focused implementation/replay harness, not a census or a
substitute for source-built Beads evidence.
