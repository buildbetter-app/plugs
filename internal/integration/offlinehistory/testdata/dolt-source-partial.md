This archive is fixed public synthetic data emitted by the Beads SQLSource
exporter on a real pinned Dolt engine. It contains no private ledger data.

Producer commit: 33890d8da19a4f4a88de74d4939cd2fced0161da.
Producer tree: 8318d54173572c4665d34556ef2c93fe8db0c7a6.
RunsOn run: 41334286284 (actual source test, race enabled, exit 0).
Archive size: 87,552 bytes.
Archive SHA256: b56f1de4a36c22cca379647d5fe1b2962e4358d9976923e76007ea84b7712582.

The source has three actual Dolt commits: empty genesis, complete supported
schema and baseline, then a title update with an additional source event. The
genesis cannot be mapped. Only source sequence 3 is a verified replay unit,
with complete sequence-2 baseline and exact gaps 1 (unmappable) and 2
(not-retained). Default/full import refuses. Explicit partial replays the
baseline and update with a floor; explicit snapshot imports current state.
This is proof of a bounded source range, not complete source edit history.

Every source image carries these opaque issue columns: agent_state, await_id,
await_type, ephemeral, event_kind, hook_bead, is_template, mol_type, payload,
role_bead, role_type, target, timeout_ns, waiters, wisp_type and work_type.
The fixture includes NUL text, JSON null and integer 9007199254740993.
Consumer tests check beads.<field> JSON bytes on memory, SQLite and Postgres,
source archive member bytes, as-of images, versions, source/import mapping,
floor/refusal atomicity and actual restore authority. Source actors remain
provenance. Live reverse conversion and Beads reimport are separate checks.

The tar is the exact PUBLIC_SOURCE_ARCHIVE_BASE64 output from the fixed public
TestSQLSourceHistoryReadsPinnedDoltCommitImages fixture in the producer run's
test.log. It was not reconstructed or edited by the consumer. Dolt commit IDs
include source metadata, so a new run may produce different bytes; changing
this archive requires a new exact producer receipt and digest.
