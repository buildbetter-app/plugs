# S1 B17–B20 and I6

This package is the focused S1 recorder and replay slice for metadata CAS,
versions/as-of reads, revert/redact/dump/restore boundaries, and ordered
events. It keeps the retained B17/B18/B19/B20 reference bytes and assertions,
then runs the corresponding Plug behavior against real memory and SQLite
stores, including SQLite close/reopen where the card requires it.

The Beads twins are pinned to `9d6fff760b7a314b476539198504d77ce0959656`.
The B17 test keeps Plug's accepted `setMeta(null)` removal rule as a bounded
deviation from Beads' stored-null projection. B18 records the adopted P6/P7/P30
identity deviation while preserving stable version/as-of meaning. B19 proves
native head snapshots, new-incarnation/floor behavior and populated/full
history refusals; it does not claim production redaction or full-history
replay. B20 proves the retained ordered journal projection and leaves
close-event production atomicity to I4.

The I6 test uses the checked-in public synthetic archive
(`c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c`). It
checks the complete source census and digest, derives a deterministic admitted
snapshot profile, restores into memory and SQLite, exercises `claimNext` and
`tx.apply`, and validates the engine-neutral native `restore.Dump` archive's
kind census and digest. The unchanged source archive is independently read and
converted through `reverseconverter.Read`/`Convert`; IDs, dependency, comment,
memory/config counts, archive rows and the exact output digest are checked.
Native Plug dump records remain a Plug-owned format and are not silently
presented as Beads JSONL. Full source-history replay remains strict by default;
the synthetic fixture's unverified history is admitted only under explicit
snapshot mode, with its archive evidence retained.

The B20 close-event implementation is owned by I4. No private data, source
store, production system or hosted deployment is used by this package.

The paired-store acceptance tests add real memory, SQLite and disposable
PostgreSQL replay without changing the retained reference bytes. They check
B17 canonical CAS and numeric-token refusals, B18's independently projected
create/no-op/update/as-of observations, B19 exact native dump bytes during a
committed interleaving and snapshot restore across all nine engine pairs, and
B20 journal content, paging, rollback and close-reason atomicity. Persistent
stores are closed and reopened. PostgreSQL uses the test-only SQL clock and
verifies actual SQL-generated persisted receipt expiry against the application
clock; SQLite uses its explicit driver clock. Engines are required, never
skipped or replaced by memory.

These tests retain the accepted null-as-deletion and P6/P7/P30 address
deviations. They do not supply new Beads captures, controlled reference-clock
proof, ephemeral-plane parity, the complete event vocabulary/pruning census,
production redaction, full native history restore, or canonical native Beads
inventory/reimport/readback. Those remain separate full-card/I6 requirements.

The fresh B18 selector slice adds input-only `asOf.instant` to `bead.get`,
`history.get`, and `history.query`. It resolves an RFC3339 instant against one
complete pinned changeset snapshot, returns a concrete revision address, and
keeps that address on continuation after an intervening write. Real guarded
point and query paths retain current authorization. Equal commit instants use
revision order; an instant before the first commit resolves to the empty
revision-zero snapshot. A retained-history floor prevents full-log resolution
and returns `history_gone`; an interior hole or missing terminal page refuses.
Resolver fault injection is labeled separately from real-store production
faults. The remote `source_compile` target runs actual Go build/vet and gives
no test-case credit. This slice does not establish B18 JCS/numeric or complete
mutator qualification, a genuine public native producer, full history replay,
or B20 vocabulary/prune/concurrent-writer completeness.

The public B18 metadata-history slice reads both sides of a committed metadata
change through `history.get` and checks literal `history.diff` operations in
memory, SQLite and PostgreSQL, repeating after persistent-store reopen. It
preserves large integer tokens, decimal/exponent and signed-zero spellings,
literal JSON Pointer keys, nested null data, the unchanged sibling, and B17's
accepted null-as-deletion rule. Read-only calls leave the durable row, head,
versions and journal unchanged. This is Plug literal-history acceptance; it
does not establish Beads JCS/numeric admission or native roundtrip fidelity.
