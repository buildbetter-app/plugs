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
