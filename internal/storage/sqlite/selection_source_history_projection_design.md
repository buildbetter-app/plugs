# Proposed shared thin projection: dependency review only

Done = preserve all derived boundary, staged and historical fact-source
observations while removing duplicate physical metadata publication, proven
by existing controls, causal controls and a matching W7 product/census pair;
a schema sketch or fewer rows does not count. This is a proposal, not an
authorized schema, query or publication edit. The eager correctness baseline
and every Ready/Selection logical row remain required.

Add nullable `derived_source_present` and `derived_source_digest` columns to
the existing `key_history` physical row. New derived publications eagerly fill
both columns from the same staged LockState whose JSON is stored: presence
is independent of nil/empty Value, and digest covers exactly Value when
present. The raw SQL key and fixed-width SQL rev already supply lossless
identity and last-mutation revision. No separate derived source-history INSERT
is needed. Record history remains unchanged. This shares the existing physical
derived representation; it does not narrow which sources can bind.

The metadata-only derived read selects `rev,derived_source_present,
derived_source_digest` from the last key_history row at the requested pin.
It never selects/decodes `data`. The existing same-token overlay continues to
replace presence/digest while retaining the pinned mutation token. Pure-reader
boundaries and committed fact envelopes use the same metadata query. Explicit
present=0 retains a tombstone's actual revision; a latest projected row cannot
be confused with missing history. Invalid/partial column pairs refuse.

For an additive transition, old rows keep both columns NULL and the existing
thin-history intervals. A NULL pair selects the existing thin-history lookup
at the same pin; it never proves absence or authorizes a body decode. If no
key_history row exists, that same legacy thin lookup preserves the prior
never-present behavior. New writes populate metadata atomically and close old
open thin intervals at the new commit revision without deleting historical
ones. Old pins retain those intervals. Cold unchanged legacy sources continue
using legacy thin state until their next mutation. No startup history scan,
JSON backfill, retention-floor movement or migration of fact boundaries is
needed. NULL metadata identifies legacy physical rows, not permission to run
old selector binaries after new sparse physical publication begins.

Open adds the two columns under its existing immediate transaction/OS lock;
new-store DDL includes them. This proposal requires a schema-version gate
(2 to 3) in that same transaction. Keeping version 2 would let old binaries
ignore the new metadata and misread absent thin rows. Old pinned readers retain
their pre-upgrade SQL snapshots and thin rows; old new-read/Commit paths must
refuse the new schema before observing or publishing it. The existing readMeta
version check supplies that boundary, subject to explicit causal controls.
If a version gate is outside the authorized schema contract, keep dual eager
publication and do not claim reduced writes. All key-history writers must
cover derived states, including offline history's scalar publishKey route.
Ready's existing per-row triggers remain on key_history; none is disabled.
Caller fact invalidation,
lease fencing, replay, Parent/family completeness and admission remain exact.

Frozen 6b4 direct dependencies (all paths under internal/storage/sqlite):

| Path / exact seam | Preimage blob | Ownership / requested change |
| --- | --- | --- |
| schema.go, schemaVersion and key_history DDL | ca74b65d2abd7fe2e429f374d16996110d4dbc9b | Unleased; root must assign schema owner; version gate and nullable physical metadata |
| store.go, Driver.Open init/migration | 407d6044b31b18f1f8ee8a2a7af5605fa9563850 | Unleased; root/schema owner; atomic 1/2-to-3 migration and column addition |
| key_publication_batch.go, keyPublicationBatch.add/flush and publishDerivedKeys | 6b875bc14c93d198762f911c18fdca874e5dfc9d | F6/shared source, not author-leased; add metadata to the same native insert, preserve 128 cap and error ordering |
| transaction.go, publishKey only (717-723) | 24ed8b0ddd8448470d30a20508e9270ace259c3e | Explicitly unleased; scalar derived metadata for offline-history route |
| selection_query.go, selectionSourceState only (32-98) | 8e16afca87b3a4ac2fcdf7ab2582ef9c151a2d1f | F6/shared, read-only for this author; metadata-only lookup plus legacy thin transition |
| selection_publication_batch.go, publisher only | b2b055da7f31588667c4d6e3041a4e40e42ac05e | Existing author lease; retain record inserts and derived legacy closes |
| BUILD.bazel | c3c2ce74988c8e0092fbc488af98f5bd3beb79b5 | F6 only; new helper/control registration |

Read-only dependencies: offline_history.go blob
`4e2a648be7a4d9d795f63f3375b9bdfbc934bb04` calls publishKey for derived states;
ready_certificate_schema.go blob `4851d38c8e948f535b01a6454e606e5a37414fc4`
already fences every key_history mutation; selection_backfill_rows.go blob
`120eaf0309aedcbc677a05b999115ae369ae5706` retains record thin backfill;
selection.go blob `af1fc5be52cea0fe5373b40b20b9aceb9bab6d99` retains admission
and all three consumers. No change to those read-only functions is proposed.
F6 must bind any integration to its actual preimages, not overlay full files.

Cost hypothesis: each already-required derived key_history INSERT gains two
small bindings; separate derived thin INSERTs disappear. Reads of new derived
states use one indexed metadata lookup; legacy states may require a second
thin lookup. Ready trigger work on key_history persists. This is neither an
exclusive timing claim nor a 2-second cure. Required checks include new/legacy
histories and old-binary refusal, pins around migration/mutation/deletion, raw non-UTF8 keys,
null/empty values, malformed pairs, rollback between publication arms, restore,
replay, floor/incarnation/branch refusals and unchanged Parent/family proofs.
Only a genuinely corrected scoped candidate should receive the focused/build
gates and W7's matching census, with all seven prescribed flags.
