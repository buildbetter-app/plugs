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
thin-history intervals. A NULL pair requires an actual covering legacy interval
at the same pin whose source_revision equals the selected SQL key_history rev;
missing legacy history or a token disagreement refuses instead of returning
miss0. The retained row itself proves that this key has a mutation history.
No fallback authorizes a body decode. New writes populate metadata atomically and close old
open thin intervals at the new commit revision without deleting historical
ones. Old pins retain those intervals. Cold unchanged legacy sources continue
using legacy thin state until their next mutation. No startup history scan,
JSON backfill, retention-floor movement or migration of fact boundaries is
needed. NULL metadata identifies legacy physical rows, not permission to run
old selector binaries after new sparse physical publication begins.

| Pinned key_history result | Metadata / legacy result | Required outcome |
| --- | --- | --- |
| Native query, scan or cancellation error | Any | Preserve that error; no alternate query or decoded body can mask it |
| Row found | Both metadata columns valid | Return that SQL rev, explicit presence and exact digest |
| Row found | Both columns NULL; one covering legacy interval with source_revision equal to SQL rev | Return the legacy presence/digest at that exact token |
| Row found | Both columns NULL; missing, overlapping or mismatched legacy interval | Refuse; a retained mutation is never a revision-zero miss |
| Row found | Partial NULL pair, invalid presence, malformed revision/digest, or nonempty absent digest | Refuse malformed metadata |
| No row | No covering legacy interval | Preserve the true never-present observation, after the existing pin/floor checks |
| No row | Covering legacy interval | Refuse the inconsistent retained witnesses; do not substitute a current image |

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

The existing compatibility contract is precise: schema.go:3 declares version
2; Driver.Open (store.go:121-143) accepts 2 or explicitly upgrades 1, refusing
other versions. readMeta (296-306) requires exact equality. reader.pin (49-57)
uses readMeta as its first SELECT, fixing a WAL snapshot; Write (transaction.go:
59-70), Lock (190) and Head (313) all use that gate. The ordinary non-replay
Commit checks current metadata at transaction.go:548 before any new source
publication. Offline-history publication starts with restoreEmpty, which calls
readMeta before writes. Existing pinned readers intentionally do not reread
the version in reader.check: they retain the old physical snapshot and cannot
raise their authority pin past its captured head. FreshSelectionExists also
uses that pinned committed snapshot. Keep these properties unchanged.

Exact replay returns before Commit's version check and must remain an
idempotent result without new source publication. Do not move a new fence ahead
of replay, header/lease checks, or earlier validation failures. Public Migrate
(store.go:321-329) supports identity only; Open owns explicit version upgrades.
Existing pending_test.go:262 covers the actual 1-to-2 upgrade's receipts,
identity and epoch; engine_test.go:457 rejects arbitrary future Migrate calls.
The new schema-fence control exercises actual API Open/Read/Write/Meta/Migrate,
an already-pinned selector, and held non-replay Commit against an unknown
persisted version. It is AUTHORED_NOT_RUN and does not substitute for eventual
real 1/2-to-3 migration and independent old/new-binary controls.

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
