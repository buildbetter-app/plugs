# B13 public memory and P85 brief connection

`memory.save` returns an explicit per-result `replaced` boolean. It reports
effective presence inside the Writer transaction, including empty values and
quiet same-value saves. Forgotten imported tombstones are absent. The stored
receipt preserves this observation on replay; every other operation omits it.
`TestB13FullcardLiteralBoundariesAndPublicSearch` adds literal slug boundaries,
Unicode identity, public key/value search, true/false replay, and real durable
bead and real ephemeral workflow decoys on all three engines. Memory reports
process checkpoint lifetime without restart persistence; SQLite and PostgreSQL
also prove physical reopen. This follows adopted PR467 and the 17:14Z ruling
that supersedes the stale memory-refusal paragraph in workflow-contract.md. The B13
native recording consumes the unchanged canonical build support and independently
checks the same public vectors against actual pinned native CLI/SQL results.

The additional `TestImportedMemoryWritesPreserveSourceEnvelopesAllDrivers` in
`converterroundtrip` covers writes after importing an explicitly authored mixed
string/secret/JSON-value archive. Legacy hidden mirrors must not poison an
unrelated public save. Joint source envelopes retain their classification and
provenance. Hidden same-key saves refuse with `invalid_params`; deletion of a
hidden row returns `not_found`. Visible source slots now accept the decided
replacement/forget overlay behavior described below. Ordinary canonical memory
save/replace/delete remains writable.
Reads and writes share the exact source-envelope classifier; malformed mirrors
remain `internal_error`, including during an unrelated save.

`TestB13JointSourceCoreProjectionAndConservation` imports both legacy and joint
public source fixtures on all three engines. Replacement writes a separate
`sourceOverlay` memory row; forget writes an attributed tombstone in that row.
The overlay pins the original Ref, revision and body digest. Reads, list and
normal brief prefer its effective value or absence. Re-remember advances its
logical generation so the previous lifetime cannot return through history.
Source bytes, original versions, import binding, retained archive and provider
namespaces remain unchanged. Writer authorization covers both source and
overlay for writes and replay. The controls retain byte RPC frames, raw state,
physical SQL snapshots and actual reverse output. SQL engines close and reopen
the same database; memory proves a fresh binding over the same live store.

The actual existing reverse path emits the effective replacement, absence or
re-remembered value, with overlay attribution reported as conversion loss. Its
existing refusal for a retained empty source value stays unchanged; the suite
forgets that existing empty slot before positive reverse comparisons.

The explicitly selected manual target
`//spec/cardb20:b13_memory_record_native_test` consumes unchanged canonical B20
build/recorder support and the existing B09 source archive and Dolt runtime.
It records real public canonical remember/recall/forget calls and raw physical
history, then compares named ordinary and constructed imported counterparts on
all three engines. Unicode identity, verbatim values, derived key limits,
empty versus absent, replacement, exact-key forget and re-remember remain
literal. Constructed source rows prove view behavior; the separate integration
suite proves actual import/reverse behavior. Neither is official producer
admission or private-store history qualification. Source, focused, whole and
public native outcomes must be reported separately after their actual runs.

The new fixture uses actual memory, SQLite and PostgreSQL stores. It checks
the complete imported records and version chains, source archive/binding rows,
all nine derived tables, joint provider checkpoints, changeset prefix and
tracked receipt scopes. Canonical writes also exercise public list/brief,
historical literal bodies, replay and deletion tombstones before and after
SQL close/reopen or a memory lease refresh. Four malformed-envelope controls
must fail atomically on every engine. These are synthetic public integration
fixtures, with no native recording, producer/preparation adoption or FullB13
qualification; each new head still needs its own remote gates and review.

Done = the scoped missing public memory-to-brief assertions are implemented in
a frozen clean candidate, proven by complete source bindings and a concrete
exact-head gate plan; source-only checks and inherited/base PASS do not count
as runtime or full-card qualification.

Scope = this new integration package, its BUILD file and an additive source
manifest proposal. Existing production memory/brief, B15 and B16 paths remain
owned by their existing authors. Author status is **AUTHORED_NOT_RUN**.

The authority is `engdocs/design/plug/spec/behaviors/content.md` B13, with P85
adopted in `engdocs/design/plug/review-2026-10-27.md` and recorded in
`engdocs/design/plug/design.md`. The frozen base is
`151cb7c15b3324b78a967cf78de106cb9ffda0b3`, tree
`32288b0f20d200caf0ade11b79362621d4d3ace7`.

## Retained coverage and the connected gap

PR267 (`9882372960d7496a2991fdfa8f2e934b49f8ef49`) is retained unchanged.
`internal/integration/memorycontract` already covers omitted-key golden
vectors, literal explicit keys, replacement identity, search, basic refusal
metadata/receipt checks, current-history authorization and SQLite reopen.
`spec/s1-b13-b16` adds bounded memory/SQLite plane projections. The retained
`internal/server/rpc/integration_test.go` already checks SQLite memory/brief
ordering, read visibility and reopen. Those are source coverage observations,
not fresh execution results or inherited runtime qualification.

The new test connects that behavior into one public lifecycle on actual
memory, SQLite and PostgreSQL stores, with independent persistence checks.
It does not duplicate the derivation vector table, implement Beads behavior,
author Beads twins or create reference recordings.

## Authored assertions

All rows below bind to `TestB13PublicMemoryBriefLifecycle/{memory,sqlite,postgres}`.
They remain AUTHORED_NOT_RUN until S1 routes an exact-head execution.

| Assertion | Connected observable behavior | Independent check |
| --- | --- | --- |
| B13-P85-01 | Public save/get, every list page and brief preserve literal content, Unicode/spaced keys, metadata and record revisions. | Literal expected values and raw core record bodies/envelopes from fresh readers. |
| B13-P85-02 | Omitted-key derivation reaches list and brief; explicit keys admit non-ASCII/punctuation content that cannot derive a slug. | One literal derivation example; no reimplementation of the slug algorithm. |
| B13-P85-03 | Existing empty content is present in get/list/brief, a missing row is not_found, and blank replacement remains refused until explicit deletion. | Required content-field presence, seeded raw empty row, unchanged version/receipt state on refusal, then a durable tombstone. |
| B13-P85-04 | Search folds the query and stored key/value; no-match and fully emptied lists/briefs are explicit arrays. | Literal mixed-case/Unicode key and value searches, both before and after replacement. |
| B13-P85-05 | Generic config/settings and ordinary beads never enter memory reads; memory records never enter ordinary bead get or brief readyTop. | Deliberately matching config bodies, a public ordinary bead, kind checks, untouched raw bodies and complete version chains. |
| B13-P85-06 | Same-key replacement preserves identity and reports a patch; old historical content survives. | Exactly one revision, patch changeset, new version and final receipt; public history returns the original literal body. |
| B13-P85-07 | Blank, underivable, malformed, missing-idempotency, stale-CAS, reused-key and wrong-plane writes refuse without durable change. | Full before/after records, versions including absent/deleted identities, log, tracked receipts, observations, derived rows and SQL data-table snapshots. |
| B13-P85-08 | A denied memory is absent from point/list/brief output; policy failure produces a complete typed refusal without partial output/body disclosure. | Persisted hidden core row plus explicit read-output and no-mutation assertions; the host read policy is a fixture, not production authz qualification. |
| B13-P85-09 | Same-file/DB close/reopen preserves state, public values and final receipt bytes; replay of an old save neither rewinds a replacement nor resurrects a deleted row. | All earlier handles/connections close, the same DSN reopens, complete state comparison, exact public result-byte replay and current-read rechecks. Memory proves fresh binding over its same live store only. |
| B13-P85-10 | Forget removes only the target identity; prefix neighbor, settings and ordinary bead survive. Recreate allocates a new identity and cannot disclose the old lifetime's history. | Independent raw-plane/version conservation, tombstones, deleted-row misses, rebind/reopen and fresh public list/brief checks. |

Replacement is reported by the adopted optional `OpResult.replaced` boolean;
memory.save always includes it, while every other operation omits it. The
Writer records transaction-observed presence and replays the original result.
Commit identity, changeset op and version independently bind the write. MemoryPage exposes
`items`/`nextCursor`; public per-row revisions bind to independently pinned
driver records. No additional public fields are assumed.

## Composition and oracle boundary

`fixture_test.go` binds the real service, ontology, Writer, RPC registry and
in-process byte transport. Only two config rows and an existing empty memory
are seeded using `storage/conformance.Seed`. They are explicitly authored
storage fixtures, not successful public blank saves, native imported stores,
reference captures or evidence of native parity. All lifecycle writes use
serialized public JSON-RPC requests. Request/response bytes are retained in
`b13-<engine>-public.jsonl` under Bazel's undeclared outputs directory when run.

`oracle_test.go` uses fresh driver readers; SQL uses a separate actual driver
handle and a separate `database/sql` connection. It checks all live record
bytes, all version chains for the fixed ID inventory and changeset references
(including tombstones and absent IDs), complete bounded changesets, every
request receipt scope, relevant predicate observations and every derived table.
The fixed limits fail if a driver returns incomplete pages. The SQL oracle
enumerates every durable data table in the retained SQLite/PostgreSQL schemas,
including receipts, states, blobs, checkpoints, sessions and allocators. Only
lease renewal and PostgreSQL lock bookkeeping are excluded. No driver wrappers,
fake transactions, pipeline memory decoder or derivation helper form the oracle.

`postgres_test.go` owns a disposable PostgreSQL engine pinned to the same image
digest as the retained driver/acceptance fixtures. It accepts no external DSN,
substitute store, engine skip or softened failure. Driver clocks, lease TTLs,
production budgets and deadlines are unchanged. The only deadline introduced
is the test fixture's context.

## Integration and proposed gates

The BUILD target is `//internal/integration/b13memory:b13memory_test`. Its local,
exclusive, network tags cover its owned PostgreSQL engine. No shared target or
resource policy is changed.

`manifest-proposal.patch` is an exact additive proposal for the shared owner to
apply to `tools/lexicon/source_manifest.txt`. The author does not edit that
shared file. A whole-tree run on the author candidate alone would hit the known
source-inventory mismatch. S1 must integrate the proposal, freeze and bind the
resulting integration SHA/tree, and then route the gates. No base/main chase is
needed. The source package is unchanged between the author and integration
trees; qualification still belongs to the actually executed integration tree.

The accompanying frozen handoff packet supplies full author SHA/tree/parent/base,
the complete binary patch, changed path/blob/mode/SHA-256 table, authority and
dependency bindings, and concrete commands using the ordinary remote-bazel
wrapper's `<worktree> <full-sha> <label> -- <bazel-args>` interface. Proposed
focus includes this target plus retained memorycontract/RPC regressions. Only
after that exact integration tree passes focus should S1 route the unfiltered
whole tree. No source pushes, gate executions or test/build commands are
authorized in this author lane.

No runtime PASS, full B13/full S1, native/reference capture, producer capability,
Beads parity, HTTP/authenticated production composition, concurrency/failover,
reverse import/readback or production qualification is claimed. FullS1 remains
0/28, E3 remains 5/5 bounded fixtures, and NativeAdmission remains None. Historical
REDs and prior candidate evidence remain unchanged.
