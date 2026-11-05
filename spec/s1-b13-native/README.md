# B13 native memory recorder

Status: **AUTHORED_NOT_RUN**. This source child is bound to the current pinned
main candidate and must earn its own changed-head compile, focused, whole and
native receipts. Source, ordinary tests, and compile-only evidence do not grant
native admission.

The source base is product commit
`7a43c3c625a5c08d3a4364a321cd47d6357ef30e`, tree
`be93a595dc4c44ef4d94d2333060788fd344dca0`, sole parent
`5812dd80f0c6e4d1e9136604941c7f54eef3238a`, with the imported-memory
projection child applied on top. The canonical obligation is B13 in
`engdocs/design/plug/spec/behaviors/content.md`. This package adds no shared
reader, Writer, importer, policy, producer or native-pin implementation.

## Accepted source and execution boundary

The recorder requires the real prepared exporter from accepted descriptor
`41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a`.
Its producer is `c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724`, tree
`930405e863284a3955cbd35e73a6097b606b91a5`. The binary's actual bytes, mode,
size, build module, manifest, workflow and version are checked and retained.
Every native child uses the unchanged `publicnative.Server.ChildEnv` result.
Default c8 object JSON includes `schema_version=1`; the expected full response
includes that field. The recorder neither strips it nor enables another
envelope after ChildEnv.

The intended manual target is `//spec/s1-b13-native:recording`. It refuses an
implicit run or an identity outside the explicitly supplied
`-b13-native-recording -b13-native-recording-id=recording1` or `recording2`,
and claims a separate retained output directory for each. The pinned-main
paired-recording mandate authorizes these two fresh RunsOn recordings. Each
invocation must use its own same-run accepted R418 preparation and all four
matching exports, the complete unfiltered target and all seven required
settings. No old fixed-2d branch, receipt or prepared path is adopted. The
trusted main workflow selects the committed preparer; this package changes no
workflow or helper pin. Actual product commit, tree, ordered parents and raw
commit bytes/hash are retained alongside the pinned base. The runner's checkout
is shallow, so ordered parents come from its original raw commit headers. Base
ancestry is verified separately before author dispatch and by source review in
the complete object store; no runtime Git fetch or workflow change is used.
Every build, test, preparation and native process remains RunsOn-only.

## Native observations

`TestB13NativeMemoryRecording` authors an actual CLI corpus in one owned,
physical Dolt database. It exercises verbatim whitespace/newlines/Unicode,
explicit non-slug keys, collapsed separators, eight segments, the 60-byte cap,
trailing-separator trimming, replacement, folded key/value search, empty search,
missing versus existing empty values, exact forget and prefix-neighbor
survival. Empty legacy memory and generic/settings survivors are recorded SQL
setup through the accepted executable. They are not claimed as successful
`remember` writes. Blank content/key and underivable content retain their own
literal CLI refusal and complete conservation images.

Every CLI operation retains argv, exact ChildEnv, cwd, UTC times, original
stdout/stderr, exits and stream hashes before interpretation. Discovered native
tables and their schemas, every row/column, Dolt HEAD/parent, prior log and
working status are captured before and after each behavioral operation. The
mutation oracle permits only the exact `kv.memory.<key>` config row and one
new commit whose parent is the prior HEAD; all other captured data and prior
history must remain equal. SQL capture preserves the CLI's exact JSON numbers,
types, nulls and duplicate row multiplicity. It is a CLI-visible physical
image, not an independent native SQL-driver type or multi-query transaction
oracle.

The history oracle retains every raw `dolt_log` column. On a successful
mutation it requires the live `HEAD -> main` decoration to move exactly from
the old head to the one new commit, whose recorded parent must be the old
head. Every other prior-row field stays equal, including additional columns;
the decoration is never removed from retained output. Counterexamples reject
rewritten history, missing/duplicate rows and any other reference movement.

One complete, unfiltered inventory/archive checkpoint is taken while the
native empty row and full supported string corpus still exist. Both exports
must conserve the complete native state. The original archive bytes are used
unchanged by every public engine. Native memory keys with edge whitespace are
also exercised verbatim **after** this checkpoint: the frozen archive reader
rejects such `Record.ID` values. No archived row is trimmed, removed or rewritten
to evade that restriction. The edge-whitespace observation and subsequent
native forget operations are later native-only evidence; their results cannot
be attributed to the earlier imported archive.

## Public boundary and conservation

`b13Public` uses that actual archive with the existing explicit snapshot
`InspectImport`/`RestoreImport` seam. The isolated recorder host's admission and
fresh profile are recorded. This is snapshot import, not source-history replay,
an external production login or admission. The faithful corpus contains actual
string memories. It does not claim mixed secret/non-string memory coverage;
input classification, source fields and original bytes remain visible without
filtering, coercion, stringification or a second importer.

The path is a real loopback HTTP listener through unchanged authn, HTTPJSON,
RPC, service, pipeline and storage. `b13Observed` delegates unchanged service
calls and saves original typed values/errors before RPC encoding or after-image
reads. `probe` retains complete HTTP request/response bytes, status, headers and
times. All initial imported `memory.get`, missing get, complete list, folded
search and `agent.brief` responses are collected before judging the imported
public contract. Actual Ref.ID/key/verbatim content, revision, metadata,
complete memory census and settings-plane exclusion are checked independently
against the real native corpus and stored image.

Every image retains complete live records; known/missing/deleted ref versions
and same-reader observations; ordered history above the actual floor; the
typed below-floor refusal; exact authenticated import receipt/floor changeset;
original retained archive; every declared operation receipt; and the nine
enumerated derived tables with the same pinned Reader's actual observed
payload/Index/revision. Original API pages and errors are saved before
aggregation. Duplicate/missing/invalid census entries refuse. Method-free raw
trees retain actual Go field types, exact numbers, nil versus empty containers,
and lossless byte contents before attempted model JSON. An encoding failure is
retained and is never repaired into a valid success object.

Only successful initial public reads reach the later authored memory writes.
Each successful write compares the full typed/HTTP commit, exact row/version
delta, unchanged prior history, one complete changeset, independently lowered
request hash and finalized receipt, and one subscriber's entire operation,
metadata, before/after body and committed row image. All other captured fields
must be conserved. Replays, fresh-key noops, typed refusals, post-delete public
not-found and final complete list have their own full before/after checks.
SQLite/PostgreSQL Close/Open must conserve the same full reader image and then
serve the exact public corpus and stored successful-request replay through a
fresh real HTTP listener. The memory engine has no SQL reopen credit. HTTP,
lease and store close returns are retained; close failures prevent a complete
marker. Ordinary lease maintenance grants no lease/clock/B05 proof.

## Known source block and remaining limits

The current source projects admitted public string memories into a canonical
`pipeline.MemoryDocument` with the stored `Ref.ID`, trimmed key, verbatim
content, and source/archive metadata. Secret-classified and non-string rows
remain in their lossless raw form and are excluded from public memory reads.
The native `core/restore/archive.go` path is distinct.

Initial reverse checks actual original native memory values and unchanged
storage. After reachable public memory changes, the frozen reverse seam
declares changed/missing/unmapped source memories unsupported. Its exact typed
refusal is retained, and **live-memory reverse parity remains unproved**.
Source classification, all retained archive bytes, generic configuration and
complete reverse output/report remain available; raw provenance is not public
memory proof.

Reader census covers live derived keys and known record refs, not unreferenced
tombstones, full physical SQL lock/read-fact tables, undeclared receipt scopes,
all possible native inputs, mixed secret/non-string visibility, live public
authentication/authorization plugins, production networking or concurrent
writers. No paired run, full-card result, whole-tree result, native admission
or runtime result exists for this source child. Historical B25/B27 bounded
results and REDs remain exact-head evidence in their separate lane.
`NativeAdmission=ADOPTED R41800330346`, `FullS1=0/28`, B26 separate, B05 PAUSED.
