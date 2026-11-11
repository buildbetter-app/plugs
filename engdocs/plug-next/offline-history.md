# Offline history import port

The optional `driver.OfflineHistoryTx` capability publishes a validated history
payload into an empty store. It is available only to the trusted restore host,
after admission, the complete lock set and Head. It is not an RPC authority
object. Ordinary writes cannot select this capability through a changeset kind.

`OfflineHistory` carries a source sequence, explicit event/commit identifiers and
complete converted before/after images. Its verification flag is output of a
source verifier, never an operator's assertion. Each batch is one **import
unit**. Its new Plug revision records import order; it makes no assertion about
source SQL transactions or a one-to-one correspondence with Dolt commits.
Dolt IDs and Beads event IDs remain provenance. The admitted restore actor and
restore time are the verified principal/time of every imported changeset.
Source authors, times and opaque fields remain data in retained source records.

Full mode requires a verified sequence beginning with a proven empty state,
complete coverage to the source head, every exact preimage, and no gaps.
Partial mode admits a verified contiguous suffix ending at that same head. It
requires a verified complete baseline immediately before the suffix. The
baseline becomes import revision/floor 1. Every earlier source sequence must be
accounted for in sorted, adjacent inclusive gap ranges with an explicit reason;
known source history outside the retained suffix is marked `not-retained`, not
claimed missing. Internal holes require retaining a later suffix and explicitly
accounting for the omitted prefix. Reads below the floor retain the drivers'
typed truncation behavior. No intermediate state is filled from the final head.

The protected restore evidence record `plug-restore-history` stores exact gaps,
floor and source-sequence-to-import-revision mapping. A final restore changeset
creates the marker and any independently rebuilt current records with no source
history. Such records are restore-time bases, never fabricated source history.
A rebuilt final image must equal the result of replay for every historical
record. Snapshot remains a separate explicit current-image import and does not
use this replay payload.

Unavailable or unverifiable source evidence continues to refuse. Capturing
Dolt commits, human audit events, per-issue versions or a clone-local journal
does not by itself prove a complete source range. Real-source qualification and
all-three-driver atomic publication require their own exact-head tests.

## Source archive adapter

The optional `history/source.json` member uses `beads-dolt-images/1`. It retains
the source Dolt graph, the complete current events table, and a checked range
of complete portable images read at explicit source commit IDs. Each frame
includes its complete events-table image. A verified linear range must preserve
every previously observed event unchanged and end at the exact archived head.
Missing parents, merges, missing images, unsupported schemas and changed audit
events cannot be promoted to a full range. Raw unverified captures remain
provenance and grant no replay capability. Combining archives with history is
refused until their independent source mappings can be preserved.

The trusted host calls `converterroundtrip.BindHistoryImport` after admission
and current-image inspection. This binds the archive, independently rebuilt
derived digest, converted history digest and actual restore time. The existing
`RestoreImportWithLease` entry point consumes the returned profile. A changed
history digest or time refuses with typed `unmappable` before publication.
Source timestamps and authors never supply imported changeset authority.

Full is still the default. A verified complete archive can replay; a verified
partial suffix still refuses a full request with typed `non-contiguous`.
Explicit partial imports its complete baseline, suffix, exact prefix gaps and
source-to-import mapping with floor 1. Explicit snapshot imports the current
image with floor 1 and a new incarnation. Below-floor reads and changeset
enumeration retain typed truncation. A driver without the optional capability
returns typed `not-yet-supported`; no fallback flattens a history request.

Portable bead, edge, comment, memory and config images pass through the same
closed-world conversion as the current image. Original source types in every
frame and partial baseline require the manifest's
explicit type directives, including their project and optional issue scope. An
earlier image may use only a subset of the directives used by the current head.
Opaque source fields retain their JSON numbers, nulls and text in `beads.*`
metadata. Current census and archive
members are retained at the final restore revision; they never fill historical
business preimages. The adapter maps each checked source commit image to an
explicit import-order revision. This proves commit-image history, not every
individual SQL edit between commits or verified source-principal attribution.

## Streaming core contract

`driver.PrepareOfflineHistoryStream` is an additive trusted-host port. It pulls
bounded typed frames directly into private staging. It does not call
`BuildOfflineHistory`, marshal an aggregate `OfflineHistory`, or collect all
versions, images or source-to-revision mappings. The existing aggregate APIs and
their limits remain in effect for their callers.

The fixed header declares the verified source, range and coverage policy. A
partial suffix begins with individually streamed exact prefix gaps, then one
proven baseline commit. Native boundaries precede their own source's commit
block, sorted by source store/project; each boundary's exact verified commit
count must match that block. Native source sequences begin at one for the
verified images, without inventing unknown earlier commits. Import sequences
remain globally contiguous. A full stream has floor zero; a partial stream has
baseline revision/floor one. One final restore revision follows the imported
units and creates the evidence and any independently rebuilt current records.

Each commit declares counts, followed by separate commit-ID, parent-ID and
event-ID frames, in that category order. Parent order, including the first
parent, remains exact. Table frames follow in bytewise order of table name and
encoded record lock key. Each table declares its converted identity and optional
before/after images, then receives offset-contiguous before chunks followed by
after chunks and an explicit table end. Nil images express converted absence;
physical table presence, schema, row multiplicity and hashes are distinct typed
metadata. Empty source commits retain their explicit provenance and boundary.
Missing preimages never come from a later image or the current head.

`MaxChunkBytes` bounds decoded bytes in one pull. `MaxFrameBytes` also bounds
the canonical JSON size of every frame, including provenance, gaps and native
boundaries. Producers enforce these bounds before allocating or decoding a
frame. The core rejects oversized frames and invalid original UTF-8 metadata
before detaching it; opaque image chunks may split at any byte. It hashes exact
converted image bytes independently. Converted commit hashes cover the domain
`plug-offline-history-commit/1` plus LF, then canonical commit, ID and table
frames, each followed by LF. Physical source table/schema/root commitments keep
their own source-format preimages; they are never inferred from converted
hashes or portable record counts.

The admitted profile's history digest covers `plug-offline-history-stream/1`
plus LF, header JSON plus LF, then every frame JSON plus LF except the final
`end` frame. Canonical JSON here means Go `encoding/json` output of the declared
typed structs with their exact field order and omission rules. The end frame
must report the actual commit/table/decoded-byte totals. It must be followed by
one clean zero-frame `io.EOF`, with no frame/error pair or trailing data. Source
ownership transfers at entry, including rejected requests. The core calls
`Close` exactly once and requires cleanup success before releasing its opaque,
single-use publication handle. Errors, cancellation or cleanup failure abort
private staging and release no handle. The caller must commit or close a
successful handle; copies share the same consumed state.

The adapter's `Publish` requires a separate opaque publication proof. Its
`Claim` getter validates the exact prepared binding and can succeed once during
that single callback. Retained/copied proofs expire as soon as the callback
returns, including on failure. Constructed informational completion metadata
cannot authorize publication after a terminal or cleanup error.

Staging adapters must validate exact JSON, replay preimages, source proofs,
duplicate provenance, final-image equality and all atomic participants with
bounded pages or spooling. Independently verified HEAD/current checkpoints bind
to the profile's archive digest, and rebuilt final-image equality binds to its
derived digest. Source authors/times remain provenance. The publication handle
binds the detached admitted profile, actual restore time, new incarnation,
head/floor and original keyed final receipt. The adapter rechecks actual target
lifetime, lease, complete locks and stage/receipt correspondence before atomic
publication of versions, changesets, current head, gaps/boundaries/mappings,
source-plane participants and receipt. An indeterminate publication retains the
original receipt key for recovery; abort discards only unpublished resources.
Cleanup failure remains diagnostic and cannot classify an indeterminate commit
as a cancellation or fencing rejection.

Core statistics report actual converted image bytes and peak observed
frame/chunk sizes. They do not report allocations or process RSS; receiving
integration must measure its own logical categories and actual RSS separately.
Memory, SQLite and PostgreSQL expose `NewOfflineHistoryStreamStage` for an
already locked real engine writer. An engine-private disk index and image spool
retain replay state, source frames, provenance and mappings. Image scans and
encoders check cancellation at each 32KiB page. SQL publication copies bounded
pages into one fenced engine transaction; memory constructs only its permanent
in-process output state. Each historical bead After is syntax checked and its
comment authority is validated one comment at a time through the existing
restore profile validator. Body strings and unknown fields are scanned without
materializing them. Duplicate authority fields refuse. A single comment
metadata field beyond 64KiB (or 16 times the admitted actor encoding, if larger)
returns typed `ErrUnsupported`; there is no aggregate history size/count cap in
this path. The ordinary transaction still holds its one rebuilt current image.

The new `historyverify.Checkpoint` is an opaque immutable verifier result,
required by every engine stage. It binds the exact admitted request (including
archive, derived and history digests), actual old target/lifetime/time,
converted HEAD/current roots, current auxiliary projection and physical
reconciliation. Engines refuse missing, mismatched or unreconciled values before
publication. Root computation is public but cannot mint a value. Physical
preimages and the original derived-image encoding remain verifier obligations.
The private constructor has no production issuer in this layer; the only test
minter is in `_test.go`. Tests use synthetic verifier results solely for actual
memory/file SQLite/pinned PostgreSQL publication mechanics, including SQL reopen
and original receipt replay. They grant no native receiving authentication.

Real receiver issuance/wiring, measured real-history and RSS proof, and the
required-before-switch follow-up remain separate work. Existing aggregate
callers and their 128MiB serialized ceiling remain unchanged.
