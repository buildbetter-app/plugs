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
closed-world conversion as the current image. Opaque source fields retain their
JSON numbers, nulls and text in `beads.*` metadata. Current census and archive
members are retained at the final restore revision; they never fill historical
business preimages. The adapter maps each checked source commit image to an
explicit import-order revision. This proves commit-image history, not every
individual SQL edit between commits or verified source-principal attribution.
