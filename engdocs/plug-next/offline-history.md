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
