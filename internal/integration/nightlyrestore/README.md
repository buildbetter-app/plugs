# Nightly fresh-store restore comparisons

The scheduled `Plug Nightly Restore Diffs` workflow runs at 04:23 UTC on a
fresh RunsOn runner. Manual dispatch tests the selected ref at its exact SHA.
It uses only checked-in public fixtures, pinned by SHA-256: the original
synthetic exporter fixture, offlinehistory's synthetic `full.tar` and
`partial.tar`, and the unchanged genuine public producer outputs from
`e25cf419` (original run `42786069732`).
It accepts no file path, external DSN, credential, or private ledger input.
PostgreSQL runs in a test-owned, digest-pinned loopback container.

The snapshot test requires two independent fresh targets on each of memory,
SQLite and PostgreSQL, then compares complete
paginated counts and IDs, fields, labels, dependency links, original source
counter declarations, explicit target allocator state, restore base versions,
retained source archives, every member blob and digest, and the restore
changeset census. SQLite and PostgreSQL must retain the same census after
close/reopen. Each target also rejects a pre-restore reader and pagination
cursor with the typed incarnation-reset boundary before its census is read.
Comment source time and attribution remain compared; only the independently
validated restore execution timestamp is excluded from cross-run equality.
Damaged archives and unsupported requests must leave targets unchanged.

The original exporter archive is never rewritten. A detached synthetic
snapshot fixture explicitly changes source IDs to the admitted hash profile,
removes unverified source versions, and uses a supported blocking relation.
Its nonzero source counters remain retained separately from the target's
admitted hash allocator. Full and partial requests against that original unverified
archive retain their exact typed `ErrRestoreHistoryRefused` reasons.

The history matrix adds 30 fresh restores: two targets per driver for strict
default full, explicit full, explicit partial, and explicit snapshots of each
history archive. Full and partial requests use the real optional
`BindHistoryImport` preparation and trusted `RestoreImport` entry point.
Every readable historical business image is compared to the checked source
image before comparing native versions and changesets to its transitions.
The checks include counts, IDs, exact fields and opaque numbers/nulls, labels,
comments and source attribution, dependencies, source counters and target
allocators, retained archive members/blobs/digests, exact source commit/event
mapping, deletion and recreation, actual restore actor/time, keyed receipt
recovery, and both durable drivers' reopened state.

Full imports must have head 5/floor 0. Partial imports must have head 3/floor 1,
the verified baseline at revision 1, sequence 4 at revision 2, and the exact
source gap [1,3] with reason `not-retained`. Both partial and snapshot modes
must return `ErrTruncated` with floor 1 below that boundary. Changeset enumeration
starts strictly after its cursor: it cannot enumerate the partial baseline at
the floor. The report names that exclusion; baseline as-of state and versions
are checked, and the final changeset is read through its real receipt key.
Snapshots must have head 1/floor 1 and no replay marker. Fifteen additional
controls require typed no-write refusal of default/full requests against the
partial archive and missing/changed history bindings, including unchanged
metadata, rows, changesets, archive blobs and allocator state.

The named native matrix adds 12 restores: two fresh targets on each driver
for the genuine shallow-boundary original and the separate complete
multiversion original. It constructs the explicit generic public policy with
the receiving executable, binds it through `BindSourceImport`, and publishes
through `RestoreImportWithLease`. The source project `public` and physical
database `public_commit_history` stay unchanged. The container and all nine
original producer output members are pinned; only detached negative copies
are rewritten.

The shallow original retains all 13 verified images through its typed
`ghost_parent` boundary at ordinal 14, plus its two current events. The
complete original retains 20 images, including the two cursor-absent initial
roots. Each image is checked under its own schema: 39, 40, 41, 61, 62 and 66
for the boundary case, with 43, 54, 55, 63 and 64 also present in the complete
case. Historical schema absences stay absent; immutable post-0062 roots never
gain the current events attachment. Exact tables/cells/multiplicity, current
family counts and IDs, source fields and dependencies, source counters and
target hash policy, coverage commitments and reports, paginated native table
versions/changesets, archive/member/events bytes and digests, receipt recovery
and durable reopen are compared. Twelve controls require typed no-write
refusal of falsely complete history, a missing present image, a ghost boundary
claiming schema zero, and a changed events commitment on all three drivers.

`nightly-restore-diffs.json`, `nightly-history-restore-diffs.json`,
`nightly-history-refusals.json`, `nightly-native-history-restore-diffs.json`,
`nightly-native-history-refusals.json`, public constructor policy/request
packets, test XML/logs, exact head/tree and the Bazel
exit code are uploaded as public fixture evidence. Status
`bounded_current_state` or `bounded_public_history` means the named synthetic
comparisons passed with the declared gaps still open.
`bounded_public_native_history` covers only the named genuine generic public
originals. An unavailable pre-boundary past is never reconstructed, and the
first imported native image is not evidence of source creation. Current events
remain an unverified attachment with no invented event-to-commit mapping.
Native table revisions do not establish portable historical business records.
These originals have no current leases and prove no positive lease filtering.
Memory recovery is limited to the same process. The two originals share the
same source project, so they do not qualify a joined two-project restore,
private capture, native full card, switch, or trial. Real private nights remain
0/3 until actual inputs and consecutive nightly receipts exist.
Snapshot versions are restore bases, not imported source history. Old storage
incarnations refuse after restore; this does not certify the `memory.list` RPC
continuation. The synthetic history images do not prove a source exporter's
capture or real-ledger fidelity, complete paired/private census, or a scheduled
main execution. One execution never establishes a three-night streak.

Focused acceptance runs through the swarm's `remote-bazel` wrapper at the
candidate SHA, targeting
`//internal/integration/nightlyrestore:nightlyrestore_test`. Whole-tree RunsOn
execution and an independent GPT-6 review remain required for PR admission.
