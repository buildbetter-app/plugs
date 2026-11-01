# Nightly fresh-store restore comparisons

The scheduled `Plug Nightly Restore Diffs` workflow runs at 04:23 UTC on a
fresh RunsOn runner. Manual dispatch tests the selected ref at its exact SHA.
It uses only checked-in public synthetic fixtures, pinned by SHA-256: the
original exporter fixture and offlinehistory's `full.tar` and `partial.tar`.
It accepts no file path, external DSN, credential, or private ledger input.
PostgreSQL runs in a test-owned, digest-pinned loopback container.

The snapshot test requires two independent fresh targets on each of memory,
SQLite and PostgreSQL, then compares complete
paginated counts and IDs, fields, labels, dependency links, original source
counter declarations, explicit target allocator state, restore base versions,
retained source archives, every member blob and digest, and the restore
changeset census. SQLite and PostgreSQL must retain the same census after
close/reopen.
Comment source time and attribution remain compared; only the independently
validated restore execution timestamp is excluded from cross-run equality.
Damaged archives and unsupported requests must leave targets unchanged.

The original exporter archive is never rewritten. A detached synthetic
snapshot fixture explicitly changes source IDs to the admitted hash profile,
removes unverified source versions, and uses a supported blocking relation.
Its nonzero source counters remain retained separately from the target's
random allocator. Full and partial requests against that original unverified
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

`nightly-restore-diffs.json`, `nightly-history-restore-diffs.json`,
`nightly-history-refusals.json`, test XML/logs, exact head/tree and the Bazel
exit code are uploaded as public fixture evidence. Status
`bounded_current_state` or `bounded_public_history` means the named synthetic
comparisons passed with the declared gaps still open.
Snapshot versions are restore bases, not imported source history. Old storage
incarnations refuse after restore; this does not certify the `memory.list` RPC
continuation. The synthetic history images do not prove a source exporter's
capture or real-ledger fidelity, complete paired/private census, or a scheduled
main execution. One execution never establishes a three-night streak.

Focused acceptance runs through the swarm's `remote-bazel` wrapper at the
candidate SHA, targeting
`//internal/integration/nightlyrestore:nightlyrestore_test`. Whole-tree RunsOn
execution and an independent GPT-6 review remain required for PR admission.
