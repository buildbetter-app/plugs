# Nightly fresh-store restore comparisons

The scheduled `Plug Nightly Restore Diffs` workflow runs at 04:23 UTC on a
fresh RunsOn runner. Manual dispatch tests the selected ref at its exact SHA.
It uses only the checked-in public synthetic exporter fixture, pinned by
SHA-256. It accepts no file path, external DSN, credential, or private ledger
input. PostgreSQL runs in a test-owned, digest-pinned loopback container.

The test creates two independent fresh targets per driver. The admitted
snapshot profile succeeds on memory and SQLite, then compares complete
paginated counts and IDs, fields, labels, dependency links, original source
counter declarations, explicit target allocator state, restore base versions,
retained source archives, every member blob and digest, and the restore
changeset census. SQLite must retain the same census after close/reopen.
Comment source time and attribution remain compared; only the independently
validated restore execution timestamp is excluded from cross-run equality.
Damaged archives and unsupported requests must leave targets unchanged.

The original exporter archive is never rewritten. A detached synthetic
snapshot fixture explicitly changes source IDs to the admitted hash profile,
removes unverified source versions, and uses a supported blocking relation.
Its nonzero source counters remain retained separately from the target's
random allocator. The original full-history request must return
`ErrRestoreHistoryRefused`; partial replay and PostgreSQL offline import also
retain their actual typed unsupported boundaries. A newly supported capability
fails the boundary expectation so this suite must be extended against the
published implementation.

`nightly-restore-diffs.json`, test XML/logs, exact head/tree and the Bazel exit
code are uploaded as public fixture evidence. Status `bounded_current_state`
means the supported comparisons passed with the named gaps still open.
Snapshot versions are restore bases, not imported source history. Old storage
incarnations refuse after restore; this does not certify the `memory.list` RPC
continuation. This fixture does not constitute the complete private paired
census, and one execution never establishes a three-night streak.

Focused acceptance runs through the swarm's `remote-bazel` wrapper at the
candidate SHA, targeting
`//internal/integration/nightlyrestore:nightlyrestore_test`. Whole-tree RunsOn
execution and an independent GPT-6 review remain required for PR admission.
