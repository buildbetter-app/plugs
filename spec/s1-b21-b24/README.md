# S1 B21–B24 and I1 connected acceptance

This package is the dedicated paired-store acceptance surface for the four
cards assigned to the S1 B21–B24 lane. It uses the production Writer, service,
RPC registry, view binding and real memory or file-backed SQLite drivers. The
batch receipt-window and untimed-defer cases also use real PostgreSQL. IDs,
UTC clock, project, actor and cursor key are fixed; every RPC exchange is
retained as an undeclared-output trace. SQLite is closed and reopened before
the replay assertions.

The reference files are two independently recorded Beads twins from the
pinned `9d6fff760b7a314b476539198504d77ce0959656` executable. Each raw
transcript retains the executable argv, actor, workspace, stdout, stderr,
exit status and minted IDs. `reference_test.go` derives the stable projection
from those raw commands and checks both source and recorder digests; it does
not accept a hand-written summary. The Beads records cover the ordered batch
and last-item rollback slice. The existing
`spec/recorded/b24_untimed_defer_deviation.json` bytes remain unchanged and
are the retained timed-defer source evidence. Plug's adopted B24 projection
is untimed defer plus explicit reopen, so no timed wake is claimed here.

The projections carry the selected design decisions: B21 `P2`/`D28`, B22
`P3` and the A6 `G1`–`G5` guarantees, B23 `P31b`/`P74`, and the recorded
2026-10-30 B24 untimed-defer decision. These IDs identify the source-backed
semantic choice; they do not turn a bounded title or type-summary case into
the unresolved ordinary exact-count contract.

The connected cases cover ordered atomic batches and late refusal (B21), a
single current-revision CAS winner with typed loser details (B22), title
substring search and an unpaged type-summary count (B23), untimed defer across
a controlled application/driver clock advance and explicit reopen (B24), and memory save/list
with durable replay after SQLite reopen (I1). The B23 type summary is the
existing explicit `groupBy:["type"], fields:[]` surface; an ordinary exact
predicate-count wire member is still a separate contract and is not inferred
from this summary.

Run the focused target with:

```sh
bazel test //spec/s1-b21-b24:acceptance --test_output=all
```

All qualification commands for this package are dispatched through the
RunsOn `remote-bazel` wrapper; no local Bazel result is a certificate.

The runtime clock controls and their limits are documented in
[the clock boundary scope](../sqlclock-coverage.md). The retained Beads reference
bytes are unchanged and are not upgraded to controlled-SQL twin evidence.
