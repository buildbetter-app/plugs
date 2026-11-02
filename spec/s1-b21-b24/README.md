# S1 B21–B24 and I1 connected acceptance

This package is the dedicated paired-store acceptance surface for the four
cards assigned to the S1 B21–B24 lane. It uses the production Writer, service,
RPC registry, view binding and real memory or file-backed SQLite drivers. IDs,
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
a controlled Go clock advance and explicit reopen (B24), and memory save/list
with durable replay after SQLite reopen (I1). The B23 type summary is the
existing explicit `groupBy:["type"], fields:[]` surface; an ordinary exact
predicate-count wire member is still a separate contract and is not inferred
from this summary.

The B22 hash-CAS regressions exercise field, metadata and memory mismatches,
absent expectation targets, corrected-key reuse and committed-response replay.
Authorized refusals report `details.reason` as `hash` or `revision`; absent
records do not invent a revision token. Expectation-only guards must pass the
existing pre-image authorization decision before any refusal details are
released. Denied or hidden expectations keep their refusal boundary, and no
CAS refusal publishes data, history or a successful receipt. These are direct
Plug contract checks, with real PostgreSQL field/metadata cases in
`internal/integration/postgresacceptance`; they add no Beads recording or
full-card parity claim.

The separately authorized B21 `CloseOp` (`kind: "close"`, `target`, optional
`reason` and `expect`) enters the ordinary close slot at its operation position.
Its preconditions read the ordered in-request image; a revision guard refuses
a row written earlier in the request. The enclosing ChangeSet's expectations
keep their existing pinned-snapshot semantics. Core selects the slot without
running subscribers; after mutators, ontology checks every request prefix and
the complete final graph. Later unlink/patch operations cannot rescue an
earlier refused close. Every required expectation target and the per-op close
permission must be authorized before private CAS details are released. A
refusal leaves no durable row, history or successful receipt; accepted batches
publish one revision and per-op results. Replays return the stored commit,
including after SQLite/PostgreSQL reopen. No new reference capture or full B21
card qualification is inferred from these direct Plug regressions.

Run the focused target with:

```sh
bazel test //spec/s1-b21-b24:acceptance --test_output=all
```

All qualification commands for this package are dispatched through the
RunsOn `remote-bazel` wrapper; no local Bazel result is a certificate.
