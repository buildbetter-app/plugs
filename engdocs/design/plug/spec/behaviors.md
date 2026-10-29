# Plug behavior catalog

Status: research-only specification for the clean-room rewrite. The Beads
reference is commit [`9d6fff760b7a314b476539198504d77ce0959656`](https://github.com/buildbetter-app/plugs/tree/9d6fff760b7a314b476539198504d77ce0959656) (`beads-base`),
with later behavior fixes re-recorded only from the `beads` branch under P82.
No Plug implementation is implied by this document.

This catalog is split because the cards are intentionally detailed. The
[work and graph cards](behaviors/work.md), [identity and content cards](behaviors/content.md),
[history and transaction cards](behaviors/history.md), and [surface and
infrastructure cards](behaviors/surfaces.md) are research input for the future
acceptance-test source.
Every card has the same fields: a short behavior statement, Beads source
function(s), exact tests that pin it, edge cases, and a Plug disposition.
`keep` means the semantic projection is retained; `deviate` names the design
change and records both projections; `drop` is only for behavior outside Plug's
contract (usually P46/P47/P48).

## Evidence and card method

All Beads source paths and symbols below refer to that frozen tree, not the
current Plug implementation. The companion [command inventory](commands.md)
and [storage census](census.md) use the same behavior pin.

The planned source census draws from the 46 role contracts in
`backend/conformance/`, the 29 protocol test files and corpus, Oracle A's 523
scenarios, the 72-item regression discovery log, the 307 command-tree paths
(306 descendants plus the root), all 13,139 Go tests, and the 72 main plus 28
ignored schema migrations. The ratchet count is 307 paths including the root.
These counts are inputs to the future census, not evidence that this research
catalog has dispositioned every item. That ratchet will require every test to
name a card, `drop:<decision>`, or `n/a:<reason>`; the store census will
require every table and column and each configuration source to have a mapping
or an explicit disposition.

Cards are expressed as Plug JSON-RPC calls, never CLI text or exit envelopes
(P46). A projection compares stable meaning: ordered ready IDs and membership,
blocked set and reasons, status category/outcome, claim holder and lease state,
labels/comments/memory values, typed errors, version/history deltas, changeset
and journal rows. A future binding `keep` card requires a recorded Beads
golden; a future binding `deviate` card stores both projections and its
decision ID (P82). The cards in this research catalog record proposed
dispositions only and do not carry those acceptance certificates.

## Deferred census and golden artifacts

This PR deliberately packages the research cards without the artifacts needed
to make them a binding parity set. The following are explicitly deferred to
the E1/E2 recorder and census work:

- the per-item ledger for the role contracts, protocol corpus, Oracle
  scenarios, regression entries, all 307 command-tree paths, and all 13,139 Go
  test functions, with each item mapped to a card, `drop:<decision>`, or
  `n/a:<reason>`;
- `spec/provenance/` twin sequences, controlled-clock inputs, recorder
  manifests, canonical Plug projections, table digests, and decision-ID map;
- byte-identical Beads golden/projection pairs for `keep` and `deviate` cards,
  including the pinned source commit and any separately reviewed P82 fix; and
- the paired main/ignored store census ledger covering every table, column,
  ignored row class, and configuration source.

Until those artifacts are recorded and reviewed, this catalog is non-binding
research input. No card here is a parity certificate, and the future
`bazel test //spec:acceptance` target is not claimed to exist in this PR.

## Deferred golden-recording harness (design only)

The following is the deferred harness contract, not an artifact produced by
this PR. It remains the required shape for the later recorder and replay work.

1. Build `bd` from the frozen `beads-base` commit in a clean disposable
   workspace and database. Reviewed fixes on `beads` are separate recorder
   inputs and never silently replace the pin. Each card has a short twin
   sequence in `spec/provenance/`, plus a fixed actor, configuration, schema
   pair, and database driver.
2. Add a test-only clock seam to every Go `time.Now` path reached by a card and
   bind SQL `UTC_TIMESTAMP()` and `CURRENT_TIMESTAMP`/`ON UPDATE` paths to the
   same instant. Seed fixture timestamps rather than relying on wall time. The
   clock must cover the hybrid readiness 48-hour cutoff, five-minute leases and
   reclaim grace, stale/defer, comment stamping, and 30/90-day compaction.
3. Make ID results deterministic with explicit prefix, title, description,
   creator, timestamp, nonce, existing collision rows, counter state, and
   seeded child/wisp counters. For graph behavior, replay fixed-seed random
   traces containing both planes, all dependency kinds, custom status
   categories, parent edges, gates, and cycles.
4. Record canonical JSON projections and table digests, retaining exact event
   sequence and ordering. Provenance includes Beads commit, recorder version,
   schema pair, driver, clock seed, graph/ID seed, config and adaptive-ID
   settings. Normalize only declared nondeterminism; never sort away semantic
   order or hide a typed refusal.
5. Run the recorder twice and require byte-identical golden and provenance
   output. Replay each vector against Plug on memory and SQLite, then Postgres
   in the integration lane; run nightly real-store `bd` versus restored-Plug
   diffs. A re-record is a reviewed PR naming the source commit and digest.
   Independent projection code, skipped-test scans, and a fresh second run are
   anti-gaming checks; a test weakened to make a golden pass fails the census
   gate.

Time-dependent cards are `keep` only when both Go and SQL clocks are controlled.
The future acceptance target is `bazel test //spec:acceptance` on memory/SQLite
and the Postgres integration lane.
