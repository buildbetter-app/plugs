# Plug behavior acceptance

Done = the initial behavior scenarios execute against real Plug layers in a
focused PR, proven by exact-head local Bazel results; authored expectations,
harness unit tests, and an unexecuted scenario do not count as connected proof.

Scope = this directory and `internal/acceptance`; production implementation,
database conversion, reference recording, and performance qualification belong
to their owners.

The source cards are the canonical files in
`/home/dev/plugs/engdocs/design/plug/spec/behaviors/`: `work.md`, `content.md`,
`history.md`, and `surfaces.md`. No legacy application packages are imported.
The connected scenario expectations are authored Plug contract assertions; the
separately reviewed Beads-base vectors live in [`spec/recorded/`](recorded/).
Passing either suite alone does not establish cross-driver reference parity.

## Running the connected scenarios

```sh
bazel --ignore_all_rc_files test //spec:acceptance //spec:cli_acceptance \
  --jobs=2 --test_output=all --lockfile_mode=error
```

`//spec:acceptance` composes the real in-process transport, RPC registry, core
service, ontology adapter, Writer, and memory driver. Graph setup and batch
scenarios use the same Writer directly for operations without a bound RPC
method. No seed transaction, substitute handler, or fake store is involved.
The selected assertions cover ready categories/order/paging, ordinary and
conditional blockers, direct claim/no-op/foreign refusal, comments, historical
reads/no-op suppression, atomic batches, concurrent CAS, and pre/post warnings.
No-op receipts are replayed after another mutation advances the head; mixed
batches must preserve each unchanged row's own revision and suppress extra
versions and post callbacks.

Every RPC stack fixes UTC time, finite IDs, store identity, incarnation, and
the cursor signing key. A positive lifecycle runs twice in fresh stores and
compares every request/response byte. Full exchanges, including those preceding
a failure, are saved as `*.rpc.json` in Bazel's undeclared test outputs under
`bazel-testlogs/spec/acceptance/test.outputs/`. The trace retains validated wire
response bytes; semantic projections normalize object keys separately. JSON
artifact formatting does not certify whitespace-identical artifact files.

`//spec:cli_acceptance` launches the actual Bazel-built `plug` executable in a
disposable memory session. It covers all eight bound verbs, historical reads,
idempotent close replay, and a typed stale-CAS refusal that stops the session
with a nonzero exit. The executable uses its production clock and random IDs;
these CLI checks assert semantics and do not claim byte-repeatable traces.

All scenarios execute, including unsupported behavior that fails its contract
assertion. Current results and dependency gaps belong in the exact-head test
report; a failing scenario is not an accepted refusal or a skipped card. The
table below records what the full cards still require beyond this slice.

## Dispositions and remaining proof

| Card | Disposition retained | Assertions and evidence still required by the full card |
| --- | --- | --- |
| B01 | deviate: A3/D3/P74 | Exact category/blocker/defer membership, page-before-limit filtering, deterministic ready order; record old hybrid order separately and measure the one-minute stored-order lag. |
| B02 | deviate: A3/P22 | Category-driven blockers, completed/canceled satisfaction, failure-only conditional satisfaction and condition-unmet events; transitive, parent, diamond, and deferred cases. |
| B04 | keep, with A4/P60 additions | Direct claim, atomic holder/status/time/lease, foreign-holder refusal, eligible walk and null empty queue; full 200-agent/lagging-snapshot benchmarks remain separate. |
| B15 | keep | Exact nonblank body and author, unknown-target and blank-body refusals, deterministic thread order/stamps, no anchor field edits, and one appropriate history/event entry. |
| B18 | deviate: P6/P30/P73/D26/P57 | Gapless changesets, one version per changed record, exact as-of boundary/refusal, base/diff replay; current permissions on old versions need the authenticated lane. |
| B21 | deviate: P2/D28 | Ordered atomic operations, backward references, 1,000-op cap, rollback, one revision, replay without repeated post calls; no second partial-close transaction. |
| B22 | deviate: A4/A6/P49 | Current/stale CAS and field guards, a single concurrent winner, rollback of records/logs/projections, retries, idempotency and epoch fencing. |
| B28 | deviate: P3/P24/P29b/P66/D28 | Fail-closed pre, explicit fail-open warning, committed post failure in `result.warnings`, no post on rollback/replay, dead letters and alerts. |

The cards' dropped surfaces remain dropped: command prose and envelopes (P46),
backend-specific commit/merge history (P6/P7), and script hooks (P29b).
Ephemeral-plane differences require a supported production plane before they
can be claimed as exercised. Unsupported behavior must return an explicit
refusal; it must never be represented by a successful test stub or a skip.

## Deterministic measuring tool

`internal/acceptance` supplies a fixed UTC clock, an explicit finite ID sequence,
byte-level JSON-RPC traces, and required JSON-pointer projections. Install its
inputs in the actual service, writer, and storage clock/ID seams. Exhausted IDs
fail; they do not fall back to random values. Advance time explicitly between
steps. Every fixture states its initial clock and IDs.

Projections retain array order, verbatim strings, null, and lossless JSON number
spellings. Object keys are sorted only for byte comparison. This normalization
is not JCS. Missing paths, duplicate JSON members, malformed envelopes, wrong
request IDs, and responses containing both result and error fail immediately.
Full exchanges remain available alongside any selected semantic projection.

The harness's own unit tests use controlled transports to test measurement
failures. Those tests are not runtime or vertical-slice evidence.

## Recorder and converter follow-up contract

The next recorder task must deliver an executable reference runner, rather than
copying these expected values into files called goldens:

1. Build the reference executable from frozen commit
   `9d6fff760b7a314b476539198504d77ce0959656`. Use disposable stores below
   `/tmp/plug-swarm/work/`; never initialize a checkout or access production.
   Fixes on the separate reference branch require their own source pin and
   reviewed re-record.
2. For each card, commit a twin operation sequence with fixed principal and
   agent label, prefix, complete configuration, both schema cursors, driver,
   UTC clock schedule, explicit ID/collision/counter inputs, and any graph
   seed. Instrument every reached Go **and SQL** clock, including comment
   stamps, lease deadlines, and readiness's 48-hour cutoff. A controlled Go
   clock with a live SQL clock cannot certify a time-dependent keep.
3. Save raw operation outcomes plus an independently produced semantic
   projection. Include ready IDs **in order**, blocked reasons, category and
   outcome, claim holder/lease deadline, verbatim comments, typed refusals,
   record revisions, versions, changesets, and event order. Record both old
   and Plug projections plus decision IDs for every deviation. Document each
   omitted field; never erase order or turn refusals into null.
4. Emit a provenance manifest with source commit, recorder commit/version,
   executable SHA-256, driver/version, main and ignored schema cursors,
   configuration digest, clock/ID/graph inputs, and raw/projection digests.
   Record twice in independently initialized stores and require identical
   projection **and provenance** bytes. A timestamped run log is separate
   from the reproducible manifest. Missing provenance must fail validation.
5. Add the actual recorded vectors and a Bazel replay test in a reviewed PR.
   Keep authored assertions separately identified, and keep historical failed
   or unverified evidence intact. Run memory first, then SQLite and Postgres
   through their actual drivers; driver unit tests cannot replace the replay.

The converter task must export `plug-dump/1` from the pinned reference build,
restore only into an empty disposable Plug store, and compare the same semantic
projection before and after conversion. Its fixtures must cover category and
conditional-edge deviations, IDs/allocator state, comments, available historical
versions versus an explicit history floor, and leaseless started records with
warnings. Verify checksums, both schema series, exclusions of ephemeral state,
and absence of fabricated historical rows. Report every missing source field or
unavailable history range instead of synthesizing a successful comparison.

These follow-ups are not implemented or certified by this harness PR.
