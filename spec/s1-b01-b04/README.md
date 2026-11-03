# S1 B01–B04 paired recorder and replay

This package is the assigned S1 acceptance seam for readiness, blockers and
status categories, custom lifecycle slots, and direct/claim-next work. It keeps
three kinds of evidence in one reviewable package:

* `run-1.json` and `run-2.json` are independent executions of the pinned Beads
  reference executable. They contain the reached command outcomes, a separate
  semantic projection, explicit omitted runtime fields, source/tree/binary
  provenance, the paired migration cursors, and stable recorder/configuration
  digests. The two fresh stores must produce byte-identical normalized vectors.
* `replay` drives the real Plug Writer, service, JSON-RPC registry, and actual
  storage drivers. It runs the same fixed-ID operation sequences against memory,
  file-backed SQLite (including close/reopen), and a disposable PostgreSQL
  database. The test records complete RPC frames in undeclared test outputs and
  compares an independently built projection to the retained reference.
* `recorded_integrity_test` rejects missing provenance, a changed source pin,
  stale executable identity, incomplete clock/ID/configuration evidence, a
  projection derived by selecting authored expected values, or a mismatch
  between either independent run.

The pinned source is Beads `9d6fff760b7a314b476539198504d77ce0959656` with
tree `cde121d6e4119e454857ad1c3ca293e2791be840`. The executable is the retained
pure-Go reference build with SHA-256
`3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4`.

The card projections retain the old/reference outcome and the adopted Plug
outcome side by side. B01/B02 use `A3/D3/P74` and `A3/P22` respectively for
category and stored-order differences. B03 uses `P10/P41/D24/A3`; B04 keeps
claim semantics with `A4/P60`. The decisions are data in the manifest, not
comments hidden in assertions. Timestamps, generated revisions, random leases,
and Dolt commit IDs are omitted only where the manifest names them. The retained
reference clock provenance remains `controlled-go; sql-driver-clock-unreached`.
New runtime boundary cases bind the application and actual driver clocks:
SQLite's optional Go clock, and a real SQL `clock_timestamp()` function in an
owned PostgreSQL database. They cross claim and receipt expiry without sleeps.
SQLite has no production SQL time calls, so native SQLite SQL-clock interception
is not applicable. These cases do not back-certify the old Beads twins, the
missing hybrid-order boundary, PR251 heartbeat, or a full card. See
[the clock boundary scope](../sqlclock-coverage.md).

The B01 recording retains the historical child-ready observation and its
`hierarchy-gap-unqualified` marker byte for byte. The Plug replay now separately
asserts canonical deferred-ancestor suppression. The public HTTP regressions
`TestB01PublicHierarchyFivePermissionRulesAcrossDrivers` and
`TestB01PublicDeferredAncestorsAcrossDrivers` in `internal/integration/batchsql`
exercise the existing authorized parented-create/reparent path, all five
permission rules, atomic refusals, aliases/final images, current authority and
history, and readiness/direct-claim/claim-next agreement on memory, SQLite and
PostgreSQL, including SQL reopen. This closes that specific hierarchy behavior
gap; it does not turn the old recording into parity evidence or qualify the
whole B01 card. SQL clock instrumentation remains separate and full S1 remains
`0/28`.

I5 authorization is qualified by the existing exact-head
`internal/server/authzrpc:authzrpc_test` target (which embeds `drivers_test.go`). It checks compiled list
membership against independent point decisions on memory, SQLite, and
PostgreSQL, no-plugin user allow, forbidden/error refusal, current permission
on history, post-image checks, dependency changes, and missing-data races. The
package records that receipt in `manifest.json` rather than copying or
reimplementing PR240.

Run the focused targets through the RunsOn wrapper:

```sh
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD s1-b01-b04-focused -- \
  test //spec/s1-b01-b04:recorded_integrity_test \
       //spec/s1-b01-b04:replay --test_output=all --lockfile_mode=error
```

The whole-tree exact-head gate remains separate. This package does not certify
latency, 200-agent contention rates, or unrelated cards.

`//spec/s1-b01-b04:reference_recording` is an explicit, manual capture target.
Invoke it only through the prepared public-reference RunsOn route. Missing
archive, source, manifest, or producer-run inputs fail the test; they never
produce a skipped success. Ordinary whole-tree tests do not execute this
environment-dependent recorder. Two fresh matching captures remain required
for complete paired reference evidence.

The B02 replay also checks that public `bead.link` and `bead.fail` release a
conditional dependent through the configured failure slot on memory, SQLite
(including reopen), and PostgreSQL. This adopted outcome check does not extend
the retained Beads traces or qualify the rest of the B02 card.

## B04 public claim assertions

`//spec/s1-b01-b04:claim_acceptance` adds real JSON-RPC assertions on memory,
file-backed SQLite, and disposable PostgreSQL. It preserves the original
replay and both reference recordings. The new checks require:

* the documented `results[0].record` to contain exactly the committed winning
  postimage, with its holder, epoch, five-minute lease and one claim version;
  the recorded B04 winner's identity, title, priority and status are compared
  directly, and the ready remainder is compared to the same reference;
* same-holder claims to preserve rows, head, history and lease while retaining
  a durable no-op receipt, including after SQLite closes and reopens;
* foreign-holder, custom non-ready category, blocked-source and malformed
  selector refusals to leave rows, head, changesets and versions unchanged and
  create no successful receipt;
* a resource/type selector to reach qualifying work beyond 1,001 ineligible
  prefix rows in a 1,004-row real store, preserve unrelated ready work, and
  replay equivalent set selectors without a second claim;
* an actually empty front to return JSON null with exactly one empty H+1
  changeset and durable receipt; retrying that key after a later arrival
  (and SQLite reopen) must still return null without claiming the arrival;
* two concurrent public claim-next requests to return distinct hydrated
  winners, each with exactly one claim version and changeset.

The front traversal uses fifty-row seed writes to establish all 1,004 rows
and renews the host's existing ten-second store lease between batches and
claim stages. It does not replace the separate 1,000-operation atomic-write
boundary gate or increase a lease TTL or write deadline.
Claims use the existing no-plugin public host and anonymous agent labels
(P60). Full installed-policy authorization, actor spelling/pool aliases,
ephemeral workflow producers, SQL-generated clocks and the 200-agent
contention benchmark remain separate obligations. A claim version's controlled
Go timestamp is checked; no new `started_at` field or public result contract is
introduced. These assertions do not qualify a complete B01-B04 card or S1.
