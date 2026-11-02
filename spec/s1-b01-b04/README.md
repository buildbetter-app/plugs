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
`33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3`.

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
