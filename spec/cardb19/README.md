# B19 native archive acceptance slice

This package selects four cases from `behaviors/history.md` B19 at admitted
base `a9a3d6266e4c10c0acfa2deec92f5bbd08140b44`:

| Case | Observable proof |
| --- | --- |
| Native head snapshot | Two Writer commits, deterministic paged dump across source SQLite close/reopen, admin restore into empty SQLite, preserved current state and store ID, new incarnation and floor 1, then target close/reopen and another public read. |
| Historical refusal | After native restore and reopen, the current base resolves; the discarded prefix is `cursor_expired {floor:1}`, unknown/future addresses are `not_found`, and source/old-target incarnations are `incarnation_changed`. Returned bounds/incarnation pairs are exact. No fallback state or write is returned. |
| Native full-history refusal | A genuine native dump contains both versions/changesets; admin restore returns `restore_violation` rather than flattening them. Target metadata and zero records survive close/reopen unchanged. |
| Populated target refusal | `restore_requires_empty` preserves an unrelated committed target record and all target metadata across close/reopen (P44). |

`cli.OpenLocal` constructs the actual service, single Writer, registry and
in-process byte transport. `cli.Session` calls bound `admin.dump` and
`admin.restore.begin/chunk/finish`; their implementation is the native admin
`restore.Manager`. Dump pages are 512 bytes. A package-local injected presence
verifier admits only this test's local proof and requires a method and digest.
This is anonymous local composition with a test host verifier, not authenticated
human-presence security certification or HTTP deployment proof. Graph setup is
through ordinary create/update RPCs. Clock/IDs are small deterministic callbacks;
there are no private ontology or acceptance-helper imports or source copies.

The separate converted-archive `converterroundtrip.RestoreWithLease` operation
is not called or certified. Its converter profile requires independently
verified converted input, not native `head`/`full` archives. Native Manager
success does not certify that importer or imply a missing native producer.

The reference is pinned to Beads source
`9d6fff760b7a314b476539198504d77ce0959656`, tree
`cde121d6e4119e454857ad1c3ca293e2791be840`. `source-binding.json` records
independent verification of all 5,087 tracked source blobs, no extra Go inputs,
selected source-file SHA-256 values, and the retained executable/build-info
digests. The source-backed command implementations are `export.go` and
`import.go`; `asof_read.go` and `version_history.go` establish why removal
seams alone are insufficient evidence of production redaction.

`record_reference.py` ran the verified retained executable in two independent
fresh source/target pairs, with isolated user configuration. It retained exact
JSONL export bytes and imported public-read JSON in `testdata/recording-1/` and
`testdata/recording-2/`. `reference.json` binds their raw digests, distinct
workspaces, workload, command-transcript digests and four-field projections.
Tests verify both raw recordings and derive expected state from their observed
output. They project only title, description, status and priority; source IDs,
clock values, issue type and native revision/incarnation are not compared.
The reference task maps to Plug's default issue type. The sole adopted status
mapping is recorded `open` to native `todo`, the default unstarted slot under
B03/A3 (`design.md` ontology example, `ontology/default.go`, and the admitted
converter's `targetStatus`). `adoptedState` applies this explicit mapping only
to the Plug comparison; the reference bytes/projection remain unchanged. Other
statuses fail closed. This slice makes no cross-format/type/ID parity claim.
Beads JSONL is not `plug-dump/1`.

Complete raw commands, stdout/stderr, exit codes, source verification and build
metadata remain under
`/tmp/plug-swarm/ci-local/ACCEPT-B19-RECORDED-SLICE-20261030-preflight/`.
The recorder refuses an existing evidence directory or retained run directory;
new recordings must use fresh evidence and an empty destination, never replace
these retained originals. The Bazel test preserves actual Plug JSON/archive
outputs in its undeclared-output artifacts.

The native format, head-only floor/new lifetime and P44 refusals are adopted
Plug dispositions from B19/P20/P44/P45, not Beads parity claims. Full native
history restore success, a full Beads history exporter, production redaction,
removal reasons/holds, protected/path-scoped revert, plugin artifact loading,
converted-store migration and cutover are **UNRUN** here. The selected native
full-history refusal is a pass only when no data is silently flattened. Any
future positive case needs its actual producer and owning implementation before
this package may add an acceptance claim.

The discarded-prefix wire assertion follows `contracts/errors.md` runtime
mapping `ErrTruncated -> cursor_expired {floor}` and design interpretation 7.
It does not classify a restore-discarded prefix as a redacted record or claim
that the distinct `history_gone` redaction outcome has been exercised.

Run `//spec/cardb19:cardb19_test` with Bazel 9.3.0. The final handoff additionally
requires the complete touched/resource reverse closure and full lexicon gate,
with a clean unchanged committed head and retained local evidence. No hosted
execution, publication or full-card/every-command certification is implied.
