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
ordinary exact-count qualification.

The connected cases cover ordered atomic batches and late refusal (B21), a
single current-revision CAS winner with typed loser details (B22), title
substring search and an unpaged type-summary count (B23), untimed defer across
a controlled application/driver clock advance and explicit reopen (B24), and memory save/list
with durable replay after SQLite reopen (I1). The B23 type summary is the
existing explicit `groupBy:["type"], fields:[]` surface; an ordinary exact
predicate-count wire is the separately adopted 2026-11-02 OPT_IN contract:
request-level `countOnly:true` returns `{items:[],asOf,count:N}`, including zero,
under current permissions and the ordinary 20,000 cap. Missing/false preserves
saved definitions and ordinary paging. This package's recorded reference twins
and type-summary proof remain bounded to their original cases.

The named `TestB23OrdinaryExactPredicateCountWireAcrossDrivers`,
`TestB23OrdinaryExactCountCurrentAsOfAcrossDrivers`,
`TestB23OrdinaryCountOptInCompatibilityAcrossDrivers`,
`TestB23OrdinaryCountWireValidationAcrossDrivers`, and
`TestB23OrdinaryExactCountCapAndRefusalAcrossDrivers` in
`internal/server/authzrpc/predicate_count_test.go` exercise the ordinary count
through authenticated, schema-checked HTTP on memory, SQLite and Postgres with
the installed fixed-permission policy. The count batch adapter also has a
named comparison against actual guarded `bead.get` responses for current fixed
principals and caveats. Qualification requires exact-head focused and whole-tree
RunsOn receipts for that bounded count behavior; they do not turn the
older grouped summary into ordinary-count proof or certify all of B23/S1.
Full S1 remains 0/28 and E3 remains 4/5 until their separate proof exists.

The paired B23 ordinary-count reference route uses the pinned public
`9d6fff760b7a314b476539198504d77ce0959656` Go-built CGO0 executable through
managed-proxied Dolt 2.2.0. The recorder authenticates the same-run public
bundle and Dolt archive before extracting executable members, writes the
private `.dolt/config_global.json` with modes 0700/0600, probes
`versioncheck.disabled` before the exact version, and binds `HOME`,
`DOLT_ROOT_PATH` and `DOLT_DISABLE_EVENT_FLUSH=1` per fresh workspace. Every
command retains exact stdout/stderr bytes, SHA-256 digests, exit/outcome and
argv. The owned `bd dolt stop --json` result must report `stopped:true` and
`verified:true` before cleanup; failed shutdown keeps the workspace.

`record_ordinary_count.py` emits one framed packet containing two independent
recordings. The selected vector is open=4, open title `needle`=3, closed=1,
absent=0, grouped open total=4, then open `needle`=2 and changed title=1.
`extract_ordinary_count_packet.py` validates framing, source/profile/runtime
identity, raw stream digests, command sequence and equal projections without
importing or executing the recorder. This external profile is not claimed
binary-equivalent to `reference-a.json`/`reference-b.json`, and it does not
promote the untimed B24 deviation. The capture target is manual; ordinary
count acceptance embeds the genuine twins extracted from the separate capture
described below. Source review and fixture policy results are not runtime proof.

Done = two fresh ordinary-count recordings pass the declared vector under this
explicit public profile, proven by the original RunsOn log/archive, matching
candidate/source/runtime identities, lossless inner and outer stream frames,
and independent offline extraction, followed by exact-head memory, SQLite and
PostgreSQL acceptance; source checks and another card's runtime result do not
count. `ordinary_count_test.go` is registered in `acceptance` with
`count-reference-first.json` and `count-reference-repeat.json`. It maps
reference task/open/closed to Plug issue/todo/done and
checks current, historical and SQLite-reopened counts through the existing RPC
fixture. The existing authenticated HTTP count tests remain a separate required
part of the eventual focused gate. No Query or authorization implementation is
changed by this route.

The genuine count twins were captured at source
`22f09c9b6bff090e080f1a6addbb50b8f8a436fc` (tree
`6f738eee9acdff22ac2285996280ea6c791d5baa`) in RunsOn run `41572443347`.
The reviewed strict offline extractor accepted the original archived frames
and independently derived all seven count values from each fresh workspace.
Both public shutdown replies were verified; raw stream equality was false,
while normalized transcript and projection equality were true. The first twin
SHA-256 is `cc392e04165b2a23498020550af9d67c5fe94a3fdde575d3c6a4e272e60bd4fa`;
the repeat is `bf86ed9979b723141e232ca74f67e717a261d0f0eb14b09984762d6758c15d20`.
These files retain the exact extractor output bytes. This capture proves the
selected public count vector. Plug acceptance and authenticated HTTP count
qualification require separate exact-head gates on the child that registers
these twins. The older reference files and B24 deviation remain unchanged.

Source attribution: archive provisioning and private runtime setup were adapted
from B01 `7bdeea0c` and B09 `cb40cab68` source atoms. The B23 profile, commands,
count projection and extractor require their own review and execution. Successful
normalization is not assumed: both raw-stream and normalized-transcript equality
are reported as observed; the exact projected counts must agree.

After exact-source review, the manual capture command is:

```sh
/tmp/plug-swarm/bin/remote-bazel "$PWD" <reviewed-source-sha> b23-count-capture -- test //spec/s1-b21-b24:ordinary_count_capture_test --cache_test_results=no --flaky_test_attempts=1 --test_output=all --test_env=GO_TEST_WRAP_TESTV=1 --test_env=PLUG_PUBLIC_REFERENCE=41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2
```

Before capture, the fixture-only focused command selects
`//spec/s1-b21-b24:ordinary_count_capture_policy_test` and
`//tools/lexicon:lexicon_gate`. The Python fixtures mock child execution and
verify timeout bytes, failed shutdown retention, forged counts, changed argv,
private environment and exact framing; they execute no source engine. The real
capture preserves setup Git output and every command's original stdout/stderr,
length, digest, exit, timeout and argv in the archived test log. The Go launcher
also frames its complete Python stdout/stderr, including failures. A failed
capture stops before a second workspace is attempted. Public shutdown failure
retains the owned workspace and runtime until the disposable runner is removed.
`--retain-block` on the extractor is diagnostic only and cannot emit twins.

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

The runtime clock controls and their limits are documented in
[the clock boundary scope](../sqlclock-coverage.md). The retained Beads reference
bytes are unchanged and are not upgraded to controlled-SQL twin evidence.

The B24 independent-clock matrix binds separate application and durable clocks
to the production RPC service, Writer, view service and real drivers. Stale
list/count uses the application cutoff and actual persisted version timestamps:
before and exactly at one day it excludes the row; the next representable tick
includes it. A durable-only advance cannot alter that membership, and a saved
continuation retains its cutoff after an application-only advance. Deferral
receipts follow durable time even when application time has already passed
their expiry. Independently advancing both inputs together checks before,
exactly at and after expiry, including actual PostgreSQL SQL/Go equality at
each phase. Memory/SQLite have no production SQL time expression.

Every read/refusal/replay checks the complete logical state and all physical
application tables on SQL stores. SQLite/PostgreSQL actually close/open before
readback; memory remains a same-store readback. Untimed deferral still requires
explicit reopen. These public Plug controls do not qualify native Dolt clock
inputs, native timed wake, protected-source import, native caps or a full card;
those require their own accepted producer material and recording evidence.

`b22_remaining_test.go` is the 2026-11-04 B22 refill (a37f2bc80) ported onto
current main. Every authorized `precondition_failed` refusal now also carries
`details.ref`, the resolved guard address, including guards outside the
mutation target. `b22_close_cas_test.go` carries the B08 line item: `bead.close`
and a `tx.apply` close naming a stale expected revision are refused with
`reason=revision`, `currentRevision` and `ref`, publish nothing, leave the key
reusable across reopen, and the expectation is checked before a repeated close.
Whether the native Beads force close applies any compare-and-set needs a
reference recording and stays open.
