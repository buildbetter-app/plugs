# Caller and shadow rehearsal preparation

The agent command slice uses the existing RPC contract: `brief`, `claim-next`,
and memory list/get/save/delete join the existing get/update/comment/close
commands. `plugins/plug/SKILL.md` gives the rewritten caller instructions.
Unknown flags fail; writes retain explicit replay keys, warnings and the
configured authenticated service identity.

`RunDisposable` executes three preparations for a selected memory or SQLite
driver. Each preparation owns two fresh stores, ephemeral API keys and loopback
HTTP/MCP endpoints. It seeds only synthetic tasks and memory, dumps the source,
restores an empty target, compares ready identities/content, checks the new
incarnation, and runs parsed agent commands through authenticated HTTP and MCP.
It checks three distinct claims and completed task lifecycles, then attempts
reverse conversion of the post-workload native dump. The owned directory,
stores, HTTP clients and listeners close before a successful report is returned.
There is no input for a server URL, DSN, bearer, command adapter or cloud account.

Agent workloads use service API keys. Dump/restore preparation uses a separate
private synthetic trusted-host session and presence verifier; service keys are
explicitly tested to fail admin admission. This fixture supplies no real login
or D32 acceptance evidence. The synthetic admin session is revoked on cleanup.

The baseline is the same-build HTTP fixture. It is **not** a measured Beads
factory pool. An authenticated loopback MCP fixture is **not** authenticated
BuildBetter staging. Neither fixture uses production or customer data.

## Execution

`three_rehearsals.sh` defaults to a harmless plan. From a Plug checkout:

```sh
tools/rehearsal/three_rehearsals.sh --plan
tools/rehearsal/three_rehearsals.sh --run FULL_COMMIT_SHA
```

The second command uses `/tmp/plug-swarm/bin/remote-bazel` exclusively. It
dispatches the caller, rehearsal and lexicon targets on RunsOn, supplies the
exact source SHA/tree, disables cached test results and prints sanitized
`I8_PREP_REPORT` rows for memory and SQLite. Both the runner's direct receipt
and its SHA/tree binding must be reviewed. Run the whole-tree gate separately
on the same final head:

```sh
/tmp/plug-swarm/bin/remote-bazel "$PWD" FULL_COMMIT_SHA i8-whole -- test //... \
  --test_output=errors --cache_test_results=no \
  --test_env=R1_SOURCE_SHA=FULL_COMMIT_SHA --test_env=R1_SOURCE_TREE=FULL_TREE_SHA
```

The `//tools/rehearsal/cmd/shadow-rehearsal:shadow-rehearsal` executable also
defaults to a plan. Its `--local --driver memory|sqlite --source-sha SHA
--source-tree TREE` mode runs the same owned fixtures on an authorized disposable
host. Run Bazel only through RunsOn in this swarm.

## Metrics and evidence windows

Rows contain only catalog method names, bounded error codes, counts and
p50/p95/p99 milliseconds. No payload, ID, principal, token or raw error text is
included. Comparison rejects unknown methods/flags, missing method coverage,
an increase in any method's error-code rate, an absolute task-completion delta
greater than five percentage points, and p95 at or above an explicit target.
Missing targets cannot pass. This tiny workload does not infer latency targets
or qualify the design's 100,000-record workload.

`--gate-evidence /tmp/plug-swarm/work/FILE.json --source-sha SHA
--source-tree TREE` loads a sanitized trusted-host record, refuses symlinks,
unknown fields, trailing objects, oversized files and a mismatched candidate,
then calls `EvaluateGate` with the clock at that execution. JSON fields follow
`GateEvidence` in `gate.go`; `latency_targets_ns` values are integer nanoseconds.
The evaluated timestamp and outstanding waits are printed; exit 3 means WAIT.
The evaluator requires:

- Three distinct clean receipt digests bound to that exact SHA/tree, completed
  on consecutive UTC dates; no future receipt. The latest must be at most
  seven days old **when the gate executes**, even if it was fresh when recorded.
- A completed shadow window of at least fourteen days and seven measured,
  complete UTC dates immediately before the window's end date. Each day is
  compared against the reference pool and explicit per-method targets.
- Separately reviewed reference-pool, managed-failover, complete-rollback,
  platform-safety and authenticated-staging evidence. Booleans in a trusted
  record describe those external reviews; they do not produce or authenticate
  the underlying proof.

## Qualification boundary

Three preparations in one session are not three nightly rehearsals and cannot
replace a shadow fortnight or its last measured week. Reports always say
`r1_qualification: WAIT`; same-session receipts are deliberately unclean for
gate admission. The native-head reverse profile currently refuses the dump.
That measured refusal is WAIT, and a future conversion alone would still need
import, loss accounting and source-system readback. No image, prompt or plugin
redeployment is performed. Managed HA, RPO/RTO and staging remain unqualified.

The separately reviewed R1 failover/rollback preparation scripts preserve the
bounded A1 fixture evidence; this package does not recertify that historical
head. This is a preparation and evidence checker, never a complete G2 decision
or permission to switch, deploy, merge, access staging or provision paid cloud.
