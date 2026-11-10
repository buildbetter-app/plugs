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
incarnation, and runs a four-agent, eight-task sampled factory pool through
authenticated HTTP and MCP. The MCP fixture uses the generic `plug_call` tool
with canonical method names; projected core tools use snake-case names. It
checks distinct claims, completed task lifecycles and the empty ready queue,
then compares three actual public read calls over independent HTTP and MCP
transport frames on the same quiescent restored store. The comparison keeps
request IDs, caller metadata, complete results and exact numeric tokens; the
ordinary sampled lifecycle writes remain outside its read-only profile.
It then attempts reverse conversion of the post-workload native dump. The owned
directory, stores, HTTP clients and listeners close before a successful report
is returned.
There is no input for a server URL, DSN, bearer, command adapter or cloud account.

Agent workloads use service API keys. Dump/restore preparation uses a separate
private synthetic trusted-host session and presence verifier; service keys are
explicitly tested to fail admin admission. This fixture supplies no real login
or D32 acceptance evidence. The synthetic admin session is revoked on cleanup.

The baseline is the same-build HTTP fixture run through the same sampled pool.
It is **not** a measured Beads reference factory. An authenticated loopback MCP
fixture is **not** authenticated BuildBetter staging. Neither fixture uses
production or customer data.

The three sampled preparations retain their descriptor-only rollback rows and
explicit WAIT results. Before returning PASS, `RunDisposable` also invokes the
separate complete owned edition transition once, requires its source-bound real
artifact/readback/cleanup receipt, and includes it as `runtime_rollback`. A
failed transition or incomplete receipt blocks the caller preparation. Neither
row qualifies a production deployment or reverse Beads data rollback.

`RunDeployableRollback` separately builds two static Bazel edition profiles from
one exact clean source SHA/tree and builds real scratch Docker images with
network-disabled builds and no registry pull. It saves reloadable image archives,
checks the image config hash and source/executable labels, and pins executable,
image, prompt, native plugin/module configuration and native snapshot file hashes.
The editions use the **same tested source/tree**: this proves disposable mechanics
and artifact binding, not old-release or schema/backward compatibility.

The runtime entry point only wires signal lifetime to `ServeRollbackRuntime`.
Each runtime has one owned mount, an internal Docker network, a read-only root,
no Linux capabilities, and a TLS listener. An owned loopback byte forwarder reaches
only the inspected address of that labeled container; it does not terminate TLS
or change HTTP admission. API keys, synthetic admin session tokens and the TLS
private key stay in memory. A one-shot 0600 Unix socket privately exchanges the
bootstrap credentials; credentials are absent from files, Docker arguments and
logs. There is no configurable external endpoint or DSN.

A complete generation of executable, rewritten prompt, plugin/module config and
snapshot is staged and digest checked, then activated with one relative-symlink
rename while the previous service is stopped. The real running executable checks
its own digest and edition. The orchestrator inspects the running image ID,
restores the exact native head snapshot through public authenticated admin RPC
into fresh empty data, and compares public ready/memory contents.
Restore consumes the installed generation's `data.dump` and checks the exact
bytes against its pinned digest before admitting a restore. Changes to the
original bundle source after activation cannot change the restored data;
missing or changed installed bytes refuse while the runtime remains empty.
Prompt canaries parse the installed commands through the existing CLI and call
the public RPC.
The native `authn.apikey` plugin supplies real process-local service keys. The
candidate adds the actual MCP transport module; initialized tool discovery and
unauthenticated refusal prove it is mounted. Old and restored-old profiles must
return 404 for that removed module. These are native plugin/module configurations,
not replacement WASI plugin artifacts or a historical plugin compatibility proof.

The drill runs old → candidate → old, changes candidate data, then restores the
old checkpoint into another fresh data directory. Post-rollback readback must
recover the old marker and contents and omit the candidate-only row. Every restore
must mint a new public `server.hello`/`bead.ready` incarnation; old memory cursors
must fail with `incarnation_changed` while current cursors continue. An explicit
write key must start without a prior receipt in each restored incarnation and
then replay the identical response. The final old prompt and module canaries
run after rollback. Native head snapshots deliberately do not claim original
receipt/history replay or Beads reverse/reimport fidelity.

Digest and source/image pin failures cannot activate a partial generation.
A failed stop cannot start a successor. A failed candidate install/start/readback
attempts one owned old-bundle recovery and still returns BLOCK. A failed rollback,
service shutdown or cleanup also returns BLOCK. Tests corrupt an actual installed
prompt after the candidate/final restore and check recovery/failure and cleanup.
Removal checks ownership labels and verifies that no owned containers, images,
networks or work directory remain. It never cleans another lane's resources.

## Execution

`three_rehearsals.sh` defaults to a harmless plan. From a Plug checkout:

```sh
tools/rehearsal/three_rehearsals.sh --plan
tools/rehearsal/three_rehearsals.sh --run FULL_COMMIT_SHA
```

The second command uses `/tmp/plug-swarm/bin/remote-bazel` exclusively. It
dispatches the caller, rehearsal and lexicon targets on RunsOn, supplies the
exact source SHA/tree, disables cached test results and prints sanitized
`I8_PREP_REPORT` and separate `I8_RUNTIME_TRANSITION` rows for memory and SQLite.
Tests discover and validate the exact clean RunsOn checkout through
`PLUG_SOURCE_MODULE_ROOT` when explicit `R1_SOURCE_SHA`/`R1_SOURCE_TREE` pins are
absent; supplied pins must match. Declared Bazel source runfiles must match that
checkout, and executable data inputs must be the explicit static edition targets.
Synthetic source markers are accepted only as negative test inputs. Both the runner's direct receipt
and its SHA/tree binding must be reviewed. Run the whole-tree gate separately
on the same final head:

```sh
/tmp/plug-swarm/bin/remote-bazel "$PWD" FULL_COMMIT_SHA i8-whole -- test //... \
  --test_output=errors --cache_test_results=no \
  --test_env=R1_SOURCE_SHA=FULL_COMMIT_SHA --test_env=R1_SOURCE_TREE=FULL_TREE_SHA
```

The `//tools/rehearsal/cmd/shadow-rehearsal:shadow-rehearsal` executable also
defaults to a plan. Its `--local --driver memory|sqlite --source-sha SHA
--source-tree TREE` mode requires a Bazel run with declared source and static
edition runfiles plus the exact clean checkout. It runs the same owned fixtures
and complete disposable rollback on an authorized disposable host. Run Bazel
only through RunsOn in this swarm.

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
import, loss accounting and source-system readback. The consumed edition transition qualifies disposable snapshot deployment/rollback
mechanics only. The sampled descriptor rows remain preparation evidence. Managed
HA, RPO/RTO, staging, old-release/schema compatibility, Beads reverse/reimport,
concurrent writers, independent principals and the full I8/R1 gates remain
unqualified. Full S1 remains 0/28 and E3 remains 4/5.

The separately reviewed R1 failover/rollback preparation scripts preserve the
bounded A1 fixture evidence; this package does not recertify that historical
head. This is a preparation and evidence checker, never a complete G2 decision
or permission to switch, deploy, merge, access staging or provision paid cloud.
