# Decisions needed from Nikhil

Decisions 1–20 are accepted. #17 fixes the project/team permission
vocabulary; #18 adopts the reviewed bounded X1–X9 candidate semantics. #20
settles only the bounded type-summary cap requirements direction. Controller v2
authorizes scoped implementation waves; contract compatibility and runtime
acceptance still require evidence. #19 sets the current local Bazel merge gate.

## Open

- **Compaction head-of-line block (raised 2026-11-23 by B12).** `admin.compact`
  works on one record per call, oldest eligible first (closed time, then ID).
  A record whose summary can never be smaller is refused on every call, so no
  later record is ever compacted. Native Beads compacts the others and leaves
  that one. Options: (a) an optional exclusive `afterId` cursor that changes
  selection only, where the refusal names the refused record and the cursor
  joins the request digest; (b) a skip marker written on refusal. Recommended:
  **(a)**, because a refusal never writes. B12 records the current behavior as
  a known gap.

New product questions need options and a recommendation; routine
implementation choices follow the recorded design. Testing runs locally
through Bazel with no hosted CI (2026-11-12), so no CI admin actions remain.

## Answered

- **Switch trial (2026-11-20 to 11-24, Nikhil):** run a supervised trial on
  disposable copies of the real ledgers, nightly or on root's go, and keep
  diagnosing, fixing and re-running until a trial is fully green (2026-11-24).
  Beads stays the system of record until Nikhil decides to switch.
- **Agent assignment (Nikhil):** B05 and B22 go to a Claude agent, with GPT
  agents excluded from those topics (2026-11-21). B12 goes to a Claude agent
  (2026-11-23).
- **Native backups (2026-11-21, Nikhil, "p25a"):** native backups carry the
  authenticated converter evidence.
- **P18, AUTHZ performance and the Dolt issue (2026-11-20, Nikhil):** P18
  approved, the authorization performance work approved, and the upstream
  go-mysql-server JSON `+Inf` issue filed.
- **Ready semantics (2026-11-22, orchestrator call, reversible):** `bead.ready`
  matches Beads. It excludes Beads' default-excluded types (merge-request,
  gate, molecule, rig, agent, role, message) with the same opt-in, and follows
  Beads' parent rules, including deferred parents (P32, P39, P43).
- **B12 native capture (2026-11-23, orchestrator call, reversible):** native
  compaction snapshots are recorded with the public `ai.base_url` pointed at a
  deterministic loopback stub. Beads stays unmodified and the recording
  asserts that no connection left the host.
- **Reverse export size (2026-11-24, orchestrator call, reversible):** the
  reverse head export and native restore stream with bounded memory. The
  former 128 MiB archive cap is now a buffer bound, and total archive size has
  a separate typed ceiling of 2 GiB (P44). Real ledgers need this, because the
  retained original captures alone exceed 128 MiB.

- **P13 HASH allocator population cache (2026-11-19, root deb51809):**
  use a reversible, process-local Writer cache keyed by store incarnation and
  exact snapshot head. The first request for a HASH prefix at a cold or changed
  head runs the existing census; warm requests reuse only the count. Successful
  commits from this Writer apply their own staged population deltas and advance
  the cache only from their actual precommit head. Other writers, restores and
  lost commit acknowledgements cause a revision mismatch and a fresh census.
  Retain one integer per requested live HASH prefix, with no record bodies,
  persistent metadata, schema, storage capability or restore changes. Candidate
  identity checks remain targeted and include staged IDs and tombstones.
  Noncanonical ID-derived prefix candidates are skipped consistently. Future
  cross-process caching may revisit persistence in a separate reviewed card;
  this choice supplies no runtime qualification by itself.

- **B02 pinned-producer stale projection (2026-11-19, root ruling
  ROOT959a61e5):** the native-only recorder observes pinned Beads, whose `ready`
  trusts stored `is_blocked`. Record that answer before explicitly invoking the
  public `recompute-blocked` repair and conserving `updated_at`. Plug's B02
  contract remains recomputation before answering (`spec/behaviors/work.md`
  lines 35–59 and `design.md` readinessPending semantics). Keep this deviation
  explicit and qualify the actual Plug ready answer independently; neither
  silent native pre-repair nor a product-gap claim follows from comparing
  these distinct surfaces. This changes the recorder's native oracle only;
  product semantics and runtime acceptance still require their own evidence.

- **Emergency-stop and G1 decision (2026-11-01, binding):** Nikhil decided
  that the live soft pause, which stops merge admission and was rehearsed on
  2026-10-31, is sufficient control-plane evidence for G1. G1 passes and work
  continues. Rehearsing remote-dev App revocation and testbox/shadow egress
  cut-off is dropped from G1 and is not a required acceptance artifact. The
  hard-stop and containment mechanisms remain documented operational controls;
  this decision changes the G1 evidence boundary only and does not authorize
  production movement, release, deployment, or a G2 switch.

- **#19 — DECIDED (2026-10-29, chat):** merge on exact-head local Bazel CI
  until hosted CI works. Record the command, version, targets, result and logs
  for touched targets plus reverse dependencies (`build` for docs-only PRs).
  Controller v2 also requires one independent Codex review without a
  correctness, security, contract, build or decision blocker, task scope, and
  no `/tmp/plug-swarm/PAUSE`. No per-PR human approval is required. Hosted CI
  becomes required again once RunsOn serves this repository and the remote-dev
  App has Workflows permission. This does not authorize workflow publication,
  settings changes, production cutover or a public release.
- **#20 (2026-10-29, in Paseo):** only fully compiled type count summaries (explicit `fields=[]`, at most 100 groups) may scan up to 100,000 inputs; ordinary views keep the 20,000 cap.
- **#18 (2026-10-29, in Paseo):** adopted the reviewed fixed-permission authorization candidate (`c9f5f29d`); create, reparent and project moves are denied under the plugin until later slices.
- **Post-stop parent/child contract (2026-10-31, binding):** the later
  parent/child decision conditionally supersedes #18's historical
  **parented-create/reparent denial boundary** for a future, separately
  admitted implementation of parent links and parent-aware epic progress. It
  does not change the historical #18 decision or claim runtime support. A
  child remains in its own project, so project moves remain outside this
  contract; child creation/reparenting do not grant cross-project access and
  non-disclosure is preserved until the implementation and qualification are
  separately admitted.

  The follow-up is tracked as the deferred contract-to-UX sequence **EP1 →
  EP2 → EP3 → EP4 → EP5 → EP6**: parented create/reparent contract,
  parent/child authorization, issueops/storage hierarchy, parent-aware
  epic-progress RPC, epic-progress view, and the fifth E3 agent-UX task. EP1–EP5
  require separate implementation, review, and qualification before EP6; the
  accepted E3 4/5 boundary remains current until then. This sequence adds no
  runtime, merge, release, or deployment claim.
- **#16 — DECIDED (2026-10-29, Paseo, about 03:37 UTC):** Nikhil chose
  **Replace inherited workflows with qualified Plug CI (Recommended)**.
  Plug-owned Bazel CI must qualify on the exact head before a reviewed CI
  cutover. #19 later selects local Bazel CI as the interim merge gate; hosted
  failures are retained as evidence. #16 authorizes no settings change. The [CI gate](clean-room-plan.md#ci-contract-replacement)
  preserves the bounded GitHub-hosted fallback, RunsOn admin dependency and
  existing ownership, merge, review and stop-line rules.
- **#17 — DECIDED (2026-10-29, Paseo):** authorization uses a fixed project
  and team permission vocabulary, Linear-style, rather than user-configurable
  policies. This requirement is distinct from #18's concrete semantic choice;
  Cedar in core is not adopted.
- **#18 — DECIDED (2026-10-29, Paseo):** Nikhil chose **Adopt reviewed bounded
  candidate (Recommended)**, the entire X1–X9 package recorded in
  [A5](design.md#bounded-fixed-permission-candidate-18). Provenance: proposal
  `c9f5f29d`, independent paper-readiness PASS `c9bb013d`, report `a64fcd3e`.
  This selects the optional native finite component, direct/one-hop explicit-team
  involvement, canonical scope/project facts, core-owned authority/context/proof
  and revocation control, atomic census/bootstrap, five-verb host-controlled
  management lowering, uniform core ready codec and restricted SQL claim/terminal
  protocol. Initial positive scope is current User reads, exact CP7, minimal
  lifecycle/management and Agent claim/update on existing assigned beads.
  Create/reparent/reassignment, project rename/delete, extra deputy/other
  permissions, generic metadata/revert, restore/rebuild and unsupported consumers
  remain denied or gated. Controller v2 separately authorizes scoped
  implementation; adoption supplies no measured candidate, runtime acceptance
  or migration permission. Failed historical
  latency, exact contract/compatibility, executable, real-host/200-agent, C7/R14
  and product cutover gates remain open; #19 governs PR admission.

- **#20 — DECIDED (2026-10-29, Paseo):** Nikhil answered **Allow 100k only
  for fully compiled type count summaries (explicit fields=[], max 100 groups);
  keep ordinary views at 20k (Recommended)**. This selects at most **100,000
  authorized inputs only for fully compiled type count summaries**, **explicitly
  present `fields=[]`**, **at most 100 groups**; **ordinary views retain 20,000**.
  This amends the previous universal-20,000 requirements direction only within
  those four bounds. See [A7](design.md#bounded-type-summary-cap-20) and the
  [plan record](clean-room-plan.md#bounded-type-summary-cap-direction-20).

  Exact answer: `/tmp/plug-swarm/observations/decision20-answer-20261029T0621Z.json`,
  SHA-256 `da8ef4a6a6b4f9322abf9682901bd1acb56eda66e9d847b9a433be2e114b0cdc`.
  Historical packet: `/tmp/plug-swarm/work/az-evidence-plan2-4l3kaz18/cap-resolution-packet.md`,
  SHA-256 `c03e698e451df2d1f30c60ab3f90aabd02d3abd1f19a0956d01a176cff4b6113`.
  Independent FINAL presentation review: `/tmp/plug-swarm/reviews/AZ-candidate-evidence-plan2.md`,
  SHA-256 `08bf78caebba628f9cb805db71b06fbd5e4601039e5d07a28b030a2a234cf7aa`;
  receipt `/tmp/plug-swarm/work/az-evidence-review2-vta96jbs/final-receipt.json`,
  SHA-256 `4d526bc5217f1e9ebfab070ff5fa0acbc1c0a899ba12caac978c0161475ae7d0`.
  That review predates this answer: its PASS WITH EXPLICIT HOLDS presentation
  verdict, AZE-1 BLOCK/AZE-2 HOLD and unadopted alternatives remain historical.

  Only the four bounds are selected. The packet's complete CAP-SUMMARY and TG
  clauses are not adopted. Exact accounting/qualification, compiler eligibility,
  type/group/result/elision/continuation, budget/refusal/error, caller/presence/
  serializer and ordinary-continuation compatibility remain held; no omitted/
  default/empty behavior or interface/version choice is settled. W1/W2/W3/W4/
  W5/AZP-1 exact integration and independent acceptance remain separate.
  No new permission, domain, API, implementation or execution is authorized.

  #20 removes the unanswered cap choice but cannot close **AZE-1** alone.
  Historical measured latency remains **FAIL/BLOCK**. Retain 100k, both engines,
  both plans, all fractions, **36 cases**, **3 warmups + 20 measured samples
  per case**, page and whole-Q5 p95 `<50 ms`, whole-Q6 count p95 `<250 ms`,
  existing resource ceilings and runtime/real-host/proof/200-agent/C7/R14/
  product-cutover gates. R14 remains **UNVERIFIED**, both findings **OPEN**.
  #17/#18/#19 remain **DECIDED**; exact-head local Bazel CI governs PR admission.
  Frozen D6/C0/PR15 bytes and earlier verdicts are not rebound to this successor.

Nikhil accepted every recommendation below on 2026-10-29 ([recorded](review-2026-10-27.md#decisions)).

## Swarm plan (accepted 2026-10-29)

| # | Question | Recommendation | Plan |
|---:|---|---|---|
| 1 | What grouped-count target should the authorization spike use? | 250 ms p95 at 100,000 beads, while pages remain under 50 ms p95 at 100%, 1%, and 0.1% visibility. | [CP7 count target](clean-room-plan.md#cp7-count-target) |
| 2 | Should 0.1.0 include the full design, or move mode-2 plugins and iframes to 0.2? | Keep the full design in 0.1.0 and re-cut at G3 only if the evidence cannot support it. | [CP2 scope](clean-room-plan.md#cp2-scope-at-g3) |
| 3 | When may BuildBetter move its data? | At G2 on measured parity, or after M4 if the inventory shows daily workflow or gate use. | [CP3 internal move](clean-room-plan.md#cp3-internal-move) |
| 4 | Which rule prevents revision reuse after uncertain replication? | New incarnation on each uncertain lease-epoch change, with managed HA Postgres and documented 5-minute RPO and 1-hour RTO. | [P70-P72 failover](clean-room-plan.md#p70-p72-failover) |
| 5 | What rollback evidence is required for the unstageable cutover? | Read-only Beads, a reverse converter, pinned software and prompts, and a full rehearsal over a proposed 30-day window. | [CP5 rollback](clean-room-plan.md#cp5-rollback) |
| 6 | How should stores with incomplete history be restored? | Events attachment plus verified history where available, with no fabricated history; rebuild from Dolt only when completeness and cost are proven. | [CP11 history](clean-room-plan.md#cp11-history-depth) |
| 7 | Which `bd` build should the factory use after the M0 inventory? | Pin the `beads-base`-derived build after copies converge on one main and ignored schema pair, with a stated tracker-feature freeze. | [CP18 factory pin](clean-room-plan.md#cp18-factory-bd-pin) |
| 8 | How long may the factory be down on switch day? | At most four hours in a weekend slot; an over-budget rehearsal blocks G2. | [CP20 freeze window](clean-room-plan.md#cp20-freeze-window) |
| 9 | Is standalone migration support for external Beads users in scope? | **Already answered:** yes. P5c ships the exporter standalone so plain Beads data can be ingested (P65) | [External converter support](clean-room-plan.md#external-converter-support) |

| 10 | **Which `bd` command families to drop** (P80). The draft drops Dolt and merge-conflict plumbing, federation and version-control sync, destructive cleanup, raw SQL and untyped key-value commands, script hooks, orchestration-only helpers, Beads ID rewrites, and legacy migration modes, in one batch. Visibly used commands go to you one by one: `bd sync`, `bd dolt push`/`pull`, `bd prime`, `bd codex-hook`, setup and hooks, `merge-slot`, `swarm`, `branch`, `worktree` | Approve the batch; the listed commands are dropped too, since D10, P46 and P29b already rule them out | [Drop families](spec/commands.md#accepted-command-family-drops-decision-10) |
| 11 | **`bd rename`.** Bead keys are immutable (A3), yet A7 mentions a rename client | Keep title and display edits through `bead.update`; drop ID and prefix rewrites | [Identity interpretation](spec/commands.md#immutable-identity-interpretation-decision-11) |
| 12 | **Tracker commands.** Do the generic `sync.*` methods replace every provider-specific tracker command? | Yes | [Trackers](spec/commands.md#tracker-integrations) |
| 13 | **UX direction from the spike.** Home is "What changed since I looked"; bounded views that say what they hid; stable deltas; compact presence; epic progress as counts; the declarative catalog (D31). Not graph-first navigation | Accept, and validate with five people against real view and watch data before the UI milestone | [UX spike decision](spike-ux-decision.md) |

| 14 | **Contract defaults from the catalog** (T4). Freeze the provisional method shapes (`admin.restore.begin/chunk/finish`, `memory.*`, `admin.compact`, gates); use the contiguous error-number allocation; carry an incarnation on every cursor; add `session.close` with bounded expiry and reconnect; add rollback and close, a changeset header, error mapping and the agent label to the driver contract | Accept as defaults; they are implementation details the contracts need settled before agents generate code | [Contracts](contracts/go-sketches.md) |

| 15 | **Authorization spike readout** (AZ1). The correctness and policy-fit criteria passed; the speed gate failed, mainly from one Postgres planner outlier, a slow plan-A implementation and Postgres grouped counts. Your rules do not filter reads. (1) A quick follow-up spike fixes the SQL plans and reruns the gate, with the swarm proceeding provisionally on Cedar in core; (2) adopt Cedar in core now for action checks, with reads unfiltered in v1; (3) revisit and lean toward Linear-style teams | (1) | [Spike results](spike-authz-results.md) |

**Original #15 process decision (accepted 2026-10-29).** The table preserves
the original readout and recommendation: run the follow-up spike, proceeding
provisionally on Cedar in core only until the rerun reports. The rerun has now
completed, ending that temporary continuation.

**#15 outcome (2026-10-29).** The repaired rerun completed at measured source
`69f28caaf71a673296abe3120c60be5cb3af5aec`. Correctness, corpus, Q5,
full-transaction closure timing, and independent differential checks pass.
The unchanged latency gate fails: page p95 `<50 ms` at 100,000 beads with
100%, 1%, and 0.1% visibility on both SQLite and Postgres, and grouped-count
p95 `<250 ms`. Under accepted #15 this triggers the authorization revisit.
That revisit led to #17's vocabulary decision and #18's adoption of the
reviewed bounded X1–X9 semantics. Cedar is not adopted. Controller v2 authorizes
scoped contract integration and implementation;
candidate compatibility, runtime and performance acceptance remain unproved.

**Evidence status.** Reviewable publication is complete at evidence-only head
`93a3cd0cca2579d307fd9a33bd70e2113bf1f6d8` ([v6 readout](https://github.com/buildbetter-app/plugs/blob/93a3cd0cca2579d307fd9a33bd70e2113bf1f6d8/spikes/authz/evidence-v6/readout.md)).
The independent R10c review admitted the published negative artifact but blocked
overall merge readiness under the historical gate. Executable manifest
validation remains open; current PR admission follows #19. Closing that
validation would not clear the
failed latency gate or supply acceptance evidence for the adopted candidate.
