# Decisions needed from Nikhil

Decisions 1–15 are accepted. The completed authorization rerun has triggered
#15's revisit; no replacement model is selected, and affected implementation
remains blocked pending Nikhil's approval of the revisited design.

## Open

| # | Question | Recommendation |
|---:|---|---|
| 16 | **Inherited Beads CI.** Upstream's workflows (`bazel.yml`, `pr.yml` and the rest) fail on this repository (missing tag `v1.2.2`, gascity's `rbe-west` farm skipped) and test the Beads tree that P5b removes. Are they gates for Plug? | No. The swarm disables or removes them, and Plug's own Bazel CI (PR #12, on RunsOn once access works, GitHub-hosted until then) becomes the required check |

Also needed, as admin actions rather than decisions: add `plugs` to the RunsOn GitHub App's repositories, and grant the remote-dev GitHub App **Workflows: Read and write** so the swarm can publish CI changes under its own identity.

## Answered

- **#17 (2026-10-29, in Paseo):** authorization uses fixed project and team permissions, Linear-style, not user-configurable policies. The repaired spike failed its latency gates; a concrete design is in progress, and Cedar in core is not adopted.

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

| 10 | **Which `bd` command families to drop** (P80). The draft drops Dolt and merge-conflict plumbing, federation and version-control sync, destructive cleanup, raw SQL and untyped key-value commands, script hooks, orchestration-only helpers, Beads ID rewrites, and legacy migration modes, in one batch. Visibly used commands go to you one by one: `bd sync`, `bd dolt push`/`pull`, `bd prime`, `bd codex-hook`, setup and hooks, `merge-slot`, `swarm`, `branch`, `worktree` | Approve the batch; the listed commands are dropped too, since D10, P46 and P29b already rule them out | [Drop families](spec/commands.md#families-proposed-for-one-batch-drop-sign-off) |
| 11 | **`bd rename`.** Bead keys are immutable (A3), yet A7 mentions a rename client | Keep title and display edits through `bead.update`; drop ID and prefix rewrites | [Open interpretation](spec/commands.md#open-interpretation-to-resolve-before-parity-cards) |
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
No concrete replacement has been selected; Cedar is not adopted. Affected
authorization implementation remains blocked pending Nikhil's approval of
the revisited design.

**Evidence status.** Reviewable publication is complete at evidence-only head
`93a3cd0cca2579d307fd9a33bd70e2113bf1f6d8` ([v6 readout](https://github.com/buildbetter-app/plugs/blob/93a3cd0cca2579d307fd9a33bd70e2113bf1f6d8/spikes/authz/evidence-v6/readout.md)).
The independent R10c review admits the published negative artifact but blocks
overall merge readiness; executable manifest validation and exact-head hosted
CI remain separate blocked gates. Closing those gates would not clear the
failed latency gate or approve the revisited authorization design.
