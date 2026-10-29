# Decisions needed from Nikhil

The design decisions are complete. This ledger contains only the plan-level choices that can change the swarm gates, cutover evidence, or release scope. Answer by number, or "accept all".

## Swarm plan

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

| 14 | **Contract defaults from the catalog** (T4). Freeze the provisional method shapes (`admin.restore.begin/chunk/finish`, `memory.*`, `admin.compact`, gates); use the contiguous error-number allocation; carry an incarnation on every cursor; add rollback and close, a changeset header, error mapping and the agent label to the driver contract | Accept as defaults; they are implementation details the contracts need settled before agents generate code | [Contracts](contracts/go-sketches.md) |

Answer by number, or "accept all".
