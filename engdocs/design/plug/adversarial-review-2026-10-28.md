# Adversarial review, 2026-10-28

Two models, Claude and GPT-6.1-Sol, each reviewed [design.md](design.md), [clean-room-plan.md](clean-room-plan.md) and [authorization-options.md](authorization-options.md) against Nikhil's decisions in [review-2026-10-27.md](review-2026-10-27.md) and the original brief (D1 to D13). Each model's findings were then checked by the other, and claims about today's code were checked against the tree. The headline: 53 findings survive, five of them blockers. 41 are fixes that change no decision. The roughly 60 open decisions come down to 20 questions for Nikhil; everything else can be adopted as recommended unless he objects.

Status key: **fix** means a factual error, an internal contradiction, a stale reference, or a gap with an obvious remedy that changes no decision. **decide** means Nikhil has to choose. "Raised" names the model that found the issue; "Confirmed" names the model that verified it ("partly" means the verifier narrowed it).

## Findings that survived

| # | Sev. | Document and section | Problem | Raised / Confirmed | Status |
|---|---|---|---|---|---|
| 1 | blocker | design.md A2 "Who applies (D16)", A7 "UI extensions", A6 "Change log", A1 `Op` union | D16 checks only the principal's kind. A `user` principal reaches `plugin.apply`, `ontology.apply` and the plugin-set, lock and ontology records through iframe bridge calls made as the user, through other-origin bearer tokens, and through generic `tx.apply`, `SetMeta` and `history.revert`. None of these paths excludes the protected records. | Claude / GPT | fixed |
| 2 | blocker | design.md A7 "Web UI", "UI extensions (P32a)" | Iframe isolation depends only on the element's `sandbox` attribute. A bundle served from Plug's origin and opened directly runs same-origin with the session cookie. The host page's CSP does not cover the frame, so the frame can send bridge results to any host. Every sandboxed frame has origin `null`, so the bridge cannot tell plugins apart by origin. | Claude and GPT / both | fixed |
| 3 | blocker | design.md A5 "The chain" (AZ5), A2 "Lifecycle" | A newly applied authorization plugin has no grants, and no decision means deny. So the applier cannot write the first grant or remove the plugin. P59 (b) drops break-glass, which leaves offline repair as the only way out. | Claude / GPT | fixed |
| 4 | blocker | design.md A6 "Topology" and G5; Operations "Backup and recovery"; P72 | With asynchronous replication (proposed RPO 5 minutes), a promoted replica loses the checkpoints along with the commits. The lost-commit check therefore never fires, and the same incarnation reissues revision numbers with different content. That breaks CAS tokens, cursors, idempotency replays and `id@revision` tracker keys. | Claude / GPT | decide |
| 5 | blocker | clean-room-plan.md "How behavior is captured" step 6, "Importing Beads data", switch criteria, CP18 | The census and converter key only on main schema version 72. Beads keeps a second cursor, `ignored_schema_migrations`, with 28 migrations that create wisps, `events`, leases and `local_metadata` ([schema.go](../../../internal/storage/schema/schema.go), `ignoredSource`). | Claude / GPT | fixed |
| 6 | major (raised as blocker) | design.md A4 sequence diagram and "Commit guard"; A6 "Locks" | The A4 diagram locks written refs first and then the read set, but "Commit guard" says one sorted order. Derived rows locked during Apply add further lock orders, so deadlocks are possible. No catalog says which sentinel each guarded query or `Slice` read maps to, or which sentinels each op and `PutDerived` table bumps. Closure rows rewritten by an ancestor's reparent carry no sentinel. | Claude and GPT / both, partly | fixed |
| 7 | major | design.md P63, A3 "Enforcement", A6 `history.redact` and "Dump and restore", A7 `sync.import`; clean-room-plan.md CP17, "After the switch" | With authentication but no authorization plugin, any agent key can call `ontology.apply`, `override_guard`, binding suspension and event skip. P63's list also omits `history.redact`, `admin.dump` with ciphertext, `history.revert` on protected records, and `sync.import`. During CP17's 12 weeks and the rollback window, the safety the plan promises therefore exists only as prose. | Claude and GPT / both | decide |
| 8 | major | design.md A2 "Who applies (D16)", A1 "Local mode" | In anonymous local mode any local process, a prompt-injected agent included, can apply a plugin that declares egress to an attacker's host and send the whole graph out. | Claude / GPT | decide |
| 9 | major | design.md Summary, A2 "Who applies" | "Only a human applies" is enforced as "only a `user` principal applies". An agent holding a user's API key or `plug login` session satisfies that check. | GPT / Claude, partly | decide |
| 10 | major | design.md A5 `Link`/`Slice`, A2 "Hot path", P52, P83, P84 | A code link backed by a mirror that listeners fill asynchronously falls outside P4's no-race guarantee, because the guard sees only `Slice` reads. A5 does not say so. | Claude and GPT / both, partly | decide |
| 11 | major | design.md A7 deployment table, A1 "Transports", A5 "Authentication", D29 | The `Plug-RPC: 1` header cannot be set on a WebSocket handshake, and `SameSite` does not separate sibling subdomains. The Origin check applies only in anonymous mode. Nothing says CORS omits credentials, what audience other-origin bearers carry, or that a plaintext non-loopback listener is refused. | Claude and GPT / both, partly | fixed |
| 12 | major | design.md A5 principal table, A4 "Delivery", A6 "Locks" | Once an authorization plugin is installed, default deny can stop `system:core` and plugin-principal work (listener writes, gate resolutions, lease reclaim). No boundary says which work bypasses the chain. | Claude / GPT, partly | fixed |
| 13 | major | design.md A5 "Writes", A4 sequence, A1 `Op` union | Writes are checked against the pre-image and the mutator-set fields, never the post-image. A principal can move a bead under an epic they cannot write, or remove a `restricted` label. Reparenting has no defined op. [authorization-options.md](authorization-options.md) Q2 requires the post-image check. | Claude / GPT | fixed |
| 14 | major | design.md A4 "Queue verbs" | `claimNext` prepares only 8 candidates. Today's claim deliberately walks the whole ready set ([claim.go](../../../internal/storage/issueops/claim.go), `Limit = 0`), so with more than 8 contending claimers some get `nothing_ready` while work is still ready. Preparing 8 also runs every mutator and the authorization check 8 times. | Claude / GPT | fixed |
| 15 | major | design.md A1 "Local mode", A3 integrity checks, A4 "Delivery", A6 sort-key job | In in-process local mode nothing owns the dispatcher, listeners, projections, lease reclaim, gate polling or the sort-key job. Every one-shot CLI call pays for the startup checks and wasm compilation. | Claude / GPT | fixed |
| 16 | major | design.md A6 "Authorization data" and "Locks", A1 error catalog, P56 | The ancestor-closure limit refuses large reparents with `authz_closure_too_large` even on servers with no authorization plugin, which contradicts "`authz_*` only where an authorization plugin is involved". Views and watches still need the closure. | Claude / GPT, partly | fixed |
| 17 | major | design.md A1 "Idempotency", A6 G4, P60 | Every anonymous client is the same principal, so two agents that reuse a short key (A1's own example is `"w7-1"`) collide, and one may receive the other's replayed `Commit`. | Claude and GPT / both, partly | fixed |
| 18 | major | design.md A7 "UI extensions", A2 manifest grants | Bridge grants are "methods and resources the plugin's grants name", but the only example is `resource: params.id`. Nothing defines how a grant scopes `tx.apply`, `bead.link` (two refs) or `plug_call`, nor any message validation or rate limits. | GPT / Claude, partly | fixed |
| 19 | major | design.md A2 "Runtimes" (egress proxy), manifest secrets | Nothing says a secret is injected only on requests to its declared hosts and stripped on a redirect that leaves them. Response sizes are uncapped. | GPT / Claude, partly | fixed |
| 20 | major | design.md A3 "Gates", A5 principal table | Manual gates count distinct principals, so one human can mint two agent tokens and satisfy a `human_approval` gate with `min_approvals: 2` alone. | Claude / GPT | fixed |
| 21 | major | design.md A6 "Locks"; clean-room-plan.md "What becomes acceptance tests", Readiness row | Fan-out beyond 5,000 dependents finishes in follow-up changesets, so `bead.ready`, view counts and watches read stale membership. The plan promises "Membership exact", and `readinessPending` appears in no read result. | GPT / Claude | fixed |
| 22 | major | design.md A6 driver interface, G5, "Topology"; P70, P71 | G5 reads the lease row but does not lock it, so a takeover can commit between that read and the old server's commit. P70 says the plugin reports health but recommends "no driver methods beyond `Lease`", which leaves `admin.health` with no source. `ActiveLease` exists only as a comment, and heartbeats to the lease table do not check the epoch. | Claude and GPT / both, partly | fixed |
| 23 | major | design.md P45, A6 `history.redact` | Redaction rewrites diffs and bases, but nothing says it reaches referenced blobs (strings over 4 KiB), dead-letter payloads or the audit table. Nor does it say which copies it cannot reach: plugin `State`, external destinations, earlier dumps and PITR backups. | GPT / Claude, partly | fixed |
| 24 | major | design.md "Interpretations to confirm" 1, A7 "Web UI"; review D1 | D1 says no UI is served by a local CLI, yet interpretation 1 lets `plug server --local` serve `/ui/` by calling it a shared server. That interpretation has not been confirmed. | GPT / Claude, partly | decide |
| 25 | major | clean-room-plan.md CP14, Risks; design.md A8 parity table | The wisps table also holds `agent`, `rig`, `role` and `message` beads, all ephemeral ([0035](../../../internal/storage/schema/migrations/0035_migrate_infra_to_wisps.up.sql)). The plan treats wisps only as workflow runs to drain, so mail and agent records have no target. | Claude / GPT | fixed |
| 26 | major | clean-room-plan.md "What becomes acceptance tests", IDs row; design.md A1 | Neither document says how a project mints IDs after a restore: counter mode, `child_counters`, wisp child counters, or adaptive hash length. Today's code seeds counters from existing IDs, but the plan never requires Plug to keep that continuity. | Claude / GPT, partly | fixed |
| 27 | major | clean-room-plan.md CP11 (d), Effort | CP11 (d) assumes Dolt history is a full record, but `bd flatten` squashes all of it ([flatten.go](../../../cmd/bd/flatten.go)) and compaction squashes old commits. Rebuilding versions across 72 schema changes has no price in the effort table. | Claude / GPT, partly | decide |
| 28 | major | clean-room-plan.md "How behavior is captured" step 3 | A Go test clock cannot control SQL-side time: readiness uses `UTC_TIMESTAMP()` ([ready_work.go](../../../internal/storage/domain/db/ready_work.go)), and columns use `ON UPDATE CURRENT_TIMESTAMP`. | Claude / GPT, partly | fixed |
| 29 | major | clean-room-plan.md "Effort and parallel tracks" | By the plan's own table, weeks 0 to 32 need about 102 engineer-weeks against about 102 of capacity, and weeks 32 to 56 need about 80 against about 77. That is no slack at all, yet the plan still calls 56 weeks honest. | Claude / GPT | decide |
| 30 | major | clean-room-plan.md Milestones (runtime spike, M3 exit) | The commit guard and `claimNext` under Postgres READ COMMITTED are first tested at M3's exit, about week 28. The spike in weeks 3 to 6 benchmarks only the lane. | Claude / GPT | fixed |
| 31 | major | clean-room-plan.md M0 "Settles", CP18 | CP18 is settled at M0 but needs M0's version inventory as input. Pinning fails if any store is newer than `beads-base`, because `bd` refuses newer stores. CP18 (a) also implies a tracker-feature freeze that the plan never states. | Claude / GPT, partly | decide |
| 32 | major | clean-room-plan.md store census, rehearsal handling | Custom statuses and types are also read from each repository's `.beads/config.yaml` ([config_helpers.go](../../../internal/storage/issueops/config_helpers.go)). Data-directory snapshots miss these files. | Claude / GPT | fixed |
| 33 | major | clean-room-plan.md "The old tree"; design.md A8 lexicon (`docs/migrate-from-beads.mdx`) | The converter goes away with the `beads` branch, yet the design reserves a public migration guide. No decision says whether external Beads users can migrate. | Claude / GPT, partly | decide |
| 34 | major | clean-room-plan.md switch criteria, "On the day" | The switch must fit an "agreed freeze window", but no decision sets that window's length. | Claude / GPT | decide |
| 35 | minor (GPT raised as blocker) | design.md A2 intro, "Codebase" | "The host sandboxes every plugin" is false for native mode-1 code. That code is trusted and checked statically; only wasm plugins are isolated at run time. | Claude and GPT / both, partly | fixed |
| 36 | minor | design.md A4 deadline table, A2 kind table | Authentication plugins, which may be wasm and sit on every request, have no deadline, breaker or caching rule in "the one table". | Claude / GPT | fixed |
| 37 | minor | design.md A4 "Post failures", "Delivery" | "Delivers calls lost to a crash once" cannot hold for a subscriber with side effects. The delivery key is not stated. "Replays skip post" is ambiguous. Projections "halt, never skip", yet `event.subscription.skip` does not exclude them. | Claude and GPT / both, partly | fixed |
| 38 | minor | design.md A1 "Agent core surface", "Discovery", "Meta" | The agent contract is ambiguous in several places: `project_required` versus the default project; `nothing_ready` is missing from the catalog; `filter` is a string in one method and an object in another; `agent.brief` and `event.list` are not marked AgentCore; `methodsHash` is global while method lists are per principal; and the idempotency key is optional. | Claude / GPT, partly | fixed |
| 39 | minor | design.md P75; clean-room-plan.md M6 exit | Rebuilding mode-2 plugins on restore makes the 1-hour RTO depend on GitHub and on historical toolchains, although the dump already holds the artifacts by digest. | Claude / GPT, partly | fixed |
| 40 | minor | design.md A7 "Watches", A4 "Limits and visibility" | A 250 ms coalesced window can contain a policy changeset followed by later events. Nothing says the later events are judged under the new policy. | GPT / Claude, partly | fixed |
| 41 | minor | design.md A3 "Migrations" | "Migration writes skip subscribers": `set_field` and `convert_field` could write plugin-owned fields or produce states a validator would reject. | GPT / Claude, partly | fixed |
| 42 | minor | design.md Summary, A6, A8 "Names" | "Data enters a server only by restore" contradicts P77's recommended `sync.import` into a populated server. The substance is P77's to decide. | GPT / Claude, partly | fixed |
| 43 | minor | design.md A1 "Reads" (cursors), A6 sort-key job | List cursors continue over live data, which the P74 job reorders, but the design does not say so. | GPT / Claude, partly | fixed |
| 44 | minor | design.md A5 `RuleSet.Eval` | `Decision` is undefined, so AZ6's effect-sensitive error rule has nowhere to carry per-policy errors. | GPT / Claude, partly | fixed |
| 45 | minor | design.md A2 runtimes; clean-room-plan.md runtime spike | The wasm ABI is not written down anywhere: memory ownership, framing, trap and error mapping, cancellation, size caps. | GPT / Claude, partly | fixed |
| 46 | minor | clean-room-plan.md M2, "The dump format" | Real-store conversion, the riskiest path, has no rehearsal before M2. | Claude / GPT, partly | fixed |
| 47 | minor | clean-room-plan.md "Shadow fortnight" | A duplicate factory pool roughly doubles agent spend for two weeks and needs a second prompt set. Neither is priced. | Claude / GPT, partly | decide |
| 48 | minor | clean-room-plan.md CP5, "On the day" | Rollback covers data only. Images, prompts, `plugins/beads` and HTTP v0 consumers are removed on the day, and their restoration is never rehearsed. | Claude / GPT | fixed |
| 49 | minor | clean-room-plan.md "The dump format", CP12 | `plug-dump/1` freezes at the switch, but M4 to M7 add new record kinds, and nothing says how a restore treats an unknown kind. | Claude / GPT, partly | fixed |
| 50 | minor | clean-room-plan.md "On the day" | The freeze step restarts Dolt servers read-only, but embedded stores have no server to restart. | Claude / GPT, partly | fixed |
| 51 | minor | authorization-options.md "The spike"; design.md P56 | The spike's 50,000-descendant reparent at depth 10 cannot pass "closure rewrites within the proposed limit" of 5,000 rows. | Claude / GPT | fixed (authorization-options.md edited afterward: the error is renamed, and the spike measures the 50,000-descendant rewrite as P56's migration-class write) |
| 52 | minor | clean-room-plan.md M0 "Settles", P80 | M0 asks for 17 decisions, and P80 asks Nikhil to review 306 command paths himself. | Claude / GPT, partly | fixed |
| 53 | minor | design.md "Open"; clean-room-plan.md "Decisions for Nikhil" | P5b, P38d, P5c, P76, P80 and P82 appear in both tables. Their wording has drifted (P76). | Claude / GPT, partly | fixed |

Counts: 5 blockers, 29 major, 19 minor; 41 fix, 12 decide.

## Disputed

Refuted, not real:

- **Watch subscription gap** (GPT). Delivery pulls from the gapless log by cursor from N+1 (A6 G1, A7 sequence), so no window exists between the snapshot and registration.
- **Failover loses leases** (GPT). Leases live in a logged table so that they replicate (A6 "Records and the lane"); G5 and the incarnation rule cover lost commits. The only useful remnant, an epoch check on heartbeats, is in finding 22.
- **List performance not established** (GPT). The 50 ms target is also a release and M3/M5 exit benchmark on the built system, and large grouped views already fail with `scope_too_large`.
- **AZ4 and P84 contradict** (GPT). They do not: AZ4 covers rules and P84 covers check-only code links, as the AZ4 row says. Both wait for the spike readout.
- **Every unmapped table needs a decision** (Claude, plan). The census allows `n/a:<reason>` dispositions, so none of these goes to Nikhil.

Split, real but narrowed by the verifier:

- claude-design-06: invariant work is already "core only" in the A4 phase table; the gap is plugin, listener and gate work.
- claude-design-08: write skew is overstated, since guarded queries map to sentinels, and moving revisions off rows would break per-record CAS.
- claude-design-12: deferring the closure without an authorization plugin would break views and watches; only the limit's name and policy change.
- claude-design-13: P60 (b) already scopes leases; only the idempotency scope is missing.
- claude-design-14: "a Slice-only link adds nothing" is too strong; deferring code links is a choice, not a consequence.
- claude-design-19: restore does not have to rebuild; the dumped artifact can be loaded and checked by digest.
- claude-design-20: these are contract-spec gaps, not proven runtime defects.
- gpt sentinels: negative reads and grant sentinels are already covered; only the catalog is missing. Major, not blocker.
- gpt platform safety: duplicates P63; the restore-staging part is moot because restore requires an empty store (P44).
- gpt native sandbox: mode 1 is code chosen at build time, and field and method limits hold at the host boundary. A wording fix.
- gpt external mirror: affects only an optional pattern under P28d. Major, not blocker.
- gpt anonymous identity: anonymous mode already allows everything (D17, D22), so a lease capability would protect nothing; only idempotency is real.
- gpt human approval: D16 already says `user` principal; step-up or WebAuthn is more than v1 needs.
- gpt secret egress: DNS, private-IP and redirect checks already exist; per-path policies are not needed.
- gpt bridge capability: the host page holds the WindowProxy and can check `event.source`; nonce and navigation threats are overstated.
- gpt OIDC contract: PKCE, revocable core sessions and cookie defenses are already specified.
- gpt watch revocation: delivery-time filtering and resets are already specified; only the coalesced window is open.
- gpt cursor instability: every live list already behaves this way, as today's moving cutoff does.
- gpt online migration: v1 refuses migrations over 5,000 rows; no fence protocol is needed now.
- gpt event delivery: `{incarnation, revision, index}` already identifies each event.
- gpt storage topology: G5, incarnations and P71 cover most of it; `Store.Health` and `ActiveLease` remain.
- gpt authz error interface: a sketch gap; cedar-go's diagnostics carry the data. Minor.
- gpt plugin ABI: the registry and OpenRPC are already normative; only the wasm ABI is unwritten.
- gpt import and restore: a duplicate of open P77.
- gpt local UI: already interpretation 1; serving a UI on an anonymous loopback server adds no authority.
- gpt redaction: changesets hold paths, not values, and events are rebuilt from diffs; fewer copies exist than claimed.
- claude-plan-03: today's allocator seeds from existing IDs, so a collision is not inevitable; the missing contract is real.
- claude-plan-04: `bd gc` does not squash commits, and deletes remove their edges; the flatten and compaction premise stands.
- claude-plan-05: the 356 `time.Now` sites include telemetry; scope the time-dependent cards first.
- claude-plan-09: the deployed version cannot be read from this repository; the freeze is conditional.
- claude-plan-11: the `beads-final` tag keeps the converter's source; the support level is the open question.
- claude-plan-14: M2 starts at week 3, so "mid-M2" is unproven.
- claude-plan-15: engineering effort for the shadow fortnight is in the estimate; agent spend is not.
- claude-plan-17: generic `records/<kind>.ndjson` could absorb new kinds; only the policy for unknown kinds is missing.
- claude-plan-18: stopping agents may quiesce embedded stores, but nothing says so.
- claude-plan-20: M0 settles 17 IDs, not 18.
- claude-plan-21: the plan's copy is an operational mirror; the real risk is drift.

## Decision triage

Rule: where the models agreed, their class stands; where they disagreed, the item goes to Nikhil. Related questions are bundled so that each row is one conversation.

### For Nikhil

| # | IDs | Question | Recommendation from the documents and reviewers |
|---|---|---|---|
| 1 | P70, P71, P72; finding 4 | Which failover rule keeps revisions honest: RPO 0 through synchronous replication, a new incarnation on every lease-epoch change, or a head watermark kept outside the database? And may P70 narrow P49 to a managed HA Postgres? | P70 (b), P71 (a) with TTL 10 s, P72 (b). The documents recommend no failover rule; a new incarnation per epoch needs no new infrastructure. |
| 2 | P63, CP17; finding 7 | Should core require a `user` principal for the full list (`ontology.apply`, `override_guard`, binding suspend, event skip, `admin.restore.*`, `history.redact`, `admin.dump` with ciphertext, `history.revert` on protected records, `sync.import`), widening D18, and should that be a G2 prerequisite? | P63 (b) with the complete list, tested before G2. CP17 (e) is safe only with it. |
| 3 | D16; findings 8, 9 | Does "a human applies" mean an interactive session, not an API key, when authenticated, and a human-presence check (typing the plan digest at a TTY, or a one-time code) in anonymous mode? | Yes to both. This is platform safety, not authorization of principals. |
| 4 | P60, P61 | Anonymous identity: an unverified `_meta.agent` label that also scopes idempotency keys and leases, and an optional `authn-local`? | P60 (b), with the label in the idempotency scope; P61 (b) as an optional provider, not the default. |
| 5 | AZ1, AZ4 (with CP7 now) | Approve three to five real BuildBetter policies before week 1. At the readout: Cedar in core, or a relationship schema? Refuse read rules that cannot compile to a filter? | CP7: three to five policies, counts p95 under 250 ms. AZ1 (v) if the spike passes, else (iii). AZ4 (a). |
| 6 | P52, P54, P83, P84; finding 10 | Do code links ship in v1? If yes: no egress in `authz.*` calls, an explicit link order, the last link may allow, and capped post-filters with mirror-revision checks. If no, the four questions retire. | Documents: P52 (a), P54 (b), P83 (a), P84 (b). Claude's reviewer: defer code links to v2. |
| 7 | P53, P57, P59, P39b | Confirm that these are settled: P53 by P14, P57 by D26, P59 by D18, P39b by P39. | P53 (a), P57 (a), P59 (b), P39b (a). |
| 8 | P76, P77, CP13; finding 42 | Does P44 govern only restores, so that an offline merge before an empty-store restore, tracker `sync.import` and late Beads data may mint new IDs? | P76 (b); P77 (a); CP13 follows P77. |
| 9 | P78 | Is a remote edit to a synced field overwritten on the next push? | (a), counted as drift. |
| 10 | Interpretation 1 (D1) | May `plug server --local` and a loopback `plug server` serve `/ui/`? | Yes; in-process mode never does. A UI on a shared host also needs OIDC (P61). |
| 11 | CP2 | Is the 0.1.0 scope the whole design, or do mode 2 and iframes move to 0.2? | Documents: (a), re-cut at G3. Claude's reviewer: decide (b) now. |
| 12 | CP3 | When does BuildBetter move? | (b), at G2 on the measured parity set; (c) if workflows or gates are in daily use. CP3, CP4 and CP5 are all needed at M0; take them together. |
| 13 | CP4; findings 29, 47 | How many engineers, how much contingency, and what agent budget for the shadow fortnight? | Four engineers; show 15 to 20% contingency, or five engineers. Price the duplicate pool, or sample about 20%. |
| 14 | CP5 | Rollback: a 30-day window with a reverse converter that loses post-switch history? | (c), 30 days, rehearsing software as well as data (finding 48). |
| 15 | CP11; finding 27 | Rebuild pre-Plug history from Dolt, given that flatten and compaction squash it? | Documents: (d). Reviewers: inventory flatten and compaction per store and price (d); default to (c). |
| 16 | CP17 | Plan shape, and about 12 weeks in production with authentication but no authorization plugin? | (e), if row 2 lands. |
| 17 | CP18; finding 31 | Pin the factory's `bd`, accepting a stated tracker-feature freeze? | (a), decided after the M0 week-1 version inventory; re-tag the spec base rather than downgrade a store. |
| 18 | New CP20; finding 34 | How long may the factory be down on switch day? | At most 4 hours in a weekend slot; a rehearsal over budget blocks G2. |
| 19 | New; finding 33 | Can external Beads users migrate to Plug? | Either BuildBetter only, or a tagged standalone converter from `beads-final` (about 1 engineer-week). |

### Defaults to adopt unless Nikhil objects

- AZ3: (a) `cedar-go`, pinned. AZ7: (a) plugin-declared grant records. P51: (a) one merged schema. All three are reported at the spike readout.
- P55: (a) a fixed authorizer view in v1.
- P56: (c), settled before the spike (finding 51).
- P58: (a) refuse scoped tokens without an authorization plugin. P62: (b) a bootstrap provider and admin in the boot tier. P64: (a) refuse multi-approval gates in anonymous mode.
- P65: archive only; moves need `map_status` and `map_type` directives. P66: (a). P67: as recommended (server image, artifacts by digest). P68: (a), then (b). P69: (a).
- P73: confirm after the `asOf` benchmark. P74: 1 minute.
- P75: revised. Restore loads the dumped artifacts after checking their digests; rebuilding becomes an asynchronous check (finding 39).
- P5c: (b). P85: (a) a core memory record kind.
- P5b: (a). P38d: (a). P48b: (a). P79: none at first. P81: the catalog as recommended. P82: (a).
- P80: revised. A delegate drafts the keep, fold or drop list, and Nikhil signs off the families to drop in one batch, escalating only drops of commands seen in use.
- Interpretations 2 to 9: as written.
- CP1 (b), CP6 (b), CP8 (b), CP9 (a), CP10 (a), CP12 (a), CP14 (a) after the per-type wisp inventory (finding 25), CP15 (b), CP16 (b), CP19 (a) and (a).

### Duplicate or obsolete

- The plan's copies of P5b, P38d, P5c, P76, P80 and P82 are settled by the matching rows in design.md "Open". Keep one table (finding 53).
- CP13 is settled by P77 (triage row 8).

### Top ten

1. CP3: when BuildBetter moves. Every date depends on it.
2. P63 and CP17: platform-safety ceilings with the complete list, required before G2, so the 12-week window without authorization is safe.
3. P70 to P72: failover consistency, RPO and RTO, and managed HA Postgres.
4. D16: an interactive session for `plugin.apply`, and a human-presence check in anonymous mode.
5. CP7 now, then AZ1, AZ4 and the code-link question (P52, P54, P83, P84) at the spike readout.
6. CP4: staffing and contingency. Four engineers has zero slack by the plan's own table.
7. CP2: 0.1.0 scope; decide now rather than at G3.
8. P76, P77 and CP13: how far P44 reaches.
9. CP5: a 30-day rollback that loses post-switch history.
10. CP18: pin the factory's `bd` after the inventory, with the freeze stated.

Next in line: P60 and P61 (anonymous identity), interpretation 1 (D1), CP11, and the freeze window (CP20).
