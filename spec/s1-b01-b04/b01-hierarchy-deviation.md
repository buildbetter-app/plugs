# B01 hierarchy: adopted reference deviations and legacy input

Binding decision: orchestrator `b0d8be0f-3bde-4dbc-b4c7-51cb125c7511`,
2026-11-03, `/tmp/plug-swarm/escalations.md:598-603`.

Done = a causal source candidate records the indefinite-reference deviation and
gives canonical Parent and legacy parent-child operations one validated hierarchy
and readiness predicate, proven at this stage by exact source bindings and
independent real-store public regression assertions; rewriting an old observation,
adding `--until` to an old capture, or requiring a terminal parent does not count.

Scope = B01 hierarchy source, focused public assertion source, and distinctly
labeled future reference definitions. Execution and full-card proof are separate.

## Indefinite deferral is an adopted membership deviation

Plug suppresses descendants of a backlog/deferred ancestor even when no until
date exists. It uses the ancestor's project ontology and current direct blockers.
An open, unblocked parent permits its children. This follows canonical B01
(`engdocs/design/plug/spec/behaviors/work.md:26-33`) and B24's no-timer decision.
P74 permits an ordering lag only; it does not authorize a membership difference.

The accepted reference observation is different. Both captures `41576108523`
and `41576714283` run the parent command
`reference-engine defer b01-parent --reason "recorded parent defer"`, with no
`--until`; their ready/explain observations retain `b01-child` as ready. Pinned
Beads `9d6fff760b7a314b476539198504d77ce0959656` always writes deferred status
but sets `defer_until` only when supplied
(`cmd/bd/defer_proxied_server.go:63-68`). Its descendant exclusion requires a
non-null future `defer_until`
(`internal/storage/issueops/ready_work.go:529-536,571-599`). This explains the
source difference; it is not a new SQL readback or a timed-parent observation.

That specific reference membership is explicitly an **adopted deviation**.
The historical raw packets, twins, recorder/manifest pins, hardcoded historical
`difference.plugObserved` marker, and completed reviews stay unchanged. Their
`raw-capture-unqualified` / `hierarchy-gap-unqualified` labels are not upgraded.
The two definitions in `b01-hierarchy-vectors.json` are future inputs with distinct
IDs, not new reference executions or a replacement for those packets.

## Timed grandchild membership is separately adopted

Binding extension: orchestrator `62a71f53`, direct disposition
`273e90f1-19bf-4ddb-84a3-1a49a957a547`, 2026-11-04 06:50Z, section
"Orchestrator decision: B01 timed-defer readiness difference" in
`/tmp/plug-swarm/escalations.md`. The received file snapshot SHA256 is
`217f002e230057806426948ea69165e49103e28a118b40938803329401d2a26c`.
This extends the indefinite decision explicitly.

Both genuine timed native captures in first physical e6 run `41784159876`
report `b01-timedgrandchild` and `b01-timedfree` ready after the parent is
deferred to the declared future date. Plug's canonical transitive ancestor
suppression requires only its mapped free ref. Record those exact memberships
side by side as an **adopted timed transitive-ancestor reference difference**.
The separate indefinite native set remains child/grandchild/free. Neither
comparison claims parity, a native or product defect, or a scheduling cause
beyond the recorded disposition.

Plug has no timed defer. A deferred ancestor suppresses readiness for every
descendant until an explicit reopen. The recorder keeps both native twins,
the exact Plug memberships and all source/read/revision/byte/cursor/reopen
controls. The e6 first physical RED remains immutable; source556 fixed only
the per-operation defer results and was not dispatched. The revised exact
recorder must earn its own source review and changed-head runtime proof.
Native clock injection, expiry, wake, B05 and full-card qualification remain
outside this bounded comparison.

## One relation at the Writer boundary

Public `bead.link` and `tx.apply` link operations with `linkKind=parent-child`
enter `internal/core/pipeline/prepare.go`. The Writer retains the legacy link
and persists its matching canonical `Parent`, using the existing parent resolver,
ordered cycle checks and closure planner. A different canonical or legacy parent
refuses `invalid_params`; missing parents, cycles, branches, and project moves
retain the existing typed guards. Matching duplicate input is accepted without
creating a second parent relation. Dependency endpoint retyping remains refused.

Canonical reparent cannot leave a conflicting retained legacy link behind.
Explicitly unlink the old `parent-child` relation before changing/detaching it;
unlink plus canonical reparent can share one atomic `tx.apply`. Unlinking the
matching hierarchy relation removes its legacy copies and canonical parent.

Persisted documents may contain canonical `Parent`, one legacy `parent-child`
link, or both matching forms. The shared decoder resolves them to the same
in-memory canonical parent, refusing conflicting forms, multiple legacy parents
and malformed refs. Original stored bytes are preserved; reads do not write back
or backfill closure. Existing closure validation remains unchanged. No restore,
allocator, native producer or child-ID fixture work is included here.

Ready pages and fresh direct/next claims now use the same ancestor/category
predicate. The child's own direct blockers and open children still control close
and settlement; scheduling ancestry supplies no close blocker or close permit.
The installed fixed authorization adapter still refuses unsupported link verbs;
compatibility input grants no new parent/project authority.

## Assertion source and release boundary

`internal/integration/batchsql/b01_legacy_hierarchy_test.go` defines independent
public HTTP assertions on real memory, file-backed SQLite and disposable
PostgreSQL stores. Literal expected sets compare open, indefinite-defer/backlog
and blocked ancestors, direct children and grandchildren, matching duplicates,
conflicts, missing parents, mixed cycles, branches and project refusal. It checks
stored/public parents, exact ready cardinality and one-row pages, direct-child
epic counts, direct/next claims, SQL reopen/history, closure, refusal receipts and
a real Writer post-commit publication probe, plus unchanged close/settlement.

There is no readiness-count RPC in this bound service. Complete ready cardinality
and paginated membership are asserted; existing public epic counts test direct
hierarchy membership separately. They do not certify a dedicated ready counter.

The source is registered by the existing `batchsql_test` BUILD glob and the source
manifest. It has not been built or executed in this lane. S1 must release fresh
changed-candidate review and ordinary gates. No capture, timed-reference retry,
clock/timer, native/PR action, or complete B01/S1 qualification is released.
Full S1 remains `0/28`; E3 remains `4/5`.
