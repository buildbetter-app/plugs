# Bounded hierarchy child consumer

## Completion contract

Done = detach and reparent discover canonical direct-Parent descendants from the
same tracked Writer snapshot, charge every ordered affected pair before any
closure enumeration, refuse the 5001st affected pair without a closure read,
and preserve exact source/closure observations and atomic refusal; proven by
reviewed source plus real Writer and persisted-state evidence. A mocked child
map, an indexed happy path without malformed-closure refusal, or a partial
legacy-link graph does not count.

Scope = core publication, tracked snapshot capability, bounded canonical
Parent planner, exact closure validation, and service consumption seams.
Storage/schema implementation, backend adoption, native restore history, PG615,
and the retained PR254 witnesses remain separate owners. The complete graph
fallback remains authoritative for legacy links, blockers, scheduling, and
all-kind deletion until their closed typed source domain is available.

## Exact source ownership

This child starts from commit `a3af0116f3f8721f2901447e7a759ef4f40f5c20`
(tree `f6b5f6f4be3a9feaa09f124c1ff00dabe97fccd1`). The owned source paths are:

- `internal/core/pipeline/hierarchy_source.go`, plus the narrow publication
  call sites in `apply.go`, `protected.go`, and current-image `restore/archive.go`;
- `internal/core/port/pipeline.go` and the tracked adapter in
  `pipeline/snapshot_hierarchy.go`;
- `pipeline/hierarchy.go`, `prepare.go`, and the bounded source planner in
  `hierarchy_indexed.go`;
- focused planner/snapshot tests and this design record.

No schema, driver, PG615, or retained PR254 implementation file is modified.
The publication adapter derives Parent and the complete outgoing Link
`(kind,target)` declaration from one exact decoded Body. When the optional
`GraphSourceWriter` is present, one call publishes both; older Parent-only
writers remain a compatibility path and do not claim graph-link coverage.

## Frozen optional capabilities

The S2 Parent API is `HierarchyChildrenReader.HierarchyChildren`; the separate
`HierarchyChildrenExcludingReader.HierarchyChildrenExcluding` applies prepared
source keys before the committed and driver-staged global limit+1. Exclusions
are bounded by `HierarchyMaxExcluded=1000`, sorted/deduplicated by the
consumer, and contain only actual hierarchy-edit keys, never read-only ancestor
loads. A zero limit is an existence probe and still validates coverage and
observes graph/blocking.

The tracked snapshot wrapper observes graph/blocking before every child or
inverse lookup, including empty, zero, More, unsupported, and error results.
Returned images are canonical bead records whose exact ref, kind, revision,
and Body agree with a fresh tracked `Get`. Closure reads enumerate only the
selected individual descendant prefixes (or their exact union), observe every
closure sentinel including missing groups, and compare exact derived bytes.
No generic Query or whole-store fallback is used by the indexed source planner.

The separately frozen inverse source capability selects unique source Records
for the closed incoming kinds `parent-child`, `blocks`, `conditional-blocks`,
and `waits-for`. It is not merged into canonical Parent membership. Complete
outgoing metadata includes all custom/association kinds and explicit empty
lists; graph-link coverage is independent of Parent coverage. For Parent-only
edits, the pipeline observes zero-limit inverse probes for reached sources and
retains the complete graph validation. Complete link coverage does not certify
global graph validity after native restore, which can import an unrelated
invalid component. The service builds a post-image guard fact slice with both canonical and legacy
direct children, outgoing blocker endpoints, canonical ancestors, and
waits-for spawner children. It falls back on missing coverage or overflow.
All-kind deletion remains on the complete fallback.

## Planner and accounting

The planner reads target and old/new ancestor chains from the pinned source
images, validates branch-zero canonical refs and identity, and rejects missing
parents/cycles. It walks direct children breadth-first in canonical record-key
order, chunks parent requests at 1024, maintains one remaining budget, and
uses the exclusion seam to merge prepared images exactly once. It refuses an
overflow witness before closure reads.

For a detach, `K` is the number of external ancestor pairs removed from each
member. With remaining budget `R`, discovery is bounded by `floor(R/K)+1` and
divides before multiplying. For a reparent, the charge is the exact difference
between old and proposed ancestor-depth maps; shared suffixes and depth
replacements are counted once. Root attach and no-op moves are handled without
division by zero. Ordered intermediate moves/deletes remain charged even when
the final image cancels them, and deleted sources remain available to later
ordered subtree moves until final-image validation.

The planner builds before/after closure rows only after source discovery and
ordered charging. It retains source record misses/images, closure sentinels,
exact derived row bytes, and misses for proposed rows in the normal lock set.
The existing complete graph walk still validates dangling links, scheduling
cycles, blockers, waits-for, and legacy parent-child facts before publication.
A partial canonical subtree is never passed as a complete graph. Omitting that
global scan for non-delete hierarchy edits requires a separately maintained
global-validity witness; the current child does not claim the 20k/2s bound.

## Required evidence and boundaries

For `a <- b <- c <- d`, detach `c` separately after removing only `d->c`,
only `d->b`, or all `d` closure rows. Each must discover `d` from source,
return `ontology_guard_failed`, and leave source/history/head/log/closure,
predicates, and the absent receipt unchanged. Preserve multilevel moves,
shared-depth replacements, ordered cancellation/cycles, root/no-op, empty/More,
5000/5001 atomic behavior, prepare-to-Lock interference, and real 20k restore /
reopen behavior under the existing two-second budget.

Remote API receipts, source review, backend adoption, focused RunsOn evidence,
whole-tree evidence, and production qualification remain separate claims.
Historical source BLOCK `03237036790e465b88468e6abdfa09504a3169fa`, focused RED
`41543324220`, and whole-tree stale-descendant RED `2e3e1681235fc4433cd9d824fe9d607d51d80bd3` remain unchanged.
