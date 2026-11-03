# Canonical direct-Parent index

Status: proposed schema/storage design for independent source review. This
document implements no capability and authorizes no execution or integration.

## Completion contract

Done = the source-index representation, publication rules, unavailable-index
behavior, and exact layer ownership are independently source-reviewed at one
frozen child; a design document, existing closure index, or unexecuted test does
not prove that the hierarchy repair works.

Scope = the direct-Parent source index and its immediate storage dependencies;
the child-reader API and the detach/reparent consumer are separate reviewed
children. Generic Query, PostgreSQL lease preparation/fencing, authorization,
workloads, and deadlines are outside this child.

The implementation series is complete only when real memory, SQLite, and
PostgreSQL evidence proves atomic source/index publication, bounded child
selection, snapshot/overlay behavior, and the original malformed-descendant,
5000/5001, two-second, and 20k browser/native restore/reopen contracts. Source
review and focused fixtures do not substitute for that evidence.

## Frozen source and the independent authority

The design worktree is
`/tmp/plug-swarm/work/pr254-child-index-schema-20261103`, on
`codex/pr254-child-index-schema-20261103`, based on
`713557b6a3abaf6fe5fc2dbf4ca38561d43f5deb`, tree
`3856e22d98194e4dc8e0a842c1dbac2d20db4321`.

`pipeline.Document.Parent` is authoritative. `pipeline.DecodeDocument` owns
its interpretation and validation. Ancestor closure is independently checked
derived data; neither ancestor selection nor an outgoing reference/attribute
index establishes complete direct-child membership. In particular, source
Parent(d)=c must still discover d if d->c, d->b, or all d closure rows are
missing.

Storage must never import pipeline, decode Parent from JSON, infer a parent
from an ID spelling, or modify Record.Body. Core supplies a validated resolved
parent with the exact record image. A root is an explicitly supplied nil
parent; missing metadata is never interpreted as a root.

## Stored invariants

For a certified `(incarnation, branch, revision)`:

1. Every live bead has exactly one source image and an explicit direct-parent
   state. A canonical root has no edge. A parented bead has exactly one edge
   from its canonical parent Ref to its canonical child Ref.
2. An edge and its source image have the same identity and revision. A source
   update with an unchanged Parent still replaces the indexed image, so a
   child read returns the exact current Body, kind, Ref, and revision.
3. Root/create/reparent/delete/recreate changes publish source, index images,
   edge lifetimes, versions, predicate revisions, log, head, and receipt in
   the existing single publication. Rollback, cancellation, fencing, or a
   rejected header publishes none of them.
4. Membership is independent of closure rows. PutDerived cannot create,
   remove, or certify a source edge. No caller can submit a stand-alone edge
   that is not bound to its staged record image.
5. Branch zero is the existing supported domain. Invalid, key/alias, foreign
   branch, and self parent references are refused. Core still validates live
   parent existence, cycles, graph consistency, and body/metadata agreement.
6. Historical membership and returned bytes belong to the requested revision,
   including revision zero, tombstones, recreation, and moves between parents.
   A historical lookup never reads today's parent relation or Body.
7. A persisted coverage marker certifies a range only after all its source
   images are represented. An absent, incompatible, stale, or unbuilt marker
   causes an explicit unavailable result before returning any children.

The index is trusted storage maintained by the supported write boundary. It
is not a second user-editable relationship. Repairing arbitrary physical
corruption of this source index is outside ordinary hierarchy writes; a
closure-row corruption must not damage or disable the independent source
index.

## Physical representation

SQL drivers use private tables. Names below are private implementation names,
not new DerivedTable values or public Query fields.

| Data | Key and content | Required access path |
| --- | --- | --- |
| `hierarchy_index_coverage` | incarnation, branch, format, built-from, certified-through, state | one metadata lookup in the same snapshot |
| `hierarchy_source_current` | incarnation, branch, child record key; parent record key or explicit root, exact Ref/kind/revision/Body | unique child key; parent/child order index for live parented rows |
| `hierarchy_source_history` | incarnation, branch, child key, start revision; exclusive end revision, parent or root, exact source image | unique child/start; parent/child/start access path with revision interval predicate |

The current table contains only live source images. Each immutable historical
image is effective on `[start,end)`; deletion closes its last interval and
leaves no current edge. Recreate starts a new interval. Updating an unchanged
Parent also closes/opens the image interval, preserving the exact Body for
every source revision. SQL publication uses existing bounded batches; memory
forks changed maps/buckets without mutating any published image or slice.

SQLite revisions retain fixed-width unsigned decimal TEXT and bytewise key
ordering. PostgreSQL retains numeric(20,0) revisions and bytea record keys.
No revision cast to a signed integer or text collation may change the existing
uint64 or canonical key domain. Parent and child keys use the existing
injective record-lock encoding; no delimiter-based Ref normalization is added.

Store exact source bytes explicitly in these images. Re-encoding a decoded
document or replaying a JSON-normalized patch is insufficient byte evidence.
Historical selection fetches only the selected source images from this
index; it does not reconstruct every bead to discover parents.

The schema child adds the representation only, with no child-reader
advertisement, semantic Parent decoding, automatic backfill, or consumer
change. Presence of empty tables is never a completeness certificate.

Memory stores an immutable hierarchy root at each certified publication.
Each root maps canonical parent keys to sorted child-image buckets and child
keys to their parent/image; historical roots are located by revision binary
search. A publication copies changed buckets and shares untouched immutable
images. A child query merges only the requested parent buckets, with a heap
bounded by the number of requested parents, and stops at limit+1 images.
Neither current nor historical discovery iterates the general records map or
all prior versions of a parent. Staged readers build the equivalent overlay
without copying the whole source index.

## Atomic write boundary and activation

The next driver child proposes an optional internal
`HierarchySourceWriter.PutHierarchyRecord(ctx, record, parent *model.Ref)`.
It stages the record and its explicit root/parent metadata in one operation,
under the existing acquired record update lock. It preserves Record and the
ordinary Reader/Query interfaces. Calling PutHierarchyRecord again replaces
both staged components; a later ordinary Put cannot retain an earlier
parent declaration by accident. Delete removes both components automatically.

This capability is available only to trusted core code. Core derives its
parent argument from the same decoded document it serializes into Body.
Storage validates the canonical Ref/branch envelope and binds the argument to
that exact staged byte image; it does not establish JSON semantics or grant
permission. Core must refuse a parent/Body disagreement before any commit.

Compatibility is explicit:

- A store with no certified index continues ordinary unindexed writes. These
  writes cannot make it certified, and child lookup returns ErrUnsupported.
- A new empty store can be certified by the first complete typed publication.
  Under the existing Head fence, the driver proves the durable target is
  empty and every staged bead has typed metadata. It then publishes coverage
  together with all source images. An empty SQL table alone cannot activate
  coverage in a previously populated store.
- Once certified, an ordinary Put of a bead without typed metadata is refused
  before staging; it cannot silently retain an old parent or invalidate
  completeness. Non-bead Put behavior is unchanged. Delete of a certified
  bead uses the stored parent metadata and enforces the same predicate locks
  as a typed write.
- Every publication advances certified-through only when it preserves
  coverage. If an older binary changes Head without maintaining this marker,
  a later reader refuses revisions beyond certified-through. A new writer
  cannot skip that gap and resume certification.

Typed writes require the existing graph/blocking update lock and atomic bump.
The child-reader wrapper always records that same predicate before a lookup,
including an empty result or More=true. This is the conservative observation
of the requested parent-membership predicate; it also guards source/index
activation and a move into or out of any requested parent. Returned children
retain their individual source observations. No new sentinel family or
independently writable parent fact is introduced in this proposal.

No read may succeed while a concurrent legacy write bypasses that predicate:
the certified-store rule rejects untyped bead Put, and certified Delete must
hold/bump graph/blocking. Lock, Head, the exact acquired lock-key header,
lease validation, and final fencing retain their present ordering and
meaning. This is separate from PG615's transaction prepare optimization.

## Existing stores, history, and restore

Opening a populated pre-index store installs only additive schema. Its
coverage is unbuilt, even if current closure happens to be complete or it has
no live children. This series selects explicit refusal until built; it does
not include an online/populated-store backfill or infer historical membership
from a head-only scan. An eventual rebuild is a separately reviewed core
operation that validates all source images and publishes one complete range.

Ordinary certified writes maintain historical intervals from activation
onward. Reads below built-from, above certified-through, across a coverage
gap, or under a different incarnation fail explicitly with ErrUnsupported or
the existing incarnation/history-floor error, without partial children.

Native current-image restore targets an empty store and already validates
bead documents in core. The restore consumer child will supply typed parent
metadata for every restored bead and publish index coverage under the new
incarnation at its actual history floor. Reopen preserves those tables and
coverage. Old-incarnation readers/cursors keep the existing refusal boundary;
they are not rebound to the restored index.

CommitOfflineHistory must not certify its imported revisions from the final
tail image. Until a separately reviewed core producer supplies validated
metadata and exact images for every imported historical bead revision, that
port explicitly leaves the index unbuilt for the imported range and head.
The restore itself retains its current history contract; child lookup is
unavailable. This limitation must be tested and reported, not hidden by a
head-only success or fabricated historical relationships. The required 20k
native current-image restore/reopen flow is covered by the typed path above.

## Reader dependency, held for the next layer

The optional `driver.HierarchyChildrenReader` proposal remains:

```go
HierarchyChildren(ctx context.Context, parents []model.Ref, limit int)
    (records []driver.Record, more bool, err error)
```

It selects live canonical direct children from the reader's bound branch,
incarnation, and revision. Inputs are canonical resolved parent Refs; empty
input selects no rows. Output is duplicate-free and sorted by canonical child
record key. Bodies are caller-owned exact snapshot bytes. Cancellation,
closure, unsupported coverage, or a malformed index image returns no partial
success. Limit validation and a maximum parent-vector size must be explicit
in that child, with arithmetic bounded before calculating limit+1.

One global storage-side limit+1 determines More. SQL must apply parent and
revision predicates before fetching source images, and may not return an
unbounded candidate set to Go for sorting/truncation. The exact multi-parent
query and historical plan are part of the driver review: DDL and a SQL LIMIT
alone do not prove read work is bounded. That review must prove indexed
parent seeks, measured candidate/image work with unrelated source rows, and
bounded historical work; if a mode cannot meet the bound it must explicitly
refuse rather than fall back to a whole-source scan.

Writer readers combine committed indexed children with all staged typed
creates, tombstones, replacements, and moves before determining the global
limit and More. They must exclude every staged child key from the base read
before its limit is applied, then merge staged matching images. A child moved
away cannot consume a page slot or mask a later committed child. A staged
child moved between two requested parents appears once. Staged reads keep
the existing committed revision tokens and never change the bound snapshot.

Memory needs parent-keyed ordered buckets in each immutable state and
revision-aware membership; iterating all record keys is not an index. SQL
needs the physical parent indexes above and snapshot-local overlay handling.
None of these methods calls, broadens, or substitutes generic Query.

## Exact ownership and stack boundaries

Only this new design file is changed now. The following are proposed future
ownership sets; each child is frozen and reviewed before its dependent child
is implemented or executed. Shared hook ownership is not implied by this
table.

| Child | Proposed owned paths | Reviewable outcome |
| --- | --- | --- |
| S0: this design | `internal/storage/hierarchy-index-design.md` | representation, invariants, refusal, and ownership decision |
| S1: schema/state | `internal/storage/sqlite/schema.go`, `internal/storage/postgres/schema.go`, `internal/storage/memory/store.go`, new `internal/storage/memory/hierarchy_index.go`; direct schema/state tests and exact BUILD/manifest registration | additive SQL layout and immutable memory index state; no advertised reader or behavioral consumer |
| S2: driver | new `internal/storage/driver/hierarchy.go` and direct contract tests; new `internal/storage/{memory,sqlite,postgres}/hierarchy_reader.go` and `hierarchy_writer.go`; publication hooks listed below, direct driver tests and exact BUILD/manifest registration | atomic typed publication, coverage/refusal, bounded pinned child reads and staged overlays |
| S3: core consumers | core-owned typed-source publication and restore adapters, then an explicitly handed-off hierarchy consumer/test child | validated Parent metadata on every production bead image; bounded detach discovery and exact consumed closure validation |

The S2 publication hooks are exact dependencies, not current edit permission:

- Memory: `transaction.go` Put/Delete, Commit publication and cleanup;
  `transaction_reader.go` staged state; `offline_history.go` coverage refusal.
- SQLite: `transaction.go` Put/Delete and Commit publication;
  `store.go` Open initialization/coverage, if DDL-only setup is insufficient;
  `offline_history.go` coverage refusal.
- PostgreSQL: `transaction.go` Put/Delete;
  `commit.go` record/index publication; `offline_history.go` coverage refusal.
  New private helpers contain index SQL. No reader.check, lease, final fence,
  or ordinary record/version batching change is part of this design.

PG615 currently owns PostgreSQL transaction.go, reader.go, lease.go, and
commit.go preparation/fencing work. Therefore the PostgreSQL S2 hooks are
held until the controller and that owner hand over exact hooks or a reviewed
base. They are not added to PG615, and no same-file overlap is attempted now.
The existing PR254 production path `internal/core/pipeline/hierarchy.go` and
its `internal/integration/batchsql/hierarchy_test.go` also remain untouched.
S3 requires an explicit later handoff of those consumer paths; this design
does not overwrite PR254's retained worktree or its uncommitted test witness.

The later detach consumer walks authoritative source children breadth-first
from the detached node. For a real detach with K old external ancestors,
discover no more than floor(5000/K)+1 subtree members and refuse over-limit
work before closure enumeration. K=0 is an explicit root/no-op case. Then
read only selected individual descendant prefixes (SQLite union is an
optimization), validate exact source Parent/cycles/graph and complete expected
closure groups, and retain source, graph, closure-sentinel, exact-derived,
and miss observations. No common/empty-prefix fallback is allowed.

The original three malformed-descendant regressions belong in that later
service/batchsql child: missing d->c, missing d->b, and missing all d rows.
Each requires ontology_guard_failed with identical source/history/log,
closure rows, predicate state, and absent receipt. They remain separate from
schema/driver proof and unexecuted until that exact child is reviewed.

## Required implementation evidence, currently unexecuted

- Schema/state: actual SQL catalog/index inspection on fresh and pre-index
  disposable databases; unbuilt refusal after reopen; immutable memory forks;
  unsigned revision and canonical key boundaries. Additive schema alone must
  never return an empty successful child result for an existing store.
- Driver: exact-byte and revision checks across create, same-parent patch,
  move/reparent, delete/recreate, old pinned readers, historical/as-of and
  staged overlays; cancellation, close, rollback, stale lease, and undeclared
  lock refusal; real indexed parent work with unrelated rows, global
  limit+1/More, duplicate/order checks, and empty/more predicate observation.
- Core/restore: typed metadata is derived from every real canonical Parent;
  native restore/reopen uses the real source index; imported-history child
  lookup explicitly refuses while unbuilt. Prove a child create/delete/move
  between prepare and Lock conflicts, and preserve all current source and
  closure observations.
- Consumer: the three malformed-descendant refusals, complete multilevel
  chain, ordered batches, cycles, root no-op, 5000/5001 atomic behavior, the
  unchanged two-second contract, and the real 20k authenticated Chrome/native
  restore/reopen flow. Preserve every prior RED and every new outcome.

All eventual builds/tests use remote-bazel in isolated Plug worktrees. No
gate is run for this design. Each implementation child needs its own exact
source disposition before execution under the controller's current direction.

PR254 `03237036790e465b88468e6abdfa09504a3169fa` source BLOCK and focus
RED41543324220 remain literal, as do the older 2e whole-tree stale d->b
failures. The retained +44 regression witness is not copied, committed, run,
or upgraded by this unit. No PR readiness, merge, approval, release, full S1,
or E3 qualification is claimed.
