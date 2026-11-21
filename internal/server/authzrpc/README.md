# Fixed-permission RPC composition

`New` constructs one pipeline Writer with the fixed Checker, EventContext and
ontology, then gives that Writer to the domain service used by GuardedService
and the authenticated HTTP/RPC Handler. Store, active Lease and authentication
Provider are supplied by the caller.

`Config.Mutators` is an optional `[]pipeline.MutatorBinding`. A trusted host can
pass the output of `pluginhost.Host.Mutators()` directly:

```go
composition, err := authzrpc.New(ctx, authzrpc.Config{
    Store: store, Lease: lease, Provider: provider,
    Facts: facts, Namespace: namespace, DefaultProject: project,
    Mutators: host.Mutators(),
})
```

These bindings enter the existing Writer through `pipeline.WithMutators`.
The pipeline retains its binding validation, declared field ownership, callback
ordering, fail-open setting, authorization before and after mutation, and
current authorization on durable receipt replay. A nil or empty slice retains
the existing behavior. This input cannot replace the Checker or EventContext.

The caller owns callback resources and must keep the host and any source
artifact resources alive while requests can use the returned composition.
Keep that composition and Handler for their intended lifetime; constructing a
second Writer or rebuilding the composition per request changes the service
and cursor-key lifetime. This package does not take ownership of the borrowed
Store, Lease, Provider or plugin host.

The host also binds the cataloged `bead.heartbeat` and `bead.release` through
`claimrpc.Bind` on the guarded service. Both public adapters require an
authenticated sponsored Agent and fence a lost storage lease before dispatch.
Only the fixed component maps these protected claim-maintenance methods to its
existing bead `modify` predicate. Other policies retain the original method and
their explicit forbids. Stored-image assignment guards and the Writer's current
holder, expiry, replay and checkpoint/record fences remain in place. Public
metadata cannot select the private `system:core` recovery path; no new reclaim
semantics or plugin policy vocabulary is introduced.

The guarded heartbeat authorizer checks the current base lease, holder, storage
epoch and effective overlay deadline before either a fresh request or a published
receipt can succeed. An old heartbeat receipt therefore does not bypass a
released, reassigned, missing or expired claim. This is an authenticated-host
guard; the accepted anonymous Writer replay path is unchanged.

Heartbeat can refresh/recreate only an overlay for an existing valid base
`ClaimLease`; a truly missing base lease returns `lease_not_held` and publishes
no durable write. The controlled constructor is exposed only in the test build;
`New` retains wall time and there is no new server configuration or RPC clock.
The new three-driver HTTP assertions are source only pending independent review
and S1-authorized execution. They do not qualify `composition.Open`, full B05,
authentic reference twins or all Go/SQL clock paths.

`claim_interleaving_test.go` covers B05 claim interleaving on memory, SQLite and
PostgreSQL over authenticated HTTP. A wrapper around each real store runs one
competing commit at the claim's `Store.Write`, after both authorization stages:
a competing claim by an agent with independent or identical grants (exactly one
claim; the loser gets retryable `busy` and no receipt), revocation of the
claimer's only path (`forbidden`, nothing published), revocation of only the
competitor's path or of one of the claimer's two paths (reauthorized, exactly
one claim), and a holder that loses permission after winning (the claim stays
live; other agents still get `busy` or `lease_not_held`; the holder's heartbeat
and release are `forbidden` without details until the grant returns). The
reauthorization rows also count the request's own `Store.Write` calls (two:
the invalidated attempt and the retry). Lease refusals (claim, release, update,
close and their `tx.apply` forms) are released only after `AuthorizeBefore`
admits the operations prepared so far; heartbeat already authorizes before it
reads the target. So an agent with no grant gets the same detail-free
`forbidden` for every one of those forms whether the bead is unclaimed, claimed
by someone else, or released, while an authorized non-holder keeps
`busy`/`lease_not_held`, still ahead of a stale CAS expectation.

The component tests exercise the actual HTTP/RPC boundary with real native-host
bindings, authenticated principals, durable mutation/refusal/replay, and
claimNext defaults and terminal receipts. They do not qualify a WASI artifact,
`composition.Open`, or an E5 concurrency or lagging-snapshot workload.

## EP2 hierarchy authorization

Done = the five recorded parent/child rules are enforced at the authorization
boundary, proven by focused and whole-tree exact-head RunsOn tests and an
independent GPT-6 review; a typed seam or static contract alone does not count.
Scope is the bounded authorization layer. Integrated hierarchy persistence,
closure/ontology guards, hierarchy write RPC and the progress producer remain EP3–EP5
dependencies and require their own end-to-end evidence.

`Config.Hierarchy` explicitly opts the trusted composition into
`Checker.AuthorizeHierarchy(ctx, stage, proposal, trackedSnapshot)`. It defaults
to false; ordinary #18 update/claim guards and unsupported write refusals are
unchanged. Only `bead.create`, `bead.reparent` and `tx.apply` containing ordered
create/reparent operations enter this adapter. The proposal carries the actual
stored `Before` and complete final `After` for each target, including repeated
targets. Aliases must resolve to an earlier allowed create. All stored hits,
creation/parent misses, project assignments, context and ACL facts join the
Writer's tracked snapshot.

An attach checks edit on the child and new parent, each in its own project.
Creation additionally checks the fixed destination-project modify permission;
the child check preserves ref caveats and code-link restrictions on the new
resource. These are existing #18 `modify` predicates, restricted to the typed
hierarchy path, with authenticated sponsored Agents and no new User/deputy
write permit. Only the fixed policy receives that predicate mapping; code
links and other rule sets keep the original method, including any explicit
reparent/transaction forbids. Reparent cannot change the child's project or other protected
assignment fields. A move/detach never asks for old-parent permission. Before,
after and current receipt-replay checks all use the installed chain; an old
receipt is never a grant. The Writer must still enforce ontology, real child
guards, cycles, atomic closure maintenance and the 5,000-row bound.

`ParentDisclosure` first requires a readable child. An unreadable parent yields
only `{exists:true}`; a readable parent may also yield its canonical Ref.
`VisibleChildren` consumes a complete direct-child candidate set from one
pinned query, validates actual parent membership, and returns readable refs
plus the exact hidden remainder. Hidden identities, projects and statuses are
never returned. The producer must refuse truncated/capped candidates and read
visible statuses on the same snapshot before grouping; this helper does not
prove query completeness or authorize a new wire shape.

`ReadAuthorityBinding(ctx, principal, snapshot)` returns an opaque digest of
the authenticated caller/epoch, namespace/incarnation, validated facts and
observed ACL/context/plugins/ontology generations and current authority revision. Continuation hosts must
obtain it from a **fresh current authority snapshot** on every request and
compare the cursor binding before serving a pinned data page or hidden count.
Reusing only the cursor's old data snapshot cannot detect revocation. The
binding is a scope identity, never a permit; per-record checks still apply.

For epic progress use `AuthorizeEpicProgress(ctx, principal, contentReader,
parentRef, completeDirectChildRefs) (EpicProgressAuthority, error)`. It opens
and fences one current authority reader, then checks the parent's and each
child's current read authority against the caller's pinned content membership.
It returns `Visible`, exact `Hidden` and `PolicyBinding` from that one authority
snapshot. EP4/EP5 must bucket only visible statuses from `contentReader` and
refuse incomplete candidate enumeration; an old content revision never keeps
revoked read grants alive. The helper freshly validates its binding before
returning. After bucketing and immediately before publishing, the host must
also call `ValidateReadAuthorityBinding(ctx, principal, PolicyBinding)`; a
changed generation refuses the whole result without recounting under a second
authority snapshot or returning any partial count.

Public `bead.get`, `bead.ready` and `history.query` project parent relations
from pinned content using current child/parent authority. A readable parent
keeps the existing bare ID string; an unreadable parent becomes exactly
`parent:{exists:true}`; roots omit the member. Request and persisted Parent
Refs are unchanged. `ParentHidden` is internal projection state only.

View producers can call `AuthorizeParentProjections(ctx, principal,
contentReader, visibleChildRefs)` once for the complete item/group output.
`ParentProjectionAuthority.Parents` is keyed by child ID and contains typed
`ParentDisclosure` values, together with one `PolicyBinding`. The singular
`AuthorizeParentProjection` delegates to this bounded batch. A source failure,
unreadable child or invalid batch refuses the complete projection. Producers
must also run the final `ValidateReadAuthorityBinding` before publication.
The binding includes the current authority revision, so concurrent source
changes conservatively refuse refresh rather than retaining a record-dependent
custom-rule or project-caveat grant. Ordinary view traversal is EP4's layer.

The shared canonical Parent read codec does not itself enable lifecycle child
status qualification. Until EP3 installs that complete guard, a tracked direct
child leaves `NoOpenChildren` unknown, refusing guarded parent close/fail rather
than claiming observed absence. Unrelated roots retain their lifecycle behavior.

## Atomic close with hierarchy enabled

A close-only `tx.apply` retains the ordinary adapter and requires separate
`tx.apply` and `bead.close` permits. A mixed batch of create, reparent and close
composes the existing hierarchy child/parent/project checks with the original
policy chain for every operation, expectation-only target and close. The fixed
CP7 policy itself gains no additional verbs. Other mixed hierarchy operation
kinds remain unsupported and refuse the complete batch.

The Writer supplies the same stored Before and final After for each operation
on a target. For an earlier created target, the hierarchy adapter checks its
tracked miss and strict create assignment, accounting only for verified reason
comments from the typed close operations. Close then checks the same final
image; it cannot acquire another parent or project assignment. Replay checks
current authority and returns the stored response without reapplying a slot.
Ordered ontology guards, rollback, provenance and one-revision publication
remain in the existing Writer.
