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

The component tests exercise the actual HTTP/RPC boundary with real native-host
bindings, authenticated principals, durable mutation/refusal/replay, and
claimNext defaults and terminal receipts. They do not qualify a WASI artifact,
`composition.Open`, or an E5 concurrency or lagging-snapshot workload.

## EP2 hierarchy authorization

Done = the five recorded parent/child rules are enforced at the authorization
boundary, proven by focused and whole-tree exact-head RunsOn tests and an
independent GPT-6 review; a typed seam or static contract alone does not count.
Scope is the bounded authorization layer. Integrated hierarchy persistence,
closure/ontology guards, public RPC and the progress producer remain EP3–EP5
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
observed ACL/context/plugins/ontology generations. Continuation hosts must
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
