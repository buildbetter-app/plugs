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
