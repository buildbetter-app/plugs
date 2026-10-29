# Plug runtime guides

These guides describe accepted source **`471d0199c89acf9e242f44978f2824120726de32`**
(2026-10-29). Start with the [disposable CLI session](cli.md) to try the
implemented lifecycle without configuring a server or database.

| Guide | Use it for |
| --- | --- |
| [CLI](cli.md) | One-process memory sessions, commands, retries and cancellation. |
| [RPC and HTTP](rpc-http.md) | Bound methods, JSON-RPC bytes, admission and HTTP limits. |
| [Native plugins](native-plugins.md) | Public SDK registration and trusted host capabilities. |
| [Events](events.md) | Committed events, current visibility, delivery and checkpoints. |
| [Storage](storage.md) | Memory, SQLite and PostgreSQL lifetimes and supported persistence. |

## Snapshot and boundaries

All implementation claims use the
[immutable source tree](https://github.com/buildbetter-app/plugs/tree/471d0199c89acf9e242f44978f2824120726de32).
Relative source links browse the checkout; use that SHA when comparing later
versions. The older [package map](../plug-packages/README.md) remains pinned to
its own earlier snapshot. The [design](../design/plug/design.md),
[OpenRPC catalog](../design/plug/contracts/openrpc.json) and
[behavior cards](../design/plug/spec/behaviors.md) cover a broader contract;
an entry there alone does not install an RPC handler or CLI command.

At this snapshot:

- The CLI composes a real core, Writer, memory store, RPC dispatcher and
  in-process byte transport. Each invocation discards its memory store.
- HTTP is an embeddable admitted transport. There is no runnable `plug server`
  command or remote CLI transport in this snapshot.
- Native plugins are compiled in and explicitly connected by a composition
  root. The public Go packages are `pluginapi` and `sdk/go`; applications
  outside this module use JSON-RPC rather than importing `internal` packages.
- All three baseline drivers implement history, receipts and checkpoints.
  Ordered audit-plan persistence is included only for memory; see the
  [driver distinction](storage.md#audit-plan-boundary).
- The optional Writer audit producer and event delivery are present. The
  protected-source ontology audit adapter and full connected conditional
  dependency behavior remain separate follow-ons. Their presence on another
  branch supplies no behavior at this pin.

OIDC/login sessions, durable API-key persistence, collaboration presence and
ontology RPC adapters are also absent from this pin. Remote CLI/HTTP-client
adapters are follow-ons even though the embeddable HTTP server transport is
present. No follow-on implementation commit is incorporated by these guides.

The local examples use synthetic disposable state. Package tests establish
their named boundaries; they do not qualify production deployment, failover,
performance, hosted CI, migration or a public release. Historical race and
benchmark results are not certification of this runtime snapshot.
