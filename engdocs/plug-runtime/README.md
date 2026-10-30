# Plug runtime guides

These guides describe the accepted runtime and CLI behavior at the audited
source snapshot
[`463881cec205e6cd3e5b85d1bac7d4018a153d5f`](https://github.com/buildbetter-app/plugs/tree/463881cec205e6cd3e5b85d1bac7d4018a153d5f)
(2026-10-30). The design and OpenRPC catalog describe a wider target surface.

| Guide | Use it for |
| --- | --- |
| [Shared server](server.md) | Starting `plug server` or `plug server --local`, storage selection, configuration, restart persistence and identity boundaries. |
| [CLI](cli.md) | Persistent local SQLite, explicit memory sessions, admitted remote HTTP, commands, retries and cancellation. |
| [RPC and HTTP](rpc-http.md) | JSON-RPC registry, explicit service bindings, admission and transport limits. |
| [Native plugins](native-plugins.md) | The public SDK, trusted native host and callback capabilities. |
| [Events](events.md) | Ordered audit plans, materialization, delivery and fenced checkpoints. |
| [Storage](storage.md) | Memory, SQLite and PostgreSQL ownership, persistence and fencing. |

## Snapshot and package boundaries

Relative source links point into this checkout; use the SHA above when
comparing a later tree. The older [package map](../plug-packages/README.md)
and canonical [design](../design/plug/design.md) remain broader references.
An entry in a contract or catalog does not install a handler or a composition
root.

At this snapshot:

- `cmd/plug` can run a persistent local SQLite session, an explicit
  process-lifetime memory session, an admitted remote HTTP client, or the
  shared HTTP server through `plug server` and `plug server --local`.
  The server and client read different, deliberately strict `.plug/config.yaml`
  shapes; use `--server` or `PLUG_SERVER` when a CLI connects to a running
  server.
- `server/rpc` owns wire validation and cursors. `BindService` supplies the
  lifecycle surface; `server/ontologyrpc` and `server/collab` are explicit
  adapters that a composition root may bind. `ontology.apply` and
  `presence.watch` remain unbound and refuse with `method_not_found`.
- `server/authn`, the process-local API-key provider and the bounded OIDC core
  session provider admit credentials only when a host composes them. The
  runnable CLI server is anonymous and loopback-only; the HTTP transport has
  no login route, cookie mode or authorization policy of its own.
- The public Go surface is `pluginapi` and `sdk/go`. Native plugins are
  trusted compiled-in code; WASM isolation, artifact builds, durable plugin
  state and automatic extension exposure are follow-ons.
- Memory, SQLite and PostgreSQL implement history, receipts, checkpoints and
  complete ordered audit-plan persistence. The connected conditional-audit
  service-to-delivery proof is qualified against memory; SQL persistence and
  the engine-specific acceptance targets are separate evidence, not a
  production recovery claim.

The current package boundaries are exercised by the real source-linked tests
in the individual guides and by the merged integration packages:
[audit](../../internal/integration/audit/README.md),
[API-key HTTP](../../internal/integration/apikey/README.md),
[SQLite connected acceptance](../../internal/integration/sqliteacceptance/README.md),
[OIDC](../../internal/integration/oidc), and
[extended behavior slices](../../spec/extended/README.md). These are bounded
package checks, not a full behavior-card, performance, hosted-CI, deployment,
failover, migration or public-release certification.

Unsupported surfaces must stay explicit: network event watches, persistent
API-key storage, public login methods, authorization-fact management,
`ontology.apply`, dump/restore commands, local PostgreSQL CLI mode,
WASM/plugin isolation and production topology are not implied by a catalog
entry, a package README or an open design item.
