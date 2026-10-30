# RPC and admitted HTTP

Source: **`127ac22e1d003cce4babe6cbcbc8367ad264e649`**;
[guide scope](README.md#snapshot-and-package-boundaries).

## Bind the installed surface

The [registry](../../internal/server/rpc/registry.go) is assembled before
`rpc.New` snapshots it. `BindService` adds exactly `bead.get`,
`bead.ready`, `bead.create`, `bead.update`, `bead.claim`,
`bead.comment`, `bead.close` and `history.query`, alongside the server's
`server.hello`, `rpc.methods`, `rpc.describe` and `rpc.discover`.
Discovery reports bound methods; it does not grant resource access.

The optional bindings are explicit composition seams:

- [`ontologyrpc.Bind`](../../internal/server/ontologyrpc/bind.go) adds
  `ontology.get`, `ontology.describe`, `ontology.schema`,
  `ontology.check`, `ontology.plan`, `ontology.history` and
  `ontology.diff`. `ontology.apply` is absent and returns
  `method_not_found`.
- [`collab.Bind`](../../internal/server/collab/rpc.go) adds
  `session.open`, `session.close`, `presence.set` and
  `presence.list`. The composed runtime's watch adapter additionally binds
  `presence.watch` when it has the real collaboration snapshot source. It
  emits a signed snapshot cursor and empty `watch.delta` signals for changed
  current `presence.list` rows; clients read the masked rows through
  `presence.list`. One-shot CLI commands do not open a collaboration session.
- The event reader and authentication providers are embeddable packages;
  this baseline registry does not silently bind event watches or login routes.

Use named-object parameters and a request ID. A write needs its own fresh
idempotency key:

```json
{"jsonrpc":"2.0","id":"ready-1","method":"bead.ready","params":{"limit":10,"_meta":{"project":"local","agent":"example"}}}
```

[Dispatch](../../internal/server/rpc/server.go) validates the closed envelope,
params and result schemas. Batches contain 1–100 calls, run sequentially and
return responses in request order; they are not atomic. Notification writes
are dropped without execution. Raw writes require an idempotency key of at
least 128 bits. The same principal/method/key scope replays for 24 hours with
response-only `_meta.replayed:true`; a different request under that key
returns `idempotency_key_reused`. Keep the original key after an uncertain
commit or lost answer. `_meta.agent` is an unverified anonymous lease label,
not an identity claim.

Project defaults, cooperative `_meta.deadlineMs` cancellation and signed
cursors are enforced at this boundary. Ready pages continue over live data and
may repeat or skip rows after concurrent changes; `history.query` pins its
`asOf` revision. Cursors bind method, filter, effective principal, ontology
and store incarnation. A stable `CursorKey` of at least 32 bytes preserves
signatures across restart; the default key is per server. Unsupported filters
and graph scans above 20,000 beads refuse instead of returning a partial page.

## HTTP admission and limits

[httpjson.New](../../internal/server/transport/httpjson/handler.go) creates a
handler for exactly `POST /rpc/v1`. Every request needs
`Plug-RPC: 1` and `Content-Type: application/json` (optional
`charset=utf-8`); cookies and Content-Encoding are refused. Admission runs
before body reading and domain dispatch. RPC results/errors use HTTP 200,
notification-only requests use 204, and transport failures use the shared
JSON-RPC error envelope with `id:null`.

| Default bound | Value |
| --- | --- |
| Body / concurrent admitted requests / headers | 1 MiB / 64 / 16 KiB |
| Header read / request read | 5 s / 30 s |
| Admission + body + cooperative dispatch | 30 s per request |
| Response write / idle connection | 35 s / 60 s |

Zero configuration fields select these defaults; negative fields refuse
construction. Oversize body is 413, bad media/encoding 415, unreadable body
400, body deadline 408, saturated concurrency 429, bad path 404 and wrong
method 405. Credential failure is 401, forbidden admission is 403 and an
unavailable provider is 503. No CORS, preflight, WebSocket, MCP, stdio,
trusted-proxy TLS or cross-origin browser mode is installed in this handler.

Anonymous listeners bind only loopback with no override. Authenticated network
listeners require a configured provider and directly terminated TLS. Host,
peer, Origin, bearer and `Plug-RPC` checks belong to
[authn](../../internal/server/authn/http.go); forwarded headers do not confer
authority. Process-local API keys and OIDC core-session tokens are bearer
credentials, not interactive proof by themselves. OIDC has no public login
route here; trusted composition must complete the flow and deliver the opaque
`plg_s_` session token. An authorization plugin is not installed by HTTP.

## Embedding and verification

`handler.Listen(ctx, "127.0.0.1:0", nil)` performs admitted binding. The owner
runs `Serve`, then `Shutdown` or `Close`, waits for Serve and application
work, and only then releases the store and lease. Root-context cancellation
cancels requests but still requires closing the server. Dispatch is synchronous
and handlers must honor context.

The [in-process transport](../../internal/server/transport/inprocess/inprocess.go)
carries the same bytes without a listener or UI. Run the focused transport and
binding checks:

```sh
bazel --ignore_all_rc_files test \
  //internal/server/transport/httpjson:httpjson_test \
  //internal/server/ontologyrpc:ontologyrpc_test \
  //internal/server/collab:collab_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900
```

The API-key and OIDC connected fixtures exercise actual loopback HTTP,
admission, service writes and refusal paths:
[`apikey_test`](../../internal/integration/apikey/BUILD.bazel) and
[`oidc_test`](../../internal/integration/oidc/BUILD.bazel). These bounded
tests do not certify deployment, performance, production TLS termination,
authorization policy integration or a network watch service.
