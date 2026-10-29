# RPC and admitted HTTP

Source: **`471d0199c89acf9e242f44978f2824120726de32`**;
[guide scope](README.md#snapshot-and-boundaries).

## Call the installed surface

The [registry](../../internal/server/rpc/registry.go) binds handlers before
`rpc.New` snapshots it. The server adds `server.hello`, `rpc.methods`,
`rpc.describe` and `rpc.discover`.
[BindService](../../internal/server/rpc/service.go) adds exactly `bead.get`,
`bead.ready`, `bead.create`, `bead.update`, `bead.claim`, `bead.comment`,
`bead.close` and `history.query`. Discovery reports bound methods, subject to
its configured visibility policy. It does not grant resource access.

Use named-object params and a request ID. For example, these are request bytes
for a bound service; the write needs a fresh random key of its own:

```json
{"jsonrpc":"2.0","id":"ready-1","method":"bead.ready","params":{"limit":10,"_meta":{"project":"local","agent":"example"}}}
```

[Dispatch](../../internal/server/rpc/server.go) validates the closed envelope,
params and result schemas. Batches contain 1–100 calls, execute sequentially
and return responses in request order, omitting notification responses. They
are not atomic. Write notifications are dropped without executing a write.
Unbound methods return `method_not_found`; `session.close`, `tx.apply`,
ontology, event, restore and other catalog entries are unbound here.

Raw writes require `_meta.idempotencyKey` encoding at least 128 bits (UUIDv4,
ULID, hex or unpadded base64url); callers supply randomness. The
[metadata validator](../../internal/api/wire/meta.go) rejects short keys and
client-supplied replay metadata. The same principal/method/key scope replays
the stored result for 24 hours, including warnings, with response-only
`_meta.replayed:true`. Anonymous scope also uses the agent label. Keep the
original key after an uncertain commit or lost answer; do not retry with a
new key. See [receipt ownership](storage.md#ownership-and-failure).

`_meta.project` uses the server default when omitted; project-scoped operations
fail with `project_required` if neither exists. `_meta.agent` asserts no
identity. Effective identity comes from admitted transport context.
`_meta.deadlineMs` supplies a cooperative per-call budget; an already canceled
call refuses with `busy`. Cancellation does not undo a committed write.

Ready/history pages allow 1–1,000 items. Ready continues over live data, so
concurrent changes may repeat or skip rows; `history.query` pins its required
`asOf`. [Signed cursors](../../internal/server/rpc/cursor.go) bind method,
principal/sponsor/caveats, anonymous agent, project/filter, ontology and store
incarnation. Keep the request scope stable across pages. A stable server
`CursorKey` of at least 32 bytes preserves signatures across restart; the
default random key does not. The service refuses views/query-string/event
filters and graph scans above 20,000 beads rather than returning partial data.

## HTTP admission and limits

[httpjson.New](../../internal/server/transport/httpjson/handler.go) creates a
handler for exactly `POST /rpc/v1`, with no query string or path aliases.
Every request needs `Plug-RPC: 1` and `Content-Type: application/json`
(optional `charset=utf-8`). Cookies and any Content-Encoding refuse. Admission
precedes body reading and domain dispatch. RPC results/errors use HTTP 200;
notification-only messages use 204. Transport errors use the shared JSON-RPC
error envelope with `id:null`; parser failures before the handler retain
`net/http` framing.

| Default bound | Value |
| --- | --- |
| Body / concurrent admitted requests / headers | 1 MiB / 64 / 16 KiB |
| Header read / request read | 5 s / 30 s |
| Admission + body + cooperative dispatch | 30 s per request |
| Response write / idle connection | 35 s / 60 s |

[Config](../../internal/server/transport/httpjson/config.go) uses defaults for
zero fields and refuses negatives. Oversize body: 413; bad media/encoding:
415; unreadable body: 400; body deadline: 408; saturated concurrency: 429.
Bad path: 404; wrong method: 405. Credential failure: 401; forbidden posture
or unsupported scoped identity: 403; unavailable authentication: 503.
This concurrency bound does not implement per-principal token buckets,
sponsor/project budgets, login throttling or separate bulk queues.

[Authentication admission](../../internal/server/authn/http.go) requires
loopback binding, peer and Host in anonymous mode, with no override. Origin,
when supplied, must match the actual scheme and authority. Non-loopback
listeners require a configured provider and directly terminated TLS. Forwarded
headers confer no authority. API keys are process-local host-issued credentials
and do not prove interactive human presence; scoped keys need an actually
installed authorizer. The HTTP handler installs no authorization policy.

## Embedding and local verification

`handler.Listen(ctx, "127.0.0.1:0", nil)` performs admitted binding; the owner
runs `Serve`, then `Shutdown` or `Close`, waits for Serve and application work,
and only then releases the store/lease. Canceling the root context cancels
requests but still requires closing the server. A missed shutdown deadline
forces cancellation and socket closure. Dispatch is synchronous: handlers must
honor context; the transport cannot forcibly stop arbitrary Go work.

The [in-process transport](../../internal/server/transport/inprocess/inprocess.go)
carries the same bytes with no listener/UI. A canceled round trip is a Go
transport error; protocol failures remain response bytes. Run the existing
[HTTP tests](../../internal/server/transport/httpjson/lifecycle_test.go) locally:

```sh
bazel --ignore_all_rc_files test //internal/server/transport/httpjson:httpjson_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_arg=-test.v --test_timeout=900
```

They compose real memory/core/RPC with disposable loopback listeners, including
HTTP/in-process parity and lifecycle. WebSocket, MCP, stdio, interactive
sessions, trusted-proxy TLS, CORS/preflight and a CLI server command remain
unsupported by this transport snapshot.
