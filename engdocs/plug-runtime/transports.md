# Selecting a JSON-RPC transport

Plug keeps one canonical JSON-RPC registry behind every transport. A transport
frames bytes, admits a caller when it has an admission port, and passes the
admitted principal and request context to `rpc.Server.Dispatch`. It does not
add methods, an authorization policy, or a second write path. A method in the
catalog is not reachable until a composition root binds its adapter.

| Adapter | Wire and selector | What owns its lifetime |
| --- | --- | --- |
| In-process | `internal/server/transport/inprocess`; use it for a local call in the same process. | The caller owns the context and the application store. It opens no listener or UI. |
| HTTP | `POST /rpc/v1`; see [RPC and HTTP](rpc-http.md). | The HTTP composition owns the listener, store, lease and shutdown. |
| stdio | Construct with `stdio.New(dispatcher, stdio.Config{})`, then call `Serve(ctx, in, out)` on the returned transport. | The caller supplies the streams, context and any admitted identity. |
| WebSocket | `GET /rpc/v1/ws` after a composition root mounts `websocket.Handler`. | The handler owns one connection; the composition owns the listener and any watch/session manager. |
| MCP | `POST /mcp` after a composition root mounts `mcp.Handler`. | The handler owns request framing; the composition owns the listener, store, lease and shutdown. |

The current server composition mounts HTTP (and its explicitly enabled UI and
watch handlers). The stdio, WebSocket and MCP packages are composition seams;
they are not implicitly mounted, and current `Runtime.Listen` has no `/mcp`
route until a host selects the MCP adapter. Do not infer a runnable route or a
CLI flag from a package, a catalog entry, or a design document. A host that
exposes one of these adapters must mount exactly the route or stream it intends
to support.

Because `stdio.New` validates its inputs and returns an error, a caller handles
construction before serving:

```go
transport, err := stdio.New(dispatcher, stdio.Config{})
if err != nil {
	return err
}
return transport.Serve(ctx, in, out)
```

## Shared identity and cancellation

The dispatcher receives the principal stamped by the transport or by a trusted
in-process caller. Request parameters cannot choose a principal. `_meta.agent`
is an unverified caller label used for anonymous lease and cancellation scope;
it is not authentication. Writes keep the canonical idempotency and replay
rules described in [RPC and HTTP](rpc-http.md), regardless of the adapter.

Every adapter uses the request context for cooperative cancellation and waits
for the synchronous dispatch call to return. A canceled connection, stream or
request must not leave detached work that can commit later. The composition
root still closes the listener, store and lease after it has drained the
adapter it mounted.

## stdio: newline-delimited RPC

The [stdio adapter](../../internal/server/transport/stdio/README.md) owns one
input/output pair. Each line is one JSON-RPC request frame. A final frame may
end at EOF without a newline. The adapter passes the frame bytes to the
canonical dispatcher unchanged and writes each non-empty response followed by
one newline. Notifications produce no output.

The default frame limit is 1 MiB and the per-frame cooperative dispatch timeout
is 30 seconds. An oversized frame is discarded through its newline, emits the
shared `invalid_request` envelope, and does not prevent a later frame from
being served. Malformed JSON stays on the dispatcher error path and returns
the same `parse_error` semantics as in-process dispatch. Clean EOF returns
success without an extra response.

Cancellation closes the input when it implements `io.Closer`, which interrupts
a blocked read. Dispatch is synchronous and receives the caller's context; no
detached goroutine is started. A read or write failure is a transport failure,
not an arbitrary peer-visible diagnostic. The adapter creates no listener,
store, session, authenticator or UI. The caller supplies the context; a host may
stamp an authenticated principal when local authentication is configured,
otherwise the dispatcher uses the anonymous principal. Stdio does not read
`Authorization`, cookies or a credential field from the JSON frame.

One transport owns one stream. If a host needs multiple callers, it must give
each caller a separately admitted stream and context rather than multiplexing
untrusted identities into `_meta`.

## WebSocket: one admitted connection

The [WebSocket handler](../../internal/server/transport/websocket/handler.go)
uses `GET /rpc/v1/ws` and upgrades only after `authn.AdmitWebSocket` accepts
the request. The handshake must use the WebSocket upgrade headers and a valid
key. Host, peer, loopback, TLS, bearer and same-origin `Origin` checks remain
with `authn`. Browsers cannot set `Plug-RPC` on a handshake, so this endpoint
does not require that HTTP marker. An anonymous connection is loopback-only;
an authenticated non-loopback listener needs a configured provider and
directly terminated TLS.

After upgrade, each masked text or binary message is one canonical JSON-RPC
payload. Fragmentation and interleaved ping/pong control frames are handled
before dispatch, and text messages must be valid UTF-8. The zero-value bounds
are:

| Bound | Default |
| --- | ---: |
| Frame and complete message | 1 MiB |
| Pending request queue | 64 messages |
| Pending response queue | 64 messages |
| Cooperative dispatch | 30 seconds |
| Read timeout / write timeout | 60 seconds / 35 seconds |

Oversized or malformed frames close the socket with a bounded protocol code
(`1009` for too large and `1002` for a protocol error). A full request or
response queue closes with `1013` instead of retaining unbounded work. A
notification that has no canonical response is reported as an `rpc.rejected`
push with reason `notification_no_result`. Responses remain the exact bytes
returned by the canonical dispatcher.

The admitted identity is stamped on the connection context and on every
dispatch. A peer close or connection cancellation cancels in-flight work;
closing the connection also stops the bounded reader, dispatcher and writer
loops. The handler owns no store, listener, session manager or watch manager.
Its `Stream` helper only converts an already opened `watch.Subscription` into
`event.notify` messages and closes that subscription on return, including on a
write failure or cancellation. Opening and authorizing a watch remain owned by
the watch/session composition. The current server composition does not mount
this route automatically.

## MCP: streamable HTTP request/response

The [MCP adapter](../../internal/server/transport/mcp/README.md) is a separate
HTTP composition seam. A host mounts its handler at exactly `POST /mcp`; the
handler does not create a listener, store, lease,
UI, session store or authorization policy. HTTP admission still requires
`Plug-RPC: 1`, `Content-Type: application/json` (optionally
`charset=utf-8`), and the existing Host, peer, Origin, TLS and bearer rules.
Cookies and `Content-Encoding` are refused. The adapter does not accept an
MCP credential as a substitute for the shared authenticator.

The zero-value request bounds are a 1 MiB body, 64 concurrent requests and a
30-second request/read deadline. A body may contain one request or a sequential
batch of 1–100 requests; batches are not atomic and `initialize` must be sent
alone. A notification-only or response-only body returns HTTP 202 with no
response body. Unsupported protocol messages, malformed objects and unknown
tools use the canonical JSON-RPC error catalog rather than a transport-specific
error language.

### Initialize and discover tools

`initialize` requires `protocolVersion`, `capabilities` and non-empty
`clientInfo.name` and `clientInfo.version`. The adapter understands the MCP
protocol offers `2025-03-26`, `2025-06-18` and `2025-11-25`, and replies with
the configured version when an offer is unknown. Its response advertises
`capabilities.tools.listChanged: true` and the composed server name/version.

On a successful initialize response, the adapter returns an
`MCP-Session-Id`. The ID is authenticated for both the adapter instance and
the admitted principal; clients send it on later requests. It is not a
durable session record, and a handler restart invalidates it. Without that
header, cancellation is limited to the current connection. A supplied session
from another principal or handler is refused with `not_found`.

`tools/list` projects only visible canonical methods marked as agent-core
tools. Dotted names use the exact underscore mapping (`bead.ready` becomes
`bead_ready`). The adapter also exposes the fixed transport tools
`plug_call` and `plug_describe`, which reach already visible canonical methods;
they do not add a second method registry. Input schemas come from the
canonical catalog. Tool pagination is unsupported, so a non-empty `cursor` is
refused. A changed visible-method hash produces
`notifications/tools/list_changed` before the next finite response, or is
passed to the composition's notification sink when one is installed.

### Call, cancel and close

`tools/call` lowers back to the canonical method request with the admitted
principal, caller metadata and a generated write idempotency key when one is
not supplied. Canonical result JSON is returned as MCP text content, so
committed warnings remain visible. Canonical domain errors are returned as
`isError: true` content, and canonical replay metadata remains in the tool
result's `_meta.replayed`. Conflicting duplicate `_meta` values are refused;
the outer MCP metadata is preserved when `plug_call` carries nested params.

`notifications/cancelled` names an active request by its JSON ID. IDs are
normalized for equivalent JSON number forms, and a duplicate active ID in the
same principal/session/agent scope is refused. Cancellation reaches the
canonical context and releases its active request slot; it cannot commit work
after the request is canceled. On current `main`, closing the composed runtime
drains HTTP first, then closes watches and sessions, cancels remaining work,
releases the lease and closes the store. A future host that separately mounts an
MCP stream owns closing that stream before invoking this runtime lifecycle;
current `main` has no `/mcp` route or continuous MCP stream.

The finite MCP adapter may use an SSE response (`event: message`) to carry a
tool-list-changed notification before the response that caused discovery. It
does not provide a continuous `GET /mcp` notification stream on current main;
that is a separate bounded stream/composition feature and must not be assumed
from `tools/list` or `capabilities.tools.listChanged`. MCP, WebSocket and
stdio are independent adapters: one is never implicitly stacked on another.
