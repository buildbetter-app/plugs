# MCP transport adapter

`New(rpcServer, authenticator, Config{})` creates a bounded `http.Handler` for
exactly `POST /mcp`. It uses `authn.AdmitHTTP` and stamps the admitted
principal into the existing RPC context; it does not add an authorization
policy or product method.

MCP `initialize`, `ping`, `tools/list` and `tools/call` are transport protocol
methods. Agent-core descriptors in the visible canonical registry are exposed
as underscore tool names (`bead.ready` becomes `bead_ready`), and the two
transport tools `plug_call` and `plug_describe` reach other visible bound
methods. Tool input schemas include the canonical definitions they reference.
Every tool invocation is lowered back to the canonical JSON-RPC
request, including caller `_meta`, generated write idempotency keys,
cancellation and the admitted principal. `initialize` returns a random,
authenticated `MCP-Session-Id` bound to that principal and handler; clients
send it on subsequent requests. Without one, cancellation is confined to the
actual connection. Agent labels add an unverified caller scope. Equivalent
JSON IDs share an active-call key, and duplicate active IDs are rejected.
POST bodies may contain one request or a sequential 1–100 request batch;
`initialize` must be sent alone. Accepted notification/response-only bodies
receive HTTP 202 with no response body. Conflicting duplicate caller metadata
is rejected, and `plug_call` preserves metadata with or without nested params.

Canonical result JSON is returned as MCP text content, so committed warnings
remain visible. Canonical domain errors are returned as `isError: true` tool
content containing the canonical error object, and canonical replay metadata
is preserved in the MCP tool result's `_meta.replayed`. Malformed protocol
requests and unsupported tools use the shared
canonical JSON-RPC error catalog. A composition root may provide
`Config.OnNotification` to forward `notifications/tools/list_changed` to its
MCP stream; without a sink, a changed tool hash is sent as an SSE notification
before the next request's response on that session. Body reads use the
earlier request/read deadline. The handler has no
listener, store, lease, UI, session store or detached goroutines; the
composition root owns those concerns.
