# MCP transport adapter

`New(rpcServer, authenticator, Config{})` creates a bounded `http.Handler` for
exactly `POST /mcp`. It uses `authn.AdmitHTTP` and stamps the admitted
principal into the existing RPC context; it does not add an authorization
policy or product method.

MCP `initialize`, `ping`, `tools/list` and `tools/call` are transport protocol
methods. Agent-core descriptors in the visible canonical registry are exposed
as underscore tool names (`bead.ready` becomes `bead_ready`), and the two
transport tools `plug_call` and `plug_describe` reach other visible bound
methods. Every tool invocation is lowered back to the canonical JSON-RPC
request, including `_meta`, generated write idempotency keys, cancellation and
the admitted principal.

Canonical result JSON is returned as MCP text content, so committed warnings
remain visible. Canonical domain errors are returned as `isError: true` tool
content containing the canonical error object. Malformed protocol requests and
unsupported tools use the shared canonical JSON-RPC error catalog. Notifications
return HTTP 204. The handler has no listener, store, lease, UI, session store or
detached goroutines; the composition root owns those concerns.
