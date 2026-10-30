# HTTP JSON-RPC transport

`New(rpcServer, authenticator, Config{})` creates a reusable `http.Handler` for
exactly `POST /rpc/v1`. It runs `authn.AdmitHTTP`, stamps only the admitted
principal with `rpc.WithPrincipal`, and writes `rpc.Server.Dispatch` bytes
unchanged. RPC errors and batches use HTTP 200; notification-only messages use
204 with no body. Transport failures use `Server.TransportError`, with a null
ID, the shared error catalog and a trace ID. Admission happens before reading
or dispatching the body.

`handler.Listen(ctx, "127.0.0.1:0", tlsConfig)` explicitly binds through
`authn.Authenticator.Listen`; the caller runs `server.Serve()` and closes or
drains it with `server.Close()` / `server.Shutdown(ctx)`. Wait for Serve and
application work to finish before releasing the application-owned store and
lease. Shutdown forces cancellation and closes sockets if its drain deadline
expires. Context cancellation cancels requests; the caller still closes the
server. Listen never selects an implicit address or bypasses admission.

Defaults are 1 MiB of body per request, 64 simultaneous admitted requests,
16 KiB of headers, 5s to read headers, 30s to read the request, 30s per request
for admission/body/cooperative dispatch, 35s to write a response, and 60s idle.
Zero configuration fields select defaults; negative fields fail construction.
A reused Handler must run on a server with connection/header/write bounds;
net/http.Server supports the handler's body read deadlines. Dispatch runs
synchronously and must honor its context; the transport starts no detached
handler goroutine.

Requests need `Content-Type: application/json` (optionally `charset=utf-8`)
and `Plug-RPC: 1`. Missing/unsupported media types or any Content-Encoding
return 415; oversized bodies return 413; unreadable bodies 400; body deadlines
408; saturated concurrency 429. Unknown paths, encoded aliases, trailing
slashes and query strings return 404; other methods return 405 with Allow:
POST. Authentication errors return 401, admission/unsupported scoped identity
403, and unavailable authentication 503. All JSON responses disable caching.
HTTP parser failures before the handler retain net/http's own status framing.

Anonymous mode binds only loopback with no override. Authenticated network
listeners need direct TLS and a real configured provider. Host, peer, Origin,
Plug-RPC and bearer checks remain owned by authn; forwarded headers do not
change their meaning. Cookies are rejected, no CORS headers are emitted, and
preflight, WebSocket, MCP, stdio, interactive sessions, trusted-proxy TLS and
cross-origin browser applications are unsupported here. API keys do not prove
interactive human presence. Scoped keys require an actually installed
authorizer wired by the composition root; this transport installs none.
The CLI/server composition is separate. This package never logs credentials
or request bodies and does not claim performance or public deployment proof.
