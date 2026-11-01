# Transport and native-host integration

This disposable memory-store fixture binds the real core service, native
validator and post callback, API-key admission, and HTTP, WebSocket, stdio and
MCP adapters to one registry. Every adapter performs a first write, then the
same canonical request is retried through all four adapters. Commit/read
bytes, large request IDs, replay metadata, native callback counts and durable
principals must agree. MCP uses its required `tools/call` envelope and
`plug_call`; its canonical content is compared after removing that envelope.
Stdio receives an identity from trusted host admission before framing input.

Negative cases cover hostile Host and Origin headers, forwarded-header
spoofing, missing HTTP markers, missing/malformed/revoked credentials, and the
real admin dump handler's human-presence refusal. API-key Users receive no
interactive proof. Non-loopback anonymous listeners and authenticated network
listeners without TLS are refused.

The native host defaults to anonymous mode. Its trusted embedding server must
set `pluginhost.Config.AuthenticationConfigured` only when an authentication
provider is configured. A manifest or request principal cannot enable that
mode. Anonymous host egress is refused before DNS or dialing, including a
declared destination. Authenticated mode retains phase, manifest and public-IP
checks. Native code remains trusted build-time code, subject to host-I/O-only
review; this does not add runtime isolation to native plugins.

The egress target is a disposable loopback HTTPS fixture. The test requires
zero requests to it in both modes: anonymous calls fail on host mode, and
authenticated calls still fail on the non-public destination. No external
target is contacted, and no private ledger or production data is used.
