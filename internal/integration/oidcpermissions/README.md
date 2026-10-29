# OIDC sessions with fixed permissions over HTTP

This package is a connected acceptance boundary for the admitted OIDC provider,
the fixed project/team permission adapter, and the real JSON-RPC HTTP transport.
The disposable TLS identity provider performs the authorization-code, PKCE,
token, and JWKS exchanges; Plug admits only the resulting opaque core session.

The tests bind the actual `authzrpc.GuardedService` to a second RPC server that
uses the live OIDC session authenticator, while retaining the ordinary API-key
provider for comparison. They exercise User reads, Agent permission checks,
team/project fact changes, session revocation and exact expiry, transport
refusals, and credential-free diagnostics. Protected apply, browser cookies,
CORS, device grants, external origins, and durable X6 challenge consumption
remain outside this package.
