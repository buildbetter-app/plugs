# Bounded OIDC core sessions

`New(Config)` requires one explicit HTTPS issuer, public client ID, exact HTTPS
redirect, authorization/token/JWKS endpoints and a provisioned subject-to-User
map. `AllowedOrigins` is an operator-owned exact-origin allowlist for browser
login and bearer requests. Only RS256 with a unique RSA signing key (2048–4096
bits) is supported.
Issuer, client audience/authorized party, signature, nonce, integer time claims,
fresh `auth_time` and a maximum five-minute ID-token age are checked. Token/JWKS
responses are capped at 64 KiB and the combined exchange has a ten-second
deadline. Redirects and ambient client cookies are disabled.

`Begin(ctx)` returns an authorization URL and a private client `Binding`.
Keep the binding with the initiating client, separate from the URL/IdP.
`Complete(ctx, state, binding, code)` consumes the flow before exchanging the
code with S256 PKCE. It returns an opaque Plug token once; IdP tokens are not
accepted as Plug credentials or retained. State/nonce/verifier/binding are
independent random 256-bit values. Matching callbacks cannot be replayed.

`HTTPHandler(authenticator, next)` composes the public-client adapter with an
existing HTTP handler. Login and callback requests must carry the
`Plug-RPC: 1` marker and pass the authenticator's Host, peer and TLS
admission before route handling. `GET /auth/oidc/login` returns the
authorization URL, state and private binding; `POST /auth/oidc/callback` accepts
one strict JSON callback and returns a short-lived Plug core-session bearer.
Both browser legs require the same operator-allowlisted Origin, and the
callback flow is bound to that origin. Browser preflights validate the origin
and requested method/headers; the subsequent request carries the marker.
Responses use `Cache-Control: no-store`, and allowlisted origins receive an
explicit, non-credentialed CORS header.
Cookies, redirect handlers and ambient credentials are not installed.

Configure `authn.Config{CoreSessions: provider, Provider: optionalAPIKeys,
CrossOriginAuthorizer: provider.AllowOrigin}` and share that authenticator
between the login adapter and RPC handler.
`plg_s_` credentials go exclusively to the trusted session authority. Ordinary
providers never confer human proof. Session authority is process-local when
no durable store is configured. SQLite and PostgreSQL stores can be passed as
`Config.SessionStore` to retain the digest, unscoped `user` principal, expiry
and revocation bit across provider restarts. Lookups remain uncached, so
revocation is effective for retained proofs as well as new HTTP admissions.
Expiry is fifteen minutes by default (at most one hour, also capped by
ID-token expiry). There are at most 128 pending flows and 1024 active sessions
by default, with explicit hard bounds and no positive cache.

The HTTP adapter's browser-origin caveat remains provider-local. A configured
durable store does not widen it: a restarted provider admits the durable
credential through the ordinary same-origin/private path, but foreign-origin
requests need a fresh browser login to establish the origin binding. While
the issuing provider is live, origin authorization rechecks durable
revocation and expiry; the local caveat never supplies session authority.

`CarryIdentity(authenticator, admittedHTTPHandler)` preserves the admitted
identity through the existing HTTP/RPC path without changing its admission
rules. Consumers call `authn.RequireInteractiveUser(ctx, executingPrincipal)`.
Retained identities recheck live revocation/expiry. Principal JSON, claims,
API keys, delegated principals and principal-only contexts convey no proof.

The host must privately keep the binding and deliver the returned token to the
same initiating client. There is no device flow, discovery, refresh,
auto-provisioning, browser cookie host
or iframe bridge support. Existing HTTP explicitly refuses cookies. Foreign
bearer requests are admitted only when the session's origin caveat and the
operator allowlist both match; API keys and private sessions remain
same-origin only. No issuer bearer JWT, browser transport relaxation or
successful placeholder substitutes for these boundaries. Collaboration
`session.*` is a separate live-client lifecycle.

The live proof is an admission predicate. It does not implement #18 X6's
durable method/digest/key challenge consumption or an atomic transaction
commit guard; consumers needing those must keep the corresponding apply
surface unsupported until that separate guard is connected.
