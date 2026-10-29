# Bounded OIDC core sessions

`New(Config)` requires one explicit HTTPS issuer, public client ID, exact HTTPS
redirect, authorization/token/JWKS endpoints and a provisioned subject-to-User
map. Only RS256 with a unique RSA signing key (2048–4096 bits) is supported.
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

Configure `authn.Config{CoreSessions: provider, Provider: optionalAPIKeys}`.
`plg_s_` credentials go exclusively to the trusted session authority. Ordinary
providers never confer human proof. Session records hold a digest, User ID and
expiry. Session authority is process-local, expires after fifteen minutes by
default (at most one hour, also capped by ID-token expiry), and is invalidated
by `Revoke(id)` or process restart. There are at most 128 pending flows and
1024 sessions by default, with explicit hard bounds and no positive cache.

`CarryIdentity(authenticator, admittedHTTPHandler)` preserves the admitted
identity through the existing HTTP/RPC path without changing its admission
rules. Consumers call `authn.RequireInteractiveUser(ctx, executingPrincipal)`.
Retained identities recheck live revocation/expiry. Principal JSON, claims,
API keys, delegated principals and principal-only contexts convey no proof.

This slice installs **no public authentication methods or login routes**.
The host must privately keep the binding and deliver the returned token to
the same initiating client. There is no device flow, discovery, refresh,
auto-provisioning, durable/multi-process session storage, external-origin
token exchange, browser cookie host, CORS or iframe bridge support. Existing
HTTP explicitly refuses cookies and foreign origins. No issuer bearer JWT,
browser transport relaxation or successful placeholder substitutes for these
boundaries. Collaboration `session.*` is a separate live-client lifecycle.

The live proof is an admission predicate. It does not implement #18 X6's
durable method/digest/key challenge consumption or an atomic transaction
commit guard; consumers needing those must keep the corresponding apply
surface unsupported until that separate guard is connected.
