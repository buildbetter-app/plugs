# API-key authentication

`New(Config{})` creates an optional, process-local `authn.Provider`. Trusted
host code provisions a key with `Issue(principal, expiresAt)` and delivers the
returned `Token` privately, once. `Revoke(id)` permanently refuses that key on
subsequent checks. These are internal host operations, not public RPC methods.
Neither environment credentials nor caller-supplied actor fields provision keys.

Each key has a random 128-bit lookup ID and a random 256-bit secret. Only a
SHA-256 digest of the full credential, its detached principal, explicit expiry
and revocation state remain in the ring. Verification compares fixed-size
digests in constant time, including unknown IDs. Missing, malformed, unknown,
mismatched and revoked keys return `unauthenticated`; only a matching expired
key returns `token_expired`. Lookup cancellation returns `authn_unavailable`.
Errors contain no credential material. Formatting and JSON encoding of an
`IssuedKey` omit its secret; callers must explicitly select `Token` to deliver it.

The provider implements only `Authenticate(context.Context, string)` for the
admission layer. API keys, including keys bound to a `user`, never convey an
interactive login session or human presence. `authn.Identity` enforces that
distinction; consumers must not infer proof from the principal kind.

Sponsored agents and any principal with caveats require the host's current
installed-authorization predicate in both the provider and authenticator.
The predicate describes actual installed enforcement, not an assertion supplied
by a client. The HTTP composition under `internal/integration/apikey` installs
no authorizer and independently refuses scoped credentials at admission, even
if a ring was provisioned for a different host with authorization installed.

Use `Authorization: Bearer <token>` with the admitted HTTP transport. Existing
`Plug-RPC`, Host, Origin, loopback and TLS checks still apply. The ring does not
persist across restarts. Durable provisioning, rotation, key-management RPCs,
OIDC, interactive sessions and UI are outside this package.
