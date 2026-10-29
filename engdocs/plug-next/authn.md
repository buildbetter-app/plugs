# Authentication and transport admission

This page uses **`9d03323f423cf28d202b328ed7c116e45db8406f`**. The source
boundaries are the [authenticator](../../internal/server/authn/authn.go),
[HTTP admission](../../internal/server/authn/http.go), and
[process-local API-key provider](../../internal/plugins/authnapikey/apikey.go).
The remote fixture exercises them through the real HTTP handler and RPC server;
see [its setup](../../cmd/plug/remote_fixture_test.go).

## Anonymous and bearer modes

With no provider, trusted in-process admission yields an anonymous identity;
HTTP admission and listeners are restricted to loopback. A supplied
credential never falls back to anonymous. A
configured provider must return a validated `user`, `service`, or sponsored
`agent` principal; internal identities and malformed subjects fail closed.
The returned identity is detached, and provider errors are reduced to the
closed authentication error catalog.

`AdmitHTTP` requires exactly `Plug-RPC: 1`, a valid authority and peer address,
loopback admission for anonymous requests, and a same-origin `Origin` when one
is present. Network listeners require a provider and directly configured TLS.
Forwarded headers do not change the peer or host. WebSocket admission checks
the handshake separately; it does not make an HTTP request exempt from the
marker.

The transport boundary is deliberately independent of caller parameters. A
JSON-RPC request cannot select its own principal or proof flags. Even a bearer
user identity returns `human_presence_required` from
`RequireInteractiveUser`; an API key is not interactive human proof.

## API keys in this snapshot

`authnapikey.Provider` issues a bearer secret once, stores only its digest and
principal in a process-local key ring, and requires a future expiry. Revocation
is effective on the next check. Scoped principals require an installed
authorization chain both when issued and when authenticated. Secrets are not
included in formatting or error messages.

The [living example](trace_test.go) uses this provider for an authenticated HTTP
run. It proves that the admitted principal, not the CLI's agent label, reaches
the real changeset and that revocation refuses a receipt replay. The
[executable authentication cases](../../cmd/plug/remote_test.go) separately
verify that expired, revoked, missing, and unknown tokens cannot write or
trigger a callback.

```text
request:  POST /rpc/v1  Plug-RPC: 1  Authorization: Bearer <issued-once>
result:   principal=service:docs-example  revision=1
replay:   same key -> revision=1, replayed=true
revoked:  error.code=unauthenticated
```

Run the real authentication tests with:

```sh
bazel --ignore_all_rc_files test //internal/server/authn:authn_test \
  //internal/plugins/authnapikey:authnapikey_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no \
  --test_output=all --test_timeout=900 --test_arg=-test.v
```

The key ring is not durable. OIDC/login sessions, durable API-key storage,
presence, trusted-proxy termination, and human challenge consumption are not
admitted at this pin. Later integration branches do not change the scope of
this snapshot.
