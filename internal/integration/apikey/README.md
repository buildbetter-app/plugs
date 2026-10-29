# Admitted API-key HTTP composition

This package connects the admitted `authnapikey.Provider` to the existing
`httpjson` transport, RPC registry, core service and write pipeline. It borrows
an opened store and active lease. It neither reads ambient credentials nor
opens a listener until the host explicitly calls the returned handler's `Listen`.

Trusted host code supplies:

```go
handler, err := apikey.New(ctx, apikey.Config{
    Store: store, Lease: activeLease, Keys: provisionedKeyRing,
    DefaultProject: "example",
})
```

Handle errors, then use `handler.Listen(ctx, address, tlsConfig)`, `Serve` and
`Shutdown`/`Close`. Release the store and lease after HTTP work has stopped.
The host maintains the active storage lease. A non-loopback listener requires
direct TLS under the unchanged admission rules. Calls use `POST /rpc/v1`,
`Plug-RPC: 1`, `Content-Type: application/json` and a Bearer key. The provider
stamps identity; request parameters and `_meta.agent` cannot supply one.

Only existing service methods (`bead.get`, `bead.ready`, `bead.create`,
`bead.update`, `bead.claim`, `bead.comment`, `bead.close`, `history.query`) and
RPC discovery are bound. The project ontology uses the admitted default
resolver. No authorization plugin is installed in this bounded composition;
unscoped keys can use those methods under the adopted optional-policy behavior.
Sponsored agents and principals with caveats fail with `authz_not_installed`,
even if the supplied ring was provisioned with a different installed-chain
predicate. A complete read/write authorizer adapter is required before enabling
such credentials; a boolean assertion cannot substitute for that enforcement.

API keys remain noninteractive, including keys identifying users. This package
does not bind protected interactive operations, OIDC, `auth.whoami`, public
key-management methods, persistent credentials or rotation. Unsupported RPCs
return `method_not_found`. Interactive session admission belongs to the separate
OIDC owner. This package does not install a fake human session or proof flag.

`//internal/integration/apikey:apikey_test` uses real issued credentials and real
loopback socket calls. It checks authoritative changeset/lease principals,
per-principal idempotency, credential-free durable data and failures, refusal of
unknown/revoked/expired keys, cancellation, scoped-key refusal and noninteractive
identity. `//internal/plugins/authnapikey:authnapikey_test` also cancels an actual
contended lookup before the key-ring lock is released. These are functional
tests with the memory driver, not persistence or performance qualification.
