# API-key SQL integration

This package connects the admitted `authnapikey.Provider` and the existing
`internal/integration/apikey` HTTP composition to the real SQLite and
PostgreSQL drivers. It does not add a credential store or an authentication
endpoint.

The fixture keeps one provider and its issued keys alive while it closes and
reopens only the HTTP listener, storage store, and active lease. SQLite uses a
disposable file; PostgreSQL uses a disposable database in the pinned real
engine image. Each test owns and removes its database.

The tests prove:

- independent authenticated User principals have separate durable receipt
  scopes for the same method and idempotency key;
- a same-key retry returns byte-identical result data and does not advance the
  SQL head, including after resource reopen;
- the authenticated principal reaches the core lease and persisted changesets;
- missing, malformed, unknown, wrong-secret, revoked, expired, duplicate and
  caller-supplied actor credentials refuse before a SQL mutation;
- records, versions, changesets, history and HTTP responses contain no bearer
  token, key ID or secret;
- a fresh provider cannot authenticate a key issued by the old process-local
  ring, while the retained provider resolves the durable SQL receipt.

The target is an integration behavior check, not a claim about key
provisioning, rotation, persistence, OIDC, authorization plugins, cancellation
races, hosted CI, failover or production deployment.
