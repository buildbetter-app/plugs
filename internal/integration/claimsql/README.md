# Expired-claim recovery on SQL

`//internal/integration/claimsql:claimsql_test` runs the admitted
`bead.release` recovery path through the real in-process JSON-RPC registry and
`claimrpc.Bind` against a file-backed SQLite/WAL store and a disposable pinned
PostgreSQL 17.11 database. Both engines are required; there is no memory,
external-DSN or skip path.

The controlled test clock proves the live-holder and foreign-holder refusals,
exact expiry and exact two-TTL grace cutoff, strict recovery after the cutoff,
ready membership, recovery event/history attribution, and a new holder's normal
claim. The original core receipt is replayed after the graph changes and after
closing/reopening the SQL store; a changed request with the same key is refused.

A database trigger at the real changeset publication boundary aborts recovery
on each engine. Public driver snapshots show the bead, versions, ready set,
history, head and receipt remain unchanged, then the same key succeeds after
the fault is removed. A released host is fenced before a replacement SQL host
recovers the claim through its own public RPC server.

This target does not add a sweeper, heartbeat endpoint, process-failover
orchestration, production source, driver behavior, or a new recovery method.
