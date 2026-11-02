# Current query read indexes

SQLite and PostgreSQL select bounded pages of current records directly in SQL
for unfiltered queries and single `kind` equality filters. The order remains
the driver's encoded record key order. Pagination retains the existing cursor
bindings, snapshot revision and offset semantics. PostgreSQL reconstructs each
selected page with one ordered, call-local `get` batch; SQLite decodes the same
stored record bytes used by its current point reads.

The indexes contain identities and kinds, never authorization decisions.
Current authority checks remain in the core and server. Reads at an older
revision, writer reads with staged overlays, and other equality filters use the
existing reconstruction path. The indexed path uses the read transaction that
pinned its metadata, so a later commit cannot change its membership or body.

These are additive physical indexes on existing source tables. SQLite creates
`objects_kind_key` under its existing open/publication lock; PostgreSQL creates
the partial live-record `records_live_kind_key` under its migration advisory
lock. Opening an existing supported database builds a missing index without
changing its logical schema version, store identity, incarnation or head.
Index creation failures cause Open to return an error. Each engine maintains the
indexes in the same transaction as the source write or restore, with no
separate projection, cache version or replay state to invalidate.

This removes repeated whole-store body reconstruction from each current query
page. It does not remove graph scans, change authorization or ordinary view
caps, or establish the CP7 latency targets. The unchanged public CP7 harness
must report successful requests and case p95 values for those claims.
