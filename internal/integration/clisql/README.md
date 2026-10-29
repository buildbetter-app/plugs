# CLI against persistent SQL stores

This package runs the built `plug` executable through the admitted HTTP JSON-RPC
transport over real SQLite and PostgreSQL stores. Each case creates a user API
key with the native provider, performs create/claim/update/comment/close/history
operations in separate CLI processes, closes the server and store, then opens a
fresh handle and checks current state, history, changesets, and the stored user
principal. PostgreSQL owns a disposable loopback Docker engine for the test.

The tests deliberately use the existing CLI and API-key/server composition. No
direct service or Writer call stands in for a CLI operation. Warning arrays and
replay metadata are checked only on the actual HTTP response; the CLI's JSON
result output intentionally contains the inner committed result.
