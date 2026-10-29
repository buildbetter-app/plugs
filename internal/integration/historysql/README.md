# SQL history acceptance

This package is a bounded connected acceptance slice for persistent
`history.query` paging. Each subtest uses the real service, Writer, registry,
JSON-RPC dispatcher and loopback HTTP transport. The SQLite case opens a real
WAL file; the PostgreSQL case starts the pinned local engine and creates a
fresh database. No memory store or replacement handler is used.

The test creates four records through HTTP, captures an `asOf`, requests one
historical row, then updates every original row and creates a fifth row. It
closes and reconstructs the HTTP server and SQL store before consuming the
remaining signed pages. The assertions require the original four bodies and
revisions exactly once, in stable order, with the late row absent. The same
cursor is checked against a changed anonymous caller label, project, filter,
asOf incarnation, and malformed token. Durable changesets are inspected
through the driver and must retain the anonymous principal stamped by the
actual HTTP admission path.

Run the focused target with:

```sh
bazel --ignore_all_rc_files test //internal/integration/historysql:historysql_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900
```

This qualifies the tested SQLite and disposable PostgreSQL history paths. It
does not claim authorization visibility, CLI behavior, authentication-provider
semantics, history APIs beyond the existing `history.query` binding, HA,
performance, crash recovery, production topology, or cutover readiness.
