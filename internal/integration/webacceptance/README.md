# Shared-server web acceptance

This package qualifies the admitted `internal/server/webui` handler through a
real HTTP composition with the service, Writer, RPC registry, JSON transport,
and disposable SQL stores. SQLite runs in its file-backed WAL mode; the second
matrix case uses PostgreSQL 17.11 from the pinned local image. The test never
replaces the RPC or web handler with a fixture implementation.

The HTTP checks cover embedded assets and content types, no-store and security
headers, mount/RPC route ownership, read-only method refusal, traversal and
credential/cross-origin refusal, actual row creation through JSON-RPC, pinned
history reads, and an empty project. The supplemental Chrome CDP check uses an
installed browser, renders a real row as text, creates another row through the
same RPC endpoint outside the page, verifies it after a UI refresh, visits the
real empty state, and surfaces lease-loss refusal in the DOM. It records the
browser RPC requests and screenshots when
`PLUG_WEB_ACCEPTANCE_EVIDENCE_DIR` is set.

The `/fixture/fence` endpoint exists only in the disposable test mux. It
releases the real store lease so the page can demonstrate the product's actual
`busy` refusal; it is not a production route and does not synthesize an RPC
outcome. Browser product requests are asserted to remain reads (`server.hello`,
`rpc.methods`, `bead.ready`, and `history.query`).

Run the bounded target with:

```sh
bazel --ignore_all_rc_files test //internal/integration/webacceptance:webacceptance_test \
  --disk_cache=/tmp/plug-swarm/bazel-disk-cache --local_resources=memory=8192 \
  --jobs=2 --test_output=all --test_timeout=900
```

This is a focused web/server acceptance slice. It does not claim full A7
coverage, authentication-provider or authorization semantics, sessions,
cookies/CORS UX, watches, CLI/HA behavior, performance, crash recovery,
production listener topology, or deployment readiness.
