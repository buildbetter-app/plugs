# Plug CLI connections

The named commands in `plug --help` use the same byte client and result
validation in local and remote modes. For a running server:

```sh
PLUG_TOKEN=<bearer> plug --server https://plug.example --project PROJECT --json ready
PLUG_SERVER=http://127.0.0.1:8080 plug --json session
```

Global `--server` wins over `PLUG_SERVER`, including when `--memory` is also
present. `PLUG_TOKEN` supplies the standard bearer header for either server
selection. An explicit empty or invalid URL fails. Server failure never opens
local storage. Server URLs may be origins or the exact `/rpc/v1` endpoint;
credentials, other paths, query strings, fragments and encoded aliases fail.
Plain HTTP is allowed only for localhost or a literal loopback address; other
servers require HTTPS with normal certificate verification.

Each call posts to `/rpc/v1` with `Plug-RPC: 1` and JSON media headers. The
transport allows 40 seconds per request, 32 KiB of response headers and 8 MiB
of response body, including chunked bodies. It honors context cancellation,
closes response bodies and releases idle connections after the invocation.
It follows no redirects, accepts no cookies/compression and never retries a
write automatically. Transport errors contain neither URL/header values nor
peer response bodies. Canonical admission errors retain their catalog code,
retryability, details and trace ID, including the endpoint's null-ID framing.

Writes generate a random key when none was supplied. If the response is lost,
stderr retains that key. Retry the identical operation explicitly with
`--idempotency-key KEY`; a changed operation under that key is refused by the
server. Every committed warning remains in JSON and appears on stderr in both
human and JSON modes. Ready/history cursors stay opaque and are passed back
unchanged for the server to verify. A `session` is a stream of command arrays,
not a presence session; it creates no `session.open` resource.

Ontology commands use the same client and registry. `ontology plan` accepts a
JSON document with `--document` and optional CAS JSON with `--expect`; `ontology
apply PLAN_DIGEST` requires `--expect` and an explicit `--proof DIGEST` (or
`--session-id ID` for an interactive session). `ontology schema` requires an
explicit `--project` and `--type`; history and diff use integer revisions.

Without a selected server, the CLI opens persistent SQLite at
`.plug/plug.db`, creating `.plug/` with private permissions when needed. A
workspace `.plug/config.yaml` may select a server or a local SQLite path:

```yaml
storage:
  driver: sqlite
  path: .plug/plug.db
```

`server: URL` is an alternative to `storage`. Unknown, malformed, duplicate,
or conflicting settings fail closed. `--memory` is the explicit disposable
choice and emits its lifetime warning; it does not reuse the persistent file.
`plug server` is a separate shared-server command with its own configuration
and lifecycle; the [runtime server guide](../../engdocs/plug-runtime/server.md)
covers it. The ordinary CLI does not open PostgreSQL directly. Login, watches
and generic RPC calls continue to fail explicitly.

The read surfaces are available as `events`
(or `events tail`), `history get`, `history diff`, and `health`; they use the
same opaque cursors, pinned revisions, bounded JSON Patch results, and
read-only health response as their JSON-RPC methods. This slice makes no full
behavior-card or performance acceptance claim.

`events --limit 0`, `events --limit all`, and `events all` read to completion
through bounded `event.list` calls. `events export` streams the same sequence
as one canonical event per JSON line. Use `--page-size 1..1000` to select the
batch size and `--after TOKEN` to resume exclusively; event/resource filters
are preserved on every call. Empty filtered pages can advance the cursor, so
completion requires an empty page with an unchanged cursor. These are
successive reads until caught up, not a snapshot across concurrent writes.
Wire `event.list` still rejects nonpositive limits. Plug retains audit history
and has no client event-prune command.
