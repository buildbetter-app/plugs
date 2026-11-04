# Shared Plug server

Source: **`463881cec205e6cd3e5b85d1bac7d4018a153d5f`**;
[guide scope](README.md#snapshot-and-package-boundaries). This guide covers
the runnable command in [`internal/cli/server.go`](../../internal/cli/server.go)
and the composition/configuration owned by
[`internal/server/composition`](../../internal/server/composition/config.go).

## Start a server

`plug server` starts the shared HTTP server described by
`.plug/config.yaml` in the current workspace. The file is required; a missing
file is a configuration refusal, not a request to start a disposable server.
The command prints the bound endpoint to stdout and stays running until it
receives SIGINT or SIGTERM.

```sh
plug server
```

For a disposable loopback server with the embedded UI, use the explicit local
mode:

```sh
plug server --local
```

Local mode always uses memory storage, binds an ephemeral loopback address and
serves `/ui/`. It prints this warning to stderr during startup:

```text
warning: memory storage is disposable; data is discarded when this server exits
```

The normal CLI's `plug --memory` option is a different in-process mode: it
starts no listener or UI and prints `warning: memory storage is disposable; all
data is discarded when this process exits`. Use either mode only when losing
the store at shutdown is acceptable.

## Select storage and listener settings

The server form of `.plug/config.yaml` accepts flat keys or one nested section
level. The supported settings are:

| Setting | Values and behavior |
| --- | --- |
| `driver` or `storage.driver` | `memory` (the default), `sqlite`, or `postgres`/`postgresql`. Memory must not have a DSN; the durable drivers require one. |
| `dsn` or `storage.dsn` | A local SQLite file path or a pgx PostgreSQL DSN. The server passes the value to the selected driver. |
| `listen`/`address` or `server.listen`/`server.address` | A literal host and port. The default is `127.0.0.1:0`, which selects an ephemeral loopback port. |
| `project` or `server.project` | The default project for requests; the default is `default`. |
| `ui` or `server.ui` | `true` or `false`; the default is `true`. Set `false` to omit the embedded UI route. |
| `ontology.source` | An existing protected branch-zero ontology ID such as `ontology/workspace`. Startup validates the installed source before admitting RPCs. |
| `auth.mode` | `anonymous` by default. The standalone CLI has no provider configuration for `authenticated` mode, so it refuses that mode rather than starting without authentication. |

An administrator can select an existing installed ontology with
`ontology.source: ontology/<id>` (or a nested `ontology` / `source` map).
This is a protected branch-zero identity, not a document, executable binding
or permission grant. Startup loads its current durable revision and refuses
missing, malformed, wrong-kind or uncompilable sources before admitting RPCs.
Omitting the key retains the default ontology. The file cannot install a
source; its initial publication belongs to an explicit authorized write/init
lifecycle. Hosts with plugin validators must provide their actual validating-pre
inventory through `composition.Config`; YAML does not register plugins.

With a configured authorization chain, installed ontology reads require the
current read adapter and protected `ontology.apply` remains unavailable until
its separate policy revalidation capability exists. Ordinary Writer authority
does not grant protected apply. Hosts exposing event reads under policy must
supply `composition.Config.EventAuthorization`, covering ordinary images and
all condition-notice resources. Missing capability with configured policy
keeps `event.list` unadvertised.

For example, a persistent SQLite server can use:

```yaml
driver: sqlite
dsn: .plug/server.db
listen: 127.0.0.1:8080
project: local
ui: true
auth:
  mode: anonymous
```

Memory is explicit when it is selected in the server config, although
`plug server --local` is the simpler disposable path:

```yaml
driver: memory
listen: 127.0.0.1:0
```

A PostgreSQL-backed server uses a real pgx DSN and keeps its durable state in
the selected database:

```yaml
driver: postgres
dsn: 'postgres://plug_user@db.example/plug?sslmode=require'
listen: 127.0.0.1:8080
ui: false
```

Keep credentials out of committed configuration and replace the example DSN
with the operator's protected value. SQLite paths are opened as local files;
the parent directory must already be usable by the process. PostgreSQL
initialization requires a role that can create Plug's private schema. Unknown,
duplicate, malformed and ambiguous YAML settings fail closed, and startup
errors redact the configured DSN.

The server and client forms of `.plug/config.yaml` are intentionally separate:

| Command | Configuration shape |
| --- | --- |
| `plug server` | `driver`/`dsn`/`listen`/`project`/`ui` and the nested aliases above. |
| `plug` client mode | `server: URL`, `storage: sqlite` with an optional local `path`, or the equivalent top-level `driver`/`path` shorthand; see [CLI](cli.md#local-storage-and-client-configuration). |

After starting a server, point the CLI at the printed endpoint explicitly or
through `PLUG_SERVER`; do not expect the client parser to reinterpret a server
DSN as a local CLI configuration.

## Use the remote CLI

The client sends the same JSON-RPC bytes as local mode to `/rpc/v1`:

```sh
PLUG_SERVER=http://127.0.0.1:8080 \
  plug --project local --json ready

plug --server http://127.0.0.1:8080 --project local --json \
  create --title "First bead"
```

Use `PLUG_TOKEN` only when a separately composed authenticated server issued a
bearer credential. An anonymous server admits loopback requests without a
token; supplying a token to it is an authentication failure. Full command,
retry and cancellation behavior is in the [CLI guide](cli.md), and transport
admission limits are in [RPC and HTTP](rpc-http.md).

SQLite and PostgreSQL preserve beads, history, receipts and other durable store
state when the server is stopped and started again with the same configuration.
Memory state ends with the process. The server's active lease, collaboration
sessions, presence, watches, API-key ring and generated cursor signing key are
runtime state; after a restart, clients must reconnect, open a new
collaboration session when needed and obtain fresh cursors.

## Identity and session boundaries

The runnable CLI server defaults to anonymous authentication. Anonymous mode
is loopback-only and has no network override. The unverified `_meta.agent`
label can scope leases and idempotency, but it is not an identity or
authorization claim. The standalone command does not expose a login route,
cookie session or CORS browser flow.

Bearer API keys and `plg_s_` core-session tokens are accepted only when a host
has composed the corresponding authentication provider. An ordinary API key
identifies the admitted request but never proves interactive human presence. A
valid `plg_s_` token is a live `CoreSession` reference issued after verified
interactive login. The `RequireInteractiveUser` check uses that session as
interactive proof and rechecks current revocation and expiry on every call.
A host-composed authenticated server must supply its provider and TLS boundary;
setting `auth.mode: authenticated` in the standalone CLI config cannot create
those providers.

There are two separate uses of “session”:

- `plug ... session` reads one JSON command array per stdin line. It is a CLI
  stream and does not allocate a server collaboration session.
- RPC `session.open`/`session.close` creates a live collaboration session for
  presence and watch ownership. It is process-local, expires, and is cleared
  when the server restarts. A bearer authentication session, when a host
  provides one, is a separate credential authority.

The server's authenticated identity is stamped by the HTTP admission layer;
caller-supplied actor fields cannot replace it. See the [HTTP admission
boundaries](rpc-http.md#http-admission-and-limits) and the
[collaboration session contract](../../internal/server/collab/README.md) for
the corresponding limits and expiry rules.
