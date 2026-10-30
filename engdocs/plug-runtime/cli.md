# Plug CLI sessions

Source: **`127ac22e1d003cce4babe6cbcbc8367ad264e649`**;
[guide scope](README.md#snapshot-and-package-boundaries).

## Run a persistent local session

The executable starts the real in-process service with persistent SQLite by
default. No listener or UI starts. Without `--memory`, the CLI opens
`.plug/plug.db`, creating its parent directory with private permissions. Use
`--memory` when a process-lifetime store is wanted; it is explicitly
disposable and starts empty in a new process.

```sh
bazel --ignore_all_rc_files test //cmd/plug:plug_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900

PLUG_SERVER= PLUG_TOKEN= bazel-bin/cmd/plug/plug_/plug --json session <<'EOF'
["ready"]
["create","--title","Original"]
["claim","@last"]
["update","@last","--title","Updated"]
["comment","@last","--body","Evidence"]
["close","@last","--reason","Done"]
["history","--as-of","@revision","--id","@last"]
["get","@last","--as-of","1"]
["ready"]
EOF
```

The real executable returns nine JSON result lines. Writes advance revisions
1 through 5; history at the final revision contains `Updated`, the historical
read returns `Original`, and the final ready page is empty. IDs and generated
keys are runtime values. The persistent invocation has no storage warning. A
`--memory` invocation prints a disposable-storage warning to stderr and a new
process starts with an empty store even when the project and agent label match.

## Select the remote path

`--server` wins over `PLUG_SERVER`. The value may be an origin or the exact
`/rpc/v1` endpoint. The client sends the same JSON-RPC bytes as local mode,
sets `Plug-RPC: 1`, accepts only JSON responses, follows no redirects and
never retries a write. Literal loopback HTTP is allowed; other HTTP servers
require HTTPS. A remote failure is returned to the caller and never falls back
to a new memory store.

Keep credentials out of shell history and logs:

```sh
PLUG_SERVER=https://plug.example PLUG_TOKEN="$TOKEN" \
  plug --project PROJECT --json ready
```

The remote executable path is exercised by
[`cmd/plug/remote_test.go`](../../cmd/plug/remote_test.go) against a real
loopback HTTP handler. It preserves the admitted principal, keyed replay and
historical reads across separate CLI processes. The HTTP client defaults to a
40-second request timeout and an 8 MiB response limit; callers can override
those only through its embedding API. It rejects credentials reflected in a
remote error and does not expose peer response text.

## Commands, ownership and refusal

The [parser](../../internal/cli/parse.go) accepts `ready`, `get`, `create`,
`claim`, `update`, `comment`, `close`, `reopen`, `defer`, `cancel`, `fail`,
`link`, `unlink`, `batch`, `history` and `session`.
Global `--memory`, `--server`, `--project` (default `local`), `--agent`
(default `plug-cli`) and `--json` precede the command. Command options may
precede or follow a bead ID. `--fields` accepts a JSON object and `--title`
overrides its title. Ready/history page limits are 1–1,000.

Writes use an idempotency key; the client generates a random 32-byte key when
one is omitted and never retries a write automatically. If an answer is lost,
retain that key and retry the identical operation while the same store exists.
A changed request under a used key is refused. In anonymous mode the agent
label scopes leases but is unverified and is not an identity boundary.

`--json` preserves results and warnings. A post-subscriber warning describes
an already committed write. A session stops at its first refusal, while earlier
commands remain committed; a session is not an atomic batch. SIGINT/SIGTERM
cancels the context, and the local owner releases its lease and closes the store
exactly once. Exit 2 is an argument/configuration refusal; execution or
cancellation failure exits 1.

## Local storage and configuration

With neither `--server` nor `PLUG_SERVER`, the CLI reads `.plug/config.yaml`
from the workspace when it exists, then selects persistent SQLite by default.
The supported config forms are `server: URL`, `storage: sqlite`,
`storage: {driver: sqlite, path: PATH}`, or the equivalent `driver`/`path`
shorthand. Relative SQLite paths resolve from the workspace. Explicit flags
and `PLUG_SERVER` take precedence over the config file; an explicit `--memory`
choice remains ephemeral and conflicts with a configured server.

Unknown, malformed, duplicate, conflicting, URI, memory, and unsupported
driver settings fail closed. A configured server never falls back to local
storage after a remote failure. The same service, Writer, lease, RPC registry
and in-process client path is used for memory and SQLite.

`plug server`, `plug call`, `plug describe`, dump/restore, plugin installation
and selecting PostgreSQL from the executable remain unsupported. Those
boundaries refuse explicitly; they are not silent local fallbacks.

[Executable tests](../../cmd/plug/integration_test.go) cover local lifecycle,
fresh-process disposal, no-op/replay behavior, stop-on-refusal and
cancellation. [Local tests](../../internal/cli/local_test.go) cover warning
visibility and lease/store cleanup. The remote lifecycle, cursor,
authentication and warning cases are in
[`remote_test.go`](../../cmd/plug/remote_test.go). These tests exercise a
disposable fixture and do not certify deployment, performance, recovery or a
public release.
