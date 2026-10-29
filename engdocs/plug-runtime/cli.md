# Try the disposable CLI

Source: **`471d0199c89acf9e242f44978f2824120726de32`**;
[guide scope](README.md#snapshot-and-boundaries).

## Run one local session

From this repository, compile and test the real executable with Bazel 9.3.0,
then run its memory-backed session. No listener, UI or database service starts.
An empty `PLUG_SERVER` selects this supported local example explicitly.

```sh
bazel --ignore_all_rc_files test //cmd/plug:plug_test \
  --jobs=2 --lockfile_mode=error --test_output=all --test_timeout=900

PLUG_SERVER= PLUG_TOKEN= bazel-bin/cmd/plug/plug_/plug --memory --json session <<'EOF'
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

Expect nine JSON result lines: writes advance revisions 1 through 5, history
at revision 5 contains the updated bead, the historical get returns `Original`,
and the final ready list is empty. IDs are generated, so do not hard-code them.
`@last` refers to the last write's returned ID; `@revision` is its revision.
The disposable-storage warning goes to stderr. Starting another invocation
creates an empty store, even with the same project and agent label.

## Commands and failures

The [parser](../../internal/cli/parse.go) is the complete command reference:
`ready`, `get`, `create`, `claim`, `update`, `comment`, `close`, `history`,
and `session`. Global `--memory`, `--project` (default `local`), `--agent`
(default `plug-cli`) and `--json` precede the command. Command options may
precede or follow the bead ID. `--fields` accepts a JSON object; `--title`
overrides its title. Ready/history page limits are 1–1,000.

Writes accept `--expect REVISION` where shown in usage and
`--idempotency-key KEY`. The [client](../../internal/client/client.go)
generates a random 32-byte key for a new write and never automatically retries
it. If an answer is lost, retain the reported key and retry the identical
operation with that key while the same store exists. A new process has lost
the memory receipt. Changing the request under a used key refuses; a fresh
key can cause another write. Anonymous `--agent` is an unverified scope label,
not an identity or isolation boundary.

`--json` preserves the complete result, including warnings. Human output
also prints warnings to stderr: a post-subscriber warning describes an already
committed write. A session stops at its first refusal; earlier commands remain
committed until the process exits. A session is not an atomic batch.

The [executable](../../cmd/plug/main.go) handles SIGINT/SIGTERM as cancellation,
including while waiting for more stdin. The [local owner](../../internal/cli/local.go)
renews the active store lease, releases it and closes the store once on exit.
Exit 2 reports configuration/argument refusals; execution/cancellation failures
exit 1. Success/help exit 0.

## Remote and persistent follow-ons

[Execute](../../internal/cli/run.go) refuses a nonempty `PLUG_SERVER` without
contacting it and refuses normal execution without `--memory`. `--server`,
`--token`, config-file selection, `plug call`, `plug describe`, persistent
driver selection, `plug server --local`, dump/restore and plugin installation
are not commands here. Do not assume a remote failure falls back to memory.
The internal `StorageFactory` seam supports later composition; it does not
make SQLite or PostgreSQL selectable from this executable.

[Executable tests](../../cmd/plug/integration_test.go) exercise the real binary's
lifecycle, fresh-process disposal, no-op/replay behavior, stop-on-refusal and
idle-session signal handling. [Local tests](../../internal/cli/local_test.go)
cover warning visibility and lease/store cleanup.
