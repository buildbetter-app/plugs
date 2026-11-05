# Remote CLI over admitted HTTP

The source pin for this page is **`9d03323f423cf28d202b328ed7c116e45db8406f`**.
The command wiring is in [`internal/cli/run.go`](../../internal/cli/run.go),
the byte client in [`internal/client/http.go`](../../internal/client/http.go),
and the executable fixture in
[`cmd/plug/remote_test.go`](../../cmd/plug/remote_test.go). The companion
[local CLI page](../plug-runtime/cli.md) covers in-process SQLite and explicit
memory storage; this page covers the admitted remote path.

## Select a server explicitly

`--server` wins over `PLUG_SERVER`. The URL may be an origin or the exact
`/rpc/v1` endpoint. The client sends JSON-RPC bytes with `Plug-RPC: 1`, allows
40 seconds per request, bounds response headers and bodies, follows no
redirects, and never retries a write automatically. Plain HTTP is limited to a
literal loopback address; other servers require HTTPS. A server failure never
opens a local memory store.

For a real deployment, keep the bearer secret out of shell history and logs:

```sh
PLUG_SERVER=https://plug.example \
PLUG_TOKEN="$TOKEN" \
  plug --project PROJECT --json ready
```

The [living example](trace_test.go) uses a disposable loopback HTTP server and
feeds JSON command arrays to the actual executable. The
[current contract expectations](cli-rpc-lifecycle-trace.json) retain the full
create, replay, CLI update, and historical-get bodies using an explicit UTC
core clock. Creation images include `createdAt`, attributed to `core`; historical
readback retains that same instant. The [historical capture](cli-rpc-trace.json)
remains unchanged. These executable fixtures do not establish native producer
admission. The raw HTTP replay marks
`_meta.replayed` beside the result; CLI JSON prints the stored result itself.
Run the example to see the complete JSON bodies:

```sh
bazel --ignore_all_rc_files test //engdocs/plug-next:examples_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no \
  --test_output=all --test_timeout=900
```

| Captured operation | Observed result |
| --- | --- |
| HTTP `bead.create` | `pl-guide-1`, revision 1, title `Original`. |
| Same HTTP request and key | Identical stored result and outer `_meta.replayed: true`. |
| CLI `update` with `--expect 1` | Revision 2, title `Updated`. |
| CLI `get --as-of 1` | Revision 1, title `Original`. |
| CLI `close`, then `ready` | Revision 3, then no ready rows. |

The example fixes `pl-guide-1` only for its disposable store. Production IDs
and generated idempotency keys are runtime values. A lost response prints
the generated key; retry the identical operation explicitly with that key.
Changing the operation under a used key returns `idempotency_key_reused`.
Committed post warnings remain in both JSON and human output, and replay does
not run the post callback again.

Run the executable's HTTP lifecycle, cursor, authn, cancellation, and warning
cases through Bazel:

```sh
bazel --ignore_all_rc_files test //cmd/plug:plug_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no \
  --test_output=all --test_timeout=900 --test_arg=-test.v
```

The fixture uses the actual RPC registry, `httpjson` admission, service, Writer,
memory storage, and a child CLI process. This remote fixture does not exercise
the separate persistent local SQLite/config path, generic RPC commands, watches,
or a network server command; unsupported commands fail explicitly.
