# Plug

## Run Plug in five minutes

Install Bash, `jq`, and Bazel 9.3.0 (Bazelisk reads `.bazelversion`). Bazel
downloads the pinned Go toolchain. From this checkout:

```sh
bazel build //cmd/plug:plug
scripts/demo.sh "$PWD/bazel-bin/cmd/plug/plug_/plug"
```

The demo prints the real remote CLI commands and JSON replies. It creates two
beads, links a dependency, checks the ready queue, then claims, updates, and
closes the work. It restarts the SQLite server and checks that the saved bead
still has the same ID, fields, and revision.

The server uses the existing anonymous loopback policy, an ephemeral port, and
a disposable directory. `quickstart` is an unverified agent label. The script
stops its server and deletes that directory when it exits.

To keep a workspace, configure `.plug/config.yaml` and run `plug server` as
described in the [server guide](engdocs/plug-runtime/server.md). Use
`plug --help` for the public CLI commands and
[CLI connections](internal/cli/README.md) for server selection and JSON output.
