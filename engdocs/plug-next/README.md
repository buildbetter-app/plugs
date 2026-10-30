# Current Plug guides

These pages describe the admitted source snapshot based on
**[`9d1845ceef104e8847f2f45cfc7ef6b16072837f`](https://github.com/buildbetter-app/plugs/tree/9d1845ceef104e8847f2f45cfc7ef6b16072837f)**
(2026-10-29) plus the current implementation. The earlier
[runtime guides](../plug-runtime/README.md) remain an intentionally immutable
snapshot at `471d0199c89acf9e242f44978f2824120726de32`; they are useful for the
baseline lifecycle and storage model, but their statements about the then
unbound remote CLI and ontology RPC do not describe this page set.

The [portability guide](dump-restore.md) separately records the audited
historical snapshot
**[`5402faf12a21bbefa325a276ecce69f90fdd6b4e`](https://github.com/buildbetter-app/plugs/tree/5402faf12a21bbefa325a276ecce69f90fdd6b4e)**
(2026-10-30). Its availability statements and reviewed-contract limits apply
to that snapshot.

| Guide | Use it for |
| --- | --- |
| [Conditional audit](audit.md) | Tracked ontology decisions, ordered notices, delivery, and refusal boundaries. |
| [SQL persistence](sql.md) | Current SQLite and PostgreSQL event-plan durability and engine limits. |
| [Ontology RPC](ontology-rpc.md) | Stored reads, checks, history, diffs, plans, and the conditional protected apply over JSON-RPC. |
| [Authentication](authn.md) | Loopback admission, bearer identities, process-local API keys, and proof limits. |
| [Remote CLI](remote-cli.md) | The current HTTP client and an actual disposable CLI/RPC trace. |
| [Dump, restore, and portability](dump-restore.md) | Bounded `plug-dump/1` validation and offline merge, plus the explicit restore and rollback refusal boundaries. |

## What this pin admits

The current tree connects the conditional audit producer to the tracked
protected-ontology service adapter and the event reader. Memory, SQLite and
PostgreSQL all validate and retain a complete ordered event plan in their
changeset record; only the connected end-to-end audit fixture is qualified
against memory at this pin. Stored ontology reads and migration planning are
bound to seven `ontology.*` JSON-RPC methods, and a composition root with the
canonical protected Writer can add `ontology.apply`. The CLI can send its
ontology commands through the admitted HTTP endpoint, and the HTTP fixture
uses the same RPC bytes and authentication boundary as the executable.

The current tree still has no durable API-key store, OIDC/login session
provider, watch registration, or production recovery integration. A catalog
descriptor does not install a handler: read-only roots keep `ontology.apply`
unbound, while protected roots must explicitly supply the Writer and live
human-proof path. The guides call out each refusal where it matters.

At the portability guide's historical snapshot, the tree has a read-only
`plug-dump/1` Beads archive validator and an offline merger. That snapshot
does not bind `plug dump`, `plug restore`, native `admin.dump` or
`admin.restore.*`, and has no reverse converter. Later main includes the
native restore and reverse-converter packages; their presence does not
establish runtime or production qualification. The guide retains the
historical archive surface and reviewed contracts within its stated limits.

## Run the examples

The [living example](trace_test.go) creates an anonymous and an API-key
loopback server, sends real JSON-RPC bytes, then runs the actual CLI against
the same memory store. It prints only synthetic bodies, never credentials.
Run it with ambient Bazel settings disabled:

```sh
bazel --ignore_all_rc_files test //engdocs/plug-next:examples_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no \
  --test_output=all --test_timeout=900 --test_arg=-test.v
```

The [remote CLI guide](remote-cli.md) shows the captured request/response
excerpt from that fixture. The other pages give focused commands for their
admitted packages. The SQL page lists the separate real-engine targets; the
PostgreSQL target creates and removes its own loopback-only Docker database.
None of these documentation checks certifies deployment, failover,
performance, migration, or a public release.
