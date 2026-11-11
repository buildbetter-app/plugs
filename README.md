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

## Evidence attachments

Use an `attach` operation in `tx.apply` to add evidence to a bead. For example:

```json
{"changeSet":{"ops":[{"kind":"attach","target":{"id":"pl-example"},"attachment":{"kind":"link","uri":"https://example.invalid/proof","mediaType":"text/plain","summary":"Review evidence"}}]}}
```

`link` and `commit` require a nonempty `uri` and no `contentRef`. Each accepted
attachment advances the bead revision and appears in `bead.get`, historical
reads, and the durable event stream. Retrying the same request with its original
idempotency key returns the original commit after current write authorization.
An `evidence_required` transition can use these attachments as evidence.

Content-backed `blob` and `log` attachments remain unavailable because the
content store is not implemented. Valid `contentRef` requests return typed
`unsupported` with reason `attachment_content_store_unavailable`. Invalid
attachments refuse without publishing any operation in the transaction.

## External dependency targets

`bead.link`, `bead.unlink`, and link/unlink operations in `tx.apply` accept an
optional `targetKind` of `local` or `external`. Omission means local: a missing
local endpoint returns `not_found`, regardless of its prefix. External targets
require a nonempty ID reference on branch zero and retain the exact ID bytes.
For example, link to an unresolved spawner with `"targetKind":"external"`
and `"to":{"id":"missing-spawner"}`.

Local and external IDs belong to separate identity domains, even when spelled
the same. External scheduling edges stay unresolved and fail readiness closed;
external association edges stay inert. They never bind to local graph members,
indexes, child fanout, blocker authorization, or outcome notifications. Admitted
archive restore and reverse conversion retain the external arm. Generated
archive checks do not establish compatibility with a native Beads importer.

## Parent changes

Moving or detaching a bead updates ancestor relationships for its entire
subtree without changing the descendants' direct parents or projects. A move
that finds inconsistent stored ancestor rows refuses with
`ontology_guard_failed` and publishes no changes; it does not silently repair
those rows. An ordered batch that would affect more than 5,000 ancestor pairs
refuses with `closure_too_large`.

Memory, SQLite, and PostgreSQL maintain direct-child and incoming-link source
indexes atomically with supported writes, independently of derived ancestor
rows. After an ontology-only bootstrap, the first complete typed bead
publication can certify the previously empty graph even though the store
already has other history. Later typed writes maintain that coverage.

On server startup, after acquiring its renewing lease and before admitting
requests, Plug upgrades missing source indexes by decoding every current bead
through trusted core and activating the complete indexes atomically. SQL
backfill holds only a bounded page of source images at a time. SQLite publishes
unavailable index pages in short transactions, allowing ordinary lease renewal
between pages; readers keep failing closed until the final fenced activation.
Memory builds private immutable indexes without blocking its lease heartbeat.
Backfill preserves record bytes, original revisions, public history and the
history floor; newly built coverage starts at the current head, not at an
unknown earlier revision. A failed or canceled build never certifies coverage
or changes public data, and discards its unpublished index rows. Interrupted
SQLite builds remain unavailable and are rebuilt on the next startup.
Already-complete indexes are not rebuilt, stale coverage gaps still refuse,
and a fresh target stays empty for native restore. Merely opening a driver
store does not certify or backfill it.
Hosts composing the Writer directly can run `pipeline.BuildSourceIndexes`
before serving requests. Until coverage is complete, readers return unsupported
and validation retains the complete source-validation fallback.

Source-index coverage proves membership, not semantic graph validity. Native
restore separately validates the complete parent and dependency graph, including
unrelated components and custom-link targets, before atomically publishing a
revision-bound validity certificate with its records, indexes, and ancestor rows.
The certificate survives physical SQLite close/reopen. A hierarchy-only move or
detach can avoid whole-graph validation only when that certificate is current
and bounded source checks prove the affected hierarchy has no incoming or
outgoing dependency links. Validated writes maintain the certificate in their
existing transaction; a graph publication without certification makes it stale.
Missing or stale certificates, link-bearing affected sources, deletes, and link
edits retain complete validation. Startup membership backfill alone never
certifies semantic validity. The ordinary write pre-phase deadline remains two
seconds.
