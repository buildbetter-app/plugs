# Offline converter reader

`Read` validates the `plug-dump/1` archive emitted by exporter
`10953a8536c3a90b7cf54896d2ed5513b48a8e99`. It accepts the producer's
`offline-head-and-source-archives` scope only. This is not the full A6 dump
schema or a restore certificate.

The reader checks the bounded tar, member registry and checksums, exact typed
JSON members, source census and column schema, source/body projections,
credential references, relation targets, allocator bounds, and paired migration
fingerprints. It retains original member bytes and all source archives.
`Archive.Manifest`, `Archive.Records`, and `Archive.Member` return detached
copies. Source nulls and JSON numbers retain their values.

`internal/integration/converterroundtrip` supplies the bounded restore seam for
this initial format. It admits only an empty memory or SQLite target, validates
the complete archive before opening a write transaction, and commits all beads
and typed/source evidence in one `ChangesetRestore`. Bead IDs and source status
are preserved through a `pipeline.Document`; unsupported source columns remain
namespaced metadata, while edge, comment, config, memory, archive, and
allocator evidence remain queryable records. The ordinary read/write services
can operate on the restored IDs; derived-table rebuild and complete A6 parity
remain out of scope, as does a restore certificate.
