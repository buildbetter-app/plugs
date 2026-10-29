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

The integration checkpoint refuses restoration into both empty and populated
stores. Ordinary creates generate fresh IDs, require the initial ontology
status, and stamp current attribution and revisions. A trusted offline restore
operation must atomically admit an empty store, preserve source identity and
allocator state, retain configuration and unverified source evidence, and
rebuild/validate derived state before a real roundtrip can be claimed.
