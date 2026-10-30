# Dump archive reader and offline merger

`Read` validates the `plug-dump/1` archive emitted by exporter
`10953a8536c3a90b7cf54896d2ed5513b48a8e99`. It accepts the producer's
`offline-head-and-source-archives` scope and the native `plug-native-store`
envelope emitted by `admin.dump`. This is not the full A6 dump schema or a
restore certificate.

The reader checks the bounded tar, member registry and checksums, exact typed
JSON members, source census and column schema, source/body projections,
credential references, relation targets, allocator bounds, and paired migration
fingerprints. It retains original member bytes and all source archives.
`Archive.Manifest`, `Archive.Records`, and `Archive.Member` return detached
copies. Source nulls and JSON numbers retain their values.

`internal/core/restore` supplies the callable empty-store writer for supported
native head images on memory and SQLite. It validates the complete archive and
binds uploaded bytes to the admitted manifest before opening the publication
transaction. Native full-history archives, allocator/project metadata, required
disclosures, and converter archives are explicitly refused before mutation
until their history and allocator invariants can be preserved. The native admin
path does not dispatch converter archives to the separate offline seam described
below. Admin operations require a host-injected, method-and-digest-bound human-presence
verifier and fail closed when that verifier is unavailable.

`internal/integration/converterroundtrip` supplies a trusted offline restore
seam. It refuses unsupported semantics before acquiring a write transaction:
parent and external relations, unverified comments, available-unverified history,
sequential/child counters, and hash configurations other than the ordinary
writer's `pl-` plus 32 hexadecimal characters. Reserved `beads.*` and
`plug.restore.*` metadata collisions also refuse. The unchanged synthetic
exporter fixture contains unsupported data and is deliberately refused.

A supported head-only input additionally requires one explicit `Verification`
argument with the exact archive SHA-256 and an independently compared derived
SHA-256. The exporter supplies no such comparison; the seam never certifies
its own computed digest or promotes `not-run` source checks to a pass. Missing
or mismatched comparisons refuse before lease/write acquisition.

The `builtin-head/1` comparison hashes compact JSON with fields `profile`,
`tables`, `rows`, in that order. `tables` contains all nine driver derived table
names in lexical order; `rows` contains `{table,key,value}` objects sorted by
table then key. An `ancestor_closure` row uses the descendant ID as its key and
an array of `{ancestor,depth}` objects as its value. Since parents are refused,
each bead's value is exactly `[{"ancestor":"<its ID>","depth":0}]`. These self
memberships exist even without an authorization provider. Each bead also has
ready state `{blocked,ready}`, an empty cut array, empty derived attributes,
a sort value `{id,priority}`, and outgoing references sorted by link kind then
target ID. Readiness uses the built-in
ontology's status and blocker rules. The `ontology/builtin` row contains the
built-in ontology document; `plugins/builtin` contains an empty array; grants
are empty. This profile admits no parents, cuts, templates, active leases,
plugins, custom ontology or authorization provider. These are restore-base
rows; the existing services continue to evaluate ordinary writes from their
live graph scans. This comparison is not an A3 or full migration certificate.

After all comparisons pass, the restore-only driver capability checks complete
target emptiness under the global Head fence and repeats it at publication.
It atomically commits source records, typed labels and internal links, all
rebuilt derived rows via `PutDerived`, exact member blobs and a durable archive
manifest/digest record, one keyed restore receipt, the source store ID, a new
incarnation and `HistoryFloor=1`. Prior native history is explicitly absent;
source revisions remain evidence rather than invented native versions. An
indeterminate outcome resolves only through a matching durable receipt;
`Recover` supports read-only recovery after restart. Ordinary restore retries
still refuse populated stores. Receipts retain the driver's 24-hour window.

`Merge(readers ...io.Reader)` validates every input and returns a canonical
merged archive as bytes. `MergeArchives(archives ...*Archive)` accepts retained
`Read` results. Both are offline operations with no store dependencies; an
error returns nil bytes. Callers publish the returned bytes only after success.

The merger preserves project prefixes and allocator state, record IDs, source
history, source content digests, secret references, and configuration paths and
precedence. It sorts projects, records, sources, and tar members, derives the
merged store ID from the sorted input digests, recomputes member checksums, and
validates the complete output through `Read`. Input order does not affect the
output. Identical global records and configuration inputs are retained once.

Duplicate or ambiguous prefixes, conflicting global identities or configuration
paths, incompatible source disposition contracts, and unsupported schema/kind
versions refuse. Nonempty conflict, working-set, or remote declarations also
refuse: this bounded format cannot independently establish that they are clean
or merged. Differing evidence for the same migration version refuses instead
of discarding either source. The input verification limits and incomplete
history disclosures remain in the output; this adds no full A6 history replay
or restore certification. Live restore and converter round-trip execution
remain with their existing owners.
