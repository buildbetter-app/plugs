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

## Bounded joined configuration

Joined native capture config is assembled through a file-backed element spool. Sort references, framing buffers and output copying are charged to the native decoded budget, and each externalEntry is framed and decoded independently with a bounded nesting depth. The 128 MiB head window is shared by the encoded member and every live frame and decoded element. The retained head remains authenticated by its manifest digest and size.

SQLite is the streamed publication path. Its native restore transaction publishes the joined config with 1 MiB blob chunks and validates the content digest while writing. Memory and PostgreSQL keep their existing single-blob representations. They refuse a joined config above the 32 MiB per-blob budget with native_decoded_limit before opening or allocating the payload. A PostgreSQL chunked representation is a follow-up when deployments require larger archives.

`internal/core/restore` supplies the callable empty-store writer for supported
native head images on memory and SQLite. It validates the complete archive and
binds uploaded bytes to the admitted manifest before opening the publication
transaction. Native full-history archives, allocator/project metadata, required
disclosures, and converter archives are explicitly refused before mutation
until their history and allocator invariants can be preserved. The native admin
path does not dispatch converter archives to the separate offline seam described
below. Admin operations require a host-injected, method-and-digest-bound human-presence
verifier and fail closed when that verifier is unavailable.

Native head images retain protected ontology source records alongside ordinary
records. An ontology source must have kind `ontology`, an ID of `ontology` or
`ontology/...`, and branch zero. Before opening a write transaction, restore
decodes the stored source with the canonical strict decoder and compiles its
base, overlays and plugin catalog. Both the source envelope and plain base
document forms are supported. The source body is preserved; restore neither
seeds replacement ontology nor installs plugins. Malformed sources, mismatched
protected identities and unknown native kinds refuse without publishing data.
This native admission does not widen the offline converter's kind registry or
relax human-presence, caller, manifest, empty-target or history checks.

`internal/integration/converterroundtrip` supplies trusted offline restore
seams. Its legacy head-only seam refuses unsupported semantics before acquiring
a write transaction:
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
table then key. An `ancestor_closure` row uses the canonical hierarchy codec:
`base64url(descendant).base64url(ancestor)` without padding as its key and a
scalar nonnegative integer depth as its value. Every bead has a self row with
value `0`; admitted parent chains add one row for each ancestor at its depth.
These self memberships exist even
without an authorization provider. Comparisons for the earlier array-valued
closure image refuse before publication. Each bead also has
ready state `{blocked,ready}`, an empty cut array, empty derived attributes,
a sort value `{id,priority}`, and outgoing references sorted by link kind then
target ID. Readiness uses the built-in
ontology's status and blocker rules. The `ontology/builtin` row contains the
built-in ontology document; `plugins/builtin` contains an empty array; grants
are empty. This built-in profile admits no cuts, templates, active leases,
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

Strict configuration union is an explicit import migration selected by
`StrictUnionOntologyVerification`, with the existing closed directive shape.
It collects normalized custom status/type rows, stored `status.custom` and
`types.custom`, and all parsed YAML configuration observations independently.
Raw records and `config/sources.json` remain unchanged, including original
paths, precedence and captured byte envelopes. Producer capture must bind
parsed YAML values to its actual bytes; consumer parser controls do not prove
an authentic filesystem observation or the companion's effective runtime view.
Opaque `store-files-and-quiescence` directories marked `contents:not-read`
remain raw provenance without parsed configuration or enumerated entries.
That exception is restricted to this family and cannot replace actual YAML/JSON
configuration: file observations still require object-valued parsed `values`,
including `{}` for legitimate empty files; null or missing values refuse.

The compatible-category `beads-compat` preset maps active to unstarted, wip to
started, done to completed/success and frozen to backlog. Absent/unspecified
categories and conflicting declarations refuse with a `MigrationRefusal`
naming the entry and origins. An explicit `map_status` with
`target:{project:<prefix>,type:"issue"}`, `from:<source-name>` and
`to:<built-in-status-key>` resolves the definition without rewriting either
raw layer. Every declared custom type requires the existing `map_type` to
`issue`. Strict mode installs source-named status/type aliases, including
unused declarations; legacy type-mapping semantics stay unchanged. The
compiled document retains built-in lifecycle slots and transition guards.

Only normalized custom status/type tables become conditionally carried, after
the complete migration validates; all other family refusals, exact raw/body
mapping, census and credential-reference checks remain. A populated declaration
without the strict disclosure refuses rather than remaining archive-only.
Strict mode currently requires one explicit project and refuses source-history
frames: project ownership and custom historical ontology must not be guessed.
`Archive.ImportMigration` exposes detached declarations, the compiled document
and exact per-record targets for the non-native restore adapter. It grants no
native runtime, history, public recorder or production qualification.


Native source schema profiles
-----------------------------

`beads-native-tables/1` accepts the original 72/28 cursor pair without a
`schema_profile`. An explicit profile binds `main_version`, `ignored_version`
and the exact ordered `absences` list. The additional 66/26 profile names three
absent families and four absent columns with the reason
`absent-by-source-schema` and the introducing main migration ordinal. Both
migration series must contain the exact hash-verified prefix. Every remaining
family, column, source record and current row stays mandatory. Other cursor
pairs refuse with `NativeSchemaError`; a missing family is never a schema
absence unless the exact accepted profile declares it.

Historical native images retain their own observed columns and cells. The
profile does not create tables, source fields, portable frames or version rows,
and never clears a retained history gap. Offline merge still requires matching
cursor pairs and preserves each original native member byte for byte.

The receiving report's `NativeSchemas` binds each original source identity to
its profile, actual `PlugDefaults` and counted `TargetOmissions`. These columns
have no modeled Plug field, so `SchemaDefaultCount` is zero; each admitted
entity omission is counted separately. The existing target allocator
initializer is reported separately with its exact committed value. Raw source
NULL, empty and present cells stay distinct from absence facts and destination
defaults. The accounting is persisted with the import receipt and recomputed
from the retained archive on recovery. The original 72/28 report encoding is
unchanged. Synthetic profile fixtures are not actual native capture proof.

Per-commit native schema facts
------------------------------

Each original native source may annotate every `images[]` entry with optional
`schema_profile` in `beads-commit-schema/1`, scoped to `immutable-commit-root`.
Within one source every image must be annotated or every image must use the
legacy unannotated wire. Original sources may retain different annotation
modes in the existing source-scoped assembly.

The exact producer ff8006bf registry is retained as
`testdata/commit-schema-registry-ff8006bf.json`, SHA-256
`cf71387bc62853dd48aa9e7e96dfe67cf1126cdd47c920758d1d4234b80cd253`.
Each physically present main prefix supports 0 through 72, and each ignored
prefix supports 0 through 28 independently. Every row and hash must match the
pinned prefix. A cursor value of zero authenticates an empty migration table;
JSON null means the cursor table is physically absent. The corrected producer
c8ab0ce7 registry is also retained as `testdata/commit-schema-registry-c8ab0ce7.json`,
SHA-256 `900320651fedf60f29686f928c4febba7513f0cb802c8f85399526d058b3aa44`.
It preserves the comparison census and all pinned prefix hashes, and admits
null main only for a complete physical root that is empty or contains only
`dolt_ignore` with exactly the columns `ignored` and `pattern`, with no ignored
cursor. This authenticates the supplied complete image census; immutable
read provenance remains the producer's separate public evidence. No cursor
inherits from the current head or from the other migration lane.

The ordered `absences` list contains `family`, optional `column`, and
`reason: "absent-by-source-schema"`. It is observed against the exact public
comparison census, including removed shipped columns, without `introduced_in`
guesses. An absent family has one family-only entry. Extra old tables and
columns, reshaped rows, SQL NULL, decimal tokens and duplicate cells remain
literal source evidence. These absences do not create history gaps or clear
existing gaps. Contradictions and unsupported prefixes return a typed
`NativeCommitSchemaError` with numeric cursor/ordinal coordinates.

The current top-level profile remains the existing 66/26 or 72/28 contract.
Native member bytes and per-image annotations survive detached access,
source-scoped assembly and retained-archive recovery. The request, current
profile file, idempotency encoding and receiving receipt formats are unchanged.
The receiving verifier also checks zero rows for declared absent families at
each original image's pinned revision. Public constructed controls of these
rules do not qualify a genuine multi-version archive or a new executable.

Native delta reconstruction retains immutable typed rows and normalized fields
under the existing decoded budget. Unchanged tables carry their verified digest
and state forward. Changed operations create shared persistent indexes; changed
tables still authenticate the complete original SHA-256 preimage. Every commit
checks the table census, schema facts, root, terminal counts and exact full-image
byte count. Rows, index nodes, table views and ownership records remain admitted
while any first-parent branch or previous emitted state can use them. The default
limit remains 2 GiB, with typed refusal and complete candidate cleanup.
