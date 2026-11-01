# Executable census ratchet

`//tools/census:census_gate` validates the reviewed E2/C7 source and store
census fixture. The fixture is pinned to `beads-base`
`9d6fff760b7a314b476539198504d77ce0959656` and itemizes all 72 main and 28
ignored migration files, the final 51 table/view/scratch shapes and 279
columns, 15 Dolt/server source classes, all 31 configuration inputs named by
the Plug storage census, and all 13,139 Go test declarations. Every migration,
table, column, store/config input, and test declaration has an explicit
disposition.

The gate checks exact pinned counts, canonical source identities, migration
version paths, SHA-256 shape, table/column vocabulary, Dolt/server source
vocabulary, configuration precedence and vocabulary, test-source membership,
and non-empty disposition syntax. It also binds the canonical fixture digest,
so an edited or shortened fixture cannot pass by changing its summary counts.
The in-memory negative cases prove the required ratchet behavior: a known
schema/table/column source removed, an unknown schema or configuration input
introduced, or a disposition deleted all fail validation.

Runtime configuration values are represented as named sources without copying
their contents. YAML precedence layers, CLI/environment overrides, legacy
metadata, hooks, formula search/reference inputs, routes, and export
cross-checks are all explicit. The pinned formula source carries its digest;
Dolt/server state is a separate store-source ledger. This keeps the fixture
reviewable and avoids placing credentials or workspace data in the repository.

Run the gate with:

```sh
bazel test //tools/census:census_gate --test_output=all --lockfile_mode=error
```

The root alias is `//:census_gate`.

The standalone checker prints the canonical counts and digest:

```sh
bazel run //tools/census:census_check -- -census-fixture=tools/census/testdata/census.json
```

## Paired store measurement

Done = every declared source has eight measured reconciliation dimensions or
typed blocks at every required stage, proven by reproducible archive and
actual-store reads plus the focused RunsOn test; source shape or retained
provenance alone does not count as live state or complete history parity.

`paired_census` adds an executable census without changing the pinned source
fixture. It never discovers or opens a database. Owners use an explicit
disposable source archive on their own host:

```sh
paired_census -mode archive -archive /explicit/disposable/source.tar
paired_census -mode behaviors -source-root /explicit/plug/checkout
paired_census -mode reconcile -inventory inventory.json -observations observations.json
```

Build target: `//tools/census:paired_census`. On the shared swarm host, all
Bazel build/test commands run through the approved RunsOn wrapper.

Archive measurement uses `dumpformat.Read`, including all 72 main and 28
ignored migration rows and content hashes. It computes counts and canonical
digests for IDs, project prefixes/modes/hash lengths, counters, dependencies,
available versions/events/snapshots, archives and content. The output contains
no raw record, ID, prefix, source path, machine, configuration value or upstream
error text. Family/column/schema and external-source completeness comes from
the canonical archive validator; unknown shapes or incomplete series refuse.

An owner with an already opened disposable Plug store uses:

```go
capture, err := census.MeasureArchive(sourceReader)
// Handle err before using capture.
snapshot, err := capture.MeasureStore(ctx, disposableStore, 256)
```

The library reads one immutable head through `driver.Store.Read`, pages every
current record, verifies live native fields and retained source fields, checks
all retained archive-member blobs, inventories versions of current records and
changesets strictly above the native history floor, and closes its reader.
It calls no lease, write, migration or raw SQL API. Both the original source
and reconstructed target facts are hashed; the live fields are independently
checked so an unchanged provenance copy cannot hide damaged current content.
Additional unknown target records refuse. This bounded built-in head profile
does not validate rebuilt ready/authorization indexes or behavior parity.

The tool marks source history `history-range-unverified`, even when version
rows were archived. It marks native allocator continuation
`allocator-continuation-unproven`, even when source counters were retained.
The native diagnostic counts cover current-record versions and changesets
above the floor; they do not claim a tombstone census or source history replay.
These blocks require genuine boundary evidence from their owning lanes.

Inventory is separate from observations:

```json
{"format":"plug-paired-census/1","complete":false,"stores":[{"alias":"store-one"}]}
```

`complete` is an owner's inventory assertion. False produces
`authoritative-inventory-incomplete`. A disposable copy is never a second
source alias. Observations use the same format with an `observations` array;
each item contains `alias`, `stage`, and exactly one of `snapshot` (a computed
measurement) or `block` (a closed code). Every source requires these nine
stages: `schema-inventory`, `export`, `archive-validation`, `memory`, `sqlite`,
`postgres`, `reverse`, `readback`, `history-range`. Every measurement has eight
dimensions: `counts`, `ids`, `prefixes`, `counters`, `dependencies`, `versions`,
`archives`, `digests`. Missing stages become typed `observation-unavailable`
blocks. Missing/duplicate dimensions, duplicate/unknown aliases or stages,
unknown fields, duplicate JSON members, negative counts, free-form blocks,
unpaired schemas and mismatched source digests cannot pass.

The reconciler is a comparison tool, not an attestation service: supplied
observations still require the owner's exact source/tool/driver evidence and
independent review. It cannot authenticate a handwritten JSON measurement.
No input assertion overrides the blocks generated by a measured boundary.
CLI exit codes: 0 for successful measurement/reconciliation, 3 for an explicit
qualification block, 1 for invalid input, 2 for invalid invocation. Archive
mode's 0 means the input was measured; individual dimension blocks remain.

## Behavior coverage census

Behavior mode reads only the four declared card files and the `spec` subtree.
It requires exactly B01–B28, parses Go test declarations rather than comments,
and hashes their files and the existing retained reference slices for
B01/B07/B17/B19/B24/B25/B28. A declaration name is a coverage hint. A retained
reference is a slice input. All 28 full cards explicitly remain
`full-card-twin-unqualified`; the earlier source fixture's 13,139
`n/a:unqualified-reference-census` dispositions stay unchanged. The tool neither
re-records goldens nor treats an authored expectation as recorded parity.

`//tools/census:paired_census_test` exercises the public synthetic archive,
real memory and reopened SQLite restores with seven-record pages, native
content changes through the ordinary Writer, retained-blob failure, read-only
reader lifetimes, missing stages/dimensions, paired schema fingerprints,
source binding, strict input, sanitization and complete behavior enumeration.
Its undeclared outputs contain only synthetic measured summaries and the
public behavior inventory. Private archives and stores must never enter CI.
