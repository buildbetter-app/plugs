# Offline reverse converter

`reverseconverter` is the rollback-side boundary for a validated
`plug-dump/1` head-state archive. It is detached and offline: `Read` delegates
to the canonical `internal/core/dumpformat` validator for the bounded archive,
and `Convert` emits deterministic `beads-jsonl/1` bytes for the pinned Beads
importer. The package never opens a
Plug store, starts `bd`, resolves a secret, or writes a target database.

```go
dump, err := reverseconverter.Read(input)
result, err := reverseconverter.Convert(dump)
```

`ClassifyKind` uses this same canonical converter vocabulary for rollback-window
write admission. Optional host mappings require `NewConverter` with executable
`RecordMapping` transformations; its `ClassifyKind` and `Convert` methods share
one immutable registry. Registration requires a canonical content target and
cannot override built-ins or disguise user content as archive/control loss.
Conversion validates the original inventory census, executes the mapping on
detached records, and validates the complete canonical output. A failed mapping
returns a typed refusal with the original input digest and no partial JSONL.
Native workflow kinds have no default mapping. The test-only memory mapping
preserves synthetic checkpoint records as JSON; it does not enable production
workflow admission or expand the canonical archive loader.

The converter preserves issue IDs, supported issue kinds (`task`, `bug`,
`feature`, `epic`, `chore`), Beads statuses and their Plug status categories,
text fields, labels, metadata, comments, timestamps, issue-to-issue links,
link metadata, and memory values. A source status of `hooked` remains
`hooked`; `in_progress` remains `in_progress`. `blocked`, `deferred`,
`pinned`, and `frozen` retain their source status while their category remains
`backlog`. A completed source must carry `success` or `failure`.

For native-status captures, `BeadsDestination` emits the stored Beads status
name, including declared custom names. The per-project category validates
that status and its outcome. The default conversion retains the portable
status projection and provenance for the legacy Plug-native importer.

Unknown source issue types are closed-world refusals unless the caller supplies
an explicit `ontologymigration.MapType` directive to
`ConvertWithDirectives` (or `ConvertArchiveWithDirectives`). The directive
selects the original project/type, may target one exact source ID, and must
match at least one source row without overlapping another directive. It
authorizes the ontology mapping while the emitted Beads
`issue_type` remains the original source value, so a decision or other custom
type is never silently collapsed into a built-in kind. Missing, conflicting,
malformed, or unused directives refuse before any JSONL is returned.

Beads does not retain dependency row IDs through JSONL import. Each emitted
dependency therefore carries a canonical metadata envelope:

```json
{"_plug_edge_id":"edge-123","_plug_metadata":{}}
```

The `_plug_metadata` member is omitted when the source edge has no metadata.
The envelope lets a later Beads export/readback recover the Plug edge ID while
keeping the importer’s graph endpoints unchanged.

Configuration records are string-valued Beads memories under the reserved
`plug.config/` prefix. The source key is retained after that prefix; for
example, a `config/github.token` record becomes
`plug.config/github.token`. Secret values remain canonical JSON references such
as `{"$secret":"config/github.token"}`. No secret provider is contacted and
no credential bytes are written to JSONL or reports.

Known source fields without a Beads JSONL column, such as `estimated_minutes`
and compaction/revision fields, are retained under the
reserved issue metadata key `_plug_source_fields` as a canonical JSON string.
This preserves large integers through the pinned importer's metadata
normalization. `Report.ArchivedSourceFields` names every archived field.
Fields describing workflows and unknown source fields fail closed. Empty
string memory values refuse because the pinned Beads importer omits memories
whose key or value is empty.

`Result.Projection` is the declared semantic readback projection: issue IDs,
kinds, statuses, categories and outcomes; edge IDs/endpoints/types; comment
IDs/owners; and memory key/value pairs. `Report` records intentional loss:
head-state conversion cannot recreate complete post-switch history, non-entity archived
source history remains report-only, and archive rows identify blob/principal
families. Warnings and configuration references are retained as well.

Conversion refuses closed-world violations rather than dropping them. Examples
include unknown archive members or kinds, malformed checksums or ordering,
unsupported issue fields/types/statuses, ambiguous physical-plane IDs,
unmapped links, invalid timestamps or typed values, duplicate
identities/keys, unclassified configuration, and malformed secret references.
The refusal is a typed `*Refusal` carrying the same structured report that
would accompany a successful result.

The archive and Beads workspaces used for qualification must be disposable.
The pinned Beads reference is recorded in `BeadsReference`; this package has no
production or upstream repository access.

The default conversion mirrors `bd export` at the pinned reference: every
`ephemeral=true` row is omitted, with its full physical source tuple in
`Report.OmittedEphemeralRows`. Physical active-plane placement alone does not
exclude a row. `ConvertWithOptions(..., Options{All: true})` and
`ConvertArchiveWithOptions` select the explicit `bd export --all` behavior.
Active rows retain the pinned explicit plane marker and their original raw IDs.
A raw ID present in both entity planes refuses with `source-plane-contract`
because the JSONL importer cannot distinguish that endpoint identity.
An executable `Converter` exposes the same `ConvertWithOptions` contract,
including ontology directives and explicit `All` selection. Mapping native
records preserves existing canonical edge versions and source-plane validation;
the report's input digest continues to identify the original inventory.

`converterroundtrip.ReverseRestoredWithOptions` applies the same selection to a
restored joint image. It verifies the original receipt/archive/plan and immutable
provider origin against current core and namespace data in one source-plane
snapshot, then exports current supported fields and labels. It never rebuilds
terminal item bodies from retained origin records. Existing limits on claims,
unsupported fields, and changed comments/relationships remain explicit refusals.

Live rollback also requires `driver.CheckpointInventoryStore`. The memory,
SQLite and PostgreSQL implementations pin every physical checkpoint key,
generation and payload with the core reader; ordinary requested-key source
snapshots keep their existing semantics. A missing complete-inventory
capability refuses with `checkpoint_inventory_unavailable`. The inventory is
bounded to `SourcePlaneMaxListEntries` keys and `SourcePlaneMaxPlanBytes` total
payload bytes and refuses an oversized census rather than truncating it.

The live report's `core_kind_losses` counts the `instance_config` and
`restore-archive` control records and report-only `beads-archive` provenance.
`namespace_losses` lists every physical
namespace and value key, including namespaces outside the admitted import
plan. Each stored workflow family (`runs`, `items`, `receipts`, `audit`, and
`source.*`) has a record count, field-instance count and disposition. Actual
record/public kinds are separate from serialized family names: the public
item kind is `workflow.item`, the import target is `workflow:item`, and runs
have no public Kind field. `plug_only_kinds` contains actual kinds, not invented
kinds derived from run IDs or family names.

Field instances count scalar/null or empty-container occurrences inside each
stored row body, without deduplicating repeated fields. Namespace revision and
origin wrappers are counted separately. Selected source content, default
exclusions, unbound native state and retained control/provenance have distinct
dispositions. These counts measure schemas Beads does not recreate; they do
not declare every schema field a unique lost value. Receipts and audit entries
remain allowed accounted loss. Unknown plugin values or opaque checkpoint
bytes are explicitly identified, with their physical unit and byte count;
the report does not claim to interpret an external plugin's content kinds.
