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

Known source fields without a Beads JSONL column, such as `no_history`,
`estimated_minutes`, and compaction/revision fields, are retained under the
reserved issue metadata key `_plug_source_fields` as a canonical JSON string.
This preserves large integers through the pinned importer's metadata
normalization. `Report.ArchivedSourceFields` names every archived field.
Fields describing workflows and unknown source fields fail closed. Empty
string memory values refuse because the pinned Beads importer omits memories
whose key or value is empty.

`Result.Projection` is the declared semantic readback projection: issue IDs,
kinds, statuses, categories and outcomes; edge IDs/endpoints/types; comment
IDs/owners; and memory key/value pairs. `Report` records intentional loss:
head-state conversion cannot recreate complete post-switch history, archived
source rows remain report-only, and archive rows identify blob/principal
families. Warnings and configuration references are retained as well.

Conversion refuses closed-world violations rather than dropping them. Examples
include unknown archive members or kinds, malformed checksums or ordering,
unsupported issue fields/types/statuses, active workflow or ephemeral rows,
external or dangling links, invalid timestamps or typed values, duplicate
identities/keys, unclassified configuration, and malformed secret references.
The refusal is a typed `*Refusal` carrying the same structured report that
would accompany a successful result.

The archive and Beads workspaces used for qualification must be disposable.
The pinned Beads reference is recorded in `BeadsReference`; this package has no
production or upstream repository access.
