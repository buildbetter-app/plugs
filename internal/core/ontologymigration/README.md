# Ontology migration planner

`Build(ctx, Request, RowSource, ValidatingPre)` uses actual compiled current and
candidate `ontology.Snapshot` values. It returns a deterministic immutable `Plan`
with a detached `Report`, or an error with no plan on cancellation/port failure.
`Ready()` means every planner check passed and each remaining row violation has
an explicit disposition. It is not an authorization decision or an apply permit.

The current and next ontology revisions, compilation digest, storage snapshot,
complete row inventory and actual enumeration digest, explicit directives,
before/after images, schema defaults, status category/outcome, validating-pre
manifest and actual dry-run results all contribute to the plan digest. A refused
enumeration also records its observed count and content digest. Rows, directives,
bindings and violations have stable ordering. Report and callback images are
deep copies; JSON numbers stay lossless.

The read adapter must obtain an authoritative inventory of **all** bead ontology
images, with explicit storage/ontology revisions, count and `RowsDigest`, then
enumerate that same snapshot. It must preserve unknown fields, raw fixed fields
and empty field objects. Counts, identities, revisions and the complete digest
are checked; a boolean completeness assertion is insufficient. This port is a
trusted storage boundary: the planner cannot discover records that an adapter
fraudulently omits from both its authoritative receipt and its scan.

Directives select original rows by exact project/type and optionally ID.
`map_type` runs before `map_status`, then custom-field operations. Type mapping
retains all fields. Status mapping uses the resulting project's effective
workflow. Overlapping writes, unknown targets, unused directives, extra members,
and no-op mappings/sets refuse. Kernel priority/template definitions and values
cannot be changed by directives. Field operations refuse ownership in either
the current source type or candidate target type, including removed owners.

Conversions are limited to `string_to_integer`, `string_to_number`,
`integer_to_string`, `number_to_string`, `string_to_boolean`, and
`boolean_to_string`. Their names must match both compiled field schemas.
Numbers use exact JSON syntax and integral checks; booleans accept only the
strings `true` and `false`. No trimming, rounding, machine-integer narrowing,
collection flattening or arbitrary conversion expressions are supported.

Absent defaults come from `ontology.Defaults`; existing invalid values and
plugin-owned defaults are never overwritten. `ontology.Check` validates the
entire resulting state. Every rule targeting its status is conjoined for an
affected row. Graph/permission guards require `GuardSource` observations bound
to that final row and candidate. Missing observations refuse; observed guard
failures are violations. Permission guards need an explicit authorizer-presence
observation, including when no authorizer is installed. Distinct guard parameter
sets retain distinct violation identities. Fixed rules and unavailable owners cannot be
grandfathered. Other grandfather directives identify one exact resulting
violation on one exact row, record a reason and leave that violation visible.

The trusted plugin host supplies an explicit complete validating-pre manifest,
even if empty. `Build` invokes each applicable callback for each affected row,
retains every rejection and requires its disposition. Callback errors and
malformed results cannot be grandfathered. Affected rows include changed
images/defaults, changed applicable ontology rules, row drift and explicitly
targeted rows. Above 5,000 affected rows the plan refuses before dry-running.

This package has no `WriteTx`, Writer, protected-record write, lock, RPC or API
integration. Runtime apply must separately authorize, pin and recheck the exact
plan, row/ontology/plugin revisions and guard observations, serialize migrations,
perform atomic protected writes, and maintain derived tables/events. Real native
host/pipeline adapters and runtime apply acceptance remain follow-up work.
