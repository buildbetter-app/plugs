# Native namespaced hashes and observed source types

Done = both reported type and identity shape classes produce validated archives
with original source IDs, types and project prefixes, proven by canonical Bazel
unit and real CLI/SQL fixture execution plus independent source review; synthetic
fixture evidence does not count as a real-store rehearsal or switch streak.

Scope is the existing native inventory, exporter and archive reader, with the
required Plug reader/restore compatibility owned by the retained B21 consumer.
Real-store success additionally requires both archives in one initially empty
local Plug store as two explicitly bound projects, with per-project counts,
diff categories and isolation verified by the existing rehearsal lane.

## Project extension

The optional `hash_namespaces` project field is a nonempty, sorted, unique array
of exact native ID namespaces. It is supported only for `id_mode: hash`.
Each namespace is at most 32 ASCII bytes, matches
`^[a-zA-Z][a-zA-Z0-9_]*(-[a-zA-Z0-9_]+)*$`, and has no empty segments.

Omission keeps the existing bare-hash rule. Explicit null, an empty array,
duplicates, unsorted names, noncanonical field spelling, unknown fields and
namespaces on sequential projects refuse. Older readers must refuse the new
field rather than silently ignoring it.

After the unchanged declared project prefix and before the first dot, a root
is either a bare `^[a-zA-Z0-9]+$` hash or an exact declared namespace, one hyphen,
and such a hash. Parse the namespace at the last hyphen. The configured
`hash_length` constrains the hash token alone. Existing positive decimal child
components and stored/observed child counter rules remain authoritative.

For example, this synthetic project admits `repro-4de`, `repro-abc-4de` and
`repro-abc-4de.2`, with the last child's counter bound to the original parent
`repro-abc-4de`:

```json
{"prefix":"repro","id_mode":"hash","hash_length":3,"hash_namespaces":["abc"],"last_id":0,"child_counters":{"repro-abc-4de":2}}
```

It refuses `repro-def-4de`, `repro-abc--4de`, `repro-abc-4_d`, `.0`, empty child
components and children beyond their bound counters. A length floor of 4
refuses `repro-abc-4de`. Namespaces never become separate projects, rename source
IDs, or select a different allocator for future writes. Native inventory emits
only namespaces actually observed in the issues table; the archived source
configuration is unchanged.

## Observed type directives

For each distinct observed non-built-in valid source issue type, public native
capture emits exactly one existing `map_type` directive scoped to the original
project and type, with `from` equal to that type and `to` equal to `issue`.
The ID is `native-map-type/` plus the SHA256 of the canonical JSON array
`[projectPrefix, sourceType]` followed by the existing terminal LF. This is
deterministic under row reordering and does not expose a private ID in the
directive name. Both `body.source_type` and `source.fields.issue_type` retain
the exact source type. Complete custom catalog/config rows remain archived and
validated; no custom lifecycle category or transition is invented.

Offline inventories still require explicit directives. Missing, overlapping,
unused and built-in-remapping directives retain their refusals. The synthetic
type spellings `aaaaa`, `zzzzzzzz`, `yyyyyyyy` and `xxxxxxxxx` cover the supplied
anonymous lengths 5, 8, 8 and 9. Roots `abc-4de`, `abc-45d`, `abc-456`, `abc-d4e`,
`abc-de4` and `abc-def` cover the six supplied anonymous ID shape classes.
These examples are fabricated fixtures and contain no real records.

## Shared-store boundary

The original source prefix, project-scoped identities, allocator metadata and
directives must survive merge/read/restore. The two original stores require
two explicit target-project bindings and per-project conservation/isolation in
one initially empty Plug store. A restore into two separate stores, a renamed
prefix, or assigning every row to one target project does not satisfy this
requirement. The consumer owner supplies the exact supported merge/import
invocation and any refusal for conflicting bindings; producer fixtures alone
do not demonstrate it.

Historical images use allocator counters derived from that image's actual rows
when validating dotted children. The input native inventory has not yet derived
those counters and cannot stand in for the mapped image. Source IDs, child
counter rows and the final manifest remain authoritative; an insufficient final
counter still refuses. A proven head image does not make an older unmappable
schema complete: retain every source commit, explicit gap and range boundary,
and require explicit consumer policy for any incomplete history.


## Native status categories

The optional canonical inventory/manifest marker `status_category_contract` is
`beads-status-categories/1`. Under it, every project has a nonempty `statuses`
array of `{name, category}` pairs, sorted and unique by exact native name. The
array contains all seven native builtins and the strict union declared by
`custom_statuses`, stored `status.custom` and captured native YAML. It includes
unused declared statuses; an observed row cannot invent a catalog entry.

Builtins call the native reference's `types.BuiltInStatusCategory`: `open` is
`active`; `in_progress`, `blocked` and `hooked` are `wip`; `closed` is `done`;
`deferred` and `pinned` are `frozen`. The spelling `frozen` itself is a custom
name and requires an explicit declaration. Custom categories must be exactly
`active`, `wip`, `done` or `frozen`. No `unspecified` category, conflicting
catalog, unknown row status or built-in category replacement is accepted.

`source.fields.status` retains the exact native name. `body.status_category`
contains its literal Beads category, with `result: success` only for `done`.
No new `body.status` field or map_status rename directive is emitted. A consumer
installs each project catalog with unchanged status names; any disposition
status pairs use identical from/to names. Equal names in separate projects
remain scoped to their own catalog. Merge/restore installation belongs to B21.

Omission retains legacy body-category behavior. Present null, empty, unknown
or noncanonical markers, missing/empty project catalogs, noncanonical pair
fields and catalogs without the marker refuse. Both builder and archive reader
check catalog conservation against captured declarations and each bead's
category/result. A source with one global catalog cannot claim different
per-project declarations; independently captured stores keep separate catalogs.

Historical base-table census reads each object's type at the named commit;
derived views are not business tables. Unknown base tables and object kinds
still refuse. A different older status catalog cannot borrow head semantics:
its image remains an explicit unmappable gap in this portable history format.
The native public fixtures retain the failed tar before assertions and report
bounded synthetic-only diagnostics if their history head is not proven.


The public two-project controls distinguish native-default stores from explicitly
tracked synthetic stores. Defaults keep their actual unverified historical head
when clone-local or event tables were never committed. The `-tracked` fixtures
set exact native Dolt ignore overrides and record their actual SQL tables before
and after the real CLI child allocation. They prove only the resulting contiguous
suffix and retain all earlier gaps. They do not reconstruct missing native
history, change a real store, or provide real rehearsal credit. Consumers must
choose explicit incomplete-history policy for these bounded fixture suffixes.

The legacy portable frame has no per-image project catalog. Under
`beads-status-categories/1`, each retained image's actual SQL catalog and stored
configuration must independently establish the same complete status catalog as
the head. Head filesystem YAML cannot fill an older missing declaration. A
changing catalog, YAML-only historical declaration or missing SQL evidence
remains an explicit gap or unverified range, even when the current head exports
successfully. No historical name/category is reinterpreted from current YAML.
