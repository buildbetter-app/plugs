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
