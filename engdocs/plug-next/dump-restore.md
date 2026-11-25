# Dump, restore, and portability

This page is scoped to the audited `origin/main` snapshot
[`5402faf12a21bbefa325a276ecce69f90fdd6b4e`](https://github.com/buildbetter-app/plugs/tree/5402faf12a21bbefa325a276ecce69f90fdd6b4e).
It separates the archive operations that are present in this tree from the
restore and rollback seams that are still under review.

## What is available on this snapshot

The current tree contains a read-only `plug-dump/1` archive reader and an
offline merger in [`internal/core/dumpformat`](../../internal/core/dumpformat/README.md).
`Read`, `ReadFile`, and `Validate` parse the complete archive in memory. They
do not open a store, acquire a lease, create a revision, or write a target.
`Merge` and `MergeArchives` accept validated archives and return one canonical
archive; they also never open or modify a store.

`plug dump`, `plug restore`, `admin.dump`, and `admin.restore.begin` /
`admin.restore.chunk` / `admin.restore.finish` are not bound in the current
executable or server composition. The catalog entries are contract
descriptors, not an installed handler. The existing [CLI guide](../plug-runtime/cli.md)
therefore continues to list dump and restore as unsupported on this snapshot.
There is no installed reverse converter in this tree either.

The command shapes and refusal rules below describe the reviewed portability
boundary so that operators do not mistake a catalog entry or an open
implementation branch for an available command.

## Validate a `plug-dump/1` archive

The current reader accepts the bounded Beads exporter scope only:

| Manifest field | Accepted value or rule |
| --- | --- |
| `format` and `schema` | `plug-dump/1` and schema `1`. |
| `beads_reference` | The pinned exporter reference `9d6fff760b7a314b476539198504d77ce0959656`. |
| `main_version` and `ignored_version` | The paired migration versions `72` and `28`. |
| `scope` | `offline-head-and-source-archives`. |
| `history` | `unavailable` or `available-unverified`; these are source disclosures, not a full-history restore certificate. |
| `projects` | At least one project, canonical prefixes, one `id_mode` (`sequential` or `hash`), and counters that cover every observed ID. Duplicate or ambiguous prefixes refuse. |

The archive is a canonical USTAR tar. `manifest.json` is first and all other
members are strictly sorted. Members are regular files with mode `0600`, zero
UID/GID and epoch timestamps; links, PAX metadata, duplicate members, unknown
members, malformed JSON, duplicate JSON keys, and trailing data refuse. The
complete archive is limited to 128 MiB. The recognized record members are
`bead`, `edge`, `comment`, `memory`, `config`, and `beads-archive`, together
with `config/sources.json`.

Validation also checks every declared size and SHA-256, the complete
configuration-provenance family, paired migration hashes, typed source-column
schemas, status and kind mappings, ID and child-counter bounds, relation
owners and targets, and the source disposition census. Active workflow rows,
operational rows, unsupported source tables, inconsistent identities or
relations, and unknown kinds are refused. The reader retains the exporter’s
source-state disclosures; the offline merger applies the separate refusal for
unmerged or dirty source declarations described below. A validation error
returns before any store operation; it never produces a partial restore.

The validator requires these two disclosure warnings in the manifest:

```text
not a complete store migration or Plug restore certificate
source history and attribution are unverified; no Plug versions, changesets or live leases invented
```

Those strings describe the evidence boundary. They are retained when archives
are merged and must not be rewritten into a claim of complete history.

## Offline merge

`dumpformat.Merge(readers ...io.Reader)` validates every input, while
`dumpformat.MergeArchives(archives ...*Archive)` consumes results already
returned by `Read`. The output is deterministic under input permutation: it
sorts projects, records, source evidence, and tar members, derives a merged
store identity from the sorted input digests, recomputes member checksums, and
runs the complete output through `Read` again.

The merger preserves project prefixes and allocator evidence, record IDs,
source rows and content digests, configuration paths and precedence, secret
references, and the original warnings and verification limits. It refuses
duplicate or ambiguous prefixes, conflicting global records or configuration,
incompatible dispositions, unsupported schema or kind versions, nonempty
conflict or working-set declarations, and differing evidence for one
migration version. A refusal returns an error and no output bytes; no live
store is touched. This is an offline merge of the bounded source archive, not
a native-store restore or full-history replay.

## Restore contract and refusal boundary

The reviewed native command shape is:

```text
plug dump --output FILE [--history full|head] [--page-size N]
plug restore FILE [--history full|partial|snapshot]
```

Those commands are reserved and unavailable on the current snapshot. The
corresponding RPC contract is paged and admin-only:

| Method | Contract shape |
| --- | --- |
| `admin.dump` | `history` is `full` or `head`; `cursor` continues a snapshot; `pageSize` is 1 through 67108864; the result is a `DumpPage` with `dumpId`, zero-based `page`, base64 `data`, optional `nextPage`, `done`, and a per-page `sha256`. |
| `admin.restore.begin` | Admits a manifest and an explicit history policy in `history` (`full` by default; `partial` and `snapshot` are opt-ins; `head` remains a compatibility alias for `snapshot`) and returns `RestoreStatus` in `receiving` state. The target must be empty before a session is admitted. |
| `admin.restore.chunk` | Appends one zero-based `page` for a `restoreId`; `data` and its required `sha256` are checked before the page is retained. Pages are contiguous and unique. |
| `admin.restore.finish` | Takes `restoreId` and `manifestSha256`, validates the complete archive, then publishes the restore atomically if the target is still empty. |

Every method requires `humanPresence`. The proof is bound to the method and
the sensitive archive or manifest digest. API-key, iframe, and other-origin
bearer calls cannot satisfy the interactive presence mode. A host that cannot
inject a trusted verifier must fail closed with `human_presence_required`.

The catalog's restore errors have distinct meanings:

| Error code | Meaning |
| --- | --- |
| `restore_requires_empty` | The target already has records, history, or a committed revision, including a race detected at final publication. Restore never clears existing data. |
| `restore_checksum_failed` | A manifest, member, page, archive, or submitted digest is malformed or does not match. |
| `restore_violation` | The validated input asks for an unsupported restore capability, has a non-contiguous or duplicate page, or carries an unrecorded disclosure/contract violation. |
| `human_presence_required` | The required trusted human-presence proof is missing, invalid, or unavailable. |

`invalid_params`, `not_found`, `busy`, `rate_limited`, `lease_not_held`, and
`lease_expired` remain the ordinary catalog errors for malformed arguments,
missing sessions, contention, and lease loss. They do not turn a refused
archive into a successful restore. A successful admitted restore returns a
`RestoreStatus`; a partial failure is represented by its `warnings` field,
while validation and capability refusals remain errors.

### Restore history modes

Restore policy is strict by default. An omitted `history` value is `full`; an
operator must explicitly choose `partial` or `snapshot` when source history
cannot be imported as complete native Plug history. The policy is bound into
the host admission digest and the transaction-local import profile.

| Restore mode | Behavior |
| --- | --- |
| `full` (default) | Requires a complete, verified, contiguous and mappable source history. It refuses with a typed `restore_violation` reason when history is unavailable, available but unverified, non-contiguous, unmappable, or when the current restore seam cannot replay it. |
| `partial` | Opts into a verified range plus an explicit gap marker and history floor. The current importer refuses with typed `not-yet-supported` until replay and exact-gap recording are available; it never fabricates history. |
| `snapshot` | Imports the current image only, establishes a new history floor at revision 1, and starts a new store incarnation. Reads below the floor return typed truncation; cursors from the old incarnation return `incarnation_changed`. `head` is accepted as a compatibility spelling. |

### Current image versus full history

Readable history is not the same capability as injecting source history into a
target. The reviewed restore adapter maps declarations as follows:

| Dump history declaration | Required mode | Boundary |
| --- | --- | --- |
| `head` or `unavailable` | `RestoreModeCurrentImage` | A current-image writer may admit it only when the caller selected `snapshot`. |
| Omitted, `full`, or `verified` | `RestoreModeFullHistory` | A target must explicitly advertise full-history injection; a readable `history` capability alone is insufficient. |
| `available-unverified` | Full-history evidence is required | `ErrRestoreUnsupported` is returned and maps to `restore_violation`; it must not be flattened into a head image. |
| Any other value | None | `ErrRestoreHistory` is returned and maps to `restore_violation`. |

The current drivers do not advertise full-history injection. The native
current-image seam therefore refuses source versions and changesets that it
cannot replay, and converter archives with incomplete source disclosures
require the explicit `snapshot` opt-in. A refusal happens after archive and
digest validation but before target mutation. No implementation may silently
turn an unverified or full-history archive into a current image.

### Allocator continuation

The archive's `projects` entries carry `prefix`, `id_mode`, `hash_length`,
`last_id`, and direct-child `child_counters`. Validation proves that observed
IDs do not exceed those bounds and that project prefixes cannot collide. That
evidence alone does not seed a live allocator on the current snapshot.

The reviewed storage seam calls the engine-neutral state `OfflineAllocator`.
Sequential state can continue top-level and direct-child IDs through
`AllocatorContinuation` in the same atomic write transaction after the
complete `AllocatorLock` set is held. Duplicate or conflicting prefixes refuse
with `ErrConflict` before publication. Hash state may be retained as evidence,
but `NextID` and `NextChildID` fail with `ErrUnsupported` until a hash
collision/nonce contract is available. SQLite restart and concurrent
sequential creates are covered by that seam; PostgreSQL continuation is a
separate follow-on. Do not infer collision-free post-restore creation from a
validated archive while that seam is not admitted.

## Reverse conversion and rollback

The reverse-converter boundary is offline and separate from forward restore.
The reviewed package reads a validated head-state `plug-dump/1` archive and
emits deterministic `beads-jsonl/1` bytes. It preserves supported issue IDs,
kinds, statuses, categories and outcomes, text, labels, metadata, comments,
memories, links, and link metadata. Configuration is represented as
string-valued `plug.config/` memories; a secret remains a canonical JSON
reference such as `{"$secret":"config/github.token"}`.

The reverse converter records an input and output digest, a semantic
projection, source warnings, intentionally lost post-switch history,
archive/blob/principal families, unsupported fields, and configuration
references. It refuses unknown members or kinds, malformed checksums or
ordering, unsupported fields/statuses/links, active or ephemeral rows,
external or dangling targets, duplicate identities, invalid typed values,
unclassified configuration, and malformed secret references. The refusal is a
typed `*Refusal` with the same structured report. The switch trial adapter
calls this package offline.

Offline reverse reopens the imported runtime against the original captures.
With streaming store blobs, the retained head decodes configuration entries,
portable history commits, events and frame rows, manifest inventory entries, and NDJSON rows
individually. A completed capture keeps the exact wire bytes and the typed
state needed for census comparisons. Native delta replay reads one logical
row at a time across authenticated parts. Catalog, migration and
source-identity checks use these validated rows and config entries. The
capture-set root and protected metadata have their own admission limits.
Startup's protected metadata allowance covers the base64 representation of
the existing binary source-plan range. SQLite startup streams the persisted
restore changeset one change or event at a time and preserves its order for
header validation. Standalone native reverse applies its transport allowance
to the receipt changeset.
Native transport history markers check their ledger entries individually
inside the separately bounded native record.
SQLite checks the workflow checkpoint's encoded size in the pinned query.
Recovery reads the typed origin and borrows the logical image directly from
the checkpoint envelope. The workflow reader then admits each logical
section under its existing section and aggregate limits.

The retained capture limit is 64 MiB per encoded JSON element. It admits
large individual source values while bounding the largest frame in memory.
The 160 MiB decoded estimate allows for the input, a decoded string copy
and structural overhead. The shared capture budget charges strict-decoder
scratch, live element work, and retained images separately from admission
of the head's wire bytes. Configuration walks borrowed spans and builds one
decoded graph, with the live element charge covering the graph and scalar
unescaping. Nesting remains capped at 128 for capture elements.
The `reverse_json_limit` refusal
includes the member, a structural JSON path and a zero-based index.
Arrays appear as `[]`, and data-defined map keys appear as `<id>`.

## Warnings and secret references

There are three different kinds of warning-like information:

1. `result.warnings` are catalog warning objects returned with a committed
   operation. The current closed warning subset is
   `ontology_drifted`, `subscriber_skipped`, `post_subscriber_failed`, and
   `visibility_lost`; clients show committed warnings in JSON and on stderr.
2. `RestoreStatus.warnings` is the status payload's warning list. It does not
   make a failed validation or unsupported capability successful.
3. `manifest.warnings` are source disclosures. For the current bounded
   exporter archive they must contain the two exact incomplete-conversion
   statements above, and a merger carries them forward unchanged.

Secret values stay references by default. The validator accepts a canonical
single-member `{"$secret":"name"}` object, rejects raw credential values or
digests under sensitive keys, and never calls a secret provider. The catalog's
`includeCiphertext` option is an explicit admin dump contract that requires
the same trusted human-presence proof; it is not enabled by default and is not
implemented by the current executable. Reverse conversion retains references
in its `plug.config/` memory representation and never emits credential bytes.
