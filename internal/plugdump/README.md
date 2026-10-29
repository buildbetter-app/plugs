# Offline Beads exporter

`bd --readonly export --format plug-dump/1 --plug-inventory inventory.json
-o dump.tar` reads a disposable frozen Beads capture through the existing
`storage.RawDBAccessor` capability. It gathers the entire read set in a
read-only, repeatable-read SQL transaction and rolls back. It performs no
storage migration, restore, remote merge, credential lookup, or hook execution.
Ordinary `bd export` retains its JSONL defaults and selection flags.

This is the connected **head-state and source-archive slice**, with scope
`offline-head-and-source-archives`. It is not full-store migration acceptance.
Every archive declares its unperformed schema-type/constraint, capture,
history, authorization, ontology, derived-state and plugin-lock checks.
A future Plug restore validator must refuse this scope unless it explicitly
supports these records and limitations. No Plug implementation is linked here.

## Archive contract

The archive is uncompressed USTAR, limited to 128 MiB in this initial
in-memory implementation. The first member is `manifest.json`; all subsequent
paths sort lexicographically. Regular files have mode `0600`, zero uid/gid,
and Unix-epoch mtime; there are no directories, symlinks, PAX entries or
timestamps derived from the export clock. JSON uses compact `encoding/json`
output with sorted map keys, typed fields in declaration order (`types.go`),
and a final newline. Integers retain their full
64-bit values. SQL NULL remains JSON null; BLOBs use `{"$bytes":"<base64>"}`.

The versioned manifest is described by [manifest.schema.json](manifest.schema.json).
The executable validator additionally enforces member order/type, exact supported
versions, complete family accounting, registry coverage, checksums, byte sizes,
record counts and strictly sorted unique `(project,id)` record identities.
Validation never extracts the tar or opens a target store.

| Member | Contents |
| --- | --- |
| `manifest.json` | `format: plug-dump/1`, `schema: 1`, pinned reference, store/machine identity, paired schema cursors, allocator projects, kind registry, file checksums/sizes, source counts/dispositions, history and verification limits |
| `records/<kind>.ndjson` | Sorted typed records for every present supported kind; an absent kind has no registry entry or member |
| `config/sources.json` | Every declared external input, path, precedence, absence or normalized JSON config; secrets are references |

Each file except the manifest has a SHA-256 and byte size in `files`. The
manifest does not checksum itself. Integrity is not authentication: a checksum
cannot prove that an operator's capture declarations are true.

Registry entries are `{name,version:1,file,count}`. Records are
`{kind,id,project,body,source:{table,fields}}`. Kinds are `bead`, `edge`,
`comment`, `memory`, `config` and `beads-archive`. `source.fields` retains all
unmapped source columns, including SQL NULL, revision/compaction metadata,
attribution and text. `issues.row_lock` and `issues.is_blocked` are excluded
with explicit rebuild dispositions. There are no fabricated Plug revisions,
versions, changesets, derived digests, leases, plugin locks or verification
successes. Available source versions, snapshots and event evidence live in
redactable read-only archive records, preserving their bytes and attribution
status; they are not represented as verified A6 history.

| Source status | Category | Result |
| --- | --- | --- |
| `open` | `unstarted` | absent |
| `in_progress`, `hooked` | `started` | absent; no live lease |
| `blocked`, `deferred`, `pinned`, `frozen` | `backlog` | absent |
| `closed` | `completed` | `success` |

Source status/type spellings remain in provenance. Supported issue types are
`task`, `bug`, `feature`, `epic` and `chore`; the Plug record type is `issue`.
Supported edge types are `blocks`, `parent-child`, `related` and
`discovered-from`. Targets retain their typed issue/external discriminator;
wisp targets and dangling issue targets refuse. Labels sort without loss;
comments keep IDs, authors, text and timestamps. Only the `kv.memory.` prefix
is stripped from persistent-memory keys.

## Input inventory

[testdata/inventory.json](testdata/inventory.json) is a synthetic example,
not an inventory of any real machine. `$FIXTURE_ROOT` is replaced only by the
test fixture. Inventory version is `beads-inventory/1`; unknown JSON members,
duplicate keys and trailing values refuse. Inventory/config files are limited
to 4 MiB each. Source text must be UTF-8.

The operator supplies store/machine identity, unique project prefixes, ID mode
(`hash` or `sequential`), adaptive hash length, top-level and child counters,
quiescence/clean-working-set/merged-remotes declarations, history availability,
every external input and per-key `config_policy`. These are operator-supplied
evidence, not independent freeze/ancestry proofs. Every read table and every
requested file must be accounted for. Multiple projects within one capture
are supported; merging separate databases/machine working sets is not.

Project counters become the maximum of inventory, stored counters and observed
IDs/child suffixes. IDs and prefixes are preserved. Unknown or ambiguous
prefixes, unsupported ID modes, invalid child IDs and unresolved counter
parents refuse. Hash collision/next-ID restore behavior remains a future
restore test; this exporter does not allocate new IDs.

`config_policy` must classify every database `config/<key>` and `metadata/<key>`
as `public` or `secret`. Unclassified/extra entries refuse. Credential-like
keys must be secret. Secret values become `{"$secret":"<table>/<key>"}`;
their original bytes and digests never enter the dump. Public free text is
operator-classified content, not an automatic secret-redaction guarantee.
JSON configuration permits nested secret references and refuses credential
values/digests. Archive checksums hash the sanitized representation only.

External entries use `{family,path,state,precedence}`. `absent` checks actual
absence at the supplied path; present or unreadable inputs refuse. `json-config`
requires an explicitly requested regular UTF-8 JSON object containing public
values and secret references. Precedence is retained; effective YAML is never
inferred. All configured paths and overrides must be declared by the capture
producer; automatic discovery of ambient configuration is not implemented.

## Complete family disposition inventory

The accepted cursor pair is main **72** and ignored **28**, from Beads
`9d6fff760b7a314b476539198504d77ce0959656`. Every numbered migration must
have the exact pinned content hash; missing historical hashes also refuse.
The hashes are pinned in `migration_hashes.go`; the input table and row column
sets must exactly match `catalog.go`. Missing, unknown or duplicate tables,
unknown columns and migration scratch residue refuse. SQL type/index/constraint
verification is explicitly unimplemented; the manifest never certifies it.

| Database source family | Initial disposition |
| --- | --- |
| `schema_migrations`, `ignored_schema_migrations` | Validate paired series; retain source cursor/hash rows as read-only evidence, never Plug schema state |
| `issues`, `labels`, `dependencies`, `comments` | Carry typed head records and provenance; rebuild the two excluded operational fields |
| `config`, `metadata` | Carry classified values/references; persistent memories become `memory` |
| `issue_counter`, `child_counters` | Reconcile project allocator maxima; retain original rows |
| `issue_versions`, `issue_snapshots`, `compaction_snapshots` | Archive available bytes and source metadata; no verified A6 history conversion |
| `events`, `provenance_events`, `bd_events_journal` | Archive event/attribution evidence; never claim a complete history |
| `local_metadata`, `bd_events_seq`, `store_epoch`, `epoch_minted_addresses` | Retain source identity/history-floor/allocator evidence; rebuild target incarnation/cursors |
| `repo_mtimes` | Record row count and rebuild disposition; target filesystem state is not restored |
| `leases` | Refuse every nonempty set, including expired rows until resolved by the capture producer |
| `wisps`, `wisp_labels`, `wisp_dependencies`, `wisp_events`, `wisp_comments`, `wisp_child_counters` | Explicitly refuse nonempty families pending row-level workflow/active/inactive classification |
| `custom_statuses`, `custom_types` | Explicitly refuse nonempty families pending overlay mapping |
| `routes` | Explicitly refuse nonempty SQL routing until multi-store source resolution is implemented |
| `interactions`, `federation_peers` | Explicitly refuse nonempty families pending redaction/credential-safe archive policy |
| `ready_issues`, `blocked_issues` | Derived views are excluded from base-table reads; rebuilding and runtime membership/cycle comparison remain unverified |

All nonempty refused families remain unsupported inputs; an empty table is
still counted. Durable rows with ephemeral/template flags, a non-versioned
storage class, or active gate/molecule/agent markers refuse as well. Unmapped
fields on otherwise supported issues remain in provenance; they do not gain
invented live Plug semantics.

Every external family below is mandatory in the inventory, even when absent:

| External source families | Initial support and remaining work |
| --- | --- |
| `user-legacy-config`, `user-xdg-config`, `user-native-config`, `project-config`, `beads-dir-config`, `project-local-config` | Absence or explicit normalized JSON only; YAML parsing, six-layer discovery and effective-key resolution unsupported |
| `environment-and-flags` | Explicit normalized non-secret JSON/ref provenance; automatic override discovery unsupported |
| `metadata-json`, `legacy-config-json` | Explicit JSON provenance; automatic legacy migration/discovery and backend binding verification unsupported |
| `beads-hooks`, `legacy-hooks`, `git-hooks`, `external-hook-managers` | Absence or supplied JSON audit provenance; script/directory discovery and manager parsing unsupported; hooks never execute |
| `routes-jsonl`, `formulas`, `formula-search-paths` | Absence or supplied JSON provenance; JSONL/TOML/formula parsing, active workflow classification and route traversal unsupported |
| `issues-jsonl`, `interactions-jsonl` | Absence or supplied JSON provenance; cross-checking legacy exports against tables unsupported |
| `dolt-ignore`, `dolt-nonlocal-tables`, `dolt-working-set`, `dolt-commit-graph`, `dolt-remotes`, `dolt-conflicts`, `dolt-history-maintenance`, `store-files-and-quiescence` | Explicit operator capture provenance only; independent ignore-pattern/ancestry/conflict/constraint/depth/flatten/prune/physical-snapshot verification unsupported |

A raw present file/directory in an unsupported format refuses. An explicit
JSON provenance object is evidence about that source, not support for executing
or restoring its semantics. Unknown requested families cannot be smuggled into
the successful archive. The inventory cannot prove that an operator omitted
an undeclared path; its declarations and verification limits remain visible.

Further work includes concrete SQL type/constraint validation, automatic
external-source census, active/inactive wisp dispositions, custom overlays,
complete history floors/A6 conversion, empty-target Plug restore, reverse
conversion and real-store rehearsals. No real-store, production data,
performance, runtime parity or cutover acceptance is claimed by these tests.

## Verification

Use only disposable synthetic data. The package suite tests deterministic byte
identity after row/column reordering, statuses, IDs/allocator maxima, typed
edges, labels/comments/memories, version-byte archives, credential exclusion,
paired schema drift, unknown/unaccounted inputs and invalid archive checksums.
`TestPlugDumpCommandEndToEnd` runs the real export handler with a synthetic
`database/sql` protocol fixture, checks transaction rollback, private atomic
file output, identical stdout serialization, and unchanged destinations on
refusal. It does not emulate a storage engine or initialize a Beads workspace.

```sh
bazel --ignore_all_rc_files test //internal/plugdump:plugdump_test //cmd/bd:bd_test \
  --jobs=2 --@rules_go//go/config:tags=gms_pure_go \
  --test_env=BEADS_TEST_SKIP=dolt --test_arg=-test.short --test_arg=-test.v \
  --test_filter='^(TestDump|TestPlugDump)' --test_output=all
```

The explicit tag replaces the repository rc's required pure-Go regex tag when
ambient rc files are disabled. A target reporting PASS without an actual
named `--- PASS` test is not behavior evidence. Legacy Dolt integration tests
require their own lane and are not certified by this fixture run.
