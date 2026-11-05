# Offline Beads exporter

`bd --readonly export --format plug-dump/1 --plug-inventory inventory.json
-o dump.tar` reads a disposable frozen Beads capture through the existing
`storage.RawDBTxAccessor` capability. It gathers the entire read set in a
read-only, Serializable SQL transaction and rolls back before publishing. It
performs no storage migration, restore, remote merge, credential lookup, or
hook execution.
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
| `manifest.json` | `format: plug-dump/1`, `schema: 1`, pinned reference, store/machine identity, paired schema cursors, allocator projects, optional ontology directives, successor edge registry, optional `source_fields_contract`, file checksums/sizes, source counts/dispositions, history and verification limits |
| `records/<kind>.ndjson` | Sorted typed records for every present supported kind; an absent kind has no registry entry or member |
| `config/sources.json` | Every declared external input, path, precedence, absence or normalized JSON config; secrets are references |

Each file except the manifest has a SHA-256 and byte size in `files`. The
manifest does not checksum itself. Integrity is not authentication: a checksum
cannot prove that an operator's capture declarations are true.

Registry entries are `{name,version,file,count}`. Legacy archives use version
`1`; a fixed-kind successor archive uses edge version `2` for the typed
local issue contract for all 19 fixed dependency kinds while all other record
kinds remain version `1`. When a stored local dependency uses an opaque kind, the edge
registry uses version `3`, a strict superset of version `2`; fixed-only edge
archives remain version `2`.

An inventory and manifest may explicitly select
`source_plane_address_contract: "beads-source-planes/1"`. Under this capability,
edge identity and order use the separate valid UTF-8 byte components project,
source table (`dependencies` or `wisp_dependencies`), then physical ID. Equal
IDs on different planes remain distinct; a duplicate complete tuple refuses.
Raw record IDs, all ten dependency columns, target arms, metadata and source
origins remain unchanged. An absent marker retains the legacy keys and
refusals; an explicitly null, empty or unknown marker refuses at both inputs.
The marker name must use the exact spelling above; case-folded aliases and
alias/canonical combinations refuse, even when their values match.
The native inventory command selects this capability explicitly; generic
inventory callers may continue to omit it.

Version `3` admits only a local `depends_on_issue_id` target whose kind is
nonempty, valid UTF-8, at most 32 bytes, and outside the fixed well-known set:
`blocks`, `parent-child`, `conditional-blocks`, `waits-for`, `related`,
`discovered-from`, `replies-to`, `relates-to`, `duplicates`, `supersedes`,
`authored-by`, `assigned-to`, `approved-by`, `attests`, `tracks`, `until`,
`caused-by`, `validates`, and `delegated-from`. These fixed names use the typed
v2/v3 path; they are never relabeled as opaque. Custom v3 wisp
targets, dangling, multiple-target and malformed external-target rows remain
refusals; typed wisp links use the faithful `wisp_dependencies` source family.
The producer still aborts before encoding when any row is invalid.
Records are `{kind,id,project,body,source:{table,fields}}`. A successor archive
also carries the optional manifest field `source_fields_contract:
"beads-issues-source-fields/1"`. Its absence is the legacy profile; an explicit
empty or unknown value refuses. When present, every issue source field is
retained with its exact SQL NULL, scalar, object, array and lossless-number
representation; this marker does not itself authorize native SQL application.
`source.fields` retains all unmapped source columns, including SQL NULL,
revision/compaction metadata, attribution and text. `issues.row_lock` and
`issues.is_blocked` are excluded with explicit rebuild dispositions. There are
no fabricated Plug revisions, versions, changesets, derived digests, leases,
plugin locks or verification successes. Available source versions, snapshots
and event evidence live in redactable read-only archive records, preserving
their bytes and attribution status; they are not represented as verified A6
history.

| Source status | Category | Result |
| --- | --- | --- |
| `open` | `unstarted` | absent |
| `in_progress`, `hooked` | `started` | absent; no live lease |
| `blocked`, `deferred`, `pinned`, `frozen` | `backlog` | absent |
| `closed` | `completed` | `success` |

Source status/type spellings remain in provenance. Supported issue types are
`task`, `bug`, `feature`, `epic` and `chore`; the Plug record type is `issue`.
Every other durable issue type is a closed-world refusal unless the inventory
contains an explicit `ontology` `map_type` directive whose target project and
source type match the row and whose `to` value is `issue`. The directive
mirrors Plug's ontology migration shape (`id`, `kind`, `target`, `from`, and
`to`), is recorded in the manifest, and never rewrites `source_type` or drops
a row. Directives that are malformed, unused, overlap, target a built-in type,
or target an unaccounted project refuse before archive publication.
All 19 fixed kinds are supported for local durable issue-to-issue rows in edge
v2/v3. The additional association/entity/tracker kinds retain their exact type
and metadata without adding readiness or close guards. Existing typed
`blocks`, `parent-child`, `related`, `discovered-from`, `waits-for`,
`conditional-blocks` and `tracks` retain their wisp/external source contract;
the other 12 kinds on those arms still refuse. Local v1 refusal remains for
all successor-only kinds. Tracks are never admitted through opaque-v3.
Targets retain their
typed issue, wisp or external discriminator; durable and wisp endpoint sets,
source plane, dangling targets and the exactly-one target arm are validated
before encoding. Dependency metadata is retained verbatim in the typed body
and source provenance. It must be valid JSON; waits-for metadata must be an
object whose known `gate`, `also_blocks` and `spawner_id` members use their
canonical types, while unknown metadata members remain opaque. The exporter
never rewrites a gate to `blocks` or drops conditional semantics. Labels sort
without loss; comments keep IDs, authors, text and timestamps. Only the
`kv.memory.` prefix is stripped from persistent-memory keys.

Nonempty wisp and catalog families are retained as read-only `beads-archive`
records with complete source fields. Stored and YAML custom status/type values
are validated as a strict union of compatible declarations, while their raw
source rows and configuration observations remain in provenance. Every status
requires an explicit `active`, `wip`, `done` or `frozen` category; missing or
conflicting categories refuse with the declaration names. Empty or duplicate
entries within a declaration and malformed nested `status`/`types` objects
also refuse. Compatible repetitions across the stored and YAML layers remain
a union; no layer takes precedence and no category is invented. Only native
filesystem families supply YAML declarations; the engine-directory census is
retained unchanged without treating its unread contents as a file tree. A
durable issue and an active wisp with the same ID remain separate source records. Native
`GetIssueInTx` reads the active image and its own labels first. The separate
consumer adoption and native runtime proof remain unqualified by this source
correction.

With the source-plane capability, a wisp may retain a custom status declared
by that strict SQL/config/YAML union. Active rows and auxiliary families remain
complete raw source records, including their lifetime, type, counter and event
fields. This is source capture; consumer workflow/status/lifetime mapping and
runtime adoption require their separate typed joint plan. Opaque kinds remain
durable issue-to-issue only under this capability. A source-plane image outside
the existing portable-history contract is reported as unverified, with its raw
head rows and source evidence retained.

## Input inventory

[testdata/inventory.json](testdata/inventory.json) is a synthetic example,
not an inventory of any real machine. `$FIXTURE_ROOT` is replaced only by the
test fixture. Inventory version is `beads-inventory/1`; unknown JSON members,
duplicate keys and trailing values refuse. Inventory/config files are limited
to 4 MiB each. Source text must be UTF-8.

The operator supplies store/machine identity, unique project prefixes, ID mode
(`hash` or `sequential`), adaptive hash length, top-level and child counters,
quiescence/clean-working-set/merged-remotes declarations, history availability,
every external input, per-key `config_policy`, and any required ontology
directives. These are operator-supplied
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
| `wisps`, `wisp_labels`, `wisp_dependencies`, `wisp_events`, `wisp_comments`, `wisp_child_counters` | Retain complete nonempty rows as read-only workflow evidence; validate plane, endpoint, target-arm and source-field mirrors |
| `custom_statuses`, `custom_types` | Retain complete rows and validate their stored/YAML union; no Plug lifecycle semantics are inferred |
| `routes` | Explicitly refuse nonempty SQL routing until multi-store source resolution is implemented |
| `interactions`, `federation_peers` | Explicitly refuse nonempty families pending redaction/credential-safe archive policy |
| `ready_issues`, `blocked_issues` | Derived views are excluded from base-table reads; rebuilding and runtime membership/cycle comparison remain unverified |

All nonempty refused families remain unsupported inputs; an empty table is
still counted. Durable rows with a non-versioned storage class refuse. The
ephemeral/template flags and active gate/molecule/agent markers on otherwise
supported issue rows are retained verbatim in `source.fields` only; they do
not gain interpreted live Plug workflow semantics. Unmapped fields on
otherwise supported issues remain in provenance under the same boundary.

Every external family below is mandatory in the inventory, even when absent:

| External source families | Initial support and remaining work |
| --- | --- |
| `user-legacy-config`, `user-xdg-config`, `user-native-config`, `project-config`, `beads-dir-config`, `project-local-config` | Absence or explicit normalized JSON sidecars; present YAML observations retain exact path/bytes/hash and parsed objects, while six-layer discovery and effective-key resolution remain unsupported |
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
external-source census, consumer active/inactive wisp semantics, complete
history floors/A6 conversion, empty-target Plug restore, reverse conversion
and real-store rehearsals. No real-store, production data,
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

## Verified source commit images

An optional `SourceHistoryReader` captures the reachable Dolt graph and complete
historical table images within the same scoped reader as the current export.
`SQLSource` implements it using Dolt SQL over the caller's transaction. It never
returns a database or transaction handle. The added `history/source.json` member
uses `beads-dolt-images/1`: source commits and opaque attribution, the complete
current events table, and an explicitly checked range of portable business
images and cumulative event images at exact source commit IDs.
Current, baseline and frame event arrays are copied and ordered by lossless
source identity, so equivalent event-row permutations produce identical history
members and archive bytes. This wire order does not imply event chronology;
stored values and caller-owned rows remain unchanged.

The ordinary read-only export command uses the optional port inside the scoped
raw transaction. It finishes rollback and connection cleanup before publishing
the archive. A source without provable ancestry refuses with a typed source
error; operator inventory declarations cannot turn missing history into proof.

A complete linear graph, every representable image, unchanged earlier events,
an exact current-head comparison and a stable final head/clean/commit census
are required for `history=verified`.
An explicitly observed tableless root is a complete empty source image: it
contains no invented schema, business records or events. This exception requires
a successful non-nil complete census at the proven root. A failed/nil read,
tableless non-root or partially initialized schema still uses the ordinary
unavailable/unmappable path. Event retention must actually cover the source
commits; opting into tracking later cannot recover an ignored event history.
Missing images or unsupported older schemas permit only a proven contiguous
suffix with a complete baseline. Exact inclusive prefix gaps distinguish
`unavailable`, `unmappable` and `not-retained`; the manifest remains
`available-unverified`. A merge, dirty working set, missing ancestry, absent
baseline or unprovable head leaves a raw unverified capture with an explicit
reason, never fabricated historical state. A failed commit/ancestry census
returns typed `ErrSourceHistoryUnavailable` and no archive bytes. Captures are
bounded to 10,000 commits and the existing 128 MiB envelope.

Plug full history remains the strict restore default. Explicit partial imports
the proven suffix with its gaps and floor; explicit snapshot imports only the
current state with a new floor. Dolt commit images are explicit import units,
not claims about individual SQL transactions or edits between commits. Audit
rows and source authors are provenance, never verified Plug principals.
The complete opaque issue-field list is `agent_state`, `await_id`, `await_type`,
`ephemeral`, `event_kind`, `hook_bead`, `is_template`, `mol_type`, `payload`,
`role_bead`, `role_type`, `target`, `timeout_ns`, `waiters`, `wisp_type` and
`work_type`. All remain in `source.fields`; this layer never interprets their
orchestration state or changes their JSON values. Preservation starts at SQL
readback; native JSON storage may normalize an inserted numeric value before
export. The exporter never reconstructs an earlier numeric representation.
Historical images may use a
subset of the head's explicit type directives; every custom historical type
still requires an exact project/type/optional issue match. Other migration
checks, including source-principal attribution, remain unperformed.

## Native public inventory

For a quiesced, disposable public native workspace, the supported `bd` executable
can measure the inventory needed for raw readback without a fixture census:

```sh
mkdir -m 700 capture
bd --readonly export --format beads-inventory/1 --plug-capture-public -o capture/inventory.json
bd --readonly export --format plug-dump/1 --plug-inventory capture/inventory.json -o capture/readback.tar
```

`--plug-capture-public` explicitly declares the target disposable and quiesced
and all unclassified source values public. Config and DB metadata policy keys
come from the actual complete driver read set; recognized credentials remain
secret references. The first command requires a fresh inventory destination in
an existing private directory, writes private normalized source JSON sidecars,
and publishes the existing `beads-inventory/1` file last. Both reads use the
supported read-only Serializable driver scope. No database handle escapes.
Sidecar paths are absolute, under the resolved physical
`<inventory-path>.sources` directory. Keep the inventory and that directory in
place: the later dump export reads those exact sidecars, without re-observing
the original filesystem paths.

All 26 external families have actual normalized observations. Present YAML,
JSON, hooks and formula paths retain file bytes and hashes; directories retain
their bounded source entries. Missing paths are reported as observed absence
inside the JSON evidence. Symlinks, special files, raw structured credentials,
oversized captures and unknown schema refuse. Storage directories are located
without reading engine contents. Public SQL measures ignore/nonlocal rules,
working state, reachable commits, remotes and conflicts. This first producer
requires one native hash-prefix project, a clean source, no Dolt remotes or
conflicts, and supported source mappings. Custom ontology still requires an
explicit operator inventory.

The DB transaction does not make the filesystem atomic. External evidence is a
capture of the resolved paths and process inputs, not a global filesystem,
backup, source-history completeness or Plug restore certificate. Native sources
whose ignored events or old schema images cannot prove continuity retain
partial or unverified history. The existing complete-root producer, raw stored
JSON/BIGINT/time values, history refusal controls and dump formats are unchanged.

Native single-ID reads hydrate the active wisp image and its wisp labels before
falling back to the durable issue image. Only an absent optional active plane or
row permits fallback; active query or label errors remain errors. Export still
retains both physical images when IDs collide. This lookup correction does not
supply consumer restore, history, expiry, restart, or GC qualification.

## Controlled public capture clocks and live active families

The disposable capture opt-in is `BEADS_CAPTURE_CLOCK_PUBLIC=1` together with
`BEADS_CAPTURE_CLOCK_FILE=/absolute/private/clock.json`. Ordinary commands with
both variables absent keep their existing wall-clock behavior. A partial,
empty, malformed or inaccessible opt-in refuses before the CLI executes. The
input and its parent must be private (0600/0700), canonical, non-symlink paths.
The bounded JSON object has exactly these four canonical, non-null fields:

```json
{"version":"beads-capture-clock/1","step":"named-step","go_time":"2026-11-03T12:00:00.123456Z","sql_time":"2026-11-03T12:00:00.123456Z"}
```

`go_time` supports nanoseconds; `sql_time` must be exactly representable in
microseconds. The instants are independent inputs. Every observation reads the
file again. Publish a new step by writing a private sibling and atomically
renaming it over the input **only while commands and SQL queries are
quiescent**. Keep that path and both opt-ins in the environment of each native
CLI process, the HTTP server and the SQL companion for their entire lifetimes.
An invalid later step fails the capture; it cannot fall back to ordinary time.
The hook covers hybrid ready ordering, active deferred filtering, stale cutoff,
expired-defer update stamps, public create/update wall-time stamps and relative
due/defer parsing and warnings. It does not control elapsed-time deadlines,
sleeps, transport timeouts, every audit timestamp or protected lease policy.

The separate Bazel target
`//cmd/plug-capture-sql-server:plug-capture-sql-server` wraps the unchanged Dolt
SQL server from the existing pinned module. `--capture-version` prints its
contract, upstream Dolt version and module versions; `--help` describes the
capture invocation. Other arguments go unchanged to the upstream `dolt
sql-server` parser. For example, start the companion in the native job with
`--host 127.0.0.1 --port <port> --data-dir <disposable-directory>`,
then use the existing public `bd init --server --server-host 127.0.0.1
--server-port <port> ...` route. The pinned server initializes its local root
account in a fresh data directory and rejects the removed `--user` and
`--password` startup options. For local native inventory capture, start the
companion in the workspace's resolved Dolt data root (`.beads/dolt` by default,
or the existing `BEADS_DOLT_DATA_DIR` setting). The client storage locator and
server must identify the same real data root; inventory records the existing
directory without reading its engine contents. The companion installs the engine's existing
query-context clock hook around the server's entire lifetime, so persistent
connections and later HTTP/CLI calls observe published steps. The normal Dolt
release executable, driver, SQL predicates and command semantics are unchanged.
This companion needs its own exact source/build/hash/size/mode/member binding
in the same official artifact; it is never a relabeled release Dolt binary.
The embedded driver overwrites query time, so **controlled embedded SQL is
unsupported**. An opted-in native inventory whose real SQL time does not match
its input refuses rather than claiming controlled time.

With the opt-in present, the existing `environment-and-flags.json` sidecar adds
`capture_clock`. It binds the exact input bytes, path, SHA256, step, actual Go
hook observation, raw real `UTC_TIMESTAMP(6)` and `UTC_TIMESTAMP()` query
results, session timezone/server version, and SQL metadata for the physical
`issues`/`wisps` creation/update/defer columns. It explicitly records 1ns Go
input resolution and 1us SQL clock/expression resolution. The pinned GMS
`UTCTimestamp.Eval` ignores its precision argument and truncates to microseconds:
both `UTC_TIMESTAMP()` and `UTC_TIMESTAMP(6)` retain fractional microseconds.
This engine behavior is preserved. The native DATETIME column precision
is retained as observed metadata. It is not upgraded to nanoseconds. Native
before/at/after points must name their actual representable boundaries.
The native controls retain the actual -1us/at/+1us query-clock observations
and a separately named `one-second-after` point, with both SQL expressions
read at every point. Unchanged Plug nanosecond fixtures remain separate.
The input hash is checked
again before inventory publication. With no opt-in the additional queries and
sidecar field are absent.

The public capture control
`//cmd/plug-capture-sql-server:native_capture_test` runs canonical `bd` and the
companion in a disposable environment. It retains independent clock readback,
real CLI/HTTP hybrid order at a native 48-hour boundary and limits 1/2/1000,
native timed/indefinite defer behavior, and real active-family production via
public create/parent/label/comment/dependency commands. The actual native
inventory/archive goes through `Build`, encoding and self-`Validate`; fresh SQL
multisets must match every row of all six nonempty active families, including
all ten dependency columns and physical IDs. No empty family, injected source
row or marker-only proof qualifies. This ordinary source gate does not admit
an official artifact or prove consumer restore, reverse, native/full-card
conservation or adoption. Those require their independently bound source and
recordings.
