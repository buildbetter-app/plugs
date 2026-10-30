# Beads storage census

Status: research-only census for T3a (2026-10-28). The source tree is
`buildbetter-app/plugs` at `9013b29da` (the pinned Beads behavior commit is
`9d6fff760b7a314b476539198504d77ce0959656`). This document describes the
storage shape that the standalone `bd export --format plug-dump/1` reader must
observe. It does not change the Beads tree, and it is not a proposal to keep
Dolt or the `bd` API in Plug.

The [main migration directory](https://github.com/buildbetter-app/plugs/tree/9d6fff760b7a314b476539198504d77ce0959656/internal/storage/schema/migrations),
[ignored migration directory](https://github.com/buildbetter-app/plugs/tree/9d6fff760b7a314b476539198504d77ce0959656/internal/storage/schema/migrations/ignored),
and [schema runner](https://github.com/buildbetter-app/plugs/blob/9d6fff760b7a314b476539198504d77ce0959656/internal/storage/schema/schema.go)
are pinned references; the paths in this census do not describe the new Plug
tree. This source-shape inventory is not an executed converter, a per-store
row census, or behavior-parity evidence; those artifacts remain deferred in
the [behavior catalog](behaviors.md#deferred-census-and-golden-artifacts).

## Contract and scope

The converter reads a store with the pinned `bd` build, validates the paired
migration cursors, and emits a checksummed `plug-dump/1` archive. The archive
contains sorted records, blobs, versions, changesets, configuration sources,
allocator state, lock/ontology/verification reports, and a manifest. Restore
is accepted only into an empty Plug store (P44). Full history is the default;
`head-only` is an explicit option (P20). Secrets are references by default and
ciphertext is included only with an explicit, human-authorized option (P19).

Every source below has one of four outcomes:

* **carry** means the value is in the archive and has a Plug destination;
* **archive** means the value is retained as a read-only, redactable
  `beads-archive` blob or attachment because it has no live Plug semantics;
* **manifest/rebuild** means the converter records enough evidence to validate
  or reconstruct state but does not restore it as a live record;
* **drop** means a recorded decision deliberately removes the old behavior.

Unknown, unreadable, dirty, or unaccounted input is a conversion refusal, not an
implicit drop. The census is paired per machine: `schema_migrations=72` and
`ignored_schema_migrations=28` must both be known and internally consistent.

The Plug destinations used in this matrix are `bead` (with `issue` as one bead
type, D14), `edge`, `comment`, `memory`, `workflow` plugin records, `config`
and project-overlay records, A6 `version`/`changeset` records, `blob`,
`beads-archive`, and manifest-only allocator/source metadata. A6's change log
is the authoritative Plug history, event, and audit path (P6); source events
are evidence and are never allowed to fabricate a more complete history.

## Final schema matrix

The columns and indexes below are the final shape after all numbered `up.sql`
migrations. A column listed in a row is included in the census even when its
value is derived, clone-local, operational, or later dropped by the converter.
Source ranges name the migration that introduced or materially changed the
shape; the complete migration ledger follows this table.

### Migration cursors

| Table | Final columns | Keys, indexes, constraints | Source and disposition |
|---|---|---|---|
| `schema_migrations` (main) | `version INT`, `content_hash CHAR(64)` | PK `version` | Runner bootstrap in `internal/storage/schema/schema.go`; `0032` removes `applied_at`. Carry only `{main_version:72, hashes}` in the manifest. Refuse an unknown version or hash mismatch; do not restore the cursor as application data. |
| `ignored_schema_migrations` (clone-local) | `version INT`, `applied_at DATETIME`, `content_hash CHAR(64)` | PK `version` | Runner bootstrap plus ignored lane. Carry `{ignored_version:28, hashes, machine}` in the manifest. It is a required paired cursor, not a Plug record. |

### Main durable/application plane

| Table | Final columns (all columns) | Keys, indexes, constraints | Source and disposition |
|---|---|---|---|
| `issues` | `id`; `content_hash`; `title`; `description LONGTEXT`; `design LONGTEXT`; `acceptance_criteria LONGTEXT`; `notes LONGTEXT`; `status`; `priority`; `issue_type`; `assignee`; `estimated_minutes`; `created_at`; `created_by`; `owner`; `updated_at`; `closed_at`; `closed_by_session`; `external_ref`; `spec_id`; `compaction_level`; `compacted_at`; `compacted_at_commit`; `original_size`; `sender`; `ephemeral`; `wisp_type`; `pinned`; `is_template`; `mol_type`; `work_type`; `source_system`; `metadata JSON`; `source_repo`; `close_reason LONGTEXT`; `event_kind`; `actor`; `target`; `payload`; `await_type`; `await_id`; `timeout_ns`; `waiters`; `hook_bead`; `role_bead`; `agent_state`; `last_activity`; `role_type`; `rig`; `due_at`; `defer_until`; `no_history`; `started_at`; `is_blocked`; `row_lock`; `storage_class`; `current_revision`; `participation_generation` | PK `id`; indexes `idx_issues_priority`, `idx_issues_issue_type`, `idx_issues_assignee`, `idx_issues_created_at`, `idx_issues_spec_id`, `idx_issues_external_ref`, `idx_issues_status_updated_at(status,updated_at)`, `idx_issues_defer_until`, `idx_issues_is_blocked(is_blocked,status)`. `idx_issues_status` is removed by `0052`; lease index and lease columns are removed by `0055`. | `0001`, `0023`, `0027`, `0033`, `0034`, `0038`, `0046`, `0049`, `0052`, `0054–0055`, `0060`, `0067`, `0070`. Carry one `bead` per row, preserving identity, text, metadata, scheduling, attribution, external/spec references, template/pinned flags, and status/type source values. Map status into the versioned ontology/project overlay (P10/P22/P41) while retaining the source value in provenance. `content_hash`, compaction fields, `no_history`, `current_revision`, and participation generation are carried as source/verification metadata; Plug recomputes its own digest and revision token. `is_blocked` and all index state are derived and rebuilt (P50). `row_lock` is a transient lock and is not restored. `storage_class` and `ephemeral` select durable bead versus workflow/ephemeral handling (P45); active orchestrator payload fields are escalated to the workflow parity set rather than blanket-dropped. |
| `dependencies` | `id`; `issue_id`; `depends_on_issue_id`; `depends_on_wisp_id`; `depends_on_external`; `type`; `created_at`; `created_by`; `metadata JSON`; `thread_id` | PK `id`; unique `uk_dep_issue_target(issue_id,depends_on_issue_id)`, `uk_dep_wisp_target`, `uk_dep_external_target`; indexes `idx_dependencies_issue`, `idx_dependencies_thread`, `idx_dep_wisp_target`, `idx_dep_issue_target`, `idx_dep_external_target`, `idx_dep_type_issue(type,depends_on_issue_id)`, `idx_dep_type_wisp`, `idx_dep_type_external`; FK `issue_id -> issues.id` and `depends_on_issue_id -> issues.id`, both cascade; check `ck_dep_one_target` (exactly one typed target). | `0002`, `0041–0043`, `0050`. Carry one typed `edge`; preserve external targets and thread/metadata. The historical generated `depends_on_id` and old composite PK do not exist in the final shape, but their source values are represented by the three typed columns. Rebuild indexes and closure facts transactionally (P50); dangling targets are reported and block G2 if unexplained. |
| `labels` | `issue_id`; `label` | PK `(issue_id,label)`; `idx_labels_label`; FK `issue_id -> issues.id` cascade | `0003`, `0042`. Carry as bead label values (or normalized `label` records) and retain unknown labels. |
| `comments` | `id`; `issue_id`; `author`; `text LONGTEXT`; `created_at` | PK `id`; indexes `idx_comments_issue`, `idx_comments_created_at`, `idx_comments_issue_created_id(issue_id,created_at,id)`; FK `issue_id -> issues.id` cascade | `0004`, `0037`, `0042`, `0049`, `0056`. Carry as `comment` records in creation order; preserve UUIDs, author, text, and timestamps. |
| `events` (ignored/local after `0062`) | `issue_id`; `event_type`; `actor`; `old_value LONGTEXT`; `new_value LONGTEXT`; `comment`; `created_at`; `id` | PK `id`; indexes `idx_events_created_at`, `idx_events_issue`; FK `issue_id -> issues.id` cascade. The table is clone-local after `0062`; it is recreated in ignored `0019` when absent. | `0005`, `0037`, `0042`, `0048`, `0057`, `0062`, ignored `0019`. Carry rows as event evidence/attachment or `beads-archive`; use `issue_versions` and Plug changesets for authoritative history (P6, P45). Never treat clone-local rows or their Dolt commits as a complete history. |
| `config` | `key`; `value TEXT` | PK `key` | `0006`, defaults `0016`, key migration `0030`, memory prefix used by `bd remember`. Carry all non-secret rows as namespaced `config` records and include the exact source key/value in the archive. `kv.memory.*` rows become `memory` records under P85 (strip only the storage prefix). Secret values follow P19: reference by default, ciphertext only with the explicit dump option; never write credential bytes into this census or report. |
| `metadata` | `key`; `value TEXT` | PK `key` | `0007`, `0030`, `0036`. Carry as namespaced source/config metadata when it identifies the project or import; migrated `tip_*`, `bd_version`, `bd_version_max`, and Dolt auto-push keys are local/history bookkeeping and are recorded in the source manifest then rebuilt or dropped. |
| `child_counters` | `parent_id`; `last_child` | PK `parent_id`; final FK `parent_id -> issues.id` cascade (re-added by ignored `0002`; main `0039` temporarily drops it) | `0008`, `0039`, ignored `0002`, `0011`. Carry the maximum counter for each restored bead and reconcile it with the maximum observed child ID (A1/P76); do not copy a stale lower value. |
| `issue_snapshots` | `id`; `issue_id`; `snapshot_time`; `compaction_level`; `original_size`; `compressed_size`; `original_content`; `archived_events` | PK `id`; indexes `idx_snapshots_issue`, `idx_snapshots_level`; FK `issue_id -> issues.id` cascade | `0009`, `0037`, `0042`. Carry snapshot bytes as history/base `blob` and attach archived events. Preserve the snapshot timestamp and compaction level; restore as Plug versions or redactable archive, never as current bead state (P30/P73/P45). |
| `compaction_snapshots` | `id`; `issue_id`; `compaction_level`; `snapshot_json BLOB`; `created_at` | PK `id`; index `idx_comp_snap_issue(issue_id,compaction_level,created_at DESC)`; FK `issue_id -> issues.id` cascade | `0010`, `0037`, `0042`. Carry as full-base/history blobs and source metadata; Plug compaction is a service changeset, so this table is not restored as a derived table. |
| `repo_mtimes` (ignored/local) | `repo_path`; `jsonl_path`; `mtime_ns`; `last_checked` | PK `repo_path`; `idx_repo_mtimes_checked(last_checked)` | `0011`, local-state flip `0028`, ignored `0001`. Drop and rebuild from the target filesystem; include a read-only digest only when diagnosing export/import drift. It is not graph data. |
| `routes` | `prefix`; `path`; `created_at`; `updated_at` | PK `prefix` | `0012`. Carry the rows as converter source/project mapping: one Plug project per prefix (P76). There is no core cross-store route runtime; preserve the input in the manifest/config archive and refuse duplicate or conflicting prefixes. |
| `issue_counter` | `prefix`; `last_id` | PK `prefix` | `0013`. Carry as per-project allocator state; seed next IDs from `max(stored counter, observed IDs, child counters)` and preserve prefix/ID mode. |
| `interactions` | `id`; `kind`; `created_at`; `actor`; `issue_id`; `model`; `prompt`; `response`; `error`; `tool_name`; `exit_code`; `parent_id`; `label`; `reason`; `extra JSON` | PK `id`; indexes `idx_interactions_kind`, `idx_interactions_created_at`, `idx_interactions_issue_id`, `idx_interactions_parent_id` | `0014`. If audit is enabled, carry as redaction-aware `audit`/`beads-archive` blobs with references to beads. Do not expose prompt/response credential bytes; P19 applies to embedded secrets. If an operator elects not to retain the audit sidecar, record an explicit drop and digest in the manifest. |
| `federation_peers` | `name`; `remote_url`; `username`; `password_encrypted BLOB`; `sovereignty`; `last_sync`; `created_at`; `updated_at` | PK `name`; `idx_federation_peers_sovereignty` | `0015`. Federation/sync has no core successor (D10). Keep a non-secret peer/provenance manifest if useful; drop remote credentials and live sync state. |
| `custom_statuses` | `name`; `category` | PK `name` | `0024`. Carry into the versioned ontology/project overlay, preserving category and source spelling (P10/P22/P41). A missing overlay mapping is a refusal, not a silent category change. |
| `custom_types` | `name` | PK `name` | `0024`. Carry into the project type overlay; `issue` remains one bead type and all other source types map to Plug ontology/plugin types (D14, P41). |
| `local_metadata` (ignored/local) | `key`; `value TEXT` | PK `key` | `0029–0030`, ignored `0001`. Clone-local version, tracker, and journal watermarks are not restored. Read and record relevant floors (especially `events_journal_autoprune`) in the manifest so a pruned journal cannot be presented as complete; rebuild Plug cursors/incarnation. |
| `provenance_events` | `id`; `issue_id`; `kind`; `actor`; `ref`; `ref_kind`; `payload`; `source`; `occurred_at`; `created_at` | PK `id`; indexes `idx_provenance_events_issue_occurred(issue_id,occurred_at)`, `idx_provenance_events_ref`, `idx_provenance_events_kind`; FK `issue_id -> issues.id` cascade | `0063`. Carry as provenance attachments or A6 attribution fields. Preserve source, actor, refs, timestamps, and payload; mark attribution unverified when the source cannot prove it (P45). |
| `bd_events_journal` (ignored/local) | `seq`; `ts`; `op`; `issue_id`; `issue_json`; `dep_json`; `comment_json`; `actor` | PK `seq`; indexes `idx_bd_events_journal_issue`, `idx_bd_events_journal_ts` | `0064`, `0066`; ignored `0022–0025`. Carry a bounded/read-only journal attachment or changeset evidence when exact reconstruction is available. Capture the highest sequence and prune floor; Plug's change log is authoritative (P6), and pruned/missing rows produce an explicit history floor/archive. |
| `bd_events_seq` (ignored/local) | `id`; `next_seq` | PK `id` (singleton `id=0` by initialization) | `0064`; ignored `0022`. Manifest-only journal head; do not restore this counter as a Plug revision. |
| `issue_versions` | `issue_id`; `revision`; `epoch`; `durable_state LONGBLOB`; `change_actor`; `change_agent`; `change_message`; `change_at DATETIME(6)`; `removed_at DATETIME(6)`; `removed_reason`; `attribution_status`; `removed_restriction` | PK `(issue_id,revision)` | `0067–0071`. Carry every available row into A6 JSON Patch versions/full bases. Preserve actor/agent/message/time, removal reason/restriction, attribution status, epoch, and durable bytes; replay at most the Plug base interval (P30/P73). `no_history` and missing rows become a declared history floor, never fabricated history. |
| `store_epoch` | `id`; `epoch`; `bumped_at`; `bumped_reason` | PK `id`; check `id=1` | `0067`. Carry source epoch/fence evidence in the manifest, but mint a new Plug incarnation on restore; old revision tokens must fail (P44/A6). |
| `epoch_minted_addresses` | `address`; `store_id`; `minted_id`; `minted_epoch`; `minted_at`; `carried_from`; `gone_at_epoch` | PK `address`; `idx_epoch_minted_addresses_carried_from`; check `gone_at_epoch IS NULL OR gone_at_epoch >= minted_epoch` | `0072`. Carry allocator/fencing history as project identity metadata. Rebuild active allocator state and never reuse a source address in a new Plug incarnation. |
| `leases` (ignored/local) | `issue_id`; `holder`; `granted_at`; `lease_expires_at`; `heartbeat_at`; `granted_node` | PK `issue_id`; `idx_leases_expires(lease_expires_at)` | Main `0054–0055`; ignored `0012`, `0016`. Do not dump live leases. Require quiescence and classify active claims before export; restore claims only through Plug's lease API, with no invented live lease for a source `in_progress` row (P49/P70/P71; behavior cutover rule). |

### Ignored/local table shapes

These tables are listed separately because their final shape is assembled by the
ignored migration lane. They are still part of the paired census and their
rows are classified before any drop decision.

| Table | Final columns (all columns) | Keys, indexes, constraints | Source and disposition |
|---|---|---|---|
| `wisps` (ignored/local) | Same issue-shaped fields as `issues`: `id`; `content_hash`; `title`; `description LONGTEXT`; `design LONGTEXT`; `acceptance_criteria LONGTEXT`; `notes LONGTEXT`; `status`; `priority`; `issue_type`; `assignee`; `estimated_minutes`; `created_at`; `created_by`; `owner`; `updated_at`; `closed_at`; `closed_by_session`; `external_ref`; `spec_id`; `compaction_level`; `compacted_at`; `compacted_at_commit`; `original_size`; `sender`; `ephemeral`; `wisp_type`; `pinned`; `is_template`; `mol_type`; `work_type`; `source_system`; `metadata JSON`; `source_repo`; `close_reason LONGTEXT`; `event_kind`; `actor`; `target`; `payload`; `await_type`; `await_id`; `timeout_ns`; `waiters`; `hook_bead`; `role_bead`; `agent_state`; `last_activity`; `role_type`; `rig`; `due_at`; `defer_until`; `no_history`; `started_at`; `is_blocked`; `row_lock`; `storage_class`; `current_revision`; `participation_generation` | PK `id`; indexes `idx_wisps_status`, `idx_wisps_priority`, `idx_wisps_issue_type`, `idx_wisps_assignee`, `idx_wisps_created_at`, `idx_wisps_spec_id`, `idx_wisps_external_ref`, `idx_wisps_is_blocked(is_blocked,status)`, `idx_wisps_defer_until`; no lease columns in the final shape. | Main `0020`, `0023`, `0027`, `0033`, `0035`, `0038`, `0047`, `0049`, `0054–0055`, `0060`, `0067`, `0070`; ignored `0001`, `0006–0007`, `0013`, `0015`, `0017`, `0020–0021`, `0027–0028`. Classify row-by-row into workflow records, active plugin/bead records, or `beads-archive`. Preserve source payload/attribution and all user-visible fields; rebuild blocked state, row locks, and Plug revision tokens. |
| `wisp_labels` | `issue_id`; `label` | PK `(issue_id,label)`; `idx_wisp_labels_label`; FK `issue_id -> wisps.id` cascade | Main `0021`, ignored `0001`, `0004`. Carry with the owning active workflow/bead record; archive dangling/inactive rows with their owner. |
| `wisp_dependencies` | `id`; `issue_id`; `depends_on_issue_id`; `depends_on_wisp_id`; `depends_on_external`; `type`; `created_at`; `created_by`; `metadata JSON`; `thread_id` | PK `id`; unique `uk_wisp_dep_issue_target`, `uk_wisp_dep_wisp_target`, `uk_wisp_dep_external_target`; indexes `idx_wisp_dep_type`, `idx_wisp_dep_wisp_target`, `idx_wisp_dep_issue_target`, `idx_wisp_dep_external_target`, `idx_wisp_dep_type_issue(type,depends_on_issue_id)`, `idx_wisp_dep_type_wisp`, `idx_wisp_dep_type_external`; FKs `issue_id -> wisps.id`, `depends_on_wisp_id -> wisps.id`, `depends_on_issue_id -> issues.id`, all cascade; check `ck_wisp_dep_one_target`. | Main `0021`, `0022`, `0047–0047` repair path; ignored `0001`, `0003–0005`, `0026`. The old generated `depends_on_id`, composite PK, and `idx_wisp_dep_type_target` are gone after ignored `0005`. Carry typed workflow edges when their owner is active; otherwise archive with the owner. |
| `wisp_events` | `id`; `issue_id`; `event_type`; `actor`; `old_value LONGTEXT`; `new_value LONGTEXT`; `comment`; `created_at` | PK `id`; indexes `idx_wisp_events_issue`, `idx_wisp_events_created_at`; FK `issue_id -> wisps.id` cascade | Main `0021`, `0031`, `0037`, ignored `0001`, `0004`, `0008`. Carry active event evidence with its workflow/bead; archive local-only history and preserve attribution status. |
| `wisp_comments` | `id`; `issue_id`; `author`; `text LONGTEXT`; `created_at` | PK `id`; indexes `idx_wisp_comments_issue`, `idx_wisp_comments_issue_created_id(issue_id,created_at,id)`; FK `issue_id -> wisps.id` cascade | Main `0021`, `0037`, `0056`, `0065`; ignored `0001`, `0004`, `0024`. Carry active comments, archive inactive comments with their owner. |
| `wisp_child_counters` | `parent_id`; `last_child` | PK `parent_id`; FK `parent_id -> wisps.id` cascade | Ignored `0001–0002`, `0011`. Carry/reconcile only for active workflow rows; seed the allocator from stored and observed maxima. |

### Ignored-plane semantic rows

The ignored migration lane is not disposable merely because Dolt does not
replicate it. It is clone/machine-local working-set state and must be read
before export. `wisps`, their auxiliary tables, `events`, the journal, local
metadata, repo mtimes, and leases are all present or absent according to the
paired ignored cursor and the machine's local history. The converter applies
this classification:

| Ignored source | Active classification and destination |
|---|---|
| `wisps` rows with `issue_type`/`storage_class` used by workflow execution | Classify each row. Workflow runs, agent/role records, and messages are distinct `workflow` plugin records or Plug beads when the parity card says they are user-visible. Carry labels, typed dependencies, events, comments, payload, `spec_id`, `mol_type`, `await_*`, and child counters with the row. Do not blanket-drop rows just because the table is ignored. |
| Inactive, unmapped wisp rows | Carry a read-only, redactable `beads-archive` blob with row digest and source key by default. A row proven to be irrelevant may be `drop:P48/P45`, but the manifest must contain its digest and reason. |
| `wisp_labels`, `wisp_dependencies`, `wisp_events`, `wisp_comments`, `wisp_child_counters` | Follow the owning wisp classification. Active relations become workflow/bead labels, edges, comments, or provenance; inactive relations are archived with their owner or reported as dangling. Typed targets (`depends_on_issue_id`, `depends_on_wisp_id`, `depends_on_external`) are never collapsed into an untyped string. |
| `events` and `bd_events_journal` | Read as local audit/change evidence. Use available `issue_versions` and event rows to reconstruct A6 changesets; retain an archive and a history floor when rows were pruned or the journal was disabled. |
| `leases` | Operational state only. Refuse conversion with unresolved live claims unless the freeze procedure has expired/released them; rebuild Plug leases after restore. |
| `local_metadata`, `repo_mtimes` | Rebuild/drop. Preserve only the journal prune watermark, schema/version evidence, tracker content hashes, and other values needed to explain a history floor or source digest. |

A row's `storage_class` is evidence, not an authorization to discard content.
The converter emits per-row counts and digests by class and by issue type.

### Derived views and migration scratch

`ready_issues` and `blocked_issues` are derived views, not dump records.
`ready_issues` selects `issues.*` for open or custom-active statuses, excludes
nonzero ephemeral rows, deferred rows, and blocked transitive dependencies;
`blocked_issues` selects `issues.*` plus `blocked_by_count` for non-done/frozen
rows with blocking edges. Their definitions were revised by `0017`, `0025`,
`0044` and `0018`, `0026`, `0045`; the final Plug readiness and blocker facts
are rebuilt transactionally from typed edges and ontology (P50). The rehearsal
must compare memberships and cycle results, not copy the view rows.

The following names are migration scratch, not durable source data:

* `__temp__wisps`, `__temp__wisp_labels`, `__temp__wisp_dependencies`,
  `__temp__wisp_events`, `__temp__wisp_comments`, `__temp__repo_mtimes`,
  `__temp__local_metadata`, and `__temp__wisp_child_counters` from ignored
  `0001–0002`;
* `__temp__leases` from main `0055` and ignored `0012`;
* `__temp__bd_events_journal` and `__temp__bd_events_seq` from main `0064`
  and ignored `0022`;
* `__temp__events_flip` from main `0062`;
* `__bd_0059_recompute_wisps` and `__bd_0059_recompute_wisp_deps` from main
  `0059`;
* `repo_mtimes_tmp` from main `0028`; and
  `__temp__ignored_schema_migrations_untrack` used by the ignored-cursor
  repair path.

A fresh export refuses or explicitly reports any such table that remains after
its migration pass. The canonical `dolt_ignore` pattern `__temp__%` prevents a
new staging table from becoming silently tracked; it is not permission to
ignore residue.

## Non-table sources

### YAML configuration and precedence

The census takes a read-only copy and SHA-256 digest of every layer that can
contribute to effective configuration:

1. legacy user `~/.beads/config.yaml`;
2. documented user `~/.config/bd/config.yaml`;
3. the native OS user config directory for `bd/config.yaml`;
4. the repository's nearest `.beads/config.yaml`;
5. `$BEADS_DIR/config.yaml`, when set; and
6. `.beads/config.local.yaml`, merged last beside a project config.

CLI flags and environment variables (`BD_*`, plus `BEADS_ACTOR`,
`BEADS_IDENTITY`, and `BEADS_NODE_ID`) override files. The archive records the
source path, precedence, normalized effective YAML, and per-key digest; it does
not copy a secret value unless the explicit ciphertext option is selected.
Malformed, unreadable, or unaccounted layers refuse conversion.

The complete YAML key surface is open-ended, so the converter captures all
keys, including unknown/user-defined keys. The known namespaces that affect
Plug behavior are:

* identity/startup: `no-db`, `json`, `db`, `actor`, `identity`, `node_id`,
  `no-push`, `no-git-ops`, `agent.profile`;
* journal/history/audit: `events-journal`,
  `events-journal-retain-days`, `events-journal-retain-rows`,
  `events-journal-auto-prune`, `audit.enabled`, and
  `versioned-history.enabled`;
* memory and validation: `prime.max-memories`, `prime.max-memory-chars`,
  `prime.disabled`, `memories.budget-chars`, `validation.*`,
  `hierarchy.max-depth`, `lint.*`, `create.require-description`;
* routing and project inputs: `routing.*`, `directory.labels`,
  `external_projects`, `repos.*`, `storage-class.*`, and `issue-prefix`;
* Dolt/server and machine state: `dolt.*`, `sync.*`, `federation.*`,
  `backup.*`, `export.*`, `import.*`, `metrics.*` (including the upstream
  endpoint), and `git.*`;
* ontology/integrations/UI: `status.custom`, `types.custom`, `custom.*`,
  `formula.*`, `ai.*`, `list.*`, `output.*`, and tracker-specific keys; and
* credentials such as `github.token`, `linear.*`, `jira.api_token`,
  `gitlab.token`, `ado.pat`, and `notion.token`.

YAML-only keys remain source/config metadata in Plug. Ontology and project
settings become versioned overlay/config records (P10/P22/P41); routing is
converter input rather than a core multi-store route; `sync.*`, federation,
and old Dolt remote behavior are dropped under D10. Secret keys are represented
as `{"$secret":"<name>"}` references by default (P19). Values hidden by an
environment override are still noted as an override and never inferred from a
missing file.

### `metadata.json` and legacy metadata

`.beads/metadata.json` is loaded by `internal/configfile/configfile.go`. Its
known fields are `database`, `backend`, `graph_mode`, `graph_scope_url`,
`graph_authority_id`, `graph_workspace`, `graph_schema_version`, `graph_ready`,
`deletions_retention_days`, `dolt_mode`, `dolt_server_host`,
`dolt_server_port`, `dolt_server_socket`, `dolt_server_user`, `dolt_database`,
`dolt_server_tls`, `dolt_data_dir`, `dolt_remotesapi_port`,
`dolt_team_server`, deprecated `postgres_dsn`, `postgres_schema`, `mysql_dsn`,
`mysql_database`, deprecated `sqlite_path`, `project_id`,
`global_dolt_database`, `global_project_id`, `stale_closed_issues_days`, and
`last_bd_version`.

Carry project/store identity, graph binding, backend/mode, database name,
retention/version provenance, and the selected non-secret connection shape as
namespaced source metadata. Do not import connection passwords, DSNs containing
credentials, TLS private material, or environment-resolved secrets. The loader
falls back from missing `metadata.json` to legacy `.beads/config.json`, writes
`metadata.json`, and removes the legacy file; the converter must inspect both
paths before declaring the source complete and record which migration path was
used.

### Hooks and external files

Inventory and digest files under `.beads/hooks/`, `.beads-hooks/`,
`.git/hooks/`, and configured external hook managers (pre-commit,
`core.hooksPath`, simple-git-hooks, IDE/agent settings). Include path, mode,
digest, manager, and the command strings in the manifest when available. The
files are not executed by conversion. Script hooks are deleted as a product
surface and Plug listeners replace them (P29b); preserve only a manifest
attachment for audit. A hook that mutates data during the freeze is a refusal
condition.

### Routes and formulas

`.beads/routes.jsonl` is a prefix-to-path routing source separate from the SQL
`routes` table. Parse every line, preserve source order and digest, and map
one prefix to one Plug project under P76. Conflicting paths, duplicate prefixes,
or a route that cannot be opened are refusals. There is no Plug core runtime
for Beads cross-store routing; the parsed mapping remains converter config and
archive evidence.

Every `.beads/formulas/` file, configured formula search path, and formula
reference reachable from metadata/config is an input. Formula and molecule
semantics move to the workflow plugin (P48/P48b): carry valid definitions and
active run references into plugin records, retain unknown/inactive definitions
as `beads-archive`, and never pretend a core bead is a workflow run. The
converter also inventories optional `.beads/issues.jsonl` and
`.beads/interactions.jsonl` exports as cross-checks; neither outranks the
transactional tables, versions, or journal.

### Memories and journal controls

`config` rows with the `kv.memory.` prefix are persistent memories. Export each
as a Plug `memory` record with its user key, content, source digest, and
redaction metadata (P85), and apply the configured `memories.budget-chars` as a
source policy only; do not truncate content silently. Journal activation and
retention are YAML-only controls. Read `bd_events_seq.next_seq`,
`events_journal_autoprune` in `local_metadata`, and any retained-row/time
floors to report the exact journal window. A missing prefix below the recorded
head becomes `history_floor`/`beads-archive`, not an apparently complete empty
history (P6/P45).

## Dolt-specific state

Dolt state is evidence about what the SQL tables mean; Dolt commits are not
Plug history. The converter captures a read-only inventory of the following
for each database and machine:

| Source | Census and disposition |
|---|---|
| `dolt_ignore` | Verify the canonical patterns `__temp__%`, `bd_events_journal`, `bd_events_seq`, `ignored_schema_migrations`, `leases`, `local_metadata`, `repo_mtimes`, `wisp_%`, `wisps`, plus gated `events` after main v62. Record stored patterns and `ignored` flags. Missing or conflicting patterns require repair/rehearsal before export. |
| `dolt_nonlocal_tables` | Capture table, target ref, and options. It describes old clone-local topology and is manifest evidence only; Plug storage owns topology (P49/P70), so it is not restored. |
| Working set/status/diff | Capture `dolt_status`, staged/unstaged rows, schema changes, and content digests. A dirty or unexplained working set, including ignored-plane residue, refuses conversion. A quiesced, explicitly accounted local write may be included in the export digest. |
| Commit graph and history | Capture HEAD, branch, tags, remotes, remote refs, commit depth, flatten/GC/squash state, and whether version history is enabled. Use `issue_versions`, snapshots, events, and journal rows where available. Dolt commits themselves are not imported as A6 changesets; missing/flattened history produces a declared floor or archive. |
| Branches/remotes/federation | Capture names, ancestry, merge bases, ahead/behind counts, and non-secret URLs. Unmerged remotes, unresolved merge state, or conflict intent is a refusal. `sync`, `vc`, `branch`, and federation semantics are dropped under D10. |
| Conflict and schema state | Inspect data/constraint/schema conflict tables, merge/rebase state, foreign-key/check violations, and migration cursor/content hashes. Any unresolved conflict, unknown schema version, or failed constraint validation refuses export. |
| History maintenance | Record compaction, flattening, garbage-collection, squash windows, pruned journal floors, and backups that may have removed old states. Do not claim complete history where the source cannot prove it (P20/P45). |
| Repository and server files | For embedded stores, digest the complete `.beads/dolt` or configured `dolt_data_dir` (and any `.beads/embeddeddolt` layout used by the build), lock/quiescence evidence, and machine-local ignored rows. For server stores, record host/port/socket/database/TLS mode from metadata and connect read-only to the exact database; capture the server-side working set plus the machine that owns ignored rows. Never copy credentials. |

A Dolt ref or commit may be retained as a provenance pointer, but it is not a
Plug `revision`, `changeset`, or cursor. Plug restore creates a new incarnation
and continues revisions from the empty target (P44/A6).

## Embedded and server behavior

The SQL schema and the main/ignored cursor pair are the same in embedded and
server modes. The ownership and freeze evidence differ:

* **Embedded:** Dolt is opened in-process and is single-process-only. Stop all
  writers, acquire the database/file lock, copy the configured data directory
  read-only, and read the ignored tables from that same clone. A copy made
  without the lock is not a rehearsal input. `repo_mtimes`, leases, journals,
  and wisps are the local machine's plane even if their tables are absent from
  the tracked commit.
* **Server/direct SQL:** open the named database through the configured
  server/socket, put the server read-only or stop writers, and capture its
  working-set/status/commit evidence. The server-side ignored tables are still
  included in the paired schema census; a client machine's unrelated `.beads`
  directory is not silently combined with the server database.
* **Proxied/team server:** record the proxy/project identity and authorization
  boundary, require an operator-provided read-only export, and refuse a local
  fallback that would create a fresh empty store. `dolt_team_server` is source
  metadata; Plug's storage plugin owns its own active/standby lease and health
  contract (P49/P70/P71).

The converter's output must be byte/digest stable for the same frozen source in
both modes. It compares IDs, counters, edges, ready/blocked sets, selected
versions, row counts, and content digests; a mode-specific difference is a
conversion failure until explained in the manifest.

## Mapping rules and deliberate drops

The following rules are binding and make the absence of a direct table mapping
reviewable:

| Beads surface | Plug result | Decision |
|---|---|---|
| Issues, labels, comments, dependencies, custom status/type data | Beads become `bead`, label, `comment`, `edge`, and ontology/project-overlay records. Every graph item is a bead; `issue` is one type. | D14, P2, P10, P22, P41 |
| Main and wisp counters, prefixes, imported addresses | Project allocator state and source metadata; seed from stored and observed maxima, retain prefix and ID mode, and test collisions/nonces. | A1, P76 |
| Issue/compaction snapshots and `issue_versions` | A6 JSON Patch versions with periodic full bases; raw bytes are blobs when needed. | P30, P73, P45 |
| Events, journal, provenance, interactions | A6 changeset/audit evidence where exact; otherwise read-only `beads-archive` with a history floor and attribution flag. | P6, P45 |
| `kv.memory.*` | Core `memory` records returned by `agent.brief`; key/content are preserved. | P85 |
| Active wisp/workflow rows and formula definitions | Workflow plugin records/runs, including active agent/role/message distinctions; no core molecule/formula commands. | P48, P48b, P45 |
| Unmapped inactive wisp rows | Redactable, read-only `beads-archive` by default; explicit row-level drop only with digest/reason. | r1-F3 census rule, P45 |
| Leases, presence/session/checkpoint state, row locks, ready/blocked views, repository mtimes | Exclude from dump and rebuild from records/driver; active leases must be resolved at freeze. | A6, P49, P70, P71 |
| Script hooks | Digest/path manifest only; Plug listeners replace execution. | P29b |
| Formula/molecule runtime in core | Workflow plugin input; no legacy command/API compatibility. | P48/P48b, P46/P47 |
| `sync`, `vc`, `branch`, federation, Dolt merge semantics | Drop as product behavior; retain non-secret refs/ancestry as provenance if useful. | D10 |
| Trackers | Export/one-shot-import destination plugin state, never two-way core synchronization. | P17 |
| HTTP v0, CLI text/exit envelopes, old public Go API, `bd` translator | Drop at cutover; behavior parity is recorded at the JSON-RPC projection level. | P35, P46, P47, P82 |
| Upstream usage metrics | Record local opt-in setting and source digest; do not import endpoint traffic or IDs. | P38a |

No row is discarded merely because its table is ignored by Dolt. The manifest
must show `carried`, `archive`, `manifest/rebuild`, or `drop:<decision>` for
every row class and every external source.

## Complete migration ledger

The table matrix above names the source migrations for each final object. This
ledger keeps every numbered file in the paired series accounted for, including
no-op markers and repair-only steps:

* **Main 0001–0018:** create issues, dependencies, labels, comments, events,
  config, metadata, counters, snapshots, compaction snapshots, repo mtimes,
  routes, issue counter, interactions, federation peers, defaults, and the
  initial ready/blocked views.
* **Main 0019–0036:** seed ignored wisp patterns; create wisps and auxiliaries;
  add wisp indexes; add `no_history`, custom status/types, started timestamps;
  move local metadata/repo mtimes to ignored; migrate local keys; add the wisp
  event index; drop main cursor `applied_at`; add `wisp_type`/`spec_id`; move
  infrastructure rows; clean old auto-push metadata.
* **Main 0037–0047:** deterministic UUID re-key shape; drop obsolete hop
  columns and child FK; register nonlocal tables; split typed dependency
  targets; cascade FK repair; remove generated dependency ID; update views;
  add/recompute `is_blocked` across mixed issue/wisp targets.
* **Main 0048–0059:** widen event/content text; deterministic dependency keys;
  remove auxiliary ID defaults; add date/keyset indexes; repair rig rows;
  create and then externalize leases; heal wisp dependency constraints; repair
  null-gate blocking state.
* **Main 0060–0072:** add storage class and no-op content-derived ID marker;
  move events to the ignored plane; create provenance and journal tables;
  widen ignored comments; add journal actor; create versioned-bead tables and
  per-record revisions; add attribution/restriction/precision fields;
  add participation generation; create epoch-minted allocator addresses.
* **Ignored 0001–0008:** materialize local wisps/auxiliary tables and repo
  state; split wisp targets; add wisp FKs and generated target/index shape;
  add/recompute wisp blocking; widen wisp event values.
* **Ignored 0009–0018:** record auxiliary re-key markers; remove wisp ID
  defaults; clean orphan counters; materialize leases; add wisp row lock and
  comment keyset index; repair null-gate state; add lease node and wisp defer
  index; record the second re-key marker.
* **Ignored 0019–0028:** materialize events; add wisp storage class and large
  text; materialize/repair journal and actor; record dependency re-key marker;
  add wisp revisions and participation generation.

The exact file inventory at the pinned checkout is 72 main `.up.sql` files and
28 ignored `.up.sql` files. A converter implementation should hash every file
and store the pair in its lock/manifest; a changed migration body at an
otherwise familiar version is schema drift and must refuse conversion.

## Refusal conditions and rehearsal evidence

The converter refuses before writing a Plug target when any of these holds:

1. either cursor is above the pinned known version, a migration hash differs,
   the main/ignored pair is missing, or final columns/indexes cannot be
   inspected;
2. the source is not frozen (embedded lock not held, server still writable,
   active lease/claim unresolved, or a concurrent export changes a digest);
3. Dolt has dirty/unaccounted working-set rows, unresolved data/schema/
   constraint conflicts, unmerged remotes, a rebase/merge in progress, or
   missing `dolt_ignore` coverage;
4. duplicate/conflicting prefixes, a route cannot be opened, an active wisp
   row has no classification, or an ontology/project overlay needs a status or
   type mapping that was not supplied (P65);
5. a config, metadata, hook, route, formula, journal, memory, or Dolt input is
   unreadable, malformed, or absent from the input manifest;
6. dependency targets or allocator state are inconsistent without an explicit
   dangling-target/history-floor decision; or
7. a requested complete-history policy cannot be met from recorded versions,
   snapshots, journal evidence, and verified source history.

A nightly rehearsal copies every inventoried embedded and server store
read-only, runs the pinned `bd export --format plug-dump/1`, validates checksums
and the lock, restores into a fresh SQLite store (then Postgres when available),
and compares per-project counts, IDs, prefixes/counters, edges, ready and
blocked sets, cycle decisions, sampled/all available history, memory/content
digests, and every archived/dropped item. It repeats with the same source under
a controlled Go and SQL clock. Three consecutive clean rehearsals, the latest
within seven days, a freeze window with margin, a clean restore comparison, and
a rollback drill are required for G2. The reverse converter is a Beads
head-state dump for rollback and must report Plug-only kinds, unsupported
fields, lost history, blobs, principals, and warnings.

During the switch, stop agents, lock or move embedded stores, make server stores
read-only, export with the pinned build, restore only into the empty Plug target,
compare, point clients at `PLUG_SERVER`, and remove `bd` so stale calls fail
loudly. While rollback remains possible, do not create Plug ontology/workflow
kinds that the reverse path cannot represent. History, audit, and archive data
remain forever except an explicit `history.redact` operation (P45).

## Column definition reference

The matrix names every column; this appendix records the type/null/default
families needed by an exporter that validates the SQL shape. `NULL` below means
no `NOT NULL` clause (the engine default), and an omitted default means no
server default.

`issues` final definition is:

```text
id VARCHAR(255) PK; content_hash VARCHAR(64) NULL;
title VARCHAR(500) NOT NULL; description/design/acceptance_criteria/notes LONGTEXT NOT NULL;
status VARCHAR(32) NOT NULL DEFAULT 'open'; priority INT NOT NULL DEFAULT 2;
issue_type VARCHAR(32) NOT NULL DEFAULT 'task'; assignee VARCHAR(255) NULL;
estimated_minutes INT NULL; created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;
created_by VARCHAR(255) DEFAULT ''; owner VARCHAR(255) DEFAULT '';
updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
closed_at DATETIME NULL; closed_by_session VARCHAR(255) DEFAULT '';
external_ref VARCHAR(255) NULL; spec_id VARCHAR(1024) NULL;
compaction_level INT DEFAULT 0; compacted_at DATETIME NULL;
compacted_at_commit VARCHAR(64) NULL; original_size INT NULL;
sender VARCHAR(255) DEFAULT ''; ephemeral TINYINT(1) DEFAULT 0;
wisp_type VARCHAR(32) DEFAULT ''; pinned TINYINT(1) DEFAULT 0;
is_template TINYINT(1) DEFAULT 0; mol_type VARCHAR(32) DEFAULT '';
work_type VARCHAR(32) DEFAULT 'mutex'; source_system VARCHAR(255) DEFAULT '';
metadata JSON DEFAULT JSON_OBJECT(); source_repo VARCHAR(512) DEFAULT '';
close_reason LONGTEXT DEFAULT ''; event_kind VARCHAR(32) DEFAULT '';
actor VARCHAR(255) DEFAULT ''; target VARCHAR(255) DEFAULT ''; payload TEXT DEFAULT '';
await_type VARCHAR(32) DEFAULT ''; await_id VARCHAR(255) DEFAULT '';
timeout_ns BIGINT DEFAULT 0; waiters TEXT DEFAULT '';
hook_bead VARCHAR(255) DEFAULT ''; role_bead VARCHAR(255) DEFAULT '';
agent_state VARCHAR(32) DEFAULT ''; last_activity DATETIME NULL;
role_type VARCHAR(32) DEFAULT ''; rig VARCHAR(255) DEFAULT '';
due_at DATETIME NULL; defer_until DATETIME NULL;
no_history TINYINT(1) DEFAULT 0; started_at DATETIME NULL;
is_blocked TINYINT(1) NOT NULL DEFAULT 0; row_lock BIGINT NOT NULL DEFAULT 0;
storage_class VARCHAR(16) NULL; current_revision BIGINT NOT NULL DEFAULT 1;
participation_generation BIGINT NULL.
```

`wisps` has the same names and types, except its four large text fields are
`LONGTEXT NOT NULL DEFAULT ''` (the issue table fields have no default), and it
is created on the ignored plane. The same final `no_history`, `started_at`,
`is_blocked`, `row_lock`, `storage_class`, `current_revision`, and
`participation_generation` additions apply. `lease_expires_at` and
`heartbeat_at` are historical columns removed by `0055` and are not final
columns.

The remaining table definitions are:

```text
schema_migrations: version INT PK, content_hash CHAR(64).
ignored_schema_migrations: version INT PK, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, content_hash CHAR(64).
dependencies: id CHAR(36) PK; issue_id VARCHAR(255) NOT NULL; typed targets VARCHAR(255) NULL; type VARCHAR(32) NOT NULL DEFAULT 'blocks'; created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP; created_by VARCHAR(255) NOT NULL; metadata JSON DEFAULT JSON_OBJECT(); thread_id VARCHAR(255) DEFAULT ''.
labels: issue_id VARCHAR(255) NOT NULL, label VARCHAR(255) NOT NULL, composite PK.
comments: id CHAR(36) NOT NULL PK, issue_id VARCHAR(255) NOT NULL, author VARCHAR(255) NOT NULL, text LONGTEXT NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP.
events: issue_id VARCHAR(255) NOT NULL, event_type VARCHAR(32) NOT NULL, actor VARCHAR(255) NOT NULL, old_value/new_value LONGTEXT NULL, comment TEXT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, id CHAR(36) NOT NULL PK.
config/metadata: key VARCHAR(255) PK, value TEXT NOT NULL.
child_counters: parent_id VARCHAR(255) PK, last_child INT NOT NULL DEFAULT 0.
issue_snapshots: id CHAR(36) NOT NULL PK, issue_id VARCHAR(255) NOT NULL, snapshot_time DATETIME NOT NULL, compaction_level INT NOT NULL, original_size INT NOT NULL, compressed_size INT NOT NULL, original_content TEXT NOT NULL, archived_events TEXT NULL.
compaction_snapshots: id CHAR(36) NOT NULL PK, issue_id VARCHAR(255) NOT NULL, compaction_level INT NOT NULL, snapshot_json BLOB NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP.
repo_mtimes: repo_path VARCHAR(512) PK, jsonl_path VARCHAR(512) NOT NULL, mtime_ns BIGINT NOT NULL, last_checked DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP.
routes: prefix VARCHAR(32) PK, path VARCHAR(512) NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP.
issue_counter: prefix VARCHAR(255) PK, last_id INT NOT NULL DEFAULT 0.
interactions: id VARCHAR(32) PK, kind VARCHAR(64) NOT NULL, created_at DATETIME NOT NULL, actor VARCHAR(255) NULL, issue_id VARCHAR(255) NULL, model VARCHAR(255) NULL, prompt/response/error TEXT NULL, tool_name VARCHAR(255) NULL, exit_code INT NULL, parent_id VARCHAR(32) NULL, label VARCHAR(64) NULL, reason TEXT NULL, extra JSON NULL.
federation_peers: name VARCHAR(255) PK, remote_url VARCHAR(1024) NOT NULL, username VARCHAR(255) NULL, password_encrypted BLOB NULL, sovereignty VARCHAR(8) DEFAULT '', last_sync DATETIME NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP.
wisp_labels: issue_id VARCHAR(255) NOT NULL, label VARCHAR(255) NOT NULL, composite PK.
wisp_dependencies: id CHAR(36) NOT NULL PK, issue_id VARCHAR(255) NOT NULL, typed targets VARCHAR(255) NULL, type VARCHAR(32) NOT NULL DEFAULT 'blocks', created_at DATETIME DEFAULT CURRENT_TIMESTAMP, created_by VARCHAR(255) DEFAULT '', metadata JSON DEFAULT JSON_OBJECT(), thread_id VARCHAR(255) DEFAULT ''.
wisp_events: id CHAR(36) NOT NULL PK, issue_id VARCHAR(255) NOT NULL, event_type VARCHAR(32) NOT NULL, actor VARCHAR(255) DEFAULT '', old_value/new_value LONGTEXT DEFAULT '', comment TEXT DEFAULT '', created_at DATETIME DEFAULT CURRENT_TIMESTAMP.
wisp_comments: id CHAR(36) NOT NULL PK, issue_id VARCHAR(255) NOT NULL, author VARCHAR(255) DEFAULT '', text LONGTEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP.
wisp_child_counters: parent_id VARCHAR(255) PK, last_child INT NOT NULL DEFAULT 0.
custom_statuses: name VARCHAR(64) PK, category VARCHAR(32) NOT NULL DEFAULT 'unspecified'.
custom_types: name VARCHAR(64) PK.
local_metadata: key VARCHAR(255) PK, value TEXT NOT NULL DEFAULT ''.
provenance_events: id CHAR(36) NOT NULL PK, issue_id VARCHAR(255) NOT NULL, kind VARCHAR(32) NOT NULL, actor/ref/ref_kind VARCHAR(255) NULL (ref_kind VARCHAR(32)), payload TEXT NULL, source VARCHAR(64) NOT NULL, occurred_at DATETIME NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP.
bd_events_journal: seq BIGINT PK, ts DATETIME NOT NULL, op VARCHAR(32) NOT NULL, issue_id VARCHAR(255) NOT NULL, issue_json/dep_json/comment_json LONGTEXT NULL, actor VARCHAR(255) NOT NULL DEFAULT ''.
bd_events_seq: id INT PK, next_seq BIGINT NOT NULL.
issue_versions: issue_id VARCHAR(255) NOT NULL, revision BIGINT NOT NULL, epoch INT NOT NULL, durable_state LONGBLOB NULL, change_actor VARCHAR(255) NULL, change_agent VARCHAR(255) NULL, change_message TEXT NULL, change_at DATETIME(6) NOT NULL, removed_at DATETIME(6) NULL, removed_reason VARCHAR(255) NULL, attribution_status VARCHAR(20) NOT NULL, removed_restriction VARCHAR(30) NULL; composite PK.
store_epoch: id TINYINT(1) NOT NULL DEFAULT 1 PK, epoch INT NOT NULL DEFAULT 1, bumped_at DATETIME NULL, bumped_reason VARCHAR(255) NULL; CHECK id=1.
epoch_minted_addresses: address/store_id/minted_id VARCHAR(255) NOT NULL, minted_epoch INT NOT NULL, minted_at DATETIME NOT NULL, carried_from VARCHAR(255) NULL, gone_at_epoch INT NULL; PK address and the gone-after-mint check.
leases: issue_id VARCHAR(255) PK, holder VARCHAR(255) NOT NULL, granted_at DATETIME NOT NULL, lease_expires_at DATETIME NOT NULL, heartbeat_at DATETIME NOT NULL, granted_node VARCHAR(255) NOT NULL DEFAULT ''.
```

In the compact block, “typed targets” expands to the three explicit columns
`depends_on_issue_id`, `depends_on_wisp_id`, and `depends_on_external`; no
final table retains the old generated `depends_on_id` column. The exporter
must validate the concrete per-column SQL metadata from `INFORMATION_SCHEMA`
(or equivalent embedded-driver introspection) in addition to checking these
names, because a source at the right cursor with an altered definition is still
schema drift.
