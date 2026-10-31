# `bd` command inventory for Plug (P80)

Status: research inventory, updated for accepted decisions #10 and #11.
This records command dispositions, not implemented behavior or parity. It
reads the command tree at behavior commit
`9d6fff760b7a314b476539198504d77ce0959656` in
[`cmd/bd/testdata/command_tree/tree.golden`](https://github.com/buildbetter-app/plugs/blob/9d6fff760b7a314b476539198504d77ce0959656/cmd/bd/testdata/command_tree/tree.golden)
and the Plug design sections A1, A7, and A8. The tree contains **307 paths**
(the root plus 306 descendants); A1's “306 command paths” sentence predates the
current golden. The inventory and census ratchet count all **307 paths**; “306”
is the descendants-only denominator used by the older design sentence.

Plug is a clean-room rewrite. A `bd` name is not a compatibility promise:
P46 says there is no `bd` translator, and P1 says that behavior is rebuilt
behind the new contract. A path can therefore be kept under a different
surface, folded into a method that already owns the behavior, or dropped. The
inventory uses these labels:

- **Keep** — a `plug` CLI command calls the named JSON-RPC method (or the
  explicitly named CLI server/runtime surface where the design has no RPC
  equivalent).
- **Fold** — the behavior remains, but under another method or namespace; the
  old path is not a second compatibility alias.
- **Drop** — there is no v1 Plug surface. A `[widely used]` marker means the
  repository's docs, skills, examples, or tests show the command as an active
  workflow. Decision #10 accepts the family batch and the named visible
  commands; these markers record source use rather than pending approvals.

A namespace-only row (for example `bd dep`) follows the fate of its children;
it is listed so every golden path is accounted for. Method names below are the
names in A1/A3/A6/A7. `x.<alias>.*` is the P26b prefix for plugin methods.
Where the design deliberately leaves a plugin's leaf names open, the inventory
uses the declared prefix rather than guessing a public method name.

## Contract anchors

| Beads behavior | Plug surface | Decision |
|---|---|---|
| Beads, dependencies, ready work, claims, leases | `bead.*`, `tx.apply`, views | D14; A1 |
| Memories | `memory.*`, included in `agent.brief` | P85 |
| Custom statuses/types and schema | `ontology.*` | P10, P22, A3 |
| Gates | `gate.*`; GitHub/timer resolvers are plugins | P15, A3 |
| Formulas, molecules, wisps | `x.<workflow-alias>.*` workflow plugin | P48, P48b, P45 |
| Change journal and watches | `event.list`, `event.subscribe`, `view.watch`, `bead.watch` | P6, A1, A7 |
| History and compaction | `history.*`, `admin.compact` | P30, P45 |
| Dumps and restores | `admin.dump`, `admin.restore.*` | P19, P20, P44, P5c |
| Tracker integrations | `sync.status`, `sync.run`, `sync.import`; `sync_destination` plugins | P17, P77, P78 |
| Storage health/topology | storage plugin, `admin.health`, `plug server` | P7, P49, P70, P71 |
| HTTP v0 | removed; JSON-RPC transports are the replacement | P35 |
| Dolt/federation/branch/version-control sync | no v1 equivalent | D10 |
| `bd` binary and public Beads Go API | `plug`/`plg`; only `pluginapi` and `sdk/go` are public | P37, P46, P47 |

The generic `sync.*` methods are a new tracker surface. They do **not** revive
Beads' `bd sync` federation loop, which is a D10 drop.

## Core graph, fields, and views

These are the commands most likely to be in parity-set cards. They are kept or
folded into the core contract rather than copied as Beads aliases.

### Creation, mutation, leases, and terminal states

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd create` | **Keep** — `plug create` → `bead.create` | D14; `[widely used]` |
| `bd create-form` | **Fold** — client form submits `bead.create` | D14; the form is a UI choice |
| `bd q` | **Fold** — minimal-output `bead.create` | D14; quick-capture is a client mode |
| `bd todo`, `bd todo add` | **Fold** — `bead.create` with the task type | D14; convenience wrapper |
| `bd todo done` | **Fold** — `bead.close` | D14; convenience wrapper |
| `bd todo list` | **Fold** — `view.run` with the task type | P31b; convenience wrapper |
| `bd update` | **Keep** — `plug update` → `bead.update` | D14; `[widely used]` |
| `bd edit` | **Fold** — field-scoped `bead.update` with `expect` | P31a; editor is a client concern |
| `bd assign` | **Fold** — `bead.update` on the assignee field | D14; no separate mutation kind |
| `bd priority` | **Fold** — `bead.update` on the typed priority field | D25; P23a |
| `bd set-state`, `bd state`, `bd state list` | **Fold** — typed fields through `bead.update`/`bead.get`; operational state is not a second graph primitive | P23a; no state-label side channel |
| `bd close` | **Keep** — `plug close` → `bead.close` | A1 slot verb; `[widely used]` |
| `bd reopen` | **Keep** — `plug reopen` → `bead.reopen` | A1 slot verb |
| `bd defer` | **Keep** — `plug defer` → `bead.defer` | A1 slot verb |
| `bd undefer` | **Fold** — `bead.reopen` or the ontology's unstarted slot | A3 slots; no compatibility alias |
| `bd delete` | **Fold** — `tx.apply` with a `DeleteOp` (a future `bead.delete` CLI may be a named wrapper) | A1's closed `Op` union; deletion is not a Beads storage API |
| `bd unclaim` | **Fold** — `bead.release` | A1; lease release is the single operation |
| `bd reclaim` | **Fold** — system lease reclaim plus `bead.release` | A1/A6; the server reclaims expired leases; no worker-recovery loop in the CLI |
| `bd heartbeat` | **Keep** — `plug heartbeat` → `bead.heartbeat` | A1; no revision is emitted |
| `bd promote` | **Fold** — workflow plugin promotion (`x.<workflow>.run.promote`) followed by normal bead writes | P48/P45; see workflow section |
| `bd note` | **Fold** — `bead.comment` (or `bead.attach` kind `log` when the caller needs evidence) | A1; comments/attachments are the durable record |
| `bd comment` | **Keep** — `plug comment` → `bead.comment` | A1; `[widely used]` in agent examples |
| `bd comments` | **Fold** — `bead.get`/`view.run` with comments selected | A1; read projection |
| `bd comments add` | **Fold** — `bead.comment` | A1; the canonical write path is singular |
| `bd comments list` | **Fold** — `bead.get`/`view.run` | The current command itself says to use `bd comments <issue-id>` |

### Reads, filters, relationships, and graph repair

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd show` | **Keep** — `plug show` → `bead.get` | A1; `[widely used]` |
| `bd info` | **Fold** — `server.hello` plus `admin.health` | A1/A6; database identity is server metadata |
| `bd list` | **Keep** — `plug list` → `view.run` | P31b; `[widely used]` |
| `bd ready` | **Keep** — `plug ready` → `bead.ready` | A1; `[widely used]` |
| `bd blocked` | **Fold** — `view.run` with the blocked predicate | P31b; readiness is a view |
| `bd stale` | **Fold** — `view.run` with a stale-time filter | P31b |
| `bd count` | **Fold** — `view.run` grouped/count result | P31b |
| `bd status` | **Fold** — `view.run` plus `admin.health` | P31b/A6; graph and driver status are separate concerns |
| `bd query`, `bd q` (read form) | **Fold** — `view.run` with the A1 filter/query grammar | A1/A7; `bd q`'s write form is above |
| `bd search` | **Fold** — `view.run`/search view | A7 views; a search index is an implementation detail |
| `bd children` | **Fold** — `view.run` over the direct parent relation | P31b/P50; parent-aware selection is direct-child only |
| `bd graph` | **Fold** — `view.run`/`view.watch` graph projection | A7; the UI draws the result without a CDN |
| `bd graph check` | **Fold** — `ontology.check` and graph invariants | A3/A6; no separate graph checker contract |
| `bd orphans` | **Fold** — `view.run` over references and status | P31b; no special storage table |
| `bd recompute-blocked` | **Fold** — `ontology.check`/admin repair of derived readiness | A3/A6; readiness is maintained transactionally |
| `bd dep` | **Fold** — `bead.link`/`bead.unlink` plus views | D14; namespace follows children |
| `bd dep add` | **Keep** — `plug dep add` → `bead.link` with a dependency kind | A1; dependencies are core |
| `bd dep remove` | **Keep** — `plug dep remove` → `bead.unlink` | A1 |
| `bd dep relate` | **Fold** — `bead.link` with `relates_to` | A1; link kind carries directionality |
| `bd dep unrelate` | **Fold** — `bead.unlink` with `relates_to` | A1 |
| `bd dep list` | **Fold** — `view.run`/`bead.get` link expansion | P31b |
| `bd dep tree` | **Fold** — `view.run` over ancestor/descendant closure | P50; no separate tree store |
| `bd dep cycles` | **Fold** — `ontology.check`/graph invariant report | A3/A6 |
| `bd link` | **Keep** — `plug link` → `bead.link` | A1; `[widely used]` |
| `bd duplicate` | **Fold** — `bead.link` with a duplicate/supersedes kind, plus a close transition if requested | A1 link union; no special duplicate record |
| `bd duplicates`, `bd find-duplicates` | **Fold** — a view/plugin-assisted candidate list, then `bead.link` | P31b and P26b; similarity/AI is optional plugin behavior, not core |
| `bd supersede` | **Fold** — `bead.link` plus `bead.close` | A1 link/slot verbs |
| `bd epic` | **Fold** — `epic` is an ontology type, not a command subsystem | D14/P10 |
| `bd epic status` | **Fold** — parent-aware `view.run` progress: visible-child counts plus “N more you can’t see” | P31b; hidden children have no status and the contract remains implementation-deferred |
| `bd epic close-eligible` | **Fold** — `view.run` followed by guarded `bead.close` | A3 graph guards |
| `bd label` | **Fold** — typed `fields.labels` through `bead.update` | P23a; no label side table in Plug |
| `bd label add`, `bd tag` | **Fold** — `bead.update` labels | P23a; tag is an alias |
| `bd label remove` | **Fold** — `bead.update` labels | P23a |
| `bd label list`, `bd label list-all` | **Fold** — `bead.get`/`view.run` projection | P31b |
| `bd label propagate` | **Fold** — bounded `tx.apply`/`bead.update` batch | P2; no recursive CLI policy |
| `bd label rename` | **Fold** — ontology/data migration plan and `tx.apply` | P10; labels are typed fields |
| `bd rename` | **Drop for immutable IDs**; a title/display edit is `bead.update` | A3; decision #11 explicitly keeps display edits and drops ID rewrites |
| `bd rename-prefix` | **Drop** — no prefix rewrite under immutable global keys | A1 global IDs, A3 immutable keys, P46 |

### Ontology, validation, and maintenance

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd types` | **Keep** — `plug types` → `ontology.describe` | P10/A3 |
| `bd statuses` | **Keep** — `plug statuses` → `ontology.describe` | P10/A3 |
| `bd schema` | **Keep** — `plug schema` → `ontology.schema` (record/OpenRPC schemas are `rpc.describe`) | A1/A3 |
| `bd lint` | **Fold** — `ontology.check` | A3 integrity checks |
| `bd doctor` | **Fold** — `admin.health`, `ontology.check`, and `authz.verify` when installed | A3/A5/A6 explicitly name these replacements; `[widely used]` |
| `bd admin` | **Keep as namespace** — `plug admin` | A6 operational surface |
| `bd admin compact` | **Keep** — `plug admin compact` → `admin.compact` | A8 says compaction is a service verb writing changesets; P45 |
| `bd admin cleanup` | **Drop** — permanent history and redaction rules replace closed-row deletion | P45; `[widely used]` only if a caller is found |
| `bd admin reset` | **Drop** — empty-store restore and operator lifecycle replace destructive reset | P44/P46 |
| `bd compact` | **Fold** — `admin.compact` | P45; storage-specific Dolt squashing is gone |
| `bd gc` | **Fold** — `admin.compact` for semantic compaction; driver maintenance is plugin-owned | P45/P7 |
| `bd prune`, `bd purge` | **Drop** — no row/history deletion outside `history.redact` | P45; destructive cleanup is included in decision #10's accepted batch |
| `bd flatten` | **Drop** — history is kept forever; no Dolt history squash | P45; D10 |
| `bd rules`, `bd rules audit`, `bd rules compact` | **Drop** — editor/orchestrator rule files are outside Plug's graph contract | P1/P46; no A1/A7 method |

## Collaboration, gates, memory, and workflow plugins

### Gates and collaboration

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd gate` | **Keep as namespace** — `plug gate` | A3 gate resource |
| `bd gate create` | **Keep** — `gate.create` | A3; gate beads remain graph nodes |
| `bd gate add-waiter` | **Fold** — `gate.create`/`bead.link` against the waiting bead | A3 gate graph; no waiter side table |
| `bd gate check` | **Fold** — resolver polling and `gate.report` | A3; GitHub/timer resolvers are plugins (P15) |
| `bd gate discover` | **Fold** — resolver-specific `x.<alias>.*` method | P15/P26b; `gh` is not a core command |
| `bd gate list`, `bd gate show` | **Fold** — `view.run`/`bead.get` with gate fields | P31b; A3 gate kind data |
| `bd gate resolve` | **Keep** — `gate.approve` or `gate.report`, depending on gate kind | A3; manual and external resolutions are distinct |
| `bd merge-slot` | **Drop [widely used in CLI/workflow docs]** — serialized merge orchestration is outside the core ontology | P1/PROJECT_CHARTER boundary; no A1/A7 method |
| `bd merge-slot acquire`, `bd merge-slot check`, `bd merge-slot create`, `bd merge-slot release` | **Drop [widely used in CLI/workflow docs]** — same fate as the namespace | P1; a project may implement an equivalent plugin explicitly |
| `bd human` | **Drop** — “human-needed” orchestration is not a core record kind | P1/P46 |
| `bd human dismiss`, `bd human list`, `bd human respond`, `bd human stats` | **Drop** — no corresponding A1/A7 method | P1/P46 |
| `bd prime` | **Fold** — `agent.brief` (and the `plug` CLI briefing) | P85/A1; **[widely used]** in every agent integration; this is a rewrite, not a `bd` alias |
| `bd memories` | **Keep** — `plug memory` → `memory.list`/search | P85; memories are core records |
| `bd remember` | **Keep** — `memory.remember` (exact verb may be normalized to `memory.put`) | P85; `[widely used]` in agent guidance |
| `bd recall` | **Keep** — `memory.recall`/`memory.get` | P85 |
| `bd forget` | **Keep** — `memory.forget`/`memory.delete` | P85 |
| `bd todo` | **Fold** — task-typed beads and views | D14; listed with core creation above |
| `bd preflight` | **Drop** — PR readiness belongs to the hosting/orchestration layer | P1/PROJECT_CHARTER |
| `bd ship` | **Drop** — publishing cross-project capabilities is orchestration policy | P1/PROJECT_CHARTER |

`agent.brief` is the intended replacement for the context payload that
`bd prime` injects. It returns identity, claims, ready work, recent changes,
ontology summary, and conventions; the old hook command must not be kept just
to preserve a prompt string.

### Formula, molecule, wisp, and swarm paths

The workflow plugin is the one deliberate exception to the “no orchestrator
policy in core” boundary. P48 makes formulas and molecules a plugin; P48b
says the workflow plugin is compiled in from this repository and has no Beads
formula files. The plugin owns its exact leaf methods under an alias such as
`x.workflow.*`; the names below are proposed aliases for the mapping card, not
new core methods.

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd formula` | **Keep as plugin namespace** — `plug workflow` / `x.workflow.formula.*` | P48/P48b |
| `bd formula list`, `bd formula show` | **Keep** — `x.workflow.formula.list/get` | P48; plugin-owned |
| `bd formula schema` | **Keep** — `x.workflow.formula.schema` | P48; plugin-owned |
| `bd formula convert` | **Fold** — source ingestion in `x.workflow.formula.import` or the build tool | P48b; no Beads JSON/TOML compatibility promise |
| `bd cook` | **Keep** — `x.workflow.template.compile` | P48; formula compilation is plugin behavior |
| `bd mol` | **Keep as plugin namespace** — `x.workflow.run.*` | P48/P45 |
| `bd mol bond` | **Keep** — `x.workflow.run.link` | P48; maps workflow dependencies to `bead.link` where needed |
| `bd mol burn` | **Keep** — `x.workflow.run.delete`/`x.workflow.ephemeral.delete` | P48/P45; history remains in the change log |
| `bd mol current`, `bd mol last-activity` | **Keep** — `x.workflow.run.get` | P48 |
| `bd mol distill` | **Keep** — `x.workflow.formula.distill` | P48 |
| `bd mol pour` | **Keep** — `x.workflow.run.instantiate` | P48 |
| `bd mol progress`, `bd mol ready`, `bd mol show`, `bd mol stale` | **Keep** — `x.workflow.run.*` views | P48/P31b |
| `bd mol seed` | **Keep** — `x.workflow.formula.validate`/`x.workflow.run.seed` | P48; plugin-owned |
| `bd mol squash` | **Keep** — `x.workflow.run.summarize` | P48/P45; writes a changeset rather than erasing history |
| `bd mol wisp` | **Keep as plugin namespace** — `x.workflow.ephemeral.*` | P48/P45 |
| `bd mol wisp create` | **Keep** — `x.workflow.ephemeral.create` | P48 |
| `bd mol wisp gc` | **Keep** — `x.workflow.ephemeral.gc` | P48/P45; retention is plugin policy and still logged |
| `bd mol wisp list` | **Keep** — `x.workflow.ephemeral.list` | P48 |
| `bd swarm` | **Drop [widely used in CLI/orchestration docs]** — swarm orchestration and roles have no Plug core counterpart | P1/PROJECT_CHARTER; the named visible command is dropped by decision #10 |
| `bd swarm create`, `bd swarm list`, `bd swarm status`, `bd swarm validate` | **Drop [widely used in CLI/orchestration docs]** — same fate as the namespace | P1/PROJECT_CHARTER |

A workflow plugin may expose a new, explicitly reviewed replacement for a
swarm behavior, but the old `bd swarm` paths are not compatibility names.

## History, events, audit, and provenance

### Change log and history

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd history` | **Keep as namespace** — `plug history` | A6 |
| `bd versions` | **Keep** — `history.versions` | A6/P30 |
| `bd diff` | **Keep** — `history.diff` | A6/P30; branch comparison is removed, so refs are revisions/records |
| `bd restore` | **Fold** — `history.get` plus `history.revert` for compacted content | P30/P45; `admin.restore.*` is reserved for dump restore |
| `bd events` | **Keep as namespace** — `plug events` | P6/A1/A7 |
| `bd events tail` | **Keep** — `event.list {after}` | A1/A7 |
| `bd events export` | **Fold** — paged `event.list` or `admin.dump` changesets | P6/P20; no unbounded one-shot response |
| `bd events prune` | **Fold** — operator retention/driver policy; cursors fail with `cursor_expired` below the floor | A1/A6; not row deletion from the graph |
| `bd audit` | **Fold** — read-only audit view over the changeset/audit log | A5/A6; authentication denials and mutations are server-stamped |
| `bd audit record`, `bd audit label` | **Drop as caller-asserted writes**; callers use normal writes and the server records principal/mutators | A5/A6; no public way to forge audit entries |
| `bd provenance` | **Fold** — changeset attribution, `event.list`, and evidence attachments | P6/P45; provenance is not a second journal |
| `bd provenance by-ref`, `bd provenance log` | **Fold** — `history.get`/`event.list` filtered by ref | A6 |
| `bd provenance record` | **Fold** — `bead.attach` with `kind: log` or a normal change that the server attributes | A1/P6; no caller-asserted provenance row |

### Export, import, and one-shot movement

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd export` | **Keep** — `plug export` → paged `admin.dump` (`plug-dump/1`) | P5c/P19/P20; the Beads-side exporter is run by `bd` on the `beads` branch |
| `bd import` | **Fold** — `admin.restore.*` for a `plug-dump/1`; `sync.import` for a tracker one-shot import | P44/P77; no general JSONL append that bypasses restore semantics |
| `bd migrate issues`, `bd migrate-issues` | **Fold** — export/restore into an empty store | P5c/P44; IDs and prefixes are carried by the archive |
| `bd backup restore` | **Fold** — `admin.restore.*` after digest and empty-store checks | P44/P75 |

The same `plug-dump/1` shape is the only Beads ingress. A tracker import is a
separate `sync.import` changeset and may mint new IDs under P77; it is not a
restore alias.

## Storage, server lifecycle, and removed version-control surfaces

### Admin, backup, Dolt, and SQL

Plug has drivers and a storage plugin, not a Dolt command family. `Store.Health`
feeds `admin.health`; `Store.Migrate` is an operator lifecycle action;
replication, failover, backup, and the lease belong to the storage plugin
(P49/P70). These rows preserve the useful operational intent without exposing
Dolt internals.

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd backup` | **Fold as namespace** — storage-plugin operations and `plug admin` | P7/P49/P70 |
| `bd backup init`, `bd backup remove` | **Drop** — backend-specific destination setup lives in the storage plugin configuration | P7/P70 |
| `bd backup status` | **Fold** — `admin.health` | A6 `Store.Health` |
| `bd backup sync` | **Drop** — managed HA/Postgres or driver backup policy, not a client push | P49/P70; `[widely used]` only if an operational caller is found |
| `bd backup restore` | **Fold** — `admin.restore.*` | P44/P75 |
| `bd dolt` | **Drop as namespace** — no Dolt in the new tree | P7/P44; D10 |
| `bd dolt clean-databases`, `bd dolt killall` | **Drop** — process cleanup is server/operator lifecycle | P7/P36 |
| `bd dolt commit` | **Drop** — every Plug write commits a changeset under the lease | P2/P6/P71 |
| `bd dolt pull`, `bd dolt push` | **Drop [widely used]** — one shared server and no push/pull | D10; repository integrations document both commands |
| `bd dolt remote`, `bd dolt remote add`, `bd dolt remote list`, `bd dolt remote remove`, `bd dolt remote reset-data` | **Drop** — no Dolt remotes | D10/P7 |
| `bd dolt set`, `bd dolt show` | **Fold** — server/storage configuration and `admin.health` | P7/P70 |
| `bd dolt start`, `bd dolt stop`, `bd dolt test`, `bd dolt status` | **Fold** — `plug server` lifecycle and `admin.health` | P36/P70; server is the runtime surface |
| `bd flatten` | **Drop** — history is append-only except `history.redact` | P45 |
| `bd sql` | **Drop** — raw driver SQL leaks the storage boundary | P7; A6 driver contract |
| `bd conflicts` | **Drop as namespace** — no live Dolt merge state | D10/P7 |
| `bd conflicts list`, `bd conflicts resolve`, `bd conflicts show` | **Drop** — conflict handling is not a Plug resource | D10/P7 |

### Branch, federation, VC, and repository/worktree commands

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd branch` | **Drop [widely used where documented]** — one shared server, no branch/push/pull model | D10; P18 reserves `Ref.Branch` but v1 writes branch 0 |
| `bd federation` | **Drop [widely used in sync docs]** — no peer-to-peer workspace federation | D10 |
| `bd federation add-peer`, `bd federation list-peers`, `bd federation remove-peer`, `bd federation status`, `bd federation sync` | **Drop** — topology is storage-plugin/HA, not a client federation graph | D10/P49/P70 |
| `bd repo` | **Drop as namespace** — multi-repository sync is an orchestration concern | D10/P1 |
| `bd repo add`, `bd repo list`, `bd repo remove`, `bd repo sync` | **Drop** — no multi-clone sync configuration | D10/P1 |
| `bd vc` | **Drop [widely used in source-control docs] as namespace** — Plug does not own source-control history | D10 |
| `bd vc commit`, `bd vc merge`, `bd vc status` | **Drop** — source-control commands are outside the server contract | D10/P1 |
| `bd sync` | **Drop [widely used]** — the old pull/check/repair/push federation loop has no successor | D10; many integration docs teach it |
| `bd migrate sync` | **Drop [widely used where setup docs use it]** — no `sync.branch` workflow | D10 |

### Migrations and local/server modes

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd migrate` | **Keep as namespace** — `plug admin migrate` for driver/ontology lifecycle | A3/A6; no Beads migration compatibility |
| `bd migrate schema` | **Keep** — driver `Store.Migrate`, exposed as an admin operation | A6 |
| `bd migrate from-proxied-server-to-server`, `bd migrate from-proxied-server-to-shared-server`, `bd migrate from-server-to-proxied-server`, `bd migrate from-shared-server-to-proxied-server` | **Drop** — dbproxy and the mode matrix are deleted | P36/P47; D10 |
| `bd migrate hooks` | **Drop** — script hooks are deleted; listeners replace them | P29b |
| `bd migrate legacy-sqlite` | **Drop in Plug** — legacy reading is a Beads-side converter concern | P5c/P47 |
| `bd init` | **Fold** — `plug server` creates/configures a store; client config is file-based | P36/P44; `[widely used]` |
| `bd bootstrap` | **Fold** — boot tier, `plug server`, and `admin.health` | P62/P70 |
| `bd init-safety` | **Drop** — no destructive `bd init` semantics or Dolt directory to guard | P36/P44 |
| `bd serve` | **Fold** — `plug server` serves JSON-RPC/WS/stdio and `/mcp` | P35/P36; the HTTP v0 API itself is deleted; `[widely used]` |
| `bd connect` | **Fold** — client `--server`, `PLUG_SERVER`, and `.plug/config.yaml` selection | A1 local/server mode; no `bd`-style attachment command |
| `bd ping` | **Fold** — `server.hello` and `admin.health` | A1/A6 |
| `bd context`, `bd where` | **Fold** — `server.hello` plus client config | A1; no repository-local database identity |
| `bd setup` | **Drop** — the Beads editor setup and generated hooks are replaced by the Plug agent plugin and published docs | P46/P29b; `[widely used]` in integration docs |
| `bd quickstart` | **Fold** — Plug docs and `server.hello`/`agent.brief` | P37/P85; CLI help is not an RPC method |
| `bd help` | **Fold** — `plug help` and generated OpenRPC/docs | P11/P37 |
| `bd completion`, `bd completion bash`, `bd completion fish`, `bd completion powershell`, `bd completion zsh` | **Keep as CLI-only** — generated `plug` completions; no RPC method | P37; client ergonomics |
| `bd version` | **Keep as CLI-only** — `plug version`; `server.hello` carries server version | P37/A1 |
| `bd upgrade`, `bd upgrade ack`, `bd upgrade review`, `bd upgrade status` | **Drop** — self-upgrade policy is packaging/operator tooling, not a server method | P1/P37 |

The root path `bd` itself is **Folded** to the `plug` binary (with `plg` as an
alias), per P37. The CLI-only exceptions above are intentionally called out:
they do not create a second JSON-RPC contract.

## Tracker integrations

A7's `sync_destination` contract is export-only for ongoing sync, with a
one-shot `sync.import`. The provider-specific `bd <provider>` trees are not
kept as aliases. A provider can expose additional methods under
`x.<alias>.*` (P26b), but the parity path is the generic `sync.*` surface.

| Golden family and paths | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd ado`, `bd ado status`, `bd ado projects` | **Fold** — `sync.status` plus plugin configuration/discovery | P17/P26b |
| `bd ado sync`, `bd ado push` | **Fold** — `sync.run` through the ADO destination plugin | P17/P78 |
| `bd ado pull` | **Fold** — one-shot `sync.import` | P17/P77 |
| `bd github`, `bd github status`, `bd github repos` | **Fold** — `sync.status` and destination configuration | P17/P26b |
| `bd github sync`, `bd github push` | **Fold** — `sync.run` | P17/P78 |
| `bd github pull` | **Fold** — `sync.import` | P17/P77 |
| `bd gitlab`, `bd gitlab status`, `bd gitlab projects` | **Fold** — `sync.status` and plugin configuration | P17/P26b |
| `bd gitlab sync`, `bd gitlab push` | **Fold** — `sync.run` | P17/P78 |
| `bd gitlab pull` | **Fold** — `sync.import` | P17/P77 |
| `bd jira`, `bd jira status` | **Fold** — `sync.status` and plugin configuration | P17/P26b |
| `bd jira sync`, `bd jira push` | **Fold** — `sync.run` | P17/P78 |
| `bd jira pull` | **Fold** — `sync.import` | P17/P77 |
| `bd linear`, `bd linear status`, `bd linear teams` | **Fold** — `sync.status` and plugin configuration | P17/P26b |
| `bd linear sync`, `bd linear push` | **Fold** — `sync.run` | P17/P78 |
| `bd linear pull` | **Fold** — `sync.import` | P17/P77 |
| `bd notion`, `bd notion status` | **Fold** — `sync.status` and plugin configuration | P17/P26b |
| `bd notion sync`, `bd notion push` | **Fold** — `sync.run` | P17/P78 |
| `bd notion pull` | **Fold** — `sync.import` | P17/P77 |
| `bd notion connect`, `bd notion init` | **Fold** — plugin plan/configuration and destination setup | P17/P26b; no provider-specific core API |

## Configuration, telemetry, and hidden integration paths

### Configuration and plugin administration

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd config` | **Fold as namespace** — `plug config` for client files and `plugin.plan/apply` for versioned server/plugin configuration | P26a; A1/A2 |
| `bd config get`, `bd config list`, `bd config show` | **Fold** — `server.hello`, `rpc.describe`, `plugin.plan`, or operator config depending on the key | A1/A2; no untyped global config RPC |
| `bd config set`, `bd config set-many`, `bd config unset` | **Fold** — compare-and-set plugin/ontology plans or local `.plug/config.yaml` | P26a; D16 applies to protected changes |
| `bd config apply` | **Fold** — `plugin.apply`/`ontology.apply` | P26a/D16; protected records cannot be changed by `tx.apply` |
| `bd config drift` | **Fold** — `ontology.check`, `plugin.plan`, and `admin.health` | A2/A3/A6 integrity checks |
| `bd config validate` | **Fold** — plan-time plugin/ontology validation and `rpc.describe` schema validation | A2/A3 |
| `bd kv` | **Drop as a public generic store** — plugin `Host.State` is private per instance; memories have their own core methods | A2/P85 |
| `bd kv get`, `bd kv list` | **Fold only for memory keys** to `memory.get/list`; other key spaces are dropped | A2/P85; no global untyped KV contract |
| `bd kv set`, `bd kv clear` | **Drop** — use typed core records or a plugin's private state | A2/P23a |
| `bd metrics` | **Fold** — opt-in server telemetry configuration | P38a/P81 |
| `bd metrics on`, `bd metrics off` | **Fold** — operator config for usage export; no command sends data by default | P38a; `DO_NOT_TRACK=1` remains a hard off switch |
| `bd metrics example` | **Drop** — no sample event payload includes IDs or text | P38a/P81 |
| `bd send-metrics` | **Drop** — hidden worker is replaced by the server's OpenTelemetry exporter | P38a; hidden internal path |
| `bd sql` | **Drop** — see storage table | P7 |
| `bd schema` | **Keep** as `ontology.schema`/`rpc.describe` — see ontology table | A1/A3 |

### Hooks and hidden process entry points

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd hooks` | **Drop [widely used in integration docs] as namespace** — script hooks are deleted | P29b |
| `bd hooks install`, `bd hooks list`, `bd hooks run`, `bd hooks uninstall` | **Drop [widely used in integration docs]** — listeners/subscribers replace hooks; no Git hook installer in core | P29b |
| `bd codex-hook` | **Drop [widely used]** — the Plug agent plugin uses `agent.brief` and event/session methods | P29b/P46; present in `plugins/beads/README.md` and Codex integration docs |
| `bd cursor-hook` | **Drop** — same listener/plugin replacement | P29b/P46 |
| `bd db-proxy-child` | **Drop** — `dbproxy` and the proxied-server twins are deleted | P47/P36 |

## Immutable identity interpretation (decision #11)

Decision #11 resolves the original tension in the design:

- A3 says ontology keys are immutable and A1 says IDs are global.
- A7's “Today”/codebase paragraph nevertheless says to build `rename` as a
  view client, while today's `bd rename` rewrites IDs and references.

Title and display edits use `bead.update`; ID and prefix rewrites are dropped.
Thus `bd rename-prefix` and the ID-rewrite form of `bd rename` have a decided
fate. They do not require a new product decision before cards can record that
deviation.

## Remaining namespaces and local helpers

These paths are short but still part of the golden inventory:

| Golden path | Fate and Plug method | Decision / use note |
|---|---|---|
| `bd batch` | **Keep** — `plug batch` → `tx.apply` with one atomic `ChangeSet` | A1/P2; the CLI is a convenience for the transaction method |
| `bd migrate-personal` | **Drop** — moving personal planning issues between repositories is orchestration policy | P1/D10 |
| `bd onboard` | **Fold** — published Plug docs and `agent.brief` | P37/P85; no server method |
| `bd worktree` | **Drop [widely used in integration docs] as namespace** — source-control worktree management is outside Plug | P1/D10 |
| `bd worktree create`, `bd worktree info`, `bd worktree list`, `bd worktree remove` | **Drop [widely used in integration docs]** — use Git directly; Plug has no worktree state | P1/D10 |

## Accepted command-family drops (decision #10)

The table records the accepted family batch from P80 and decision #10. It excludes
provider-specific paths and aliases that are folded into generic Plug methods.
The named visible commands are included in that acceptance. Source-use notes
remain context for caller migration; they do not reopen the drop decision.

| Dropped family | Golden paths covered | Decision anchor | Source-use context |
|---|---|---|---|
| Dolt and merge-conflict plumbing | `bd dolt`; `bd dolt clean-databases`, `killall`, `commit`, `pull`, `push`, all `remote` paths; `bd conflicts` and all children; `bd flatten` | D10, P7, P45, P47, #10 | The named visible `bd dolt push`/`pull` are dropped; `dolt status/show/start/stop/test/set` fold to server/admin methods |
| Federation and branch/version-control sync | `bd branch`; `bd federation` and all children; `bd sync`; `bd repo` and all children; `bd vc` and all children; `bd migrate sync` | D10, P18, #10 | The named visible `bd sync` and `bd branch` are dropped |
| Destructive row/working-set cleanup | `bd admin cleanup`, `bd admin reset`, `bd prune`, `bd purge`, `bd init-safety` | P44, P45, P46, #10 | Any active cleanup caller needs migration; the drop is decided |
| Generic untyped KV and raw SQL | `bd kv`, `bd kv set`, `bd kv clear` (non-memory `get/list` uses), `bd sql` | A2, P7, P23a | memory-key `get/list` folds to `memory.*` |
| Script hooks and hidden agent process hooks | `bd hooks` and all children; `bd codex-hook`; `bd cursor-hook`; `bd db-proxy-child`; `bd send-metrics` | P29b, P38a, P36, P47, #10 | The named visible hooks and `bd codex-hook`/`bd prime` integration are dropped |
| Orchestration-only helpers | `bd merge-slot` and all children; `bd human` and all children; `bd preflight`; `bd ship`; `bd rules` and all children; `bd mail`; `bd setup`; `bd upgrade` and all children; `bd swarm` and all children | P1, P46, PROJECT_CHARTER, #10 | The named visible setup, merge-slot, swarm, and worktree callers need migration |
| Beads-only identity rewrite | `bd rename-prefix`; the ID-rewrite form of `bd rename` | A1, A3, P46, #11 | Title/display edits remain `bead.update`; ID/prefix rewrites are dropped |
| Legacy migration mode | the four `bd migrate from-*-server-to-*` paths; `bd migrate hooks`; `bd migrate legacy-sqlite` | P5c, P29b, P36, P47 | `bd migrate issues`/`bd migrate-issues` fold to export/restore; converter work stays on the `beads` branch |

The accepted named visible-command set from decision #10 is:
`bd sync`, `bd dolt push`, `bd dolt pull`, `bd prime`, `bd codex-hook`,
`bd setup`/hooks, `bd merge-slot`, `bd swarm`, `bd branch`, and `bd worktree`
where repository docs show them. “Visible” means documented
or wired in the pinned Beads checkout, not measured production frequency. A
runtime argv inventory remains caller-migration evidence before cutover, not
a prerequisite for publishing this catalog or a new drop approval.
