# ADR-2026-10-27: Plug design decisions

- **Status:** accepted
- **Date:** 2026-10-27 through 2026-10-28
- **Scope:** repository identity, clean-room boundaries, contracts, storage,
  authorization, plugins, UI, operations, and publication gates.

This is the scrubbed decision record for the Plug design. It records the
accepted outcomes that the retained design and implementation plan rely on.
Source recordings, transcripts, private working notes, and personal data are
intentionally omitted. The design document remains the detailed specification;
this ADR is the stable provenance target for its decision references.

## Decisions

| ID | Decision |
|---|---|
| D1 | UI is remote only; no UI served by a local CLI |
| D2 | Authorization is a plugin interface, kept current through resource-change events; realized by P4, P42 and D18 as plugin-declared rules whose facts core derives in the write's transaction, with listeners left for plugins that mirror external engines (P28d) |
| D3 | Gates, types and statuses customizable at Linear's level |
| D4 | Behavior enforced per type through event subscribers |
| D5 | Plugins have their own configuration |
| D6 | Two plugin modes: compiled in, or GitHub repositories with a manifest at the root |
| D7 | A synchronous subscriber can change the write |
| D8 | Storage plugins treat versioning as first-class |
| D9 | Name `plug`, short form `plg`, not `pg` |
| D10 | One source of truth, the shared server; no push or pull; the whole state can be dumped |
| D11 | The API is JSON-RPC |
| D12 | Clean agent interfaces and clean layer boundaries |
| D13 | Big choices are surfaced to Nikhil as decisions |
| D14 | Every graph item is a bead; `issue` is one type |
| D15 | The repository stays `buildbetter-app/plugs` |
| D16 | Agents plan plugin changes; only a `user` principal applies them (was P27), enforced only with an authentication provider (D18) |
| D17 | Authorization exists only as optional plugins; core has no authorizer, and with none installed every request is allowed |
| D18 | Core keeps no authorization of principals: D16 stays as platform safety; plugin sandboxing stays in the host; rules are plugin data core evaluates; the authentication plugin issues scoped agent tokens and the chain enforces them |
| D19 | Policy enforcement is completely optional |
| D20 | Policy enforcement requires authentication; `plugin.plan` refuses an authorization plugin without a provider |
| D21 | An anonymous principal mode, the default with no authentication provider |
| D22 | Anonymous mode serves only localhost, with no override |
| D23 | Authorization plugins form a chain; how links combine is AZ5 |
| D24 | Project overlay definitions overwrite base definitions with the same key |
| D25 | Fixed in v1: bead templates, role-scoped workflow permissions, priorities, dependency types |
| D26 | An old version is readable only by someone who can read the bead now |
| D27 | Local mode runs the server, and its plugins, in the CLI process |
| D28 | Partial failures are `result.warnings` from the error catalog, never an `error` beside a `result` |
| D29 | Two kinds of custom UI: same-origin `ui` plugins on session cookies, and apps on other origins that sign users in with OIDC PKCE as public clients and send short-lived bearer tokens, only from an operator-configured CORS allowlist (was P32e (b)) |
| D30 | Plugin UI bundles are prebuilt and committed at the pinned commit, served as-is after a digest check; no JavaScript build on the server in v1 (was P32c (a)) |
| D31 | Declarative panels, columns and buttons stay beside iframes; iframes are for custom logic and drawing (was P32d (a)) |
| P1 | (b) Clean-room rewrite, not strangler slices (plan) |
| P2 | (a) One typed `ChangeSet`, a union over all change kinds |
| P3 | Pre subscribers (before the transaction, may change the write) and post subscribers (after commit, before the answer, cannot) |
| P4 | (a) Authorization data derived in the write's transaction, when a plugin is installed |
| P5 | (a) Hard fork |
| P6 | (a) A Plug change log in every driver |
| P7 | (a) Record store plus `sqlkit` |
| P8 | (a) Mode 2 runs in-process on wazero |
| P9 | (a) Mutators in dataflow order, one owner per scalar field |
| P10 | (a) Ontology stored on the server, versioned, migration-like |
| P11 | (a) Go registry generating OpenRPC |
| P12 | (a) Many projects in one graph |
| P13 | (a) Manifest sets maximum reads and owns; the ontology narrows them per type |
| P14 | (a) Authorization plugins compiled in only, for v1 |
| P15 | Storage and secret providers mode 1 only; `authz` mode 1 in v1; wasm sync subscribers by grant |
| P16 | The narrowed spike validates the core-hosted Cedar path; if it fails, authorization is revisited toward simple Linear-style team permissions (AZ1, AZ2) |
| P17 | (b) Trackers: export plus one-shot import |
| P18 | (a) Reserve `Ref.Branch`, build nothing |
| P19 | Secrets in dumps as references, ciphertext by flag |
| P20 | Full-history dumps, head-only by flag |
| P21 | (a) Violations on ontology apply need directives; atomic below 5,000 rows |
| P22 | (a) Import accepts Plug's semantics |
| P23a | (a) Typed `fields` apart from `metadata` |
| P23b | (a) A fixed guard catalog; anything conditional is a validate subscriber |
| P24 | (a) Fail closed by default; per-binding fail-open and audited `admin.binding.suspend` stay |
| P25 | (a) Mode-2 sync phases by operator grant |
| P26a | (a) Plugin set and lock as versioned store objects |
| P26b | (a) Plugin methods only under `x.<alias>.*` |
| P28a | (b) Zero or more chained authorization plugins |
| P28b | (a) Unreadable blockers shown as a count |
| P28c | (a) OIDC through revocable core sessions |
| P28d | Not a core decision: external engines are the authorization plugin's business |
| P29a | (a) Lagging consumers stall: listeners at 7 days, projections at 30 |
| P29b | (a) Script hooks deleted; listeners replace them |
| P30 | (b) Versions stored as diffs |
| P31a | (a) Field-scoped compare-and-set for text |
| P31b | (a) Views are a core resource |
| P32a | (c) Plugin UI in sandboxed iframes with a narrow message bridge |
| P32b | (a) UI embedded in the binary |
| P35 | (a) HTTP v0 deleted at cutover |
| P36 | (a) Driver lock plus `plug server --local` |
| P37 | (a) Docs print `plug`; `plg` is an alias |
| P38a | (b) Usage metrics opt-in, exported to an operator's OpenTelemetry collector |
| P38b | MIT license |
| P38c | (a) This repository's history, after `git filter-repo` removes personal data |
| P39 | (a) Transcripts never committed; public docs link ADRs; Mintlify site |
| P40 | Public at 0.1.0, after the gate |
| P41 | (a) One organization per deployment; server base ontology plus per-project overlays |
| P42 | (a) Declarative rules that core runs in the transaction |
| P43 | (b) Mode-2 plugins always built from source at a pinned commit, Go only at first |
| P44 | (a) Restore only into an empty store |
| P45 | (a) History and audit kept forever; only `history.redact` removes; ephemeral runs unversioned |
| P46 | (a) No `bd` compatibility |
| P47 | (a) `beads.go` and `backend/` deleted, with the rest of today's public Go API; the new `pluginapi` and `sdk/go` are the only public Go |
| P48 | (b) Formulas and molecules become a plugin |
| P49 | (a) One active server plus a standby; the storage plugin owns replication, failover, backup and the lease |
| P50 | (a) Transactional ancestor closure plus a semi-join |
| AZ2 | The narrowed spike validates P16; if it fails, revisit authorization altogether, leaning toward simple Linear-style team permissions rather than the relationship-schema fallback |
| AZ5 | Any link may deny or append context; only the end of the chain allows; no decision denies; Cedar rules from every plugin are one merged set |
| AZ6 | An erroring forbid denies; an erroring permit is skipped, alerted and reported by `authz.verify` |
| AZ8 | Role-scoped workflow permissions stay out of the ontology in v1 |
| AZ9 | The `permission` guard stays in the fixed catalog and asks the chain with a `transition` action |
| AZ10 | Adopt the authorization data contract before choosing a policy language |
| AZ1 | (v) core hosts an inert Cedar evaluator and plugins supply policies as data, if the spike passes. If it fails, authorization is revisited altogether, leaning toward simple Linear-style team permissions; this replaces the (iii) fallback |
| AZ3 | (a) `cedar-go`, pinned, with Plug's subset compiler; revisit Rust partial evaluation once it is stable |
| AZ4 | (a) read rules that cannot compile to a list filter are refused at plan |
| AZ7 | (a) grants are plugin-declared records, seen by policies as principal attributes |
| P51 | If schema plugins are used, they merge into one schema at the end of the chain; a failed authorization spike reopens the authorization approach under AZ1 |
| P52 | (a) no egress in `authz.*` calls; external engines are mirrored by listeners |
| P53 | (a) policy-data-only authorization plugins are mode 1 only in v1 (P14) |
| P54 | (b) an explicit code-link order in the plugin set, defaulting to apply order and shown by `plugin.plan` |
| P55 | (a) a fixed authorizer view in v1; `authz_visible` ontology fields only after the spike prices their indexes |
| P56 | (c) a closure rewrite above the limit is refused with `closure_too_large`, or runs as one locked migration-class write by explicit admin call |
| P57 | (a) no `authz.explain {asOf}`; history uses current permissions (D26) |
| P58 | (a) the authentication plugin refuses scoped agent tokens (`authz_not_installed`) when no authorization plugin is installed |
| P59 | (b) no break-glass `authz.rootPrincipals`; recovery goes through P62's bootstrap admin or offline repair |
| P60 | (b) in anonymous mode an unverified, client-asserted `_meta.agent` label scopes leases and is recorded beside `anonymous` |
| P61 | (b) `authn-local` (the OS user) survives as an optional provider over in-process, stdio and Unix-socket peer credentials only |
| P62 | (b) the boot tier may hold one authentication provider and a bootstrap admin, so a network-facing server starts authenticated |
| P63 | (b) with authentication, `ontology.apply`, `override_guard`, `admin.binding.suspend`, `event.subscription.skip` and `admin.restore.*` need a `user` principal; widens D18 |
| P64 | (a) `ontology.plan` refuses manual gates needing more than one approval when no authentication provider is configured |
| P65 | Overlays only archive base elements; moving a bead between projects whose overlays differ needs `map_status` and `map_type` directives. Plain Beads data must be ingestible into Plug |
| P66 | (a) a failed post subscriber returns the `Commit` with `post_subscriber_failed`, is dead-lettered and counts toward its breaker; post subscribers make no graph writes; egress within 1 s |
| P67 | Toolchain in the server image; artifacts stored by digest, so the standby never builds; reproducibility checked in CI; old artifacts kept until a plan rebuilds; a per-principal build quota |
| P68 | (a) local mode takes mode-2 plugins only on Linux at first, then (b) builds in a Linux container, then (c) unsandboxed builds by explicit flag later; not "never (c)" |
| P69 | (b) end users see every warning; the catalog's `audience` field does not filter warnings |
| P70 | (b) in v1 the storage plugin delegates replication, failover and backup to a managed HA Postgres; `Store.Lease` and `Store.Health` are its topology methods; narrows P49's wording |
| P71 | (a) `Store.Lease` with fenced `Commit`; a server that loses its lease stops reads and writes; TTL 10 s, renewal 3 s |
| P72 | (b) RPO and RTO documented per storage plugin; Postgres meets 5 minutes and 1 hour |
| P73 | JSON Patch diffs with a full base every 32 versions, confirmed after benchmarking `history.query {asOf}` |
| P74 | The stored ready order lags by at most 1 minute |
| P75 | Restore loads the dumped plugin artifacts after checking each against the lock's digest; the rebuild from the pinned commit runs as an asynchronous check that alerts on mismatch |
| P76 | (b) the export merges several Beads stores, one project per prefix; an offline merge before an empty-store restore is within P44 |
| P77 | (a) tracker `sync.import` may run into any project, minting new IDs, head state only; P44 governs restores, not writes that mint new IDs |
| P78 | (a) a remote edit to a synced field is overwritten on the next push and counted as drift |
| P83 | (a) the last code link may return `Allow`; from an earlier link it is an error (`authz_link_allow_not_last`) |
| P84 | (b) check-only code links are allowed in lists when the operator accepts a capped post-filter that fails with `authz_postfilter_cap`, never a short page |
| P85 | (a) memories are a core record kind with `memory.*` methods, returned in `agent.brief` |
| P5b | (a) the new tree sits at the root of `main`; Beads is read from `beads-base` or the `beads` branch |
| P38d | (a) `git filter-repo` runs early, while the repository is private, before tags exist |
| P39b | (a) design docs are committed without transcripts, citing ADRs |
| P48b | (a) the workflow plugin is compiled in from this repository, without Beads formula files |
| P79 | No remote build cache at first; a BuildBetter cache only for cgo or remote execution |
| P80 | A delegate drafts the keep, fold or drop list for `bd`'s command paths, each drop tied to a decision; Nikhil signs off the families to drop in one batch, and only drops of commands seen in use go to him one by one |
| P81 | Usage-metrics fields: method, result code, latency bucket, principal kind, client version; no IDs or text; any OpenTelemetry collector |
| P82 | (a) behavior goldens frozen at `beads-base`, plus recorded fixes from the `beads` branch, each re-record a reviewed PR to `main`, backed by nightly diffs |
| P5c | (b) Beads stores are read by `bd export --format plug-dump/1` on the `beads` branch, then restored; shipped standalone, the same exporter lets outside Beads users ingest plain Beads data. No Dolt in the new tree |
| D32 | "A human" for D16 means an interactive login session, not an API key, when authenticated, and a human-presence check, such as typing the plan's digest, in anonymous mode |
| D33 | The nine interpretations are confirmed |

## Open questions

1. The authorization spike outcome decides AZ1, AZ3, AZ4, and AZ7. The
   performance follow-up and its fallback remain described in the design
   documents until that evidence is complete.

## Provenance policy

Design and plan documents cite this ADR for accepted decisions. Future
revisions add a new ADR or an explicit superseding entry; they do not add
transcripts or private source material to the repository.
