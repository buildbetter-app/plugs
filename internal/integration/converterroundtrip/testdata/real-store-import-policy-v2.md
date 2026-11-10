# Real-store rehearsal import policy v2 (orchestrator, 2026-11-10; supersedes v1 f886df72)

Scope: importing the restored Beads Dolt-native backups of cody-ops and signals-v2 (schema v66, own-version capture) into ONE initially empty, disposable Plug store, for rehearsal. This policy authorizes nothing live and no switch. Private data rules are unchanged: work only on disposable copies, report counts only, and delete everything after use.

Every rule below cites the binding ruling it comes from in /tmp/plug-swarm/escalations.md or engdocs/design/plug/decisions-needed.md. If a case isn't covered here, the importer refuses with a typed error that names the family, field and row count. It never infers a rule.

## 1. Admission, principal, provider, lifetime
- **Principal:** one local restore principal, `plug-restore-rehearsal`, created only inside the disposable store. Its sole capability is the admin restore surface (`admin.restore.begin/chunk/finish`, decision #14). It holds no user, project-admin or plugin capability.
- **Provider:** the disposable store's built-in local provider. There's no external auth provider and no network identity, and no BuildBetter or host credential is used.
- **Admission:** the caller supplies the source bindings exactly, never inferred:
  - the ordered projects `[cody-ops → database ops, signals-v2 → database sv2]`;
  - each project's restored-backup archive SHA-256;
  - the pinned v66 profile (2f/564 mechanical derivation from f791).
- **Idempotency key:** SHA-256 over (both archive hashes, the ordered bindings, the profile hash, the policy file hash).
- **Lifetime:** one restore session, from begin to finish, with a 2-hour ceiling. On expiry or cancellation the session aborts atomically, leaves the store empty, and the store is deleted.

## 2. Durable records: identity, type, status, fields
- **Identity:** native issue IDs are kept exactly, with no rekeying (ID style A: restored IDs are untouched). Every record carries its source project in its provenance (two-project provenance ruling). Plug record keys are unique across the whole store by design (A3), so an ID string that occurs in both projects is never rekeyed, merged or dropped: the import refuses it with a typed `cross_project_id_collision` error and reports the count. The projects use distinct ID prefixes (ops, sv2), so this count is expected to be 0; a count above 0 comes to the orchestrator.
- **Type:** the native issue_type name is kept exactly. Each project's custom types come from its own declarations (the custom_types table and YAML). Compatible declarations are unioned; a conflict refuses with a typed error.
- **Status:** the native status name is kept exactly, with its Beads category from custom_statuses (custom-status ruling). A status with no category, or conflicting categories, refuses with a typed error. No fixture ontology is substituted.
- **Fields:** every native field with a Plug slot maps to it. Every native field without a Plug slot is carried byte-for-byte as opaque `beads.<field>` metadata, never interpreted, and written back on reverse (orchestrator call 2026-10-31 05:45Z and 06:55Z, e.g. agent_state, await_id). Integers stay exact (json.Number). Post-v66 columns are recorded as absent-by-source-schema; Plug-side defaults are counted, not invented (v66 ruling and correction).
- **Dependencies:** native dependency types are kept as-is. Custom or opaque dependency kinds are stored as opaque kinds (B07 producer v3 rule). Parent-child links in the same store record the parent; a link to an outside tracker stays a plain link (2026-11-04 parent-child call).

## 3. Active and auxiliary families: all inert on import
- Workflows, molecules, formulas, wisps and their children, gates and routes are imported as recorded data only. **Nothing goes live:** no workflow run starts, no route or hook fires, no gate is evaluated by the importer, and no plugin executes.
- Leases are imported only if unexpired at import time, as recorded state. The importer never renews them, and they expire by their own deadlines. The count is reported (unversioned-family ruling).
- Unversioned-by-design families (events, events journal/seq, wisps*, leases, repo_mtimes, local_metadata, ignored_schema_migrations) follow the unversioned-family ruling: working-set rows are kept exactly, labeled, with no synthesized history. Machine-local bookkeeping may be omitted, with counts.
- Retained origins are inert provenance only. They confer no authority.

## 4. Principals, entities, routes, relations, origins, namespaces
- **Actors:** actor names in native records (created_by, assignee, actor, owner and the like) are imported as **opaque external actor references**. No Plug principal is created for them, and they grant nothing. Mapping Beads actors to real Plug principals is a switch-day decision for Nikhil, out of scope here.
- **Entities and namespaces:** each source project becomes one Plug project with its own namespace (`ops`, `sv2`) and its own ontology (types, statuses, categories).
- **Relations and origins:** cross-project references are kept as recorded, never rekeyed or deduplicated. Origins carry source project, database, commit and row provenance.
- **Unsupported:** anything outside sections 1–4 refuses with a typed error and a count.

## 5. Reporting
Counts only. Per project, the report lists each family's rows imported, rows omitted and why, absent-by-schema counts, Plug default counts, opaque-metadata field counts, refusal counts by code, leases imported and leases expired. No values or IDs appear.
