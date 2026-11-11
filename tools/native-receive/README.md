# Native archive receiving

Done = a pinned executable imports the exact own-version source and emits
committed, read-back defaults and omissions, proven by public synthetic v66
and genuine native v66/v72 subprocess receipts on all three stores; callable
library APIs or declared counts alone do not count.

This is a trusted local disposable receiving command. The caller supplies the
authenticated external source-store/project binding and the complete explicit
source-plane mapping. Admission is a trusted host input, not a network login.
The command accepts only the existing native full-history import contract.
It never migrates a source or changes the input archive.

Build target: `//tools/native-receive:native-receive`.

A native capture set uses its complete directory as input:

```sh
native-receive --driver sqlite --target /absolute/disposable/receiving.db \
  --capture-set /absolute/capture-set --request /absolute/receiving-request.json \
  --expected-binary-sha256 EXACT_REVIEWED_EXECUTABLE_SHA256
```

The directory contains `capture-set.json`, `head.tar` and the declared `parts`.
Select exactly one input form. For capture sets, the existing trusted request's
`archive_sha256` and source plan bind the exact `capture-set.json` bytes. Supply
all source dispositions, admission and target lifetime explicitly. This route
accepts an external request; its policy, bindings and profile options must be
absent. The archive-only `prepare-request` constructor remains available for
portable archives.

The capture-set route calls the production directory reader and accounted
Bind/Restore functions, verifies every mapped snapshot and complete final
HEAD/current census, then closes and reopens persistent drivers, recovers the
committed receipt and repeats byte readback. Memory reacquires the same store's
lease. A fresh disposable destination is required; SQLite publishes its staged
file only after all checks succeed.

Successful capture receipts include `capture_set_verification` and
`capture_set_measurements`. Measurements contain decoded accounting, peak
retained-batch bytes, ownership after release, and process RSS/HWM in bytes.
The decoded ceiling remains 2,147,483,648 bytes with no CLI override. On refusal,
stderr includes a `plug-native-receiving-refusal/1` JSON object with the phase,
available decoded usage, the complete typed decoded-limit refusal when present,
and process memory. Missing process measurements carry an explicit error.
Process RSS includes the CLI and storage engine and has a separate domain from
controlled decoded accounting; these metrics do not establish full allocation
coverage or permission to switch a live store.

```sh
native-receive --driver sqlite --target /absolute/disposable/receiving.db \
  --archive /absolute/original.tar --request /absolute/receiving-request.json \
  --expected-binary-sha256 EXACT_REVIEWED_EXECUTABLE_SHA256
```

`memory` requires an empty target and verifies in its own process before exit.
`sqlite` requires an absolute database path. `postgres` requires a disposable
DSN. Both persistent stores are closed and reopened before receipt recovery
and table readback. A populated target refuses; use a new disposable target.

The request is one JSON object, without unknown fields or trailing JSON:

```json
{
  "format": "plug-native-receiving-request/1",
  "archive_sha256": "EXACT_ORIGINAL_TAR_SHA256",
  "source_store_id": "EXTERNALLY_BOUND_SOURCE_STORE",
  "projects": ["EXTERNALLY_BOUND_PROJECT"],
  "admission": {},
  "idempotency_key": "EXPLICIT_EXISTING_CONTRACT_KEY",
  "source_plan": {}
}
```

The abbreviated objects above must be replaced with valid existing
`driver.RestoreImportAdmission` and `driver.SourcePlanePlanV1` JSON. Their Go
field names are the wire names where those existing types have no JSON tag.
The source plan declares every disposition, field commitment, principal,
entity, route, relation, origin, provider, namespace and lifetime explicitly.
No mappings are inferred. Only `Target.StoreID` and `Target.Incarnation` must
be empty: the command fills these from the actual target's Meta. Target head
zero, required-empty, runtime contract and driver lifetime remain explicit.
An empty provider image SHA with zero size requests the existing checked
`BindSourceImport` calculation. A supplied image commitment must match.

Archive SHA, manifest source store, ordered project set and source-plan source
binding must agree before destination creation. The archive reader validates
the exact 66/26 or 72/28 schema/cursor pair. A genuine unavailable-history
boundary carries explicit verified coverage as described below. Arbitrary
gaps, missing present images and contradictory completeness still refuse
before destination creation. The import APIs repeat all admission, history,
mapping, target and transaction guards.

A successful exit emits exactly one `plug-native-receiving-receipt/1` JSON
object on stdout. It contains `binary_sha256`, `request_sha256`,
`archive_sha256`, `driver`, `history_mode: "full"`, the committed bound
`restore_time`, `verification`, and the unchanged `committed_report`.
Failures print a reason to stderr and emit no success receipt. A failure
after publication must be investigated through existing recovery; an absent
stdout receipt alone is not an assertion of rollback.

`verification` counts actual compared current families/rows and original
history images/tables/rows. It records the exact original archive-blob
comparison and `reopen` as `driver-reopened` or `same-process-memory`.
Every current raw table and every original image table is compared after
recovery, including columns, number tokens, SQL NULL, empty strings and row
multiplicity. Only current lease eligibility uses the committed restore
clock; original expired cells stay in the archive and historical evidence.
These raw tables do not invent absent SQL types or schema defaults.

The recovered report's `NativeSchemas` distinguishes seven source-schema
absences from actual `plug_defaults` and `target_omissions`. The existing
receipt recovery recomputes accounting and checks destination state. A source
column with no target slot is counted as an omission, without a made-up
mapping. The public actual fixtures admit the ordinary entity and retain the
other source row as an inert origin under its explicit disposition; the
duplicate observed-tuple guard remains mandatory. Legacy v72 omits this v66
report field. Native immutable images remain separate from unverified
portable history; no portable frames are synthesized.

Allocator accounting reports the exact state selected by the admitted restore
plan, including its source hash policy and counters. Reporting does not select
an allocator or replace the existing imported project binding.

Historical image schema annotations use the optional
`images[].schema_profile` contract `beads-commit-schema/1` described in the
dump-format README. Each original source is checked against its own physical
cursor prefixes and absence facts; those facts never substitute for the
current 66/26 profile file. Recovery checks the original archive bytes and
detached native views, then compares present image tables and verifies zero
rows for absent image families at their pinned revisions. Original old or
reshaped columns remain source evidence. This adds no request, profile-file,
idempotency or receipt field. The existing ordered original-input and policy-v2
bindings cover the exact changed archive hashes through the same invocation.
Fixed-version receipts and constructed annotation controls are separate from
genuine multi-version receiving qualification.

The original public multi-version fixture from producer `072a1a8f`, run
`42759960008`, is retained byte for byte in
`internal/integration/converterroundtrip/testdata/public-multiversion-072a1a8f.tar`.
Its current profile is 66/26; its 20 immutable images include the original
empty and ignore-only initial roots and populated checkpoints at 39, 40, 41,
43, 54, 55, 61, 62, 63, 64 and 66. Receiving controls compare the original
typed sidecar against every input cell, then compare current and all-image
tables after import and recovery across memory, SQLite and PostgreSQL. The
same executable constructs the explicit policy request and performs receiving;
the policy selects opaque metadata for fields without a core slot. These
controls retain the original dropped/reshaped data, verify zero rows for
absent historical families, and independently recount source/imported/expired
rows and opaque fields. Memory has the explicitly reported process lifetime;
persistent drivers are reopened. This fixture does not change the ordered
two-original policy-v2 invocation, profile encoding or idempotency preimage.

The report contains source identities and destination values needed for
trusted verification. A private public-aggregate projection must drop
`NativeSchemas.source` and every `plug_defaults[].value`, and export only
explicitly whitelisted counts/categories. Never upload a private request,
archive, DSN, report or source-plane payload. Source/tree/ELF identities and
the exact invocation are bound by the remote build and acceptance receipts;
public proof does not qualify a later private lifetime by itself.

## Explicit request construction

The same executable supports read-only construction from an explicit host
policy. This validates the complete mapping through `BindSourceImport` with
a temporary new-target identity, and opens no destination:

```sh
native-receive prepare-request --driver sqlite --archive ORIGINAL_TAR \
  --policy EXACT_HOST_POLICY_JSON --expected-binary-sha256 EXACT_ELF_SHA256 \
  > PRIVATE_REQUEST_JSON
native-receive --driver sqlite --target ABSOLUTE_DISPOSABLE_PATH \
  --archive ORIGINAL_TAR --policy EXACT_HOST_POLICY_JSON \
  --request PRIVATE_REQUEST_JSON --expected-binary-sha256 EXACT_ELF_SHA256
```

`plug-native-receiving-policy/1` requires the exact archive SHA, aggregate
source store, ordered projects and ordered original `sources` store/project
tuples from the authenticated capture. It also requires the full host
`admission`, explicit `idempotency_key`, trusted `principal`, and `provider`
declarations. There is no authority or principal default. Provider lifetime
must match the selected driver; logical-image hash and size are empty/zero
for calculation by the existing binder.

The general JSON-policy constructor supports the following explicit semantics:

| Policy field | Required declaration | Result |
| --- | --- | --- |
| `entity_policy` | `durable-bead-identity` | Each admitted durable bead remains an ordinary bead. |
| `tuple_policy` | `copy-observed-exact` | Deduplicate exact observed type/storage/flag tuples; never reinterpret them. |
| `status_policy` | `source-name-identity` | Preserve exact source status names; the existing native catalog validator must accept them. |
| `target_id_policy` | `source-id` | Preserve exact original durable IDs. |
| `route_policy` | `active-first` | Declare the existing source-facing route with its exact durable inverse. |
| `field_mappings` | Explicit `From`/`To` pairs | Only declared existing core slots are mapped. Missing or duplicate source fields refuse. |
| `unmapped_fields` | `origin-only` or `opaque-metadata` | Retain every raw field commitment; the latter also carries each unmodeled field into reserved `beads.<field>` metadata. |
| `other_records` | `inert-origin` | Active and auxiliary rows remain original inert evidence. Dependencies are inert unless the explicit `dependency_policy: exact-typed` declaration selects the existing typed relation adapter. |
| `sidecars` | `retain-captured-origins` | Retain original configuration state, bytes, precedence, nulls and absences. |

For example the public receiving proof explicitly maps `id`, `title`,
`description`, `priority`, `status`, `metadata`, `created_at` to the same core
field and `issue_type` to `type`. This is a chosen public policy, not an
authorization to apply it to a private backup. The host must supply the
authorized policy for each real source; the constructor cannot decide
whether active state or relations should be live. The exact rehearsal policy described below is also supported. Other semantic policies
are typed refusals in this version. A complete manual source plan remains
supported for separately authorized workflow/run/relation mappings.

The constructor derives raw addresses, complete raw-field commitments,
observed-tuple declarations, exact identity status correspondences, entity
routes and captured origins mechanically under the supplied policy. It
does not require the fixture-specific test helper. Source SQL NULL and
empty strings remain distinct. Source type/status/flag values and raw
opaque data are read losslessly. No missing source column is added.

The generated request carries `policy_sha256`. Receiving requires that
exact policy file, reconstructs the request independently and compares
every declaration before destination creation. The success receipt also
carries `policy_sha256`. Requests, policy values, original source bindings,
principals and complete mappings remain private when the source is private.
The constructor interface and public proof do not replace a missing host
policy or authorize a private capture/import lifetime.

## Ordered two-source rehearsal with policy v2

The supported rehearsal uses two separately captured originals and one new
empty destination. Pass the exact read-only policy file with SHA-256
`3afb19b934e69e90b9f8e966d5e4e4d1fab27a47686eacf38543e23d1c7542d9`.
The caller supplies an external binding file of this exact shape:

```json
{
  "format": "plug-native-joined-inputs/1",
  "policy_sha256": "3afb19b934e69e90b9f8e966d5e4e4d1fab27a47686eacf38543e23d1c7542d9",
  "profile_sha256": "SHA256_OF_EXACT_PROFILE_FILE",
  "sources": [
    {"rehearsal_project":"cody-ops","database":"ops","source_store_id":"ORIGINAL_OPS_STORE","source_project":"ops","archive_sha256":"ORIGINAL_OPS_TAR_SHA256"},
    {"rehearsal_project":"signals-v2","database":"sv2","source_store_id":"ORIGINAL_SV2_STORE","source_project":"sv2","archive_sha256":"ORIGINAL_SV2_TAR_SHA256"}
  ]
}
```

The database labels are caller assertions authenticated against this fixed
policy, not database identities discovered from a root hash. Each source
store, single original project, verified history coverage, archive digest and schema
profile must match its original archive. Other source namespaces typed-refuse.
The profile file is the exact `encoding/json.Marshal` encoding of
`dumpformat.NativeSchemaForVersion(66, 26)` plus one LF byte. Field order is
`main_version`, `ignored_version`, `absences`; each absence has `family`,
optional `column`, `introduced_in`, `reason`. Absences follow the pinned
registry order. No whitespace alternatives, other versions or guessed schema
facts are admitted by this rehearsal constructor.

Use the same exact executable and all the same input files for both commands:

```sh
native-receive prepare-request --driver sqlite \
  --archive ORIGINAL_OPS_TAR --archive ORIGINAL_SV2_TAR \
  --policy EXACT_POLICY_V2_MD --bindings EXTERNAL_BINDINGS_JSON \
  --profile EXACT_PROFILE_JSON --expected-binary-sha256 EXACT_ELF_SHA256 \
  > PRIVATE_REQUEST_JSON
native-receive --driver sqlite --target ABSOLUTE_NEW_TARGET \
  --archive ORIGINAL_OPS_TAR --archive ORIGINAL_SV2_TAR \
  --policy EXACT_POLICY_V2_MD --bindings EXTERNAL_BINDINGS_JSON \
  --profile EXACT_PROFILE_JSON --request PRIVATE_REQUEST_JSON \
  --expected-binary-sha256 EXACT_ELF_SHA256
```

The executable checks the union of native issues and wisps IDs across current
rows and every retained image before assembly or destination creation. Any
ID string shared by projects typed-refuses `cross_project_id_collision` with
the number of distinct shared strings. Nothing is rekeyed, merged or dropped.
The later capture owner reports that count first and stops above zero. A
successful receipt records `cross_project_id_collisions: 0`; this is a result
of checking the actual supplied inputs, not an expected prefix count.

Assembly uses the existing strict `dumpformat.MergeArchives`. It copies each
original native member byte for byte to
`history/native-tables/ORIGINAL_PROJECT.json` and retains original source
store/project/commit mappings. The generated assembly identity and
`archive_sha256` describe only this deterministic container. They never
replace either original identity. The request and receipt additionally carry
`original_inputs`, the complete ordered external binding above. No separately
normalized physical image or synthetic portable frames are used.

The idempotency preimage is the UTF-8 `encoding/json.Marshal` encoding of an
ordered struct, with no whitespace and no trailing newline. Field order is:

```json
{"format":"plug-native-rehearsal-idempotency/1","archive_sha256":["OPS_HASH","SV2_HASH"],"bindings":["ORDERED_ORIGINAL_INPUT_OBJECTS"],"profile_sha256":"PROFILE_FILE_HASH","policy_sha256":"POLICY_FILE_HASH"}
```

`bindings` contains the actual two objects from `sources`, not strings. Each
object's field order is `rehearsal_project`, `database`, `source_store_id`,
`source_project`, `archive_sha256`, exactly as shown above. Every digest is
lowercase hexadecimal SHA-256. SHA-256 of that preimage supplies both
`idempotency_key` and the local admission digest. The policy hash also becomes
`policy_sha256` in the request and receipt; the explicit principal's `Source`
binds it in the complete protected source plan. Original file digests and the
profile file digest remain inputs to the key even though assembly has its own
digest.

The policy declares one local restore-only `plug-restore-rehearsal` principal.
Source actors become opaque external references in reserved `beads.<field>`
metadata; none creates a principal or conveys authority. Durable IDs, types,
status names and source categories stay exact. The explicit core mappings are
`id`, `title`, `description`, `priority`, `status`, `metadata`, `created_at`
to the same slot and `issue_type` to `type`. Every other captured column gets
an explicit `opaque-metadata` rule targeting `beads.<column>`. Source SQL NULL,
empty text, JSON text and exact number tokens remain distinct. Post-v66
absences never become invented raw fields. Exact typed dependencies use the
existing checked relation and opaque-kind adapter; unsupported endpoints
refuse. Active/auxiliary rows and retained origins remain inert recorded
state. The plan starts no runs and invokes no plugin, hook, gate or renewal.

A restore session starts at receiving entry, has a two-hour ceiling, and may
be shortened with `--session-timeout`. Policy preparation opens no store.
SQLite requires an absent path and uses a sibling disposable staging store
through commit, close, reopen and verification. Only the fully verified closed
file is atomically linked to the absent final path. Cancellation, expiry or
failure removes staging. PostgreSQL requires an absent `plug_postgres` schema
in a caller-owned disposable database; an unsuccessful session drops that
owned schema. The caller must exclusively own that disposable destination.
Memory has only process state. Cleanup failures are errors, never success
receipts. Publication is complete only when session finish succeeds; the
procedure owner deletes successful disposable stores after collecting counts.

The returned committed report is recovered from destination state and compared
to the original publication report before success. In addition to raw current
and history readback, `verification.opaque_metadata_fields` counts actual
opaque destination values compared to their source fields. For v66 each
`NativeSchemas[].opaque_metadata_fields` counts initialized metadata slots
under that source's explicit plan. Default and omission counts retain their
separate existing dispositions. The new request/receipt fields contain source
bindings and may expose private data. Keep stdout, stderr and all intermediate
files private; public output is an explicit counts/categories whitelist only.
An authored constructor or public fixture does not enable the later backup
lifetime without its existing qualified executable and reviewed procedure.

## Verified history through an unavailable boundary

Done = both real stores import verified history through their unavailable
boundaries plus actual events, proven by the retained public boundary and
multiversion reviews and the separately authorized policy-v2 actual terminal;
synthetic transport controls or a public fixture alone do not count.
Scope = the existing reader, archive, importer and final caller flow; live
switching and production are outside this unit.

The optional `NativeHistory.HistoryCoverage` field is encoded as
`history_coverage` within each original `history/native-tables.json` member.
Complete originals omit it. Its `NativeHistoryCoverage` type contains:

| JSON field | Go type / required value |
| --- | --- |
| `format` | string, `beads-native-history-coverage/1` |
| `status` | string, `verified-to-boundary` |
| `verified_commit_count` | uint64, exactly `len(commits) == len(images) >= 1` |
| `boundaries` | nonempty `[]NativeHistoryBoundary`, sorted uniquely by `commit_id` |
| `events_attachment` | `NativeEventsAttachment` |

Every boundary has `commit_id` string, sorted unique `child_commit_ids`
strings, positive `commit_ordinal_from_head` uint64, optional
`commit_schema_version` *uint64, `reason` string, and optional `sql_dolt_codes`
[]uint16. Reasons are closed to `ghost_parent`, `missing_root_chunk`,
`missing_table_chunk`, `unreadable_schema`, and `other`. SQL codes are sorted,
unique and nonzero, and omitted when absent. A missing schema stays omitted;
the current schema is never substituted. Unknown or null fields refuse.
The exact producer contract also refuses any schema claim for a `ghost_parent`.
The source contract is pinned to producer
`4d0282cef9523d2fbf3de776f05e63a8ccd7b90c`, tree
`271aa745f21d6aec33b97948a4ae86ba67826afa`, API SHA-256
`5b3a68f18990afbe6faddb8c9eda456a8ca810c435fcbef78f88733a270c3fbe`.
This source binding is separate from the original runtime artifacts and
retained public reviews required to qualify a genuine boundary archive.

Every external parent edge must match exactly one boundary and its actual
child census. Boundary IDs must be absent from the verified commit census.
The ordinal is reverse deterministic parent-before-child DFS order, extended
with each boundary ID as a terminal leaf, sorting parents as for the native
commit census. HEAD is 1; a linear N-image history has boundary ordinal N+1.
Multiple DAG frontiers remain distinct. All reachable verified commits and
their complete physical images must be supplied; readable HEAD is mandatory.
`complete` remains false and `gaps` remains empty. A boundary does not admit
a missing present image, corrupt ancestry, falsely complete history or an
arbitrary named gap.

The events attachment has required `member` string exactly
`history/source.json`, `sha256` lowercase hexadecimal SHA-256 of that exact
original member, and `rows` uint64. Its portable range remains unverified
with reason `native-history-boundary`. Events must equal the actual current
native events and archive event records with multiplicity preserved. Current
events are not assigned to immutable commits. Historical events tables stay
in each actual image where present.

Assembly retains that original member byte for byte at
`history/source-attachments/ORIGINAL_PROJECT.json`, alongside the unchanged
original native member at `history/native-tables/ORIGINAL_PROJECT.json`.
The coverage object still names its original `history/source.json`; scoped
validation resolves the retained source attachment. It verifies its original
hash and events against the merged source scope. Missing, substituted and
orphan attachments refuse. Nested assembly preserves both original members.

The committed report adds optional `NativeHistories` []`NativeHistoryReport`.
Each entry contains `source` (original store/project), `complete` bool,
`verified_commit_count` uint64 and optional `history_coverage` carrying the
exact descriptor. If any source has a boundary, reports include every source
so that independent completeness/counts remain visible. If every source is
complete, the new field is omitted and original report bytes stay unchanged.
The existing accounting record carries the same entries at
`source_histories`. Existing `NativeSchemas` defaults and omissions and
`NativeFamilies` source/imported/expired row counts retain their meanings.

The internal driver payload and `HistoryImportEvidence` add optional
`nativeBoundaries`. Each commitment contains `sourceStoreId`, `project`,
`verifiedCommitCount` and `boundarySha256` (SHA-256 of the descriptor's Go
`encoding/json.Marshal` bytes). Imported sequences describe only actual
verified images. There is no fabricated prefix count, baseline, unknown
commit image or event assignment. Publication and recovery recompute these
commitments and the report from the retained original archive. Complete
originals omit this optional driver field.

The request, explicit policy, per-project namespace and idempotency contracts
remain unchanged. The same executable verifies original archive bytes,
current families, all supplied images, pinned absent tables and recovered
reports. Memory recovery remains in the same process; persistent stores
close and reopen. These archive/report fields contain source IDs and belong
in private evidence. The final caller owns the separate safe count/code
projection and authorization for the later two-store unit.

The public receiving tests also retain nine byte-exact producer outputs from
`e25cf419c9bc516fa3dd91394ee93a846255d7ba`, original run `42786069732`, in
`testdata/public-history-boundary-e25cf419.tar`. They include the genuine
shallow archive, physical sidecar, donor/clone facts and safe output, plus the
separate complete multiversion archive, sidecar, facts and historical refusal.
The original generic `public` project and `public_commit_history` database
remain literal. The observed shallow input has 13 verified images, a ghost
frontier at ordinal 14 and two current events; the complete input has 20
images. Every original member and the enclosing container are hash pinned.

The tests compare the actual donor genesis and retained parent edges,
own-image schema/checkpoints, current and historical typed cells, exact
coverage/events attachments and committed reports. Both originals pass
through library and same-executable receiving on memory, SQLite and
PostgreSQL. Persistent destinations close and reopen; memory uses the same
process. Current events remain separate from pre0062 immutable events.
Earlier complete producer originals and synthetic two-source policy tests
remain separate controls. Source, producer execution and receiving results
remain separate evidence; these public inputs grant no private two-store,
positive lease, live switch or production qualification.
