# B14 public waits-for metadata contract

Done = named bead.link and tx.apply persist typed waits-for metadata and use it in real readiness/close guards with normal endpoint authorization, CAS, replay, atomic refusal and history, proven on memory, file SQLite and mandatory disposable PostgreSQL; a codec-only pass or private seeded graph does not count.

All product writes run through the default public JSON-RPC registry, service and Writer. Assertions read public bead.get/history.get and readiness; administrative store metadata, history-entry and receipt reads verify no-write/single-commit invariants. The original metadata tests privately seed no ontology, bead, status, graph or receipt. SQL reopen checks preserve metadata and history. PostgreSQL setup is mandatory and fails rather than skips.

The existing metadata atom implements the authorized optional metadata {gate?: all-children|any-children, also_blocks?: boolean}. Endpoint to remains the spawner; absent metadata preserves the legacy fallback. Explicit metadata follows the pinned9d6 waits-for formula, with terminal slots interpreted by the existing effective ontology. Gate creation/listing and broader B14 gate-field/resolver behavior remain separate obligations. Original PR246 and PR267 evidence is unchanged.

The existing finite CP7 policy intentionally refuses bead.link even with an involved project grant; the HTTP refusal test retains that boundary. Separate public RPC tests install an explicit composition-root Authorizer callback through the normal Writer option, exercise before/after/current-replay stages against the real endpoint snapshot, and prove revocation cannot reuse a receipt. That callback contract does not qualify a broader fixed policy.

## Authenticated consumer profile

Done = view.explain and bead.ready agree for public-created all/any/also_blocks and zero-child cases under the declared ontology and installed host test policy, proven on memory, physical SQLite reopen and disposable PostgreSQL with current-policy replay/refusal and history; anonymous callbacks or refusal-only tests do not establish this profile.

`consumer_host_test.go` installs `b14-consumer-host-test-policy/v1` through the normal Writer Authorizer and core authz.Chain. Real loopback HTTP uses the actual API-key provider, authn admission, JSON-RPC binders, service and Writer; RPC identities are never injected. A finite method list and explicit User/project grants check stored source and endpoint projects before/after writes and against current policy on receipt replay. This test policy is not the production fixed-permission policy and does not expand CP7. Its sequential grant replacements do not qualify concurrent grant fencing, generic list pagination or management.

The host seeds one protected ontology source as declared test configuration, plus a saved read-only view definition. Every bead, parent, status, link, metadata mutation and receipt is produced by authenticated public RPC. The child project's public close slot is `accepted`, absent from the spawner's effective ontology; this exercises per-child ontology resolution. SQL reopen does not reseed content or the protected source. Tests compare the actual stored consumer verdict with bead.ready for empty/mixed/terminal children, terminal-spawner legacy behavior and explicit metadata. They also cover named/typed transaction writes, CAS, one changeset, author history, current-policy revocation, denied source/endpoint/project, atomic refusal, no receipt on refusal and hidden-spawner redaction.

This is consumer and declared host-policy acceptance only. It contains no genuine Beads recording, full-card parity, production fixed-policy link permit, B14 gate-kind/resolver qualification or full S1 qualification.

## Public Plug dump and restore

Done = public-created external waits-for identity and metadata survive an actual Plug dump and public restore across memory, file SQLite and PostgreSQL, including SQL close/reopen and local index/readiness isolation, proven by named source/target cases in the owning focused and whole receipts; generated census archives do not count.

`TestB14PublicPlugDumpRestoreExternalWaitsForAcrossEngines` uses the unchanged mandatory engine harness for all nine source/target pairs. The default local runtime, JSON-RPC client and registry produce named and transactional external links and a genuine local positive with the same target spelling. It captures unmodified `admin.dump` bytes, validates their native Plug scope, and receives them through `admin.restore.begin/chunk/finish` with an explicit host-owned presence verifier. Public get/history.get/ready and inverse-source-link reads check exact opaque UTF-8 IDs, both metadata variants, fail-closed external readiness and preservation of the genuine local index member before and after physical SQL reopen.

This is head-only public Plug producer/receiver acceptance. Native Beads producer/importer admission, full-history parity, gate creation/listing/resolvers, fixed-policy link permission, full B14/FullS1 and hosted delivery remain separate qualifications.
