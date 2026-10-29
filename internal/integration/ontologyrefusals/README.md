# Ordinary-write ontology refusal acceptance

`//internal/integration/ontologyrefusals:ontologyrefusals_test` composes the
canonical default `service.NewOntology(nil)`, production service and Writer,
and the production memory and file-backed SQLite drivers. No permissive
ontology, fake Writer, direct transaction seeding or replacement guard is used.

The same matrix exercises protected status patches (including same-value and
removal attempts), initial-status admission, invalid claim sources, template
and real stored-blocker guards, malformed string/priority/template values,
unknown fields, core envelope shadowing, reserved metadata and invalid slot
batch shapes. A valid update/create/comment prefix followed by an invalid
operation must publish nothing. Ordinary create/update/claim/comment/close,
same-holder claim and repeat-close no-ops, historical reads and a valid batch
are positive controls. SQLite is closed and reopened to verify durability and
continued status protection.

Every refused request compares exact pre/post metadata and head, all records,
all versions, the full changeset log, record and relevant predicate
observations, service get/ready/history views, all successful fixture receipts
and changeset-by-key lookups, and absence of the denied key's receipt. Small
inventories must fit complete pages; partial results fail the tests.

Boundaries: this is in-process service/Writer acceptance, not HTTP or CLI
qualification. `id`, `type`, `project` and other envelope names in `fields`
refuse as unknown typed keys; they are not top-level identity mutation APIs.
Qualified paths refuse, while parented create and project moves explicitly
remain unsupported. Existing Writer slot contexts cover fail/cancel/defer and
reopen; this does not claim those methods are bound by the service or RPC.
The default has no configurable immutable-field flag or plugin-owned field.
No ontology planning/apply, migration, permission-provider, gate, parent/child,
performance, receipt-expiry or lease-expiry parity is asserted. Writer/service
clocks are fixed; SQLite's real storage lease is renewed using its own clock.
