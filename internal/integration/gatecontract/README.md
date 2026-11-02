# B14 public waits-for metadata contract

Done = named bead.link and tx.apply persist typed waits-for metadata and use it in real readiness/close guards with normal endpoint authorization, CAS, replay, atomic refusal and history, proven on memory, file SQLite and mandatory disposable PostgreSQL; a codec-only pass or private seeded graph does not count.

All product writes run through the default public JSON-RPC registry, service and Writer. Assertions read public bead.get/history.get and readiness; administrative store metadata, history-entry and receipt reads verify no-write/single-commit invariants. No test privately seeds an ontology, bead, status, graph or receipt. SQL reopen checks preserve metadata and history. PostgreSQL setup is mandatory and fails rather than skips.

This atom implements the authorized optional metadata {gate?: all-children|any-children, also_blocks?: boolean}. Endpoint to remains the spawner; absent metadata preserves the legacy fallback. Explicit metadata follows the pinned9d6 waits-for formula, with terminal slots interpreted by the existing effective ontology. Viewops readiness, gate creation/listing and broader B14 gate-field/resolver behavior remain separate obligations. Original PR246 and PR267 evidence is unchanged.
