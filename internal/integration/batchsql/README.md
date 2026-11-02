# Batch SQL acceptance

This package qualifies the existing protected `pipeline.Writer` directly on
memory, file-backed SQLite, and real PostgreSQL. It sends one atomic
create/patch/comment `ChangeSet`, resolves backward aliases, references a bead
owned by another project, and checks records, head, versions, changesets and
the durable idempotency receipt.

Late missing targets, bad compare-and-set preconditions, forward aliases, and
cross-project batches that fail later are required to leave every durable
surface unchanged and publish no receipt. Reopened SQL stores must preserve
the state and replay the original response; reusing its key with different
parameters must return `idempotency_key_reused` without writing.

The original batch tests call the direct Writer boundary and label that
evidence. Graph cycle qualification belongs to the graph acceptance package.

Hierarchy cases qualify ordered parented creates and subtree moves at that same
Writer boundary on all three stores. An independent traversal of persisted
direct parents checks the complete ancestor table, including depth replacements,
root moves, canonical encoding, pinned reads, and SQL reopen. The tests retain
the 5000-row acceptance and 5001-row atomic refusal under the normal two-second
preparation budget, exercise final child-status guards and insertion races, and
inject failures after actual record, closure and sentinel writes. Missing or
corrupt existing closure refuses without a migration or automatic repair.

`TestB01PublicHierarchyFivePermissionRulesAcrossDrivers` and
`TestB01PublicDeferredAncestorsAcrossDrivers` additionally cross real loopback
HTTP, API-key admission, the fixed-policy hierarchy checker and public
`bead.create`/`tx.apply` lowering on all three drivers. They preserve child
project ownership, require child/new-parent edits without old-parent grants,
project hidden parents as existence only, and count visible children with a
status-free hidden remainder. Ordered aliases, atomic refusals, final images,
current-authority receipt replay, pinned history, and SQL reopen are exercised.
Deferred ancestors suppress ready descendants and direct/next claims; public
reparent and reopen update that membership. Fixed policy continues to refuse
lifecycle verbs: a separate supported no-plugin public host supplies the real
status observations and lifecycle guards. No hierarchy permit is fabricated.
