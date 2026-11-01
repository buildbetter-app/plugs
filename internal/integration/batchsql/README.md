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

The tests call the direct Writer boundary and label that evidence. `tx.apply`
is not claimed as an RPC surface because it is not bound in the current
registry. Graph cycle qualification belongs to the graph acceptance package.

Hierarchy cases qualify ordered parented creates and subtree moves at that same
Writer boundary on all three stores. An independent traversal of persisted
direct parents checks the complete ancestor table, including depth replacements,
root moves, canonical encoding, pinned reads, and SQL reopen. The tests retain
the 5000-row acceptance and 5001-row atomic refusal under the normal two-second
preparation budget, exercise final child-status guards and insertion races, and
inject failures after actual record, closure and sentinel writes. Missing or
corrupt existing closure refuses without a migration or automatic repair.
