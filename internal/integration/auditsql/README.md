# Conditional audit composition on SQL

`//internal/integration/auditsql:auditsql_test` runs the same connected cases
against file-backed SQLite in WAL mode and a disposable real PostgreSQL 17.11
container pinned by image digest. Missing Docker or PostgreSQL fails the test;
there is no memory substitute, external DSN, or skipped engine.

The tests compose the real service ontology normalizer and tracked source
adapter, Writer, SQL publication, event Reader/HistoryMaterializer, dispatcher,
and lease-fenced SQL checkpoints. Only the protected ontology source is seeded,
with explicit retained system attribution. Service Create/Close and real Writer
conditional links create every graph record and event descriptor.

The close case checks the entire ordered plan, resolved decisions, provenance,
source revision/digest, blocker project and dependent/edge source revisions.
Dependent records and all their retained versions remain unchanged. A SQL
checkpoint trigger interrupts acknowledgement of the first notice after its
side effect. Closing every store handle and reconstructing the service,
Reader and dispatcher preserves the receipt, event plan and checkpoint; the
same delivery key and payload retry with `late=true`. A second reopen retains
the completed checkpoint. Changing current ontology before recovery does not
reinterpret historical notices. Same-key service replay returns the exact
original commit without publishing another revision.

The failure contrast makes the same status key mean failure in the blocker's
project and success in the dependent's project. The actual Writer fail slot
retains only `bead.failed`, including across reopen. Service has no named Fail
method in this slice; this test makes no such claim.

A second SQL trigger fails at receipt insertion after verifying the real
three-event close plan is already visible inside that transaction. Fresh and
reopened readers, version history, SQL payload-family counts and checkpoints
show complete rollback. Retrying the same request commits the next gapless
revision once.

This target proves store-handle restart/reopen on both engines. OS-process
crash, database restart/failover, installed authorization/event-context chains,
RPC/transports and automatic service post-phase completion remain separate
acceptance. Full B02 also needs ordinary blocks, parent/waits-for graphs,
transitive/diamond graphs and durable/ephemeral readiness. No dependency,
driver, service or production implementation is modified here.
