# Connected audit integration

This target composes the real protected ontology source, `core/service`'s
tracked ontology adapter, the single pipeline `Writer`, memory publication,
the core event reader, the server history materializer, and persisted event
checkpoints. It proves the runtime boundary for a successful close and cancel:
one atomic ordinary event is followed by sorted typed conditional notices whose
images remain nil and whose dependent records do not gain versions.

The protected source gives `failed` different outcomes in the blocker and
dependent projects, so failure suppression must use the blocker's own project.
Assertions retain the exact compilation digest, full resolved decisions,
source/edge revisions and provenance after the protected ontology changes and
after consumers mutate returned copies. They also cover failure/open/no-op/
dry-run/replay suppression, deterministic source invalidation,
`ResourceAuthorization` refusal and masking, and a lost-ack retry at logical
index 1 with the same delivery key and payload.

Create and close use the public service methods. Conditional edge batches and
cancel/fail/defer use the real `Writer` directly: this service slice exposes no
named `Fail`, `Cancel`, or `Defer` methods. These operations are labeled in the
test names. The source fixture uses the storage-owned conformance seed helper;
there is no protected ontology apply route in this slice.

The fixture seeds only the protected ontology record. Beads and conditional
edges are created by the actual service or Writer. This proves the guarded
conditional notice path through the actual adapter and memory runtime.
Remaining B02 obligations include ordinary `blocks`, parent-child and
`waits-for` behavior, transitive/diamond graphs and durable/wisp readiness.
Public RPC/transport wiring, an installed permission chain, SQL persistence,
process restart and failover are outside this target's acceptance claim.
