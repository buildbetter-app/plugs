# Connected audit integration

This target composes the real protected ontology source, `core/service`'s
tracked ontology adapter, the single pipeline `Writer`, memory publication,
the core event reader, the server history materializer, and persisted event
checkpoints. It proves the runtime boundary for a successful close and cancel:
one atomic ordinary event is followed by sorted typed conditional notices whose
images remain nil and whose dependent records do not gain versions.

The tests also cover failure/open/no-op/dry-run/replay suppression, deterministic
source-pin invalidation, explicit `ResourceAuthorization` refusal and masking,
and a lost-ack listener retry with the same delivery key. Bead failure and
cancel are direct real `Writer` operations because this service slice exposes no
named `Fail` or `Cancel` route; the test labels that boundary explicitly.

The fixture seeds only the protected ontology record. Beads and conditional
edges are created by the actual service or Writer. The target does not claim
full production B02/service-adapter composition beyond this in-process runtime
boundary, and it does not exercise SQL drivers.
