# B11 imported edge acceptance

`TestB11AdmittedMixedEdgeMetadataIdentityAndReopen` uses the retained authored
B07 fixture. Independent literal expectations cover both mixed directions,
same raw ID in different physical tables, nested semantic metadata equality,
changed metadata, direct and late typed retype refusal, complete state
conservation, original response replay and SQL physical reopen. This fixture
does not count as a native capture.

`TestB11NativeCapturedPublicEdgesMetadataAndSQLReopen` uses the committed public
input in `testdata/public-r5-edge-inputs.tar.gz`. The nine unchanged members
include one complete authenticated native capture set and its independent SQL
witnesses. The adjacent provenance records every member's original path,
size and SHA-256. The test authenticates all nine members, reads the complete
capture through the existing native capture-set interface with its default
decoded accounting, and supplies explicit host `SourcePlanePlanV1` intent to
the installed receiver. No producer, importer or lifecycle implementation is
added.

The independent expectations name all seven physical source edge tuples and
target arms. An independently expressed UUIDv5 oracle checks the captured row
IDs. Public reads and reverse output conserve the endpoint/type/metadata tuple;
the public interface does not expose a dependency-row UUID or local target-arm
field. Physical arms remain separate source/readback assertions. The stored
metadata witness is a native JSON object token, while the authored fixture
also tests metadata text carried by JSON string tokens. Core no-op receipts,
mixed receipt-only publication, changed mixed metadata, rollback, original
receipt recovery and replay have separate effect assertions.

All three engines run the positive imported/mixed cases. Memory asserts process
checkpoint lifetime and effective persistence false; its same-process handle
reacquisition supplies no restart credit. SQLite and PostgreSQL close and
physically reopen the target. Existing immutable-key display/move controls,
pinned native metadata HTTP controls and recorded native external/unresolved
controls remain inherited and independently qualified.

This acceptance requires the shared B14 gate-reader source from PR494,
`73d8ef4b45377119fda0b8d5ce9ae31c27a2f4db` (tree
`7e0279296de311683e7d2448549abcb57433e94d`). The frozen B11 parent lacks that
reader correction, and the unchanged capture includes a native gate item.
The shared reader must be supplied by its owner/train at composition; it is
not copied here. Its bounded source/focus qualification and whole lexicon RED
remain separate from this acceptance's future execution. No runtime, hosted,
native-history or full-card PASS is asserted by this source addition.

The ordinary converterroundtrip test glob includes both new tests without a
filter. Its declared `//spec/cardb11:public_native_edge_inputs` runfiles carry
the committed input; runner paths never refer to this host's `/tmp` files.

Native reverse bookkeeping fills absent `_plug_native_status`,
`_plug_native_status_category`, and `_plug_native_type` metadata fields. These
names remain admitted user metadata: equal existing values and unrelated user
metadata survive; any unequal existing value, including JSON null, explicitly
refuses reverse conversion with `conflicting_native_metadata` and no output.
`TestB11AdmittedNativeReverseMetadataPreservationAndConflicts` covers admitted
import through reverse JSONL across memory, SQLite and PostgreSQL using a
separately declared synthetic history; it earns no native capture credit.
