# Native row records

This is the pure codec and reconstruction layer of the reviewed row-granular
native-history contract, revision 2, SHA-256
`388c529cb8518750c4201aa224ccd9104cb4685b328c564cdc4100942088f115`.

Done for this layer means row/meta records reconstruct exact old table-body
bytes, proven by independent serializer units and malformed-input controls.
Receiving, persistence, complete public-history and physical undo proofs belong
to the following integration layers.

`NativeV1TableRecords` takes validated, canonically ordered normalized tables.
It assigns explicit `v1-content-bag` identity: SHA-256 of exact row JSON plus a
dense zero-based occurrence. A content edit changes identity. It needs no SQL
types or PK information and makes no physical SQL claim.

`NativeV2TableRecords` takes complete typed D3 table state and an authenticated
physical schema. Identity is `d3-physical`: ordered typed PK bytes, or the D3
typed-payload digest plus occurrence for an authenticated keyless schema.
Missing descriptors never establish keylessness. Schema/source/root authority
remains the receiver's responsibility; the codec validates supplied preimages.
`NativeV1RowRecord`, `NativeV2RowRecord` and `NativeMetaRecord` also expose the
per-record operations needed by subsequent incremental lowering.

Each present table has one meta and one row record per represented row. Empty
tables retain one meta; absent tables have no records. Row bodies contain the
source tuple, table, exact history classification, identity kind, base64 key,
normalized fields, and (for v2) exact ordered typed cells. Row bodies omit the
schema ID, so unchanged rows survive schema-only changes. Current history
classes retain `versioned`, `unversioned-in-beads-by-design`, or the existing
`opaque-native-physical` label for inert system-table evidence. The latter does
not execute view definitions or authorize physical writes. Meta contains sorted
normalized columns, the physical schema where applicable, row count, ordering
rule and both commitments. It contains no row-ID vector.

All integer preimages are uint64 big-endian. Let `B(x)=U(len(x))||x` and
`S(t)=B(UTF8(t))`. Stable generic-driver record IDs are:

```
T       = S(store_id)||S(project)||S(table)||S(identity_kind)
row ID  = P + "row/"  + hex(SHA256(S("plug-native-row/1")||T||B(key)))
meta ID = P + "meta/" + hex(SHA256(S("plug-native-meta/1")||T))
```

`P` is `source/native-history/` or `source/native-current/`. Both records have
branch zero and Kind `beads-archive`. Constructors return detached canonical
JSON bodies, ordered by the existing encoded driver record-lock key.

`ReconstructNativeRowTable` consumes a complete group for one expected
source/table/plane. It checks strict canonical JSON and Refs, exactly one meta,
all row schemas, typed/normalized coherence, dense multiplicity, full-payload
digest collisions, census and commitments. D3 commitments sort decoded raw
keys. The normalized table instead sorts exact `json.Marshal(Row)` bytes and
then raw key bytes. It retains lexical `json.Number`, SQL NULL, binary
envelopes and duplicate rows. V2 normalized payloads include their D3 LF;
stored record bodies and the old table envelope have no LF.

`NativeTableBodyBytes` serializes the old ordered envelope
`source_identity,table,history,columns,rows`, with default JSON escaping and
no newline. The independent test oracle uses the old envelope directly.

Complete readback must first enumerate unfiltered `Query{}` through the terminal
cursor in one fixed snapshot, then select native namespaces and require Kind.
The caller must compare all groups and expected Refs to independently
authenticated source membership. Passing only known records cannot prove
completeness. The reconstruction API refuses extra or wrong-Kind input rather
than choosing a valid subset, and does not perform storage queries itself.

The receiver still owns simultaneous retained/transient accounting under the
unchanged 2 GiB decoded ceiling; cumulative work is diagnostic. Physical undo
additionally needs the original typed immutable and complete working ports,
same-source preserved copy and independent request/readback byte oracles.
Normalized v1 equivalence does not grant permission to invoke that writer.
