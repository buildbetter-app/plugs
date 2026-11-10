# Explicit native delta capture sets

This route implements the immutable D2 contract
`7d44af86416ee72d0feb3c4e71818ceb98010d8f20652bd3c8476a68d100288c`
plus D3 amendment
`96a1a79ed42d3f8942a4d574091f6181ae61c746bcc0ec1d65b0a1617bb2b2b6`.
Normative D3 Go wire SHA256 is
`1134f49bf52bfd72a2f18a72d69ca1d7daacc18a1d44b45fadff4cb6662cbfc5`.

The producer's explicit `--native-delta` output has an `OUTPUT/capture-set`
directory containing exactly `capture-set.json`, `head.tar`, and `parts`.
Inventory and physical evidence remain siblings outside that directory.
Legacy `--archive` construction and receiving keep their existing paths/limits.
A v2 `head.tar` requires its complete authenticated set; standalone import refuses.

The explicit CLI sequence is:

```
native-receive prepare-request --capture-set OUTPUT/capture-set --policy POLICY --driver sqlite --expected-binary-sha256 ELF_SHA
native-receive --capture-set OUTPUT/capture-set --policy POLICY --request CONSTRUCTOR_STDOUT --driver sqlite --target OWNED_ABSENT_PATH --expected-binary-sha256 ELF_SHA
```

Memory and PostgreSQL retain their existing disposable-session and lifetime rules.
The policy's `archive_sha256` and the constructed request bind the exact root
`capture-set.json` bytes. The root authenticates head and the whole part chain.
No driver or importer API changes. Every part and reconstructed state is verified
before lowering. Existing admission, lease, transaction and publication rules apply.
Reopened persistent readback checks every imported commit/table and source events.

The full physical current census may also contain exactly `dolt_ignore`,
`dolt_nonlocal_tables`, and `dolt_schemas`, outside the portable BASE TABLE
census. The verified v2 path retains these as inert `beads-archive` records with
`history=opaque-native-physical`, exact rows and per-family source/imported
counts. Original typed cells/schema/hash commitments remain in the authenticated
native stream and retained blobs for reverse/undo. SQL view or trigger definitions
are stored as evidence only; import does not create or execute them. Current
readback checks these opaque records too. Required portable families still have
exact columns, rows and dispositions; every other extra current name refuses.
The versioned `dolt_schemas` HEAD/current normalized census agrees exactly.
Nonlocal declarations remain raw evidence; import does not resolve aliases.
Legacy portable/native-table admission keeps its exact closed census.

## Temporary decoded accounting

`--native-decoded-limit-bytes` accepts a positive uint64; default 2147483648.
The library uses `NativeCaptureSetOptions.DecodedLimitBytes` (zero selects the
default). CLI receipts identify `default` versus `explicit flag`; the library
uses `explicit library option` unless its caller supplies an explicit origin.

`native-delta-decoded-accounting/1` counts **cumulative logical payload charge**.
`peak_accounted_decoded_bytes` is that monotone cumulative sum, not live bytes,
heap size or RSS. Charges never release. Shared/discarded representations still
count in their specified categories. Hashes, pointers, container allocation
overhead and the Go allocator are not claimed to be a measured heap model.
Wire part count/bytes have no aggregate cap and are outside this decoded domain.

All JSON charges use the exact named Go type with compact encoding/json behavior,
HTML escaping, its actual `omitempty` tags and one terminal newline. The counter
walks the fields and bounded scalar encodings; it does not marshal/Encode a whole
image, table body or batch merely to count it. Supported custom scalars are
model.Natural, model.Ref, json.Number and json.RawMessage; unallocated custom
marshalers refuse. Map order does not affect byte length. Types below are from
dumpformat unless explicitly qualified.

| Category | Exact charge per occurrence |
| --- | --- |
| head_tar | Exact authenticated head.tar bytes before parsing |
| head_tar_copies | Twice head.tar byte length, reserved before bounded legacy parsing for raw/member payload and retained tar copies |
| header | NativeDeltaHeader, including actual ignore and config policy |
| schemas | Each retained NativeDeltaSchema |
| commit_metadata | Each NativeDeltaCommit opening record |
| decoded_operations | NativeDeltaRow JSON; a delete uses its exact bounded raw record bytes, including newline |
| decoded_baseline_rows | Each NativeDeltaRow reloaded from a first-parent staged table |
| reconstructed_table_metadata | NativeTable with its sorted columns and empty non-null Rows |
| reconstructed_rows | Each normalized Row before append to that table |
| image_metadata | NativeHistoryImage with empty Tables, sequence, ID and actual schema profile |
| reconstructed_images | Complete NativeHistoryImage, reserved before its disk encoding |
| aggregate_images | Same verified image byte count, reserved before final aggregate decoding/append |
| checkpoints | Complete independent HEAD/current NativeHistoryImage, even the discarded HEAD duplicate |
| commit_proofs | NativeDeltaCommitProof including every table's Rows/SHA256 commitment |
| detached_history_views | Complete NativeHistory before each detached import/readback copy |
| detached_history_metadata | NativeHistoryMetadata before a metadata-only detached copy |
| retained_capture_ledger | struct{Name string `json:"name"`; File File `json:"file"`}, before each ledger insertion |
| offline_batches | Complete driver.SourceHistoryBatch, including every before/after Record and base64 Body, before building bodies/changes/appending |
| history_baseline | driver.OfflineHistory.Baseline as []driver.Record |
| history_gaps | driver.OfflineHistory.Gaps as []driver.SourceHistoryGap |
| history_boundaries | driver.OfflineHistory.NativeBoundaries as []driver.NativeHistoryBoundary |
| history_envelope | driver.OfflineHistory with Batches/Baseline/Gaps empty and NativeBoundaries nil; actual remaining scalar fields |

These are deliberate duplicate charges: typed row and normalized row; metadata
and full image; staged image and decoded aggregate; independent snapshots;
each detached view; normalized tables and their lowered before/after Record
bodies. The final envelope charges empty container syntax, not those payloads
again. Every batch still charges its own full before/after bytes even when Go
shares earlier bodies. Categories and charge counts are receipt maps, so their
sum independently reconciles to the cumulative total. They describe only this
receiver accounting domain. The CLI propagates WithNativeDeltaAccounting through
constructor, import and read-only recovery, so their repeated reconstructions
share the same cumulative operation budget. Separate library operations use
independent budgets unless their host propagates that accounting context.

Comparison is checked `admitted + next`: equality fits. The first exceeding
candidate returns `*NativeDeltaDecodedLimitError` before its retention/mutation.
It includes `accounting`, `phase`, `measured_bytes`, `limit_bytes`, `overflow`,
`admitted_bytes`, `rejected_charge_bytes`, and detached `admitted_usage`
(including category bytes/counts and limit origin). Overflow saturates the
candidate measurement at MaxUint64 and sets `overflow=true`; a literal
MaxUint64 measurement with no arithmetic overflow keeps `overflow=false`.
No omitted commits or truncated data are returned.

Success adds `native_delta` to `plug-native-receiving-receipt/1`:
`format=plug-native-delta-receiving/1`, `verification` (contract/totals,
HEAD/current roots, every commit's root/schema/table proofs, decoded_usage),
`decoded_limit_origin`, and `rss`. Table proof digest objects use the normative
Go helper's exact JSON names `Rows` and `SHA256`.
A decoded refusal emits `plug-native-delta-refusal/1` JSON on stderr, containing
`refusal` and `rss`, followed by the existing human-readable refusal line.

RSS is the actual receiver process lifetime peak from Linux
getrusage(RUSAGE_SELF).ru_maxrss, KiB multiplied by 1024 for `peak_bytes`.
`status`, `method`, `process_scope`, `platform`, `source_units`, and `peak_bytes`
are explicit. Child/wrapper RSS is excluded. Unsupported/failed measurement is
NOT_MEASURED. A public wrapper may independently measure the same receiver
child with /usr/bin/time; that is separate original evidence.

The existing downstream aggregate importer cap remains unchanged. Any narrower
measured refusal is reported literally. The mandatory lowest-layer streaming
OfflineHistory follow-up remains required before switch; this temporary lowering
does not complete that work or authorize private execution.
