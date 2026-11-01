This trusted offline harness imports a validated Beads archive into an empty
disposable memory store, runs a claim/release and an idempotent live edit, then
reverses the captured live state. It is not an RPC or an admission service.

Build the public executable on RunsOn with target `//tools/roundtrip:roundtrip`.
Run the delivered binary with `--input <archive> --output-dir <new directory
under /tmp> --history-mode full|partial|snapshot`. The strict default refuses
incomplete source history. Partial requires a verified suffix and baseline;
snapshot is explicit. `--exercise=false` checks the unmodified imported head.

Standard output contains only aggregate counts, digests and fixed error codes.
The new output directory uses mode 0700 and the reverse JSONL files use 0600.
Keep private archives, stores and reverse output under /tmp; delete them after
the separately pinned Beads importer/readback comparison. A reverse PASS does
not prove that later comparison. The head-state reverse report discloses the
history that Beads JSONL does not replay.
