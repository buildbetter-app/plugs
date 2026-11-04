# B09 accepted-native recorder source

`record_native_test` is an explicit, manual RunsOn recording target. It accepts
only the four `PLUG_PUBLIC_BEADS_*` exports from the existing trusted
`70230ccd` public-bundle preparer on the fixed-2d private route. The test binds
the accepted producer run, manifest, source commit and
tree, and executable bytes before running any native command. It creates fresh
Git-backed workspaces with an explicit child environment and retains every
command's raw streams, typed JSON result, exit code, full native inventory,
26 sidecars, validated `plug-dump/1` archive, and complete workspace/home file
snapshots in `TEST_UNDECLARED_OUTPUTS_DIR`.

The corpus records default length 3, a count-driven length 4 with
`max_collision_prob=0.000001`, configured length 10, explicit-ID collision
refusal, real Beads JSONL export/import followed by a new native mint, and a
separate one-character diagnostic that occupies all 36 suffixes and forces
the actual allocator's ten-nonce exhaustion. That diagnostic is
outside B09's ordinary 3-through-10 policy and is labeled accordingly. The
recorder compares archive records and every workspace/home byte before and
after each refusal; any difference blocks conservation. Native JSONL import is
not Plug snapshot restore. This source makes no Plug parity, restored-ID,
legacy-clamp, or full-card claim by itself.

The accepted inventory declares `store-files-and-quiescence` engine contents
`not-read`; the independent file snapshots are required for a physical
conservation claim. This source is not a recording. No native execution is
authorized by adding this target.
