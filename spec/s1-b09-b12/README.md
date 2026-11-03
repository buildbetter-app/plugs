# S1 B09–B12 focused slice

This package owns the narrow allocator and dependency-identity implementation
for B09–B11 and the evidence boundary for B12. The source pin is
`9d6fff760b7a314b476539198504d77ce0959656` with tree
`cde121d6e4119e454857ad1c3ca293e2791be840`.

`replay_test.go` runs the Plug allocator and link paths through real memory and
SQLite drivers. It fixes the Go clock, principal, project, allocator state,
collision inputs, direct-child history and hierarchy depth, then reopens the
SQLite store. The projection helper preserves ordered outcomes and decision
IDs independently from the raw fixture. B11 also carries the pinned UUIDv5
endpoint vectors in `internal/storage/depid`.

The retained `vectors.json` and root `provenance.json` are the earlier
synthetic/replay boundary and remain unchanged. The `reference/` directory is
the separate historical pinned-Beads recording. Its version 1 files are retained
byte for byte. Their `rawDigest` / `rawSha256` values hash normalized decoded
transcripts; exact stdout/stderr bytes, clean-checkout enforcement and credential
isolation were not retained or proven. The version 1 recorder is preserved in Git
at `aedeca725:spec/s1-b09-b12/record_reference.py` (SHA-256
`3a979c3b1bb5e1e45643adf1e13ebf675d56c5549fdc9b6fa0962f5594251936`).
The old B09–B11 disposition links to `work.md` are historical errors; all four
canonical cards are in `engdocs/design/plug/spec/behaviors/content.md`.

The repaired `record_reference.py` emits version 2 only. It rejects tracked or
untracked source changes before evidence hashing, reads pinned Git blobs, and
runs in fresh disposable Git-backed stores with private HOME/XDG directories,
fixed Git identity/configuration and an explicit environment. Ambient tokens,
credential/config paths and SSH agents are excluded. No private or production
store is accessed.

Each version 2 command retains exact stdout/stderr bytes as base64 and per-stream
SHA-256 values. `rawStreamsSha256` hashes the canonical lossless stream envelope;
`normalizedTranscriptSha256` separately hashes normalized decoded commands and
observations, and `projectionSha256` hashes the semantic projection. The same
names are used in each run and its summaries. `equality` reports raw byte
equality per command and overall independently from normalized transcript and
projection equality. Runtime timestamps and generated IDs may make raw bytes
differ even when the normalized transcripts agree; that result is retained.

The six-path recorder repair at `b0adfdd4f3f8669f8faba03637c813b99f396fde`
passed independent source review and focused RunsOn run `41553992037`. That
result covers its policy regressions and retained paired-driver replay; it
does not transfer to this source successor or provide fresh version 2 bytes.
`TestS1ReferenceRecorderContract` uses temporary Git repositories and mocked
streams. Those are unit fixtures, not source-engine acceptance evidence.

`public_reference_profile.py` adds one separate fixed public build profile:
`s1-b09-b12-public9d6-original-go-v1`. It binds descriptor
`41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2`,
the exact source commit/tree/5087-entry manifest, producer workflow/build and
toolchain metadata, ZIP/tar identities, and executable SHA-256
`3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4`.
This is the original Go-built public profile, explicitly not canonical Bazel.
The recorder's legacy `33cdadbe...` pin and every version 1 byte stay unchanged;
the common source pin does not establish binary equivalence. The new wrapper
has no user-selected hash, executable, URL or fallback route.

The main-helper prerequisite passed in run `41553917261` at control/input SHA
`713557b6a3abaf6fe5fc2dbf4ca38561d43f5deb`. Its metadata receipt is not a
reusable VM path or executable. For an authentic capture, the existing trusted
test route prepares the fixed ZIP, source, manifest and build-run environment
in that same fresh RunsOn job. After independent review of the exact successor,
the concrete invocation is:

```sh
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
/tmp/plug-swarm/bin/remote-bazel "$PWD" <reviewed-successor-sha> s1-b09-b12-public-v2 -- \
  test //spec/s1-b09-b12:reference_capture_test --nocache_test_results --test_output=all \
  --test_env=PLUG_PUBLIC_REFERENCE=41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2
```

Use a non-login shell. This explicit manual Go target is excluded from `//...`.
Its allowlisted environment omits ambient credentials and configuration. The
wrapper snapshots both prepared inputs privately, checks all source bytes/modes
against the pinned Git tree, verifies the fixed ZIP/manifest/tar and both loose
executables, and grants execute permission only after those checks. It invokes
the unchanged reviewed recorder functions twice per card in eight new Git
stores. Unexpected exits and timeout partial streams remain in a BLOCK packet.

The entire canonical v2 JSON packet is emitted between literal BEGIN/END lines
in the retained `test.log`, with byte length and SHA-256 repeated in both
markers. The existing workflow archives that log for 14 days. The packet
includes exact stream bytes/digests, profile and candidate module hashes, raw
source manifest, and the selected test/migration blobs with Git/SHA-256 checks.
`extract_reference_packet.py` imports no recorder/capture code. It independently
derives normalized transcripts and semantic projections from raw stdout/stderr,
measures all three equality kinds, validates source/schema/profile digests, and
rejects absent, duplicated, truncated, altered, incomplete or BLOCK packets.
The capture invokes it before labelling a packet PASS; after collection the
same candidate's extractor must validate the archived log again:

```sh
python3 -B spec/s1-b09-b12/extract_reference_packet.py \
  --log <retained-reference_capture_test/test.log> --source-root "$PWD" \
  --candidate <reviewed-successor-sha> --tree <reviewed-successor-tree> \
  --output <new-external-v2-packet.json>
```

The output path must be new. Keep these lossless v2 packets separate from
`reference/` and its version 1 provenance; do not overwrite or relabel historical
recordings. Source SHA, workflow-control SHA, complete same-run receipt and
archive retention must also be audited externally. The packet itself does not
establish that hosted binding. Raw byte inequality is retained honestly and
cannot become raw equality through normalization.

`//spec/s1-b09-b12:reference_capture_policy_test` checks the transport-free
consumer, private snapshots, failure streams, complete framing and independent
validator using synthetic unit fixtures. It executes no reference product and
cannot substitute for the explicit manual capture or paired-store acceptance.
Authentic v2 captures for this successor remain pending.

The recordings are evidence for the selected B09–B12 cases, not full-card
qualification. B12 records the embedded reference's honest `--apply` and
restore refusals; Plug's adopted permanent diff history and `history.revert`
contract remains a documented deviation until a producer and replay proof are
admitted.

B12 remains explicitly `full-card-unqualified`: this head has no admitted
public Plug compaction producer or reversible-snapshot replay. Its accepted
contract still requires eligibility, snapshot-before-overwrite, permanent
history, restore and controlled Go/SQL clock boundaries before qualification.

The package does not alter the seven retained reference slices or their
goldens. It is a focused implementation/replay harness, not a census or a
substitute for source-built Beads evidence.
