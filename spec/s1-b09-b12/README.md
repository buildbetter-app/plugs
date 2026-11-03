# S1 B09–B12 focused slice

The new `accepted_source_test.go` / `accepted_replay_test.go` consume the exact
accepted public packet in `reference-v2/cb40-41563023339.packet.json`: authentic
run `41563023339`, producer `cb40cab68`, 1,514,945 bytes, SHA-256
`33cd8fdcb66b5b93f4a36329fc0598cb9c5d012fc350804809da5eb5a38d4067`.
Its provenance file binds the original archive and independent byte audit.
This packet retains all source/profile/config/runtime/stream bytes and the
original `rawStreamsEqual=false`; normalized and projection equality are true.
Initial configuration copies prove only the pre-runtime/init setup. The
producer candidate remains distinct from each subsequent Plug replay candidate.
The complete packet is declared by exact path and SHA-256 in the existing
lexicon immutable-reference policy. Original focused run `41574255757` at
`cfff5a066` passed the replay and pipeline tests but failed the tree gate on a
pinned source-test name inside those bytes. The declaration preserves the
original packet, with changed bytes and undeclared sibling files still refused.

The new Go source projector imports no producer/extractor and decodes original
stdout/stderr with per-stream checksum checks. Both fresh source runs supply
separate inputs to new memory and SQLite stores. Projections derived from
persisted Plug bodies are compared with independently derived raw-source
projections; each operation logs its actual request, commit/error, pinned store
head, record bytes, versions and allocator state. A SQLite reopen precedes the
post-deletion child mint and follows final dependency unlink. Source packet
identity, stream corruption, incomplete shutdown and ignored producer-summary
negative cases are separate from the real-store runs.

The comparison mappings are explicit. Source explicit parent/child IDs use the
existing driver restore seam with a stale child counter before product writes.
The source explicit hash-ID collision maps to occupied trusted-generator
candidates in the real writer, with a typed refusal and unchanged durable
state; it does not imply a user-selectable ID RPC. Hash allocation itself uses
the actual restored hash allocator and a controlled Go clock. Ordered close
uses a fixed test ontology with open/closed slots; this proves allocator
continuation after the persisted close, not B08 authorization/guard parity.
The B11 regression uses the recorded `blocks` to `relates-to` pair literally.
The tests-first child `75f74ab11` ran as `41573678135` and is a preserved RED:
all four B11 cases accepted that retype, advanced head 3 to 4 and stored both
kinds. B09/B10 passed their new comparisons. The causal product repair removes
only the association-kind exemption from endpoint identity and wrong-kind
unlink checks; dependency graph effects remain separate from identity. Extra
product cases check the reverse type conflict, earlier-batch rollback, absent
refusal receipts, and atomic unlink/link replacement, while an association on
a different endpoint remains legal. Historical replay tests and recordings
retain their original bytes; the old test comments about association exemption
describe the prior defect and do not override canonical B07/B11.
B12 remains NOT EXECUTED, and all four-card, full-S1, native-producer and hosted
qualification boundaries remain unchanged. No new reference capture is needed.

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

The original embedded public route at `b7fca102dc8747bd3d4ba8fb1fceb64a1f34e111`
has since run once: authentic Run `41557556883` is RED, exit 3/evidence ok.
The fixed public binary was built with `CGO_ENABLED=0`; its first default
embedded init failed. The retained complete BLOCK packet contains the exact
1,337-byte stderr stream. No card or pair completed, so all equality is NOT
MEASURED. The original profile, recorder, extractor and BLOCK bytes remain
historical evidence; their source/policy passes do not establish store support.

The additional `proxied_reference_profile.py` and
`reference_proxied_capture.py` select B09–B11 only, under profile
`s1-b09-b11-public9d6-proxied-dolt220-v2`. B12 is explicitly NOT EXECUTED and
UNQUALIFIED. The four-card completion contract above still applies. This route
makes no claim about proxied compaction or equivalence to the embedded mode.

Done for this bounded repair = two fresh B09–B11 captures with the fixed
9d6fff source/tree and public 3c6 binary in declared managed-proxied Dolt 2.2.0,
proven by complete retained raw packets, argv/runtime/lifecycle identities and
independent source/profile/card-set/stream/projection/pair validation; policy
tests or successful init do not count.

The owned Go provisioner adapts B01's atom at `56fa6812`, with a fixed public
HTTPS URL, no proxy/credentials, limited release-asset redirects and archive
SHA-256 `1f7ad8c2622995789420a3fb0f2d16b4aa7430000a825dd91d5938f36480cbf6`.
That pin comes from original9d6 `tools/bazel/dolt.bzl`, whose own source SHA-256
is `699d49317af8c669b92ee2cc03fdaed75aeac2f873d7c1221dcd751cde20f49f`.
The Python consumer authenticates the same-run archive again before extracting
only the exact regular `dolt-linux-amd64/bin/dolt` member. It reconstructs
mode 0700 and measures executable size/SHA-256 before running its exact version
probe in a private environment. The full probe streams and digests are retained.
The packet binds the pin, init, provider and lifecycle source blobs as well.
The measured executable digest is not a separately pre-pinned release digest:
its derivation also requires the original exact-source RunsOn receipt.

Every selected card uses two new private Git stores and explicit
`init --proxied-server`. Only the verified private runtime directory is added
to the child PATH. `bd dolt stop --json` runs after success or failure, with
exact bytes retained. Deletion requires `stopped: true` and `verified: true`;
failed shutdown retains both workspace and runtime and yields BLOCK. The new
independent extractor requires precisely the selected three-card set, six
distinct workspaces, matching per-run runtime digests and verified shutdown.
The original four-card extractor still requires B12 under its original profile.

After exact retained source review, the changed-source capture command is:

```sh
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
/tmp/plug-swarm/bin/remote-bazel "$PWD" <reviewed-proxied-sha> s1-b09-b11-proxied-v2 -- \
  test //spec/s1-b09-b12:reference_proxied_capture_test --nocache_test_results --test_output=all \
  --test_env=PLUG_PUBLIC_REFERENCE=41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2
```

Use `login:false`, preserve the sole original collector and complete framed
test.log, and use `extract_proxied_reference_packet.py` with the same arguments
as the historical extractor. It independently computes raw/normalized/projection
equality, rejects BLOCK and creates only a new output file. The explicit manual
target is excluded from `//...`; the additional
`reference_proxied_capture_policy_test` executes labelled synthetic policy
fixtures without a product or network. No changed-source runtime evidence is
claimed by this source successor. B12/full-card/full-S1 and whole/hosted
qualification remain incomplete.

The original proxied v1 head `e71f09310eb5558c29230f691cc4bd5bb6afb443`
ran once in Run `41561273036`: RED, exit 3/evidence ok. Archive authentication
and executable extraction succeeded, but `dolt version` exited 0 with a
177-byte stdout containing an online release warning. Strict version identity
rejected those bytes before any store/card/init/shutdown. The complete
1,385,352-byte BLOCK packet SHA-256
`6d9123141e5186692381a6356f422a409b1e64d68c795bc051d314b1678de4ca`
and exact warning streams remain retained. No card or pair completed; all
equality is NOT MEASURED. Its source and fixture-policy PASS remain historical
and bounded; no unchanged repeat is valid.

The causal proxied v2 profile keeps the same source, public binary, Dolt
archive/member, storage mode, card set and strict version output. Before any
Dolt process, it exclusively creates the fixed private
`HOME/.dolt/config_global.json` with mode 0600 under a 0700 directory. Exact
bytes set `metrics.disabled` and `versioncheck.disabled` to true, following pinned
`internal/testutil/localdoltserver.go`. `DOLT_ROOT_PATH` points to that same
private HOME; `DOLT_DISABLE_EVENT_FLUSH=1` follows pinned
`tools/bazel/test_env.sh` and prevents detached metrics children from recreating
removed roots. Neither ambient configuration nor the host HOME is used.

The runtime preparation additionally executes the supported public
`dolt config --global --get versioncheck.disabled` before `dolt version` and
retains both probes' exact argv, exit and stdout/stderr bytes/digests, including
partial timeout/failure bytes. Both assertions remain strict. Each selected
workspace receives its own fixed configuration before init. Actual file bytes,
modes, derived environment and private paths are retained for the probe and
each workspace and independently checked against this explicit profile and
the workspace identities. Configuration failure or incomplete data blocks
acceptance; warnings are never stripped from actual version output. The two
additional original9d6 configuration source blobs are retained and Git-bound.
Fresh authentic execution remains required for this source successor.
