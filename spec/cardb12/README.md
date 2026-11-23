# B12 compaction and reversible snapshots: native vectors and Plug pairing

Canonical card: `engdocs/design/plug/spec/behaviors/content.md` B12.

## What runs

- `//spec/cardb12:native_compaction_record_test` (manual) builds the pinned
  Beads source `9d6fff760b7a314b476539198504d77ce0959656` with the shared
  canonical recipe (`internal/publicnative.SourceBuilder`) and records every
  vector below through public `bd` commands against an owned Dolt sql-server
  (`init --server --external`; native manual `--apply` requires server mode).
  SQL is read-only observation. The recording is compared with the committed
  `testdata/b12-native-vectors.json`; any difference fails the target.
- `TestB12NativeVectorPairing` in `//spec/s1-b09-b12/compaction:compaction_test`
  reads the same committed vectors and runs Plug's public counterpart for each
  vector on memory, SQLite and PostgreSQL. Each pair is labeled **SAME**,
  **ADOPTED DEVIATION** or **KNOWN GAP** in the test table and its log line
  `B12_PAIR`.

Run the native recording through local-bazel:

```sh
/tmp/plug-swarm/bin/local-bazel "$PWD" <label> -- test //spec/cardb12:native_compaction_record_test \
  --test_env=GO_TEST_WRAP_TESTV=1 --test_output=all --cache_test_results=no --flaky_test_attempts=1 \
  --test_timeout=900,900,900,9000 --lockfile_mode=error --keep_going
```

## Native AI provider

Native AI provider stubbed via public ai.base_url; summary content not under
test; Beads source unmodified (pinned 9d6fff76 source build; the built binary's
SHA-256 is logged as `B12_NATIVE_BINARY` and kept in the build receipt).

- `bd admin compact --auto` runs its real client. The public `ai.base_url` key
  in the workspace's `.beads/config.yaml` (appended after `init`, bytes kept as
  `project-config.yaml`) names a stub on `127.0.0.1:<ephemeral>`; AI vectors add
  a dummy `ANTHROPIC_API_KEY`. The stub returns a fixed short summary, or a
  fixed longer one when the prompt carries the `LONG-SUMMARY-FIXTURE` title, so
  native's own summary-not-smaller check runs for real.
- Each stub request's shape (method, path, model, max_tokens, roles, prompt
  length and SHA-256, dummy key header) is part of the committed vectors; every
  prompt is retained in the run outputs. The prompts carry only public fixture
  text.
- Every `bd` child routes HTTP(S) proxying to a loopback sink. Go never proxies
  loopback destinations, so only a non-loopback attempt can reach the sink; the
  recording fails if the sink sees any connection (`B12_NATIVE_NO_OUTBOUND`).

Binary identity: vectors are recorded with the source build of
`9d6fff760b7a314b476539198504d77ce0959656` (built binary SHA-256
`1c24abce808b21e551e1fdb4890fe16ac9fb6e9295dc0f5822e5e8c432b9ce5f`). The same
commands are rerun once with the switch trial's pinned binary
(`9370e9a9234aed2697403a59a7d0c65b6d261c99720d386f5ad446535fc0bdd5`); the run
logs `B12_TRIAL_BINARY identical=<bool>` and keeps
`b12-trial-binary-comparison.json`; a difference is recorded, not failed. The
rerun needs that host-local trial binary path. Result on 2026-11-23: identical
on all 16 vectors (re-checked by each focus run's log line).

Root ruling 2026-11-23: the stub is a test double for an external provider
through public config; it neither invents a callback nor changes Beads code.

## Vectors and labels

| Native vector | Label | Plug counterpart |
|---|---|---|
| apply-open-refused | SAME | open record ineligible, no write |
| apply-recent-refused | SAME | closed under 30 days ineligible; exact 30/90-day boundaries pinned by Plug's controlled clock tests |
| apply-summary-not-smaller-refused | SAME | refused, original intact |
| apply-tier-one | ADOPTED DEVIATION | Plug keeps the original as a permanent history version; native manual `--apply` writes no snapshot row and appends a comment |
| apply-tier-one-repeat-refused | SAME | a tier-one record is not compacted again at tier one |
| restore-after-apply-no-snapshot | ADOPTED DEVIATION | native cannot restore a manual apply; Plug restores via D32-checked `history.revert` |
| apply-tier-two-level-zero-refused | ADOPTED DEVIATION | the pinned native CLI refuses every `--tier 2` ("not yet implemented"); Plug derives the tier from the level, so an old level-zero record goes to tier one |
| apply-tier-two-level-one-refused | ADOPTED DEVIATION | native CLI refuses tier two although its storage eligibility defines it; Plug implements the card's tier two (level one, closed 90+ days) |
| apply-dry-run | ADOPTED DEVIATION | native `--apply --dry-run` writes (level 1, summary, audit comment); Plug dry-run never writes (05:25Z ruling) |
| auto-missing-key-refused | SAME | no summary source, no write (CP6: no outbound key path by default) |
| auto-dry-run | SAME | candidate named, no provider call, no write |
| auto-tier-one | SAME | provider called once, original retained (native snapshot row; Plug history version) |
| auto-summary-not-smaller-refused | ADOPTED DEVIATION | native writes a "skipped" warning comment; a Plug refusal writes nothing |
| restore-preview | SAME | original readable without writing (`history.get`) |
| restore-apply-snapshot | SAME | original text and level zero restored, audit kept; Plug also requires D32 |
| auto-batch-mixed | KNOWN GAP (head-of-line), pending decision | Option A (2026-11-02): one record per `admin.compact` call; a record whose summary is never smaller stays first in line and later records are not reached, while native's sweep continues. Public API shape is with Nikhil |

Snapshot/update fault conservation has no native public vector (native cannot
inject a storage fault through its CLI). It is covered on the Plug side by
`TestB12LateUpdateFailureConservesTextAndPermanentAudit` and
`TestB12SuppliedSummaryValidationAndStagedFailureRollback`, with the native
pinning test `TestCompactTier1_SnapshotError` cited from the card.
