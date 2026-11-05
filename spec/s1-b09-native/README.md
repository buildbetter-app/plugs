# B09 admitted native recording and paired continuation

`record_native_test` is an explicit manual RunsOn target. The committed main
workflow uses its pinned `70230ccd` public-bundle preparer to validate descriptor
`41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a`
and inject all four `PLUG_PUBLIC_BEADS_*` exports. The test binds the actual
binary, manifest, producer source/tree, run and workflow before execution.
The same run's preparation receipt, helper binding and literal command are
retained by the RunsOn collector alongside the test's command/environment,
raw streams, inventory, 26 sidecars, validated archive and physical file copies.
`native_inputs.json` retains the exact external protocol names as reviewed
data. Its embedded bytes are digest-bound and copied into the runtime receipt;
all admission and source-family comparisons use these independent expectations.

This case explicitly writes the real source file `.beads/config.yaml` with
the exact bytes `export:\n  auto: false\n`. It is the **configured YAML object**
case. It does not repair or clear the genuine initializer's comment-only YAML
omission, including the historical `425d9` RED. The accepted CLI records its
default three-character IDs, explicit-ID refusal conservation, and JSONL
export/import continuation. This corpus has not recorded configured adaptive
length or nonce exhaustion; the native CLI's configuration interface and those
cases still require their own actual recordings.

The exact archive from the third native create is read directly by
`InspectImport` and authenticated-profile `RestoreImportWithLease(snapshot)`
on memory, SQLite and PostgreSQL. Snapshot mode explicitly establishes the
history floor because the producer declares `available-unverified` history;
the test does not fabricate or certify source history. The paired sequence
checks every restored field and source row, complete unaffected records and
versions, exact write/receipt/allocator deltas, ordinary compaction and history
reversal, raw forward patches and actual `HistoryGet` replay of the current
state and preimage, and independent continuation IDs. Immediate complete equality checks
precede both continuation writes after reopen. SQLite/PostgreSQL reopen actual
stores; memory reacquires its lease on the same in-memory store.

Each stage retains all physical driver SQL tables and exact SELECT commands.
The raw restored image is retained before the real host startup
`pipeline.BuildSourceIndexes` call; its separate census checks complete source
images/coverage and exact conservation of all other state before either reopen.
The conservation oracles compare those full row images, preserve unaffected
rows and immutable history/receipts, and permit only the named write or lease
transition. Store and PostgreSQL-container cleanup errors fail the test.
Restore admission and digest-bound presence are test adapters; this target
does not qualify public authentication, hardware presence, source history,
hosted delivery, B09's full adaptive corpus, or FullS1.

Run through `remote-bazel` with the descriptor marker and all seven controls:

```sh
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD b09-native -- test \
  //spec/s1-b09-native:record_native_test \
  --test_env=PLUG_PUBLIC_BUNDLE=41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a \
  --cache_test_results=no --flaky_test_attempts=1 --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_output=all --test_timeout=900,900,900,9000 --jobs=2 \
  --local_resources=memory=8192
```
