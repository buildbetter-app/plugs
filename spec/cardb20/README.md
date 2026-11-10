# B20 current working-set events

This fresh slice extends the retained `spec/s1-b17-b20` tests without changing
that package, its recordings, or PR370's source-owner hunks. Its base is
`0d90d7a06dc14abed19cfbfab990ae4ae5a0b562`.

Completion requires original exact-candidate RunsOn execution, independent
source review, and two independently initialized public reference stores.
Authored source, an old recording, or a passing target with missing named cases
does not qualify it. The controller receives the immutable source tuple and
original focused and canonical whole-run receipts separately.

The four observation groups use actual service/Writer/byte-RPC behavior on
memory, SQLite, and PostgreSQL:

1. Committed create/update/close rows, a same-value no-op, a transactional
   missing-target refusal, a successful next write that proves no revision was
   burned, ascending one-row pages, and a stable caught-up cursor.
2. Live subscription delivery and reconnect in the same runtime/signing-key
   lifetime, compared to the actual committed pull projection.
3. Actual SQLite/PostgreSQL close/reopen, exact journal conservation and a
   subsequent write. Memory checks only the same store. Old signed cursors
   never cross the reopen.
4. A real host installation of `authz/fixed` with its validated project/grant
   facts, production Writer authorizer/event context, and a narrow event-read
   adapter calling the production checker on fresh real-store snapshots.
   Allowed events carry the actual chain marker; a sequential restrictive
   project caveat filters them. Trusted RPC principal injection does not
   qualify public authentication or an operator policy-management API.

The explicit `record_native_test` reuses the existing admitted-main exact
source build recipe in `spec/s1-b09-native/pinned_source_test.go`, its immutable
archive/5087-entry Git manifest, and `internal/publicnative`'s pinned owned Dolt
runtime. It builds the production executable from native source
`9d6fff760b7a314b476539198504d77ce0959656`, preserving canonical validations,
Bazel 9.3.0, Go 1.26.10, the actual executable/build receipts, raw version,
commands, output bytes/digests, and process cleanup. Only its build/version
label identifies this public B20 fixture. No exporter or importer runs.

Each independent source store records actual current `events` audit rows in
`(created_at,id)` order, `bd_events_journal` rows in sequence order, and the
`bd_events_seq` singleton, with read-only single-statement SQL. It also retains
the current issue rows. The source's supported CLI writes run with journal
activation applied after `ChildEnv`, and actual persisted configuration is
read back. A real batch updates the issue and then refuses on its second,
missing-target operation; a successful close follows. Original follow-process
delivery must equal the final journal export. Positive-limit since-exclusive
and caught-up reads are independently observed. Raw rows, timestamps, and
identities remain in each recording; only explicitly selected non-temporal
projections must match between the twins.

The 0062/0064 rule is binding: these three event families are local unversioned
working-set state. The recorder never creates per-commit event images or uses
an archive as a replacement for current rows. Native `next_seq` is observed as
source head evidence and is never restored as a Plug revision. For this
single-mutation-per-commit corpus only, actual ordered source operations are
compared to actual ordered Plug changesets. A4/P6 explicitly adopts always-on
changesets instead of opt-in journaling, ontology `todo`/`done` instead of
native `open`/`closed`, and separately named fixture principals. Original
source attribution is retained before that explicit comparison projection.
The audit and journal projections remain distinct.

The native CLI accepts a zero limit as unbounded; Plug requires bounded pages.
Although the old B20 card says nonpositive limits refuse, this slice preserves
that exact-pin difference and compares positive limits only. It covers no
comment/engine-only vocabulary, pruning, concurrent writer, claim overlap,
compare-and-set, cross-process cursor, full-card, hosted-delivery, official
producer admission, private store, live switch, or trial qualification.

Run all builds/tests through `/tmp/plug-swarm/bin/remote-bazel` from this
isolated worktree. The focused command selects `:current_test`,
`:record_native_test`, the owning event/storage targets and the full lexicon
gate, with the seven canonical controls. The native target additionally needs
the trusted RunsOn variables `BAZEL_BIN`, `BAZEL_RC`, `BAZELISK_HOME`,
`BAZELISK_VERIFY_SHA256`, `GITHUB_RUN_ID`, `GITHUB_RUN_ATTEMPT`, `GITHUB_SHA`,
`GITHUB_WORKFLOW_REF`, and `PLUG_B20_REQUEST_SHA=<exact-candidate-sha>` forwarded
as test environment. The runner supplies its own checkout root.

The unchanged canonical whole command is:

```text
test //... --cache_test_results=no --flaky_test_attempts=1 --test_env=GO_TEST_WRAP_TESTV=1 --test_output=all --test_timeout=900,900,900,9000 --jobs=2 --local_resources=memory=8192
```

Runtime recordings and comparisons are retained in original test output
artifacts and the author packet under
`/tmp/plug-swarm/reports/S1-B20-events-current-20261110`. Before their actual
execution their literal status remains `AUTHORED_NOT_RUN`/`UNRUN`.
