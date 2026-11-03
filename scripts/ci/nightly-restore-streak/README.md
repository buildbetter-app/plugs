# Offline nightly restore streak verifier

Done = three distinct consecutive successful scheduled-main UTC nights are
recognized from saved original run/artifact metadata and complete comparisons,
proven by exact source objects, artifact digests and every required result;
two nights, manual dispatches and repeated same-night runs do not count.

This evaluator extends N1 qualification tooling from PR218, whose runtime work
was absorbed through PR228 (verified history matrix) and PR237 (exact checkout
and cursor binding). It leaves their workload, schedule and ordinary CI defaults
unchanged. It does not run stores, fetch GitHub data or modify receipts/source.

Use existing offline evidence, with one directory per run containing:

- `workflow-run.json`: the unmodified native GitHub Actions REST workflow-run
  object, including repository identities, event, branch, workflow path, full
  head SHA, attempt, completion/conclusion and UTC timestamps.
- `artifact.json`: the unmodified native REST artifact object, including its
  workflow-run binding, exact name, ID, size, SHA-256 digest and expiration state.
- `artifact.zip`: the original downloaded artifact bytes. An extracted or
  repackaged artifact cannot replace the original digest-bound ZIP.

The artifact's root must contain the existing workflow's `run.json`, `exit_code`,
`bazel.log` and `test-results/` tree. The three comparison reports retain their
existing schemas and locations under `test-results/test.outputs/`.

```sh
python3 scripts/ci/nightly-restore-streak/verify.py \
  --source-repository /path/to/offline/plug \
  /path/to/saved/night-one /path/to/saved/night-two /path/to/saved/night-three
```

The local Git repository must already contain every recorded source commit and
its fixture blobs. The verifier resolves each SHA/tree separately without
replacement objects, compares it to the exact checkout receipt, and checks report
fixture hashes against those source blobs. Source heads may differ across nights.

Every supplied run must be a completed successful first-attempt `schedule` event
on `main` in this repository and the fixed nightly workflow. It verifies the native
artifact/run/repository identity and ZIP size/digest before reading the original
checkout receipt, zero Bazel exit, uncached execution, and exactly 54 unique named
RUN/PASS/XML entries without failure/error/skip. Comparison completeness requires
all six snapshot restores, all thirty history restores and all fifteen typed
no-write refusals across memory, SQLite and PostgreSQL. It checks census presence,
fixture bindings, history revision coverage and equality across each six-store
comparison group. The original driver tests remain the semantic oracle; this
tool does not replay restores or invent new report data.

Dates come from native `created_at` in UTC, including GitHub's scheduling delay.
Input order does not matter. Duplicate IDs/dates, gaps, retries, manual runs,
failures, expired/mismatched artifacts and missing/contradictory evidence block
the whole supplied set; the verifier never filters them out to find a green
subset. Three or more complete consecutive nights pass, reporting the latest
three. Zero, one or two complete consecutive nights remain `WAIT`.

The CLI emits JSON and exits 0 for `PASS`, 1 for `BLOCK`, or 2 for `WAIT`.
It trusts the provenance of the supplied native metadata; unsigned offline JSON
cannot independently prove authenticity or discover omitted GitHub runs. A pass
is bounded public fixture scheduled-main evidence, not source-exporter,
real-ledger, full S1, production, G2 or G3 qualification. It applies no wall-clock
freshness policy beyond the requested consecutive-night contract.

The Bazel test target is `//scripts/ci/nightly-restore-streak:verifier_test`.
Its sanitized realistic records, ZIPs and throwaway Git objects are explicitly
**synthetic evaluator tests**, with fake run IDs and dates. The normal CLI
refuses those labels. The test-only Python evaluation mode emits `TEST_PASS`
with `qualified=false` and cannot grant actual qualification; there is no CLI
switch to enable it. These tests never supply a real third receipt. The recorded
actual nights 41388680824 and 41578175388 remain two nights; actual three-night
qualification stays `WAIT` until actual evidence exists.
