# Real CP7 product latency baseline

Done = existing PR260 is handed to the train after exact-head independent
source review and focused/ordinary whole RunsOn originals. The original 100k matrix,
including cold first requests, three warmups and 20 measurements per case,
remains the baseline before optimization. Historical latency REDs remain RED.

This is new evidence. It does not reproduce an identified historical product
benchmark. Permissions, APIs, caches, caps and driver settings are unchanged.
Opt-in request-local production hooks observe existing read work; they do not
add a selector or change execution. The original declaration is
[workload.json](workload.json), preserved byte-for-byte.

The corpus contains 100,000 canonical bead documents in one registered project,
in independent chains of eleven beads (maximum parent depth ten). All are
ready issues with priority zero and deterministic titles. Setup writes real
records, versions and the existing core ancestor relation through the driver;
it does not implement authorization. Every body and ancestor row is read back
and checked before measuring. Setup and successful claim resets are outside
the timer. SQLite uses its product defaults, including its synchronous mode.
PostgreSQL is a pinned, disposable, loopback-only container on the runner.

CP7 permits an unrestricted User to read every bead without project grants.
Restricted Users intersect that permit with the adopted `refs` caveat: every
100th or 1000th ID gives exactly 1,000 or 100 readable beads. Parent links never
grant permission. Agents do not borrow User read permission. For claims the
Agent's User sponsor has direct or explicit one-hop team involvement, intersected
with the same resource caveats. These are host-validated fixed-policy facts,
not spike reader grants or a cut closure.

Each store runs all three fractions and these product calls:

- `GuardedService.Ready`: real domain readiness and finite `Chain.Compile` /
  `FilterTemplate.Match`, subject to the existing ordinary list postfilter cap.
  A cap or deadline may stop the baseline before compilation; it stays RED.
- `GuardedService.ClaimNextWithOutcome`: complete selection, current modify
  authorization, Writer preparation, recheck, locks, durable receipt and commit.
  A successful result must change exactly one permitted bead. Validation and
  resetting that bead happen after timing; requests never reuse a timed receipt.
- `viewops.Service.Run`: the existing closed query parser, current read
  authorizer and real paged storage queries. The ordinary grouped view uses
  nonempty fields and preserves the 20,000 cap. It does not use the 100k summary
  exception or B23's later `countOnly` API. Counts cover the complete predicate
  set before paging, including the elided tail.

The timer starts immediately before the service call with a trusted host-bound
principal and ends at its complete return. It includes current authorization,
query work and commit, but excludes authentication transport, token issuance,
setup, oracle checking and reset. This is a service benchmark, not an HTTP or
real-host SLO. The 30s cooperative request deadline is the frozen HTTP product
default; the Writer's shorter existing pre deadline is preserved. Cold means
the first request of an operation after a fresh composition, with no warmup.
Fixture setup has already touched the database and OS caches are uncontrolled.

Each case logs one cold, three warmup and 20 measured `CP7_SAMPLE` JSON records.
`CP7_CASE` records report response p95 for all 20 attempts, and a successful
latency p95 only when **all twenty complete correctly**. Refusals, deadlines and
SLO misses remain explicit RED results with `target_pass=false`. Correctness
errors, unexpected errors, partial results, stale authority, wrong membership,
wrong counts or a false claim receipt fail the Bazel test. A green harness
execution therefore does not assert that the performance targets passed.
API counters retain Query/Get calls and Query output rows; observed keys are
null because Observe/Lock internals are outside the released scope. Additional
counters observe actual payload reconstructions and body bytes (including
discarded candidates), raw Query key scans, prefix metadata state decodes and
encoded bytes, grouping input visits/groups, and item construction. These
hooks also run through concrete WriteTx readers. Product composition receives
the raw store, preserving every optional reader/transaction/store capability.
SQL executor visits, Observe/Lock state decoding, and historical patch replay
bytes remain UNMEASURED. Logical reconstructed body bytes are not replay bytes.
PostgreSQL reports each successfully scanned live current body before snapshot
filtering, including bodies discarded by historical fallback. Reconstructed
historical bodies and repeated fetches are counted at their actual boundaries;
batched reads do not invent repeated current-row scans. Callback counts do not
prove physical SQL work.

The explicit raw-group Q5 regression distinguishes the correct winner from
rotation over authorized members alone. The Ready regression changes an
authorized tail beyond the first page, preserving that page and authorization
generation while requiring the membership-specific cursor refusal.

The ordinary `cp7_dense_semantics_test` keeps both semantic controls in `//...`
with a 20,001-row corpus, the smallest real corpus that still proves the
20,000 ordinary candidate cap. It requires the original nonempty Q6 to return
`scope_too_large` and checks the positive `fields=[]`, `groupBy=["type"]`
summary, including its aggregate work and no-payload-row result. The
engine-specific `cp7_dense_semantics_sqlite_test` and
`cp7_dense_semantics_postgres_test` are tagged `manual`; each repeats those
same controls against the unchanged 100,000-row corpus. The daily and
on-demand workflow selects both manual targets explicitly. Like the existing
correctness checks, these semantic calls use Background; they supply no
benchmark latency or deadline-success claim. The original timed Q6 and
cold/3/20 matrix are unchanged. Accounting explicitly exposes existing
summary item construction even though its output contains no payload rows.

The inherited compiled-authority and zero ordinary payload/aggregation controls
apply at both selected corpus sizes. The full 100k manual targets also retain
`CertifiedIndexedGuardClaimWork`, with the original root-with-child winner,
fresh indexed membership observations, physical accounting lower bounds,
claim receipt/reset and complete coverage/corpus checks. The 20,001-row ordinary
control supplies no full-size indexed-claim qualification. The small authority
recomposition and queue semantic controls remain in ordinary `//...`.

Run the complete manual latency qualification explicitly through the approved
remote wrapper:

```sh
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD cp7-qualification -- test \
  //internal/integration/cp7latency:cp7_latency_test \
  --cache_test_results=no --flaky_test_attempts=1 \
  --test_env=GO_TEST_WRAP_TESTV=1 --test_output=all \
  --test_timeout=900,900,900,9000 --jobs=2 --local_resources=memory=8192
```

The scheduled/on-demand workflow uses the same wrapper-equivalent Bazel
selection explicitly, once for each engine:

```sh
engine=sqlite # or postgres
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD cp7-dense-$engine -- test \
  //internal/integration/cp7latency:cp7_latency_${engine}_test \
  //internal/integration/cp7latency:cp7_dense_semantics_${engine}_test \
  //tools/lexicon:lexicon_gate \
  --lockfile_mode=error --jobs=2 --local_test_jobs=1 --local_resources=memory=8192 \
  --nocache_test_results --test_env=GO_TEST_WRAP_TESTV=1 --test_output=all \
  --test_timeout=900,900,900,9000
```

The ordinary whole-tree command uses the same options with `//...`. It includes
`cp7_correctness_test` and the 20,001-row `cp7_dense_semantics_test`, while the
two full-density semantic targets and both timed engine targets carry the
`manual` tag and stay out of automatic expansion. Explicit suite execution
retains the ordinary controls plus both 100k semantic targets, all 18 timed
cases, cold requests, three warmups and 20 measured attempts per timed case,
with every existing assertion, cap and deadline unchanged. Running the full
suite may execute the two stores concurrently; measurements retain that
workload context. An ordinary whole PASS does not qualify latency or erase
historical REDs.

Full S1 remains 0/28 and E3 remains 4/5 until actual proof. This baseline does
not qualify product authorization races, terminal claims, the 200-agent runtime
or a durable SLO.
D productization requires a later independently reviewed PR against this
harness. The reviewed D fixture and rejected split-lock variants are unchanged.

W11 current-main controls are in `cp7_qualification_test`: complete grouped
pagination at 1,000 bodies/all three fractions, a real publication during the
pinned readiness callback on both engines, and strict measurement/refusal
validation. They exercise the existing public fallback composition. PR388's
compiled Selection/final-authority/terminal tests and six timed summary cases
remain separate, unadopted dependency-bound work; these controls supply none
of their qualification. The original 18-case workload and deadlines stay intact.

The accounting regressions exercise real PostgreSQL current/historical/staged
reads, absent/deleted keys, duplicate output, repeated fetches and observer
parity, including refusal precedence. The stored-row scan and reconstructed
historical-body boundaries are measured separately from emitted output clones
and from SQL executor physical work. W3's returned-clone observations are a
separate composition; this child does not adopt them.
