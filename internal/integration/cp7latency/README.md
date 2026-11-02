# Real CP7 product latency baseline

Done = a new 100k CP7 product harness is reviewable before optimization, proven
by named, uncached SQLite and PostgreSQL executions with cold first requests,
three warmups, 20 measurements per case and exact-head whole-tree RunsOn;
historical fixture measurements do not count.

This is new evidence. It does not reproduce an identified historical product
benchmark. Production code, permissions, APIs, caches, caps and driver settings
are unchanged. The workload declaration is [workload.json](workload.json).

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
Driver counters count API calls and returned rows, never executor visits.

Run all declared cases through the approved remote wrapper:

```sh
remote-bazel "$PWD" HEAD cp7-focused -- test \
  //internal/integration/cp7latency:cp7_latency_test \
  --nocache_test_results --test_output=all --test_arg=-test.v \
  --test_timeout=900,900,900,9000
```

The final whole-tree command uses the same options with `//...`. Only these
new tests declare the eternal timeout class at the frozen base; existing test
budgets remain 900s. The scheduled workflow gives each engine its own RunsOn
job. A combined whole-tree run may execute the two stores concurrently, so its
measurements also record that broader workload context. No test is manual,
filtered out or skipped to make the whole gate pass.

Full S1 remains 0/28 and E3 remains 4/5. This baseline does not qualify product
authorization races, terminal claims, the 200-agent runtime or a durable SLO.
D productization requires a later independently reviewed PR against this
harness. The reviewed D fixture and rejected split-lock variants are unchanged.
