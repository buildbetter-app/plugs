# B14 fixed-policy public member regression

Done = ordinary public subjects cannot acquire core-owned gate members through
an admitted update, and current permission governs receipt replay without
reapplying stale public readiness membership, proven by authenticated loopback
HTTP, the installed fixed checker, Writer event/history/receipt conservation,
and physical SQLite reopen; the PR294 host test policy does not qualify this
fixed-policy edge or full B14.

Scope = one isolated regression on fresh memory and file SQLite. The exact
production `authzrpc.New` handler owns authentication, the unchanged fixed
chain, read filtering, ontology, tracked audit, and the single product Writer.
The existing hierarchy option admits sponsored-agent creation; it adds no
link, resolver, lifecycle, or transaction permit. Only protected default
ontology configuration is seeded. Every subject, template edit and receipt is
created by authenticated public RPC. Grant replacement uses the existing real
`MutableFacts` host seam and publishes its own ACL revision.

`TestB14FixedPolicyGateMemberRefusalPreservesPublicMembershipAndCurrentReplay`
checks these behaviors independently on `memory` and `sqlite`:

* Each of `gate_kind`, `gate_state`, `gate_approvals`, `gate_approval_count`, and
  `gate_details` mixed with otherwise admissible title/template edits refuses
  with `ontology_use_slot_verb`. The complete content, versions, changeset/event
  history and metadata remain identical; no refused-key receipt exists.
* A corrected ordinary template update can reuse a refused key. Exactly one
  fixed-policy-attributed write/event and final receipt publish, and public
  readiness excludes the template while retaining an independent control.
* A subsequent ordinary update restores membership. Replaying the older
  template response returns its original commit without applying its stale
  image to today's subject or readiness set.
* Missing/invalid API credentials conserve all stored state. Revoking the
  sponsor's actual project grant refuses both accepted-key replay and a fresh
  update, preserves the original receipt, and creates no fresh receipt.
* SQLite closes the HTTP server, releases the lease, closes the driver, and
  opens the same database with a new composition and the revoked facts. It
  preserves identity, head, all content/history, membership and receipt, and
  still refuses replay. No source/content is reseeded. Restoring the actual
  grant then permits the original exact receipt without reapplying its image.

The oracle declares the reserved member list, typed refusal, complete public
field image, and ready membership explicitly. It calls no ontology evaluator,
pipeline document decoder, canned response, or test policy to derive an
expected result. Generated subject IDs are bound only from public create
results. Administrative readers inspect actual complete content/version/log
pages and the authenticated principal/method/key receipt scope. The source
uses real driver time and makes no timing claim.

This ordinary-subject conservation edge does not create a configured gate,
resolve an approval, construct a waits-for graph, or qualify gate listing,
any/all/also_blocks semantics, PostgreSQL, paired Beads behavior or full B14/S1.
The fixed host currently has no configured gate-kind ontology input and its
finite policy refuses named links/resolver verbs. Those production composition
and independently recorded full-card dependencies remain with S1; this test
does not substitute a permissive host or privately seeded gate graph for them.

Source status: **AUTHORED_NOT_RUN**. Runtime, build, test, remote dispatch and
publication were not allocated. After source review and controller allocation,
the proposed isolated argv is:

```sh
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
/tmp/plug-swarm/bin/remote-bazel "$PWD" <frozen-candidate-sha> s1-b14-fixed-policy -- \
  test //internal/integration/b14fixedpolicy:b14fixedpolicy_test \
  --nocache_test_results --flaky_test_attempts=1 \
  --test_env=GO_TEST_WRAP_TESTV=1 --test_output=all \
  --test_timeout=900,900,900,9000
```

This target has one top-level test, two engine subtests, and five reserved-member
subtests per engine. Execution evidence belongs only to its matching SHA/tree
and receipt; none is inherited from existing B14 tests.
