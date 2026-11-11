# B21 public batch acceptance

This independent acceptance package sends real HTTP requests through the public
registry, Service and Writer on memory, file-backed SQLite and disposable
PostgreSQL. It retains inherited suites without changing importer ownership.

The cases cover declaration order and backward keys, create/update/close in one
batch, typed later-key and final-item refusals, the actual 1,000-item bound,
one durable revision for a real metadata rewrite, no-op history, stored replay
without post delivery, and indexed close results. A mixed close refusal proves
Plug's atomic ChangeSet; native indexed survivor commits remain a separate
accepted deviation. Repeats preserve the first close reason and time: empty or identical reasons
are no-ops; a different nonempty reason appends one comment as the binding
17:19 Plug deviation. Legacy unknown reasons follow the 17:24 no-backfill rule.
SQL cases close and reopen the real store and public endpoint before readback.

The ephemeral case follows adopted PR467 and the binding 17:14 reconciliation:
all three engines perform real public invocation, batch mutation and exact item
readback without core durable history. Memory asserts process checkpoint
lifetime and Persistent()==false; only SQLite/PostgreSQL promise reopen. The
older workflow-contract memory-refusal text is superseded by that ruling.

The explicit manual native target reuses the admitted pinned source builder
and disposable database ports from the existing acceptance package. It records
new public HTTP observations and independent SQL expectations, including the
native item Changed=false / durable metadata delta counterexample. It performs
no importer, producer or official recording action. All execution occurs through
remote-bazel on RunsOn. Source, focused, canonical whole, native, hosted,
history and full-card qualifications remain distinct until originals establish
each one. Public disposable fixtures only; private HARD_DISABLED; official
UNSPENT.
