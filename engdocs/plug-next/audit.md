# Conditional audit and delivery

All implementation claims on this page use
**`9d03323f423cf28d202b328ed7c116e45db8406f`**. The useful source entry points
are the [pipeline producer](../../internal/core/pipeline/audit.go), the
[tracked ontology adapter](../../internal/core/service/audit.go), the
[connected fixture](../../internal/integration/audit/README.md), the
[event descriptor model](../../internal/core/model/audit.go), and the
[driver changeset header](../../internal/storage/driver/driver.go).

## One close produces one ordered plan

The Writer builds the audit plan after mutators, ontology validation, claims,
and actual-change detection. It scans the committed bead graph at a bounded
snapshot, identifies `conditional-blocks` edges whose blocker changes status,
including proposed changes, and asks the service adapter for a decision. The
adapter reads a protected,
branch-zero `ontology` or `ontology/...` record from that same snapshot,
decodes and compiles it, and evaluates the blocker's own project. A source
locator cannot provide a digest or decision by itself.

An embedding host enables both ordinary lifecycle enforcement and the audit
adapter on its existing Writer:

```go
writer := pipeline.New(store, lease,
    pipeline.WithOntology(workflow),
    pipeline.WithOntologyAudit(service.NewOntologyAudit(locator)),
)
```

Here `workflow` is the normalizer/validator over the real compiled source and
`locator` selects its protected record. An installed authorizer also requires
the host's trusted `WithEventContext`; request parameters cannot supply that
provenance. The CLI's ordinary local composition does not enable the audit
producer automatically.

The commit guard covers the source and edge reads. The final plan contains the
ordinary event followed by notices sorted by dependent ID. A notice pins the
source reference, source revision and compiled digest, both blocker images,
the edge revision, the resolved before/after decisions, and trusted
provenance. The dependent is not rewritten and receives no extra version. A
non-nil plan is validated against the actual changes and the allocated
revision before a driver publishes anything; the complete plan is capped at
1,000 descriptors and the graph scan at 20,000 beads.

The connected fixture uses a real memory store, `service.Service`, `pipeline.Writer`,
the source adapter, event materializer, and fenced checkpoint store. It creates
one blocker and two dependents, closes the blocker, then reads the resulting
events. Its asserted ordering is:

```text
close revision=6
event[0] bead.closed  resource=pl-audit-blocker
event[1] dependency.conditionUnmet resource=pl-audit-dependent-a
event[2] dependency.conditionUnmet resource=pl-audit-dependent-b
source=ontology/audit revision=1 project=blocker-project
```

Run that real fixture and its named cases with:

```sh
bazel --ignore_all_rc_files test //internal/integration/audit:audit_test \
  --jobs=2 --lockfile_mode=error --color=no --curses=no \
  --test_output=all --test_timeout=900 --test_arg=-test.v
```

The fixture's [integration test](../../internal/integration/audit/integration_test.go)
checks this plan, historical materialization, copy isolation, source-change
invalidation, masking, lost-ack retry, and replay suppression. The source
fixture exercises public service create/close and direct Writer
cancel/fail/defer; the current service has no named `Fail`, `Cancel`, or `Defer`
methods.

## Delivery and refusal behavior

The event reader materializes pinned conditional details while applying current
resource authorization. A denied ontology resource suppresses the notice;
missing authorization capability refuses. A checkpoint save is a fenced
generation CAS, so a lost acknowledgement retries the same delivery key and
payload. Post callbacks run only for a fresh commit; replay and durable no-op
receipts do not run them again.

The producer refuses an invalid or unprotected source, a malformed blocker
image, a source race, an invalid edge, a decision that does not match the
final images, and an unsatisfied terminal decision from an untrusted method.
Open or already-satisfied conditions emit no notice. A failed post callback is
a warning on an already committed result. SQL storage keeps the plan in the
changeset, but the connected fixture's complete service-to-delivery proof is
memory-only here; see the [SQL persistence matrix](sql.md).

The [baseline event guide](../plug-runtime/events.md) still describes the
older `471d0199` delivery snapshot. Use it for the unchanged cursor and
at-least-once contract, then use this page for the current tracked-source and
SQL-plan additions.
