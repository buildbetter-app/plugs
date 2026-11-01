# R1 preparation harnesses

These scripts prepare the later R1 gate without touching managed, staging,
production, or customer resources. They are intentionally safe-by-default:
`managed_postgres_failover_drill.sh --plan` and
`rollback_rehearsal.sh --plan` only write disposable plans under
`/tmp/plug-swarm/work/`.

## Managed-Postgres failover drill

`managed_postgres_failover_drill.sh --local-preflight` creates two disposable
containers from the pinned PostgreSQL image on an internal Docker network,
probes readiness, and removes both containers. It is a topology smoke only; it
does not claim streaming replication, promotion, Plug workload, RPO/RTO,
managed HA, or production behavior.

The actual bounded A1 workload is already implemented by the exact-main
`internal/integration/e5acceptance:local_wasi_test` cases
`TestPublicHTTPClaimNextTwoHundredAgentsPostgresFailover` and
`TestPublicHTTPClaimNextTwoHundredAgentsPostgresLateFailover`. To run those
cases on disposable RunsOn resources, pass a full local commit to
`--runs-on`. The wrapper uses `/tmp/plug-swarm/bin/remote-bazel`, disables
result reuse, and records the wrapper receipt path. The test receipt must carry
all acceptance fields: acknowledged claims at kill, distinct winners, zero
acknowledged loss, old-writer fencing, and kill-to-first-new-primary commit
seconds.

No DSN or ambient `PGHOST`/`PGSERVICE` is accepted. The image is pinned to
`postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3`.
The existing fixture fixes the load at 200 agents and kills after 1 or 100
acknowledgments; options cannot relabel it as a larger test. The local preflight
pins the Unix Docker socket, publishes no host ports, and always removes only
the resource IDs it created, including on failure.

The plan records the later managed drill sequence and the decided targets
(RPO 300 seconds and RTO 3600 seconds, design P70–P72). It contains no provider
failover action. A managed-topology drill requires separate authorization and
disposable resources, a provider-specific action, an acknowledged-claim ledger,
new-incarnation and old-writer checks, application recovery measurements, and
topology restoration. A1's container timing does not qualify those targets.

## Rollback rehearsal

`rollback_rehearsal.sh --rehearse` accepts only disposable inputs under
`/tmp/plug-swarm/work/`. It copies the Beads source read-only, records a
content digest before and after the copy, freezes the copied tree, and records
pre/post software manifests plus a Plug head-state dump by digest. An optional
`--reverse-command` test adapter receives `INPUT_DUMP OUTPUT_DIR`; it must
write only the disposable output directory. The adapter is trusted code, not
sandboxed: it runs from its output directory with a clean environment, and must
not contain credentials or connect to any external store. Its successful exit
qualifies only adapter execution. Reverse fidelity, loss accounting, software
redeployment, and the complete software/data rollback claim remain explicitly
`UNQUALIFIED` until separately reviewed evidence exists.

Both scripts resolve paths before use and confine work-root overrides, inputs,
and evidence to `/tmp/plug-swarm/work/`. Evidence directories must be new;
rollback sources must contain only regular files and directories, and output
cannot be nested in the source. Frozen copies are a permission-bit rehearsal,
not an operating-system security boundary against their owner or root.

The later R1 gate still requires three clean rehearsals, shadow metrics,
platform safety, authenticated deployment evidence, a production-topology
failover drill within the decided RPO/RTO, and complete software/data rollback.
These prep scripts do not provide those qualifications.
