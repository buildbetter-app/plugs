# E5 runtime and concurrency spike

This package is a stop-line harness for the runtime candidate. It does not
install a plugin host, change the storage contract, or claim production
failover. The default run uses the existing memory driver; the PostgreSQL run
uses the same driver-facing harness against a disposable DSN.

## Candidate ABI

The candidate has two function exports and one host import:

```text
plug_alloc(size: i32) -> i32
plug_handle(ptr: i32, len: i32) -> i64
env.plug.call(ptr: i32, len: i32) -> i64
```

`plug_alloc` returns guest-owned linear-memory space. The host copies exactly
one JSON-RPC message into that range and calls `plug_handle`. The guest passes
the pointer and byte length to `env.plug.call`; the host returns a packed
`(ptr,len)` result, with the pointer in the high 32 bits and the length in the
low 32 bits. The returned range remains guest-owned until the next allocator
operation. The host rejects empty, invalid, or larger-than-1 MiB messages.
`spikes/runtime/abi_test.go` executes this module under wazero and checks that
the import/export set is exact. A host error propagates as a failed call.

The fixture is a minimal WebAssembly module with one memory page and no WASI
imports. It is intentionally smaller than a Go `wasip1` artifact so the ABI
can be reviewed independently of the source builder. A source-built Go guest,
artifact digest lock, pool quotas, and trap disposal are follow-up work.

## Sandbox and egress

`EgressPolicy` is deny-by-default. A request must be HTTPS, have no userinfo,
match an exact declared `host:443`, and resolve only to public addresses. The
resolver is injected in the tests, which proves loopback, RFC1918,
link-local, multicast, and unspecified targets are refused without opening a
network connection. Redirects are checked hop by hop; a secret grant is valid
only for the exact resolved host and cannot cross a redirect. The ABI fixture
has no filesystem, socket, or environment import. This is a policy harness,
not an operating-system sandbox or an HTTP client.

## Lock order, lazy claim, and leases

Each write constructs one complete sorted lock set using the driver's canonical
order: records, sentinels, then derived rows. The harness rejects unsorted and
duplicate sets before any staging. `claimNext` walks candidate IDs lazily;
each candidate is locked with skip-locked semantics, its ready state is
rechecked while locked, and the ready list is reread after a full pass. A
mutator uses the same total order and updates only its locked record.
The PostgreSQL driver prepares previously unseen lock-row placeholders through a
reserved connection and avoids nested pool acquisition while a writer holds a
transaction connection; this keeps the wide probe bounded by the driver's
96-connection pool without changing the storage contract.

The acceptance run starts 200 claimers and 200 mutators together over 200
ready records. Every invocation gets a fresh run ID, which namespaces its
record IDs, lease holders, and idempotency keys; callers may pass a validated
`ProbeConfig.RunID` when they need a reproducible replay test. The runner
fails if the current invocation's claim counter is not exactly the configured
record count, even if an existing database already contains rows that look
claimed. It injects one stale-snapshot conflict through the real driver,
records the retry, then releases the active lease while a transaction is
staged. The stale transaction receives `ErrFenced` and cannot publish. A fresh
holder observes a higher epoch. The stress lease is extended to the probe
deadline so the 10-second production lease does not expire while the database
is under load; the production default is unchanged, and explicit release still
proves stale fencing. The report records the failover decision: managed HA
PostgreSQL, 5-minute RPO, 1-hour RTO, and a new incarnation on every uncertain
lease-epoch change. A local harness cannot prove managed HA or the production
topology; those remain separate admission gates.

## Reproducible commands

From the repository root:

```sh
go test ./spikes/runtime -run 'TestCandidateABI|TestEgressPolicy|TestLockOrder' -count=1 -v
go test ./spikes/runtime -run TestMemoryE5ProbeRunsConcurrentClaimersAndMutators -count=1 -v
go run ./spikes/runtime/cmd/runtime-spike -engine=memory -output=/tmp/e5-memory.json
# Optional: pin a validated namespace to prove replay refusal on a reused DSN.
go run ./spikes/runtime/cmd/runtime-spike -engine=postgres -run-id=review-replay -output=/tmp/e5-replay.json
```

For the real PostgreSQL lane, start a disposable loopback-only container and
keep the DSN in the calling environment. Do not place credentials in a
command, report, or artifact:

```sh
docker run --rm --name plug-swarm-build-06 -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_DB=plug_e5 -p 127.0.0.1::5432 postgres:17.11
export E5_POSTGRES_DSN='postgres://postgres@127.0.0.1:<port>/plug_e5?sslmode=disable'
go test ./spikes/runtime -run TestPostgresE5ProbeWhenDSNIsProvided -count=1 -v
go run ./spikes/runtime/cmd/runtime-spike -engine=postgres -output=/tmp/e5-postgres.json
docker rm -f plug-swarm-build-06
```

The focused Bazel target is:

```sh
bazel test //spikes/runtime:runtime_test --test_output=all --cache_test_results=no
```

The PostgreSQL test is skipped when `E5_POSTGRES_DSN` is unset; the skip is a
deliberate boundary so ordinary Bazel runs never contact an external database.
