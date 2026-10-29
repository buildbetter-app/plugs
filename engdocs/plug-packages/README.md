# Plug package map

This map is pinned to the merged source snapshot
`3577f0387caf4b96a63dd4bee12a249c0e6d133b` (accepted `main`, 2026-10-29).
Relative source links describe that snapshot; recheck the named SHA when later
merges change a linked file. The design catalogs describe broader intended
behavior than these implemented packages.

## Layers

| Layer | Package | Responsibility at the source snapshot |
| --- | --- | --- |
| Core values | [`internal/core/model`](../../internal/core/model/doc.go) | Closed JSON domains for refs, principals, `ChangeSet`, commits, warnings, events, history and typed errors. It imports neither storage nor transport. [`changes.go`](../../internal/core/model/changes.go) and [`precondition.go`](../../internal/core/model/precondition.go) define the operation union and atomic batch bound. |
| Core workflow | [`internal/core/ontology`](../../internal/core/ontology/types.go) | Compiles project workflow/type documents and evaluates their fixed rules. It is a core compiler/evaluator, not a storage engine or transport. |
| Migration planning | [`internal/core/ontologymigration`](../../internal/core/ontologymigration/README.md) | Builds bounded, digest-bound plans from complete row inventories and compiled ontologies. Runtime authorization, locking and protected apply are follow-ons. |
| Core authorization | [`internal/core/authz`](../../internal/core/authz/types.go) | Pure finite contracts, dependency identities, chain evaluation, filter compilation and the host-side existing-record mutation guard. It has no installed policy and never opens a transaction ([package boundary](../../internal/core/authz/doc.go)). |
| Write path | [`internal/core/pipeline`](../../internal/core/pipeline/options.go), [`internal/core/port`](../../internal/core/port/pipeline.go) | The single `model.Writer` implementation prepares proposals, invokes read-only callbacks, checks guards and publishes through `WriteTx`. `OutcomeWriter` carries transient replay information outside the durable `Commit`. |
| Domain service | [`internal/core/service`](../../internal/core/service/types.go) | Owns domain reads and implemented lifecycle verbs. Writes lower to the supplied Writer; the composition root owns the store, lease and their lifetime. These Go entry points do not establish RPC/CLI bindings. |
| Storage contract | [`internal/storage/driver`](../../internal/storage/driver/driver.go) | Internal `Driver`, `Store`, `Reader`, `WriteTx` and optional receipt/checkpoint capabilities. Only drivers and the core write path may use `WriteTx`; plugins receive no engine handle. |
| Disposable storage | [`internal/storage/memory`](../../internal/storage/memory/store.go) | Copy-on-write store implementing leases, snapshots, history, receipts, no-op commits and checkpoint CAS. `New` creates an independent store; `Driver.Open` accepts only an empty DSN or `:memory:` with no options. Its state lasts only for that store's lifetime. |
| Persistent storage | [`internal/storage/sqlite`](../../internal/storage/sqlite/README.md) | Local file-backed WAL storage, driver-owned file locks, pinned SQL readers and atomic fenced publication. Records, history, receipts and checkpoints survive close/reopen. No standby, replication/failover or automated backup is implemented. |
| Shared verification | [`internal/storage/conformance`](../../internal/storage/conformance/conformance.go) | Engine-neutral `Factory` and real-store cases for lifetime cleanup, G1–G5, receipts, checkpoints, history and pagination. The package does not import the memory implementation. |
| Event delivery | [`internal/core/events`](../../internal/core/events/ports.go), [`internal/server/events`](../../internal/server/events/storage.go) | Delivers committed changesets through read/materialization/authorization ports and lease-fenced checkpoint CAS. Storage and opaque-cursor adapters are implemented; network subscription/watch registration is separate. |
| Authentication adapters | [`internal/server/authn`](../../internal/server/authn/authn.go), [`internal/plugins/authnapikey`](../../internal/plugins/authnapikey/apikey.go) | The server package admits transport credentials and stamps identities. The optional API-key provider retains only credential digests in a process-local key ring; issuance and revocation are trusted host operations. Neither supplies interactive human proof. |
| Authorization adapter | [`internal/plugins/authzfixed`](../../internal/plugins/authzfixed/rules.go) | The adopted finite CP7 rule set and validation of complete team/project/grant facts. The host owns persistent source facts, management, transaction guards and query execution; this package is not a management API. |

[`pluginapi`](../../pluginapi/doc.go), [`sdk/go`](../../sdk/go/doc.go) and [`cmd/plug`](../../cmd/plug/main.go)
are placeholders at this snapshot. They do not establish a versioned plugin
ABI, a client implementation, an RPC server or a CLI command surface. The
merged [audit model types](../../internal/core/model/audit.go) define descriptor
validation; driver persistence, Writer descriptor production and event emission
for those descriptors remain follow-ons at this SHA.

## Storage ownership in one sentence

The model owns values and wire validation; the driver owns storage boundaries,
leases, snapshots, lock identities and typed storage errors; each engine owns
its implementation and resource cleanup; the core write path
must choose semantic predicates and call the driver without exposing its
transaction to plugins. The pure authorization package supplies predicates and
guards. A runtime host still needs authoritative fact/context adapters and a
translator that applies the compiled predicate before SQL pagination/counting;
the reference interpreter is not that integration or performance evidence.

SQLite is merged in this snapshot. [Postgres PR #33](https://github.com/buildbetter-app/plugs/pull/33)
remains an open follow-on at the documentation pass; its branch is not evidence
of merged Postgres behavior. Follow-on package guides can be added as their
source and integrations merge.

For the storage lifecycle, G1–G5 guarantees, receipts and maintainer commands,
see [`storage.md`](storage.md).
