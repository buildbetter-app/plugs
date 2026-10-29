# Plug package map

This map is pinned to the merged source snapshot
`18f7883b6b7d8f9edf8a6bb9f1effaa4e88c10ce` (accepted `main`, 2026-10-29).
It describes code that exists at that snapshot; a design document or an open
branch is not merged behavior.

## Layers

| Layer | Package | Responsibility at the source snapshot |
| --- | --- | --- |
| Core values | [`internal/core/model`](../../internal/core/model/doc.go) | Closed JSON domains for refs, principals, `ChangeSet`, commits, warnings, events, history and typed errors. It imports neither storage nor transport. [`changes.go`](../../internal/core/model/changes.go) and [`precondition.go`](../../internal/core/model/precondition.go) define the operation union and atomic batch bound. |
| Core workflow | [`internal/core/ontology`](../../internal/core/ontology/types.go) | Compiles project workflow/type documents and evaluates their fixed rules. It is a core compiler/evaluator, not a storage engine or transport. |
| Core authorization | [`internal/core/authz`](../../internal/core/authz/types.go) | Pure finite contracts, dependency identities, chain evaluation, filter compilation and the host-side existing-record mutation guard. It has no installed policy and never opens a transaction ([package boundary](../../internal/core/authz/doc.go)). |
| Storage contract | [`internal/storage/driver`](../../internal/storage/driver/driver.go) | Internal `Driver`, `Store`, `Reader`, `WriteTx` and optional receipt/checkpoint capabilities. Only drivers and the core write path may use `WriteTx`; plugins receive no engine handle. |
| Merged storage | [`internal/storage/memory`](../../internal/storage/memory/store.go) | Disposable copy-on-write store implementing the driver contract, leases, snapshots, history, receipts, no-op commits and checkpoint CAS. `New` creates an independent store; `Driver.Open` accepts only an empty DSN or `:memory:` with no options. |
| Shared verification | [`internal/storage/conformance`](../../internal/storage/conformance/conformance.go) | Engine-neutral `Factory` and real-store cases for lifetime cleanup, G1–G5, receipts, checkpoints, history and pagination. The package does not import the memory implementation. |
| Authentication adapters | [`internal/server/authn`](../../internal/server/authn/authn.go), [`internal/plugins/authnapikey`](../../internal/plugins/authnapikey/apikey.go) | The server package admits transport credentials and stamps identities. The optional API-key provider retains only credential digests in a process-local key ring; issuance and revocation are trusted host operations. Neither supplies interactive human proof. |
| Authorization adapter | [`internal/plugins/authzfixed`](../../internal/plugins/authzfixed/rules.go) | The adopted finite CP7 rule set and validation of complete team/project/grant facts. The host owns persistent source facts, management, transaction guards and query execution; this package is not a management API. |

`pluginapi`, [`sdk/go`](../../sdk/go/doc.go) and [`cmd/plug`](../../cmd/plug/main.go)
are placeholders at this snapshot. They do not establish a versioned plugin
ABI, a client implementation, an RPC server or a CLI command surface. Audit
descriptor/emission work is also a follow-on and is not represented here.

## Storage ownership in one sentence

The model owns values and wire validation; the driver owns storage boundaries,
leases, snapshots, lock identities and typed storage errors; the memory package
owns the disposable implementation and resource cleanup; the core write path
must choose semantic predicates and call the driver without exposing its
transaction to plugins. The pure authorization package supplies predicates and
guards, while runtime adapters bind authenticated context and validated facts.

Persistent SQL drivers are deliberately absent from this source snapshot.
[SQLite PR #31](https://github.com/buildbetter-app/plugs/pull/31) and [Postgres
PR #33](https://github.com/buildbetter-app/plugs/pull/33) are open follow-ons;
their branches must not be used as evidence for the merged package map.

For the storage lifecycle, G1–G5 guarantees, receipts and maintainer commands,
see [`storage.md`](storage.md).
