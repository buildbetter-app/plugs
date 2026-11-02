# Public label field contract

Done for this producer = the normal default-ontology public RPC path stores
sorted exact labels, rejects values over 255 Unicode scalars atomically,
preserves labels through other writes and SQLite reopen, inherits a tracked
parent's labels at creation, and records only actual set changes, proven by
the named memory/SQLite tests and uncached exact-head focused/whole receipts;
custom schema fixtures or invented label patch objects do not count.

The existing `fields.labels` string array is a whole-set replacement.
Add/remove, remove-wins ordering, and bounded rename fold into ordinary
revision-guarded `bead.update`/`tx.apply` sets. There is no new namespace,
parameter, side table, or client-asserted author. Empty entries are dropped;
nonempty case/space/Unicode values remain exact. The production authorization,
ontology, idempotency, and history pipeline stays in the path.

Reference source is Beads `9d6fff760b7a314b476539198504d77ce0959656`,
`internal/storage/issueops/{labels,aggregate,execution}.go`,
`internal/types/types.go:CheckFieldLen`, and `cmd/bd/create.go` default
parent-label inheritance. These source-derived expectations are not genuine
paired Beads recordings. Full B16 filter/view, ephemeral isolation, and paired-store
qualification remain separate until their real public paths are proven.

The real authenticated fixed-permission HTTP composition is also exercised.
Its recorded finite slice permits assigned-bead scalar updates and equal label
sets, refuses permission-bearing label reassignment, and rechecks current
authority on receipt replay. This producer does not widen that policy.
