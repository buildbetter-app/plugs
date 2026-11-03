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

The label-query continuation adds exact membership through the existing
`Filter.Query`: `label` and `labels` are aliases, with label-only Boolean
expressions using NOT > AND > OR and parentheses. Scalar clauses remain root
AND conjuncts and retain their existing token bytes, validation, exact identity
matching and case-sensitive title substring behavior. Separate any groups
joined by AND retain their grouping. The candidate and explanation paths use
one predicate; eventual list/count composition must use that same predicate.
No driver pushdown or countOnly behavior is added here.

Canonical B16 exact values and B23 exact collation select literal `none` and
`null`, including when quoted. This deliberately differs from both Beads
`9d6fff` `query/evaluator.go` `applyLabelFilter` and `buildLabelPredicate`,
and `query/query_test.go:436-440,641-642`, which select label absence; the
pinned OR optimization instead collects raw `LabelsAny` strings. This is the
recorded canonical-card interpretation, not identical pinned DSL parity.

Only label values gain single/double quoting. Decoding preserves exact UTF-8,
case and whitespace; defined newline/tab/backslash/quote escapes are accepted.
Missing or decoded-empty values, unknown/dangling escapes, unmatched quotes,
trailing garbage and scalar-containing OR/NOT/groups refuse with no result or
write. There is no absence alias, inequality or pattern expansion.

Named public-RPC controls in `query_test.go` cover all/any/exclude, Boolean
precedence/grouping, exact case/Unicode/literal sentinel strings, quotes and
operator text, scalar token/error/project preservation, shared explanations,
SQLite reopen, and authenticated fixed-policy non-disclosure. All records
enter through ordinary named writes. The fixed-policy control permits User
reads and denies Agent reads even with a sponsor grant, using the real guarded
read service. It adds no policy or positive Agent-read qualification.

Completion for this query slice requires these same tests unchanged between a
meaningful tests-first public-RPC RED and exact changed-source GREEN, followed
by focused/whole uncached receipts and independent review. Proposed source or
these controls alone do not qualify the frozen producer whole RED, prepared
capture, paired reference execution, full B16/S1, or later integrated counts.
