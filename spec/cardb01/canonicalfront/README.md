# Canonical public B01 ready-front counterpart

This is new B01 operational acceptance based on saved landed main
`2902006beaf1f271dfc86237dd5f3bdcc64f1f89`. The closed `0d4e24b2` public and
hosted unit remains separate. The new parent and exact-priority selectors extend the existing ready/list filter
contract; the closed unit remains separate.

The required controls compare real canonical Beads public `ready` output with
the corresponding normal Plug `bead.ready` byte-RPC results on memory, SQLite,
and PostgreSQL, retaining actual SQL close/reopen evidence:

| Control | Required observed result |
| --- | --- |
| Active status and exclusions | Open/custom-active work is eligible; closed, pinned, and hidden infrastructure rows are excluded. |
| Deferred ancestors | Record timed and indefinite native vectors across durable/wisp dependency planes; retain Plug's adopted transitive deferred-ancestor suppression separately. |
| Blocked front and page limit | Ineligible front rows cannot consume the requested qualifying page. |
| Parent transitivity | An open parent alone does not block; parent filtering includes actual descendants. |
| Stable default order | Preserve literal native priority/created/ID default, separately explicit hybrid 48-hour order, and Plug's P74 stored bucket/rank vector. |

The native recorder reuses B20's unchanged canonical source-build recipe and
the declared `//spec/s1-b09-native:lexicon_files` source archive/manifest, plus
`//internal/publicnative` and its pinned `dolt_runtime`. There is no second
general source builder or official producer action. Actual source, binary,
request, raw stdout/stderr and projection hashes must accompany runtime claims.
Native fixtures use disposable public stores only.

Native wall-clock observations, explicit controlled fixture times, the stored
Plug order, and the adopted one-minute order-lag allowance are distinct facts.
No fixture claims uncontrolled cutoff-boundary parity or clock-expiry behavior.
Source/parser/synthetic passes alone do not qualify native behavior or all B01
prerequisites. The exact changed head needs independent source review, named
focused acceptance and the canonical seven-control whole gate with
`--lockfile_mode=error`. No earlier head's result transfers here.

The pinned public CLI defaults to `--sort priority` (`cmd/bd/ready.go`);
the behavior card's hybrid-default wording describes a different lower-level
empty-policy default. Default argv is retained without an override. The corpus
contains old P0 versus recent P2 and reverse-insertion equal-time ID ties.

Native `no_history` rows are imported into and read back from `wisps`; every
parent/child dependency table and target plane is checked from actual SQL.
The corresponding Plug scenarios intentionally use ordinary durable documents:
this proves their public semantic counterpart, not a raw native-plane import.
The public `service.Filter.parent` selects transitive descendants for ready and
direct children for the history snapshot list, matching the two native commands.
Both use the certified storage hierarchy index before ordering and pagination.
Native and Plug parent vectors are compared, with
exact priority/type/resource intersections, positive limits, complete page counts,
unknown roots, parent/priority-bound cursor refusals and actual SQL reopen.
The dedicated parent corpus includes a great-grandchild, a separate unrelated tree,
an outside-subtree blocker, and high-priority blocked/hidden descendants. Native
`list --sort id` is compared with the receiver's existing `history.query` snapshot
list order and direct-child membership; ready priority/hybrid/P74 vectors remain
distinct. The actual `4ade` native twins disproved the earlier assumption that
JSON list selected all descendants. The binding correction preserves the
transitive ready oracle and changes list's oracle to the observed direct set.
Memory offers no persistent ephemeral-workflow checkpoint capability. No
substitute plane, automatic timed wake, background rank-worker
SLA or full B01 qualification is claimed.

`canonical_public_test` executes the new three-engine public controls.
`record_native_test` explicitly builds the unchanged canonical source using
`//spec/cardb20:native_build_support`, records two fresh native stores, and
compares their literal vectors with the named public outcomes. It is manual
because canonical source build is explicit public operational recording.

Parent membership is selected using certified same-snapshot storage hierarchy
reads. Ready traverses descendants; list reads direct children once. The root is
excluded; unknown roots are empty in both commands. Missing index coverage,
cycles and indexed bounds refuse without a partial membership/count.
Mixed durable/ephemeral provider parent filtering still requires a complete
same-snapshot capability and refuses explicitly; ordinary durable parent parity
is proven separately on all three engines. No protected writer is changed.

Done = supported ordinary durable parent/priority membership and complete ordered
pages match actual canonical public observations and the declared adopted
vectors on all three engines, proven by independently reviewed exact source
and original focused/canonical whole receipts plus source/ELF/request/raw bindings;
source-only, parser or predecessor results do not count.

The saved SQL SelectionReader deliberately refuses all plans. This change reuses
the storage-owned certified hierarchy index to compile a bounded descendant
set before the existing service orders/pages complete candidates; it does not
create a second SQL selection executor or change index publication. All errors
and bounds return no partial set. Standalone total-count RPC is unavailable;
complete concatenated page cardinalities and every prefix are asserted instead.

The binding B01 parent interpretation accepts this certified indexed mechanism;
compiled SQL pushdown, a standalone exact-count executor and 100k-scale cost
proof remain a separate selection-lane gap. `parent_bounds_test` seeds actual
certified memory/SQLite/PostgreSQL source snapshots through the existing typed
driver conformance helper. These large bound fixtures are storage controls,
separate from the ordinary Writer and native semantic counterparts above.
A direct 1025-parent request refuses with ErrUnsupported and no rows/More,
while a complete 2001-parent frontier succeeds through 1024/977 batching.
A real 10001-child lookup exposes More and the descendant filter refuses with
filter_too_broad(limit=10000). A distinct 20010-descendant tree keeps every
lookup below 10000 and refuses with limit=20000, returning no partial set.
Both overflow paths reach normal ready byte-RPC errors with no result/page/count.
The 10001 direct children also exceed list's single-lookup bound. The separate
20010-descendant tree has 2001 direct children: list returns that exact complete
set across 1000/1000/1 pages and preserves its limit-one prefix. SQL reopen retains
the transitive total refusal and complete direct list. The large fixture maintains
its normal 10-second lease with the existing 3-second renewal interval, joins the
renewal lifecycle before release/reopen, and records any renewal error. Production
bounds, TTL and controlled Go/SQL clocks are unchanged. Exact source/journal/rank
digests before and after reads remain equal.
