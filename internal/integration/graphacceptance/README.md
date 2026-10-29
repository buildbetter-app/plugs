# Graph connected acceptance

Done = the supported graph slices below execute through the real service,
ontology, Writer, memory driver and file-backed SQLite driver, with exact
history and failed-write projections. These tests are additional acceptance
coverage, not a frozen Beads golden or a claim that every behavior card is
complete.

The fixture uses `service.Create`, `service.Ready`, `service.Close` and
`service.History` where those methods are bound. `bead.link`, `bead.unlink` and
the failure lifecycle slot are not bound by the current RPC registry; tests
label those calls as direct production `pipeline.Writer` operations. No graph
evaluator, seed record, alternate store or status-field bypass is used.

| Card slice | Executed test | Evidence asserted |
| --- | --- | --- |
| B06/B07/B08 ordinary, conditional and association edges | `TestB06B07B08GraphCardsAcrossRealEngines` | A multi-bead diamond stays blocked until both blockers close; ordinary dependents settle after close; conditional edges distinguish real failure from completed success; an open `relates-to` target has no readiness effect; close guards refuse blocked dependents without changing graph state. |
| B06 cycle refusal and conservation | `TestB06CycleRefusalAndFailureConservationAcrossRealEngines` | Self and two-node blocking cycles return `ontology_guard_failed`; records, graph sentinel, log, versions, head and receipts remain unchanged after each refusal. |
| B07/B21 aliases and mixed guards | `TestB07B21AliasesAndMixedBatchGuardsAcrossRealEngines` | A create plus backward-key link resolves the actual ID in one revision; forward aliases and graph operations mixed into a close slot refuse atomically. |
| B07 edge history | `TestB07SupportedEdgeHistoryReplayAcrossRealEngines` | The real driver reconstructs no edge, the link, and the unlink at exact revisions; `service.History` exposes the corresponding bead revisions (its projection has no Links field). SQLite retains all three states after close/reopen. |
| B06 competing links | `TestB06CompetingLinkRaceAcrossRealEngines` | A scheduling-only pre-validator barrier pins both first attempts at revision 2. Real ordinary/conditional graph guards and `graph/blocking` locks allow one commit at 3; the loser retries at 3 and refuses its cycle without publishing a receipt or endpoint version. |
| Unbound public surface | `TestGraphUnboundPublicMethodMatrix/<method>` | Each catalog method outside the actual bound service/server set has a named matrix entry asserting a catalog-valid `method_not_found` response and no graph mutation. |

Parent/child, waits-for, external targets, custom edge kinds, graph diagnostics,
`claimNext`, condition-unmet audit events, and PostgreSQL qualification remain
outside this slice. Unbound RPC entries explicitly refuse; no complete-card or
reference-parity claim follows. The existing `spec` acceptance and open extended
acceptance lane own memory/RPC lifecycle paths; this package adds cross-engine
multi-bead checks, pinned competing writes and durable edge replay without
changing those frozen sources.

Run the target with:

```sh
bazel --ignore_all_rc_files test //internal/integration/graphacceptance:graphacceptance_test \
  --jobs=2 --test_output=all --lockfile_mode=error --test_timeout=900
```
