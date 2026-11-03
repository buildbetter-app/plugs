# B16 captured source label event through public consumers

Status: `AUTHORED_NOT_RUN`. This test-only child retains the frozen 81b
source, focus, whole and review units unchanged. None of that evidence
qualifies this new control. No test, build, capture or gate ran for this child.

Done = the retained public producer's actual label-add boundary is preserved
through admitted full-history import, public historical list/count and a
revision-guarded live rename across real SQL reopen, proven by complete
exact-child runtime receipts and independent review; authored assertions,
archive registration and inherited PASS do not count.

Scope is one connected producer/consumer seam in
`TestB16CapturedLabelEventMapsToHistoricalPublicFilterAndRenameAfterSQLReopen`.
It adds no copy of the existing nine `b16labels` controls and changes no
production, validator, policy, schema, RPC binding or reference source.
The only shared edit adds this package's five paths to the source manifest.

The adopted contracts are [B16](../../../engdocs/design/plug/spec/behaviors/content.md#b16--labels-and-label-filters),
[command folds](../../../engdocs/design/plug/spec/commands.md), and the
[typed label contract](../labelcontract/README.md). Rename consumes complete
current public bead projections and folds to an ordinary typed patch in one
`tx.apply`, guarded by the observed bead revision. Counts are planned until
that exact ChangeSet commits. The importer must preserve valid label values
for this consumer; a caller must not repair a malformed projection to bypass
the strict planner.

## Retained producer and independent oracle

The unchanged `converterroundtrip:public_source_fixtures` data dependency
provides `public-complete-root-990eb800.tar` (78,848 bytes), SHA256
`a2d933a7a54b53303d33a20d5b96eaac0a85c079c5b08cae553aa2a654d820b5`.
The producer commit is `990eb8006a7e3979fef2e4e1b1fbda1324677704`, tree
`9d026de307ee915953b247cf42758d0c53303574`; retained command/source receipts
are 41376934076/41376943877. Its existing
[provenance](../converterroundtrip/testdata/public-source-990eb800.md) describes
public synthetic SQL schema fixtures with real source-driver actions and
Dolt commits. This is existing bounded exporter evidence, not a new native
reference capture or independent reverse import/readback.

The oracle reads raw tar and JSON with standard library decoders. It does
not use `dumpformat.HistoryImage`, the importer, matcher or planner to derive
membership or new event IDs. It pins `history/source.json` SHA256
`049b7602c5360e6be2b69ad25a5fae0cb77b7a1fc2af2b18da9a41eb71242b62`,
then reads the observed bead's exact labels and the actual cumulative event
sets. Event `1cea927e-0758-53e6-847b-cf53140d1962` first appears in sequence 6.
The source snapshot before it (5) has no labels; sequences 6 and 7 retain
`public-source-label`. These are observed bytes, not an authored reference
recording. The import mapping uses source sequence as import order, without
claiming those Plug revisions reconstruct source transaction identity.

## Authored observations

| Phase | Required observation |
| --- | --- |
| Actual admitted importer | `InspectImport`, trusted test-host admission, `BindHistoryImport` and `RestoreImportWithLease` consume the untouched archive. Full mode publishes head 8/floor 0/new incarnation and its real bound receipt. Source author/event/time never supplies host authority. |
| Source event mapping | Every stored source commit/new event ID maps to the independently decoded frame at revisions 1 through 7; the label event belongs to 6. |
| Historical public consumers | `bead.get` and public `view.run` list/count at 5, 6, 7 and import completion 8 match the raw labels. The negated filter proves an empty label match is a retained bead without the label. Destination membership is zero throughout those snapshots. |
| First SQL reopen | SQLite closes/reopens its actual file and PostgreSQL its actual pool. Complete durable state and all historical public observations survive before the migration seam is attempted. Memory is a real third driver without a claimed physical reopen. |
| Live public migration | Unmodified current `bead.get` results enter `PlanLabelRenameFromResults`. The one selected imported bead yields renamed=1/merged=0 and one typed labels patch guarded by its actual current revision, then public `tx.apply` commits revision 9. |
| Source conservation | Only that live bead gains one version. Its other fields/metadata, imported versions, earlier log, unrelated records, import receipt and source mapping remain exact. Historical source labels remain unchanged while the current old-label count becomes zero and destination count becomes one. |
| Second SQL reopen | Complete durable state and both historical/current public observations survive another physical file/pool reopen. |

Read-only windows and each SQL reopen compare metadata, exact record bytes,
every record's full versions, complete changeset log, all nine derived tables,
and both the actual import receipt and attempted public write receipts.
The PostgreSQL fixture starts the existing digest-pinned engine on private
loopback, checks its real version and uses a fresh owned database; missing
engine access fails instead of skipping. No external substitute DSN is used.

## Source-backed expected result and open dependency

Desired behavior is success through the connected observations above. Current
source inspection predicts a RED at the migration seam: `buildPlanMode` in
`converterroundtrip/restore.go` constructs typed fields without `labels` while
setting `Document.Labels`; `service.beadValue` and `rpc.beadJSON` preserve the
asymmetry; `renameProjectionLabels` requires matching `fields.labels` and
`labels` and refuses with `invalid_params`. This is a source prediction, not
an observed execution result. The test first checks SQL reopen, then demands
successful planning without modifying producer bytes or the public response.
If planning refuses, it checks typed failure, an empty plan and complete state
conservation before failing the regression. It does not accept refusal as the
success contract or relax the strict validation.

Any causal importer repair requires a separate S1 production-path allocation
and a new candidate. No production repair is part of this child. This seam
does not require a successful ephemeral or mixed-plane operation; that policy
remains unadopted and dependent on B26. No such operation is invented here.

The existing `spec/s1-b13-b16/record_reference.py` requires a pinned executable,
source and separately released capture. Its bounded label add/remove/show
receipts and projection do not establish paired filter/rename behavior, and
its README retains the missing reference artifact dependency. No reference
artifact is created, extrapolated or promoted by this package. Paired/full
B16, actual reverse import/readback, native/reference admission and full S1
remain unqualified: `FullS1=0/28`, `NativeAdmission=None`.

The frozen external packet carries one literal proposed ordinary remote-bazel
command for `//internal/integration/b16sourcehistory:b16sourcehistory_test`
and the lexicon gate with verbose uncached output, one flaky attempt and CP7
timeouts. That proposal is `NOT_RELEASED`; S1 owns review and any execution
release. There is no whole-tree or reference-capture command in this task.
