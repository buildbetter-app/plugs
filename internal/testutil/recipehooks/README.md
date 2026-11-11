# Public recipe retained-store handoff

The complete `TestPlugDumpCompleteRootFromPublicDoltActions` recipe and partial
`TestSQLSourceHistoryReadsPinnedDoltCommitImages` recipe accept the test-binary
flag `-plug-recipe-retain-store=/tmp/plug-swarm/<private-parent>/<fresh-name>`.
Select exactly one recipe when using the flag. The immediate parent must already
exist, be private (mode 0700), and have no symlink aliases. The destination must
not exist. Use `BEADS_TEST_DOLT_SERVER=local` and the repository's pinned Dolt
test environment. This flag is never registered on `bd` or another command.

By default the flag is empty: neither source lifecycle nor output is changed.
With the flag set, the recipe closes its SQL clients, stops its own isolated
server, and copies the entire data root (including Dolt metadata) at the clean
export point. Ordinary recipe cleanup still removes the original root. The
caller owns the retained directory and its cleanup. A rejected destination is
never overwritten, and a failed copy removes only its newly created directory.

The complete recipe stays stopped after the export. The partial recipe restarts
its original server and runs the existing dirty-working-set refusal; that later
mutation is absent from the retained copy. This preserves its original controls
and does not change the recipe's SQL or operations.

`//cmd/bd:public_recipe_handoff_test` and
`//internal/plugdump:public_recipe_handoff_test` execute both default-off and
enabled paths with pinned local Dolt required. The enabled controls read the
retained database after the original recipe cleanup, using the actual same-run
exported identity for the complete recipe. The shared helper's tests cover
disabled output/lifecycle preservation, stop-before-copy ordering, byte retention,
path refusals, and failed-copy cleanup.

These are public synthetic hook controls. They do not record or qualify a native
continuation comparison, a typed capture, historical archive equality, or private
data. Fresh native recording and consumer comparisons are separate work.
