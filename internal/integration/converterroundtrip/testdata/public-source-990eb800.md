These unchanged public archives came from the supported Beads exporter on
RunsOn at source 990eb8006a7e3979fef2e4e1b1fbda1324677704, tree
9d026de307ee915953b247cf42758d0c53303574, based on the assigned
 a8a2a629e6f774928b190f3b9751d6bf95f21ea2. They are paired public synthetic SQL
schema fixtures, with real Dolt commits and Beads-driver actions. They contain
no private or original ledger data. The canonical format is plug-dump/1,
schema 1, with history/source.json in beads-dolt-images/1.

The command receipt is RunsOn 41376934076; the source receipt is 41376943877.
The retained producer packet is
/tmp/plug-swarm/reports/complete-root-source-990eb8006a7e-20261102T022355Z-artifacts/.
Its SHA256SUMS digest is
480ba7c0eca6038f3e4dbf652253cba6a6073102e9734e49717afee0852dd2af.
The exporter executable hash
64a0a7698c16f14aaa0d90510d3ad8ea6132802ad8a150761e8d6a4ce342d478
identifies the canonical Bazel command test executable that ran exportCmd.RunE.
It is not a delivered standalone importer executable or reimport proof.

public-complete-root-990eb800.tar is 78848 bytes, SHA256
a2d933a7a54b53303d33a20d5b96eaac0a85c079c5b08cae553aa2a654d820b5.
Its source history member SHA256 is
049b7602c5360e6be2b69ad25a5fae0cb77b7a1fc2af2b18da9a41eb71242b62.
The manifest is verified: first 1, head 7, complete true, seven real oldest-first
Dolt commits, explicit SQL NULL root parent, empty root image, no baseline or
gaps. Cumulative actual event counts are 0,0,0,1,2,3,4; these correspond to
create, update, label and comment actions. A comment event is not proof of a
portable comment row. SourceEventID orders identity, not chronology.

public-partial-990eb800.tar has SHA256
2dd0bc5cb41ee5ae410659fdff91c4ee6b865098b371a2e1be9b819187271e53.
Its verified suffix is first 4, head 4, complete false: exact prefix gaps are
1 not-retained, 2 unmappable, 3 not-retained. It is distinct from the older
b56f1de4 partial fixture; neither is promoted to complete history.

Actual stored JSON payload.n is 9007199254740992, while BIGINT timeout_ns is
9007199254740993. Preserve these observed values without requested-value
reconstruction. The complete source stores pinned as the exact TEXT value
"0"; all raw source fields and original tar bytes remain unchanged here.
Native Beads/private/original history, actual reimport, independent post-import
readback and complete roundtrip are separate unproven boundaries.
