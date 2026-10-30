# Recorded behavior references

This package contains the first independently recorded reference vectors for
BUILD-09. The vectors were produced from the pinned `beads-base` source commit
`9d6fff760b7a314b476539198504d77ce0959656` in disposable workspaces, then
recorded a second time from a fresh workspace. The committed test checks the
source pin, recorder inputs and hashes, the exact raw-vector digest, and
byte-identical repeatability.

The reference engine is embedded Dolt in the pinned source. These files are
reference expectations only; they do not claim that Plug has passed B17 or B24,
and they do not replace the existing connected acceptance targets. Future
SQLite/Postgres replay work must consume these vectors and record any
projection as an explicit deviation.

Run the focused check with:

```sh
bazel --ignore_all_rc_files test //spec/recorded:goldens_test \
  --test_output=all --lockfile_mode=error
```

The exact recorder sources are versioned as UTF-8 strings in each card's
`sourceFiles[].content` in `goldens.json`, alongside their paths and SHA-256
hashes. Extract those bytes to the recorded paths before using the retained
commands against a disposable copy of the pinned reference source. They are
reference-only inputs; the Bazel test does not compile or execute them.

B17 executes 13 metadata conformance cases, including the ephemeral swap and
exactly-one-history-entry cases. Each outcome is emitted only after its case
passes, and incomplete or failed runs cannot emit a recording. The raw vector
and semantic projection retain the measured durable swap delta (1), durable
no-op delta (0), ephemeral history delta (0), and ephemeral durable event count
(0). Both fresh runs must reproduce those observations.

B24 runs the pinned `bd` binary through fixed past/future defer inputs and
explicit reopen/claim probes. Its raw vector and artifact are unchanged by the
B17 repair. Both recorders use runtime clocks and omit timestamp fields. B24
remains an outcome golden rather than a controlled-time certificate.

Each card retains its catalog disposition with scope `recorded-reference-vector`.
That disposition records the catalog decision; it does not certify the entire
card or cross-driver parity. The recorded census remains two cards (B17/B24);
the other 26 cards, complete store/configuration census, controlled-clock B24,
and connected replay remain outside this package.

`goldens.json` is an immutable reference input because it retains the exact
recorder source bytes, source-test names, commands, and legacy output values.
The lexicon policy admits only `spec/recorded/goldens.json` at its pinned
SHA-256 `26a086334357d55ea847daa301f45c9435a71bba75fb6c324456aebf459fbf32`;
changed bytes and any unlisted path under the immutable reference prefix fail
closed. The validator source is still scanned as ordinary Plug test code. Its source-test
and command evidence is bound by a digest over the manifest arrays, preserving
their exact bytes without duplicating reference identifiers in validator code.

`provenance.py` independently projects each fresh raw artifact and emits its
provenance. It checks every reference source blob against the local pinned tag,
the executable and recorder hashes, driver version, paired schema inventory,
and configuration/input digests. The schema cursors are the maximum versions
in the pinned migration series; B17 also reads both initialized schema tables.
Each schema digest hashes canonical JSON of the sorted migration paths and
their individual SHA-256 hashes. B24's CLI does not expose raw SQL in embedded
mode, so its schema evidence is the pinned migration inventory.

Run the projector separately for each card and each fresh artifact:

```sh
python3 spec/recorded/provenance.py B17 <artifact.json> <reference-source> \
  <reference-binary> <extracted-recorder-source> <provenance-output.json>
```

The manifest retains each run's canonical projection and provenance bytes with
separate raw, projection, configuration, input, source, and provenance digests.
The validator binds repeated provenance to all retained evidence and rejects
missing or mismatched recorder, driver, schema, configuration, clock/ID/graph,
projection, or disposition fields. Its negative cases remove evidence from
both cards and attempt to rehash incomplete repeated provenance.

## B24 adopted untimed defer deviation

The orchestrator decision of 2026-10-30 07:45Z adopts **an intentional
deviation** from the recorded timed-defer behavior. Plug's application request
is `bead.defer={id, reason?, expect?}`, or
`plug defer ID [--reason TEXT] [--expect REVISION]`. It selects the ontology's
defer slot. A nonempty reason becomes a comment in the same ChangeSet, and
`expect` supplies preconditions. The supported way to make deferred work
workable again is an explicit `bead.reopen`, subject to the ontology and other
ready predicates. Status names come from the ontology.

| Retained Beads projection | Adopted Plug projection |
| --- | --- |
| An expired dated defer wakes on ready and clears its defer time. | The request has no target time and no automatic expiry wake; an untimed defer needs explicit reopen. |
| A dateless defer stays deferred. | Untimed defer stays outside ready; an optional reason is a comment. |
| A future dated defer stays deferred. | There is no future target field or time-based release. |
| An expired dated ephemeral item wakes on ready and clears its defer time. | No dated ephemeral wake is supported or qualified by this record. |
| An expired dated item is claimable and becomes in progress. | Expiry does not make deferred work claimable; explicit reopen restores otherwise-ready work. |
| Reopen makes the reference item open and ready and clears its defer time. | Reopen selects the unstarted slot; there is no target-time field to clear, and existing comments remain. |

[The separate B24 deviation record](b24_untimed_defer_deviation.json) binds
these projections to Beads source `9d6fff760b7a314b476539198504d77ce0959656`,
the immutable reference manifest, and Plug source/contract pin
`db028d2e9369fa0a645c5506507f3774d2a9554c`. It retains exact reference outcome
keys, digests, recorder commands, and source blob/hash evidence. Consumers can
use Bazel data target `//spec/recorded:b24_deviation`.

This record does not rerun the recorder. The raw reference, goldens,
provenance and original tests remain immutable; the older B24 card and its
catalog `keep` disposition remain historical evidence. Both reference and
Plug projections stay visible rather than converting a timed expectation
into an untimed golden. The reference permits an omitted reason but rejects
an explicitly blank one; that differs from Plug's optional reason/comment
shape and the older behavior card's reason/notes wording.

The runtime-clock limitation above and all historical failures remain. This
decision adds no target-time field, wake scheduler or controlled Go/SQL clock
evidence and certifies no positive timed behavior or full B24 parity. The
existing category/reopen and claim-lease tests retain their original limits;
the separate connected acceptance owner verifies the adopted untimed path.
