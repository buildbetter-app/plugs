# S1 slice 4: B13–B16

This package is a bounded, exact-head Plug replay lane for four behavior cards.
It runs the admitted behavior through the real service, Writer, storage driver,
RPC registry, and in-process byte transport. Each selected Plug case is run in
fresh memory and SQLite stores; SQLite cases close and reopen the same file.
The semantic projector is independent of response assertions and compares the
two fresh runs byte-for-byte after selecting only declared stable fields.

The selected bounded evidence is:

* **B13** — memory content/key fidelity, Beads-compatible omitted-key
  derivation through public RPC, replacement, search, deletion, ordinary-bead
  plane isolation, typed blank/underivable refusals, and SQLite reopen.
* **B14** — admitted manual-gate approval through the real RPC/Writer path,
  sponsor-resolved duplicate suppression, terminal slot settlement, and
  SQLite reopen, plus actual `waits-for` link persistence and reopen. Explicit
  any/all member metadata, `also_blocks`, public gate creation/listing, and
  the full gate-member contract remain unqualified.
* **B15** — exact comment body/principal/order and controlled same-clock stamps,
  one durable changeset/event per accepted comment, untouched anchor fields,
  typed refusals with no receipt/head advance, and SQLite reopen. A public
  comment-list/keyset producer and the ephemeral plane remain unqualified.
* **B16** — public typed label creation and persistence, plus typed no-write
  refusals for invalid values and the unsupported filter shape. The separate
  `internal/integration/labelcontract` package exercises the default public
  producer and revision-guarded whole-set patch composition. Full filters,
  durable/ephemeral rename and paired qualification remain **UNQUALIFIED**.

Reference recording is deliberately separate from these Plug projections. The
`record_reference.py` runner requires an exact Beads executable and exact
source pin `9d6fff760b7a314b476539198504d77ce0959656`, creates fresh disposable
stores, and refuses to emit an artifact when either input is absent or drifted.
No authored expected value is stored as a reference recording. This checkout
does not retain a reference artifact: the pinned executable/build and the
required disposable RunsOn invocation are an external dependency for this
slice. Full-card qualification, staging/reference/nightly/fortnight evidence,
Postgres parity, reverse import/readback, and B1 are not claimed.

The tests intentionally use fixed IDs, actors, request keys, cursor key,
store identity (memory), and UTC clock values. The SQLite driver supplies the
real SQL clock/storage boundary; these cases do not claim Postgres coverage.

`//spec/s1-b13-b16:capture_regressions` executes the downloader through an
intercepted urllib transport with fake credentials and blocked sockets. Its
synthetic ZIP/tar cases test destination and redirect authorization, pin/hash
refusals, containment, and executable reconstruction after mode stripping.
GitHub credentials are allowed only on the exact pinned HTTPS API endpoint;
public/signed object routes receive none, redirects are checked individually,
and unsupported routes refuse. The runner runtime token is never used.
Only the verified regular 0755 tar members receive execute bits after all
bundle guards pass. These source regressions do not record Beads behavior or
authorize an actual reference capture.
