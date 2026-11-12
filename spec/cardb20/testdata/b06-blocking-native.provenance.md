# B06 public mixed blocking diagnostic request and independent expectations

`b06-blocking-native.json` is an authored public/synthetic operational request
and oracle, SHA-256
`5dcc2babbbd85903149137264a278b48b27db6c8a3a885d5c07942ae139d4c0c`.
It contains no captured outputs, private source data, or import payload. It is
not a native capture set and is never submitted to an importer. Native results
are unqualified until the actual remote execution records them.

The existing authenticated source-build port uses commit
`9d6fff760b7a314b476539198504d77ce0959656`, tree
`cde121d6e4119e454857ad1c3ca293e2791be840`, from its existing committed source
archive and Git manifest. The new manual target uses that unchanged port and
two independently initialized public disposable databases. Every command,
actual schema, SQL row image and canonical detector output is retained by the
existing recorder. This introduces no producer, importer or lifecycle service.

The request first creates the declared durable and ephemeral members through
the actual public native CLI and adds an acyclic chain. The two cycle-closing
ordinary mutations must refuse without changing source data. Explicit SQL
diagnostic insertions then install those facts, outside ordinary admission.
The expected first path is a -> z -> y -> b -> a; its lowest ID is a. The second
is c -> v -> c. Parallel a -> z facts must leave those two paths unchanged.
Actual native metadata selects the single endpoint uniqueness index relaxed
for that diagnostic. Actual dependency-family foreign-key DDL is retained
before removing hydration for z while preserving every dependency row.

The complete/parallel paths and original edge requests remain unchanged. Only
for the missing fixture, the three a -> z facts move to a -> w, and one w -> z
blocking fact is inserted by explicit diagnostic SQL. The independently
expected bridge path is a -> w -> z -> y -> b -> a; c -> v -> c stays unchanged.
The extra w member is isolated during the original complete/parallel checks.
This preserves z's two incident facts on surviving workflow neighbors when
its hydration disappears, without publishing workflow refs as durable Graph
source facts.

The independent Plug expectations are authored in `b06_blocking_support_test.go`
and use public workflow invocation labels and returned addresses, not a copy of
the native oracle. Explicit driver/checkpoint diagnostic seeding supplies the
otherwise forbidden blocking cycles. The durable Graph declaration contains only b -> a, using canonical durable
refs. Cross-plane relations use workflow checkpoint Links/Inbound. Removing z
retains w's outgoing fact and y's inbound fact. The public five-member result
must retain z at position 2 (zero-based) with an unreadable placeholder; the
native result retains its raw ID and absent image.
Both raw originals are retained, and explicit invocation-role correspondence
connects them without rewriting either result.

The exact native deletion oracle is independently derived from the pinned
source, not observed output. The ignored migration
`internal/storage/schema/migrations/ignored/0004_add_wisp_aux_fks.up.sql`
adds `wisp_events.issue_id -> wisps.id ON DELETE CASCADE`; dependency constraints
are separately removed by the existing diagnostic fixture, and the event
constraint remains installed. `internal/storage/issueops/create.go` emits one
created event for z. `internal/storage/issueops/dependencies.go` emits one
dependency_added event for the successful ordinary z -> y blocks edge.
`helpers.go:RecordEventInTable` stores its payload in new_value, with old_value
empty; `derivedid.go:normalizeAuxEvent` stores unset wisp event values/comments
as empty strings. The authored event signatures therefore require exactly one
created row with empty old/new/comment and exactly one dependency_added row
with empty old/comment and new_value
`Added dependency: b06-z-block-z blocks b06-z-block-y`.
Only z's hydration row and those two victim-owned event rows may disappear.
Their full preimages remain recorded. All unrelated rows, including other
wisp_events, and both complete dependency families must retain exact values.
This is diagnostic cascade/conservation proof, not history-import proof.

Memory proves process-lifetime state with Persistent false. SQLite and
PostgreSQL each physically reopen and reconstruct service/provider/RPC for both
complete and partial images. All reports conserve the exact committed core,
history, receipts and checkpoint. Existing mixed tracks, waits-for, installed
policy, ordered refusal/replay and landed diagnostic tests remain unchanged.

Only this new missing behavior executes in the new native target. No prior
native unit is replayed as a new qualification. All historical RED/incomplete
originals, the late focus scheduling breach, and separate source/focus/whole,
hosted/native/history/full-card qualifications remain literal. Official input
is UNSPENT; private data is HARD_DISABLED; FullS1 receives no source-only credit.
