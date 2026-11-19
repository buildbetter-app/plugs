# Native receiving recovery

Done = an admitted source whose expanded workflow image exceeds 64 MiB recovers
successfully, oversized encoded plans remain refused, and a post-commit
verification failure identifies the committed revision and original receipt key,
proven by the bounded regressions, the complete graph certification focus, two
independent source reviews, and a whole-suite run; diagnostic runs do not count.
Scope is native receiving publication and its workflow image inspection.

## Independent bounds

`plugins/workflow/source_restore.go:InspectSourceReverseImage` inspects an
already bound provider image. The encoded plan is admitted through
`driver.NewSourcePlaneImportBindingV1` and the unchanged 64 MiB codec bound.
The expanded logical image retains plan declarations and raw source records,
including JSON encoding of byte buffers; its aggregate size can exceed that
plan bound. It must not be rejected by applying the plan bound a second time.
`sourceWireRecords` still bounds raw record bytes and count independently.
Before decoding, inspection checks a composed aggregate and borrowed section
lengths. Plan declarations allow JSON Unicode escaping of the encoded-plan
bound; retained record JSON allows base64, escaped DTO identities and closed
per-record framing, while decoded raw bytes and record counts keep their original
limits. Mutable runs, items, receipts and audit each use the existing plan-byte
constant as their section limit. No numeric limit constant is raised.

Inspection uses the caller context and NativeDelta budget. Compatibility callers
without a budget receive the existing default decoded budget, never unlimited
allocation. Input, decoder, decoded and canonical candidates reserve measured
bytes before decode. Raw clones share that context; origin encoding and result
ownership reserve before allocation. Temporary reservations release on every
exit, with only successful detached results retained by the caller owner.

Canonical encoding, complete field census, origin identity and graph checks
remain required. Preparation retains the admitted NativeDelta budget and its
measured refusal and ownership rules. No constant increases.

## A committed import with failed verification

In `receiving_stream.go:restorePreparedNativeReceivingStream`, successful
`prepared.Commit` and commit identity validation precede `receivePlan` and
`recoverSourceImport`. Failures from those two post-commit checks now report
that the import committed, with its revision and receipt key. The diagnostic
cause is formatted with `%v`, deliberately without unwrapping to a conflict
or cancellation that would imply publication was rejected. The caller can
resolve the original key through `RecoverImport`; it must not choose a new
key to retry publication. The wire catalog maps this ordinary failure to
`internal_error`, not a conflict. The native receiver's error message preserves
the revision and key; the general wire catalog sanitizes implementation text.
P8's `ErrIndeterminate` and original-key recovery path remain unchanged: an
uncertain commit and a known committed import with failed verification are
different observations.

The regression forces post-commit snapshot failure, checks the serialized
wire error code, extracts the message key and recovers its actual original
receipt, including after closing and reopening SQLite. The size regression
uses a single large raw record instead of an 8000-item graph. The focus still
includes the original graph test through its 8000 cases on all stores.

## Resource failures during inspection

Inspection preserves measured decoded-budget refusals and context cancellation
or deadline causes through its origin, text and field checks using wrapping.
Recovery and reverse adapters distinguish those resource failures from malformed
source images: resource errors retain their typed identity, while semantic
invalidity keeps its established conflict or reverse-refusal mapping. This does
not change the separate known-postcommit sanitizer described above. Deterministic
allocation observers exercise cancellation after admission and a shared budget
exhausted at the next commitment charge, including actual recovery/reverse calls
and complete failed-owner cleanup.

The caller regression uses the actual admitted import profile for receipt lookup,
with a bounded record/version/log/derived/blob census and an explicit workflow
checkpoint comparison. It makes no assumption about the legacy restore identity.

## Known-commit ownership handoff

The receiving planner's private payloads now have a shared, explicitly closable
lifetime. Publication snapshots use their caller's publication owner, rather
than silently charging the leaf plan. The logical input has a separate owner,
so releasing a consumed plan cannot leave inspection borrowing uncharged bytes.
The receiving transaction and prepared evidence remain charged through
`recoverImportReader`: its history and native-family checks are their last live
uses. Only a successful commit with the expected stream identity can install
the subsequent `beforeInspection` handoff. That handoff reserves the returned
family/schema/history/binding/receipt fields, closes the finished writer,
discards publication arrays and plan views, closes the consumed plan, and
releases publication and preparation owners before the first inspection input
reservation. Single-set verification also closes its consumed private stream;
a collection retains its spool/current archive because the returned report owns
those original-file readback resources. No source archive or retained result is
released merely to make an admission succeed.

The deterministic small public-capture test observes the real commit path. At
the first inspection input reservation it checks every live category against
the retained source baseline, the separately held logical input and the exact
returned-result reservation. Its first four-candidate peak is exactly that
handoff total plus four measured logical lengths. A second independent import
uses the first import's measured peak as a small explicit limit: it succeeds
without the released simultaneous ownership, while retaining the dead scopes
would exceed that same limit. It verifies returned ownership and final cleanup.
The admitted two-GiB ceiling and every source/plan/section limit are unchanged.

Plan copies now share the complete payload holder, including profile bytes,
canonical-plan binding and archive reference. Close clears that holder before
releasing its private owner; a collection report's closed Plan alias retains no
private payload. Tests check both copied-plan nil references and the actual
successful collection report. The replacement result owner transfers immediately
after admission, before writer or preparation close can fail, so cleanup-error
reports remain charged and keep the known revision/key/receipt. A real committed
single-set test forces the preparation-close error and checks its exact result
reservation and final cleanup. Existing per-engine no-transient-leak assertions
include only the independently measured exported-report reservation; all other
categories and charge counts must still match exactly. The small-budget handoff
test compares the complete expected and observed maps, including missing keys.


## Recovery-private readback and inspection scratch

Recovery enables prepared readback only on its private accounting view. It
never changes the original retained archive's readback flag, so clearing the
receiving preparation cannot leave that public root requiring a missing cache.
A direct regression inspects the root flag/cache after recovery and then runs
independent original-source readback. P7's pinned source-plane snapshot keeps its
optional reader capabilities throughout these checks.

Before returned-result admission, any receiving failure clears transient public
payload views while preserving the known durable revision and original key.
After admission, cleanup errors keep the charged result. Reverse inspection
uses local context and archive scopes for lookup maps and preparation clones;
none survive merely because the converted reverse result is returned. Success
and failure accounting checks cover those boundaries.

The strict receiving baselines independently marshal the live exported report
(including the JSON newline) and add its one retained charge to the source and
bound-profile inventory. They still compare complete byte, category and charge
maps. The refusal fixture observes construction and binding as well as import,
then chooses a limit strictly between the measured preceding peak and the
result-admission live bytes. It retains the exact refusal phase, durable outcome,
empty payload and full cleanup assertions without changing production limits.


## Returned recovery owners and reverse consumption

Prepared readback follows ownership. A supplied original receiving root never
changes mode; only the operation's private view uses its preparation. Standalone
recovery returns a newly owned archive whose authenticated cache remains charged
and whose prepared-readback mode survives closure of the pinned source reader.
The direct two-branch regression verifies both flags/cache lifetimes, independent
readback after snapshot close, and the complete live accounting baseline after
the recovered owner is released.

Collection Report.Close clears every owned result reference, including Receipt,
Commit.Results, native history and graph report, before closing the collection
owner. Its real success test checks all payload references, zero outstanding
bytes and repeated Close. Reverse consumes its recovered archive/collection and
releases it after the last use on success and every subsequent error. The reverse
regression reuses one caller budget across successful calls and a deterministic
post-recovery cancellation, checking the complete independent caller baseline
before teardown rather than releasing the parent to hide residual source charges.

B19's strict expected inventory includes the actual exported report measured by
an independent marshal plus LF. The pre-handoff failure fixture builds host intent
in a temporary archive scope so its discarded SourcePlaneRecords do not inflate
the retained source root. Profile outputs remain under the original live owner.


## Committed uncertainty uses the same result handoff

A matching original-key ErrIndeterminate from the real prepared stream Commit
resolves through the existing pinned receipt/history/source verification using
its retained prepared evidence and the same returned-result admission callback.
It uses the bounded uncancelled recovery context, never replays the stream or
opens a second writer, and never reconstructs a returned source beneath a
publication owner that is about to close. Until durable verification succeeds,
a lookup failure preserves the original indeterminate error. Once verified,
postcommit failures retain the known-commit reporting and output ownership rules.

The driver-stage regression delegates actual publication, then returns the
original-key ErrIndeterminate from Publish. For both native input shapes it hits
the real prepared.Commit branch, verifies usable returned source readback,
independently marshals the complete returned report plus LF, checks its live
result reservation and one writer/publication, and verifies exactly one release
with harmless repeated Close. Collection Close also removes owned receipt views.
B19 adds that independent expected result reservation only in full streaming
mode; all non-full modes keep their strict original complete accounting maps.
