# Epic progress qualification

This executable EP6 harness crosses real loopback HTTP, API-key admission,
the installed fixed checker, hierarchy Writer, snapshot-bound view adapter,
and physical SQLite dump/restore/reopen. It has no replacement progress
responses, prewritten bead rows, skipped capability checks, or reduced-load
fallback. The protected ontology fixture alone uses the conformance seed port.
Status setup and mutation use real lifecycle slot verbs on a separate admitted
anonymous loopback fixture host over the same owned store. The fixed policy
continues to refuse those lifecycle routes. Hierarchy writes, authorization,
counts and revocation use the authenticated fixed-policy endpoint; the fixture
host supplies no count responses or hierarchy writes.

The scenarios check direct children against grandchildren and unrelated beads;
visible status counts with an authorization-only hidden remainder; a refresh
without a hidden-page cursor; historical content with current authority;
credential revocation and current write-grant revocation on receipt replay;
child/new-parent permissions without an old-parent grant; existence-only
hidden-parent disclosure; cycle, project-move and mixed-batch atomic refusals;
explicit capability refusal; and hierarchy persistence after native head-state
restore into an empty store and a physical SQLite close/reopen.
Restore also compares every canonical hierarchy closure key/value before host
composition and after physical reopen, then requires the existing cycle
refusal and a successful real detach with updated counts and closure rows.
The fixture never repairs missing derived state.

The load case retains 20,000 actual Writer-created beads and 50 distinct
sponsored agents. It enumerates persisted records and verifies all direct
children before asking the fifth task's progress question. This is a runtime
behavior check, not a usability threshold or human study. The verbose result
records the actual `view.run` request duration separately from corpus creation,
along with the observed total and status counts; it imposes no new latency target.

Run only through the authorized RunsOn wrapper at a committed, integrated head:

```sh
export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD ep6-focused -- test //internal/integration/epicprogress:epicprogress_test --cache_test_results=no --test_output=all
/tmp/plug-swarm/bin/remote-bazel "$PWD" HEAD ep6-whole -- test //... --cache_test_results=no --test_output=errors
```

EP5 owns the actual embedded progress screen and its real anonymous
Writer/viewrpc/Chrome path. Its browser test must be run at the same integrated
head; an earlier presentation fixture is not integrated proof. The existing UI
does not accept authenticated sessions. These API-key HTTP tests do not claim
authenticated browser support or a real hidden-count browser outcome. The
anonymous no-plugin browser and fixed-checker HTTP proofs remain distinct.

Historical E3 remains at the accepted four of five until EP1–EP5 are separately
implemented, reviewed, admitted and this complete integrated proof is reviewed
independently. Draft or ancestor-head evidence cannot raise that score. No
staging, production, customer data, managed cloud, release or deployment is
part of this harness. Restore uses a fixture-only trusted presence verifier
on the separately admitted anonymous loopback admin route, never an API-key
user masquerading as an interactive administrator.
