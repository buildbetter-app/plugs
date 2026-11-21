package authzrpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// B05 claim interleaving: two authenticated claim requests whose permission
// paths overlap, and authority changes that land between a claim's
// authorization and its commit. The Writer finishes preparation, including
// both authorization stages, before Store.Write; the hook below runs a real
// competing commit at exactly that point, so the claim's commit guard, retry
// and reauthorization are exercised rather than simulated.

// claimRace fires one armed hook at the next Store.Write. attempts counts the
// Store.Write calls made outside the hook, i.e. the writes of the request
// under test; the hook's own competing commit is not counted.
type claimRace struct {
	hook     atomic.Pointer[func() error]
	fired    atomic.Int32
	attempts atomic.Int32
	inHook   atomic.Bool
}

func (r *claimRace) arm(hook func() error) {
	r.attempts.Store(0)
	r.hook.Store(&hook)
}

func (r *claimRace) beforeWrite() error {
	if !r.inHook.Load() {
		r.attempts.Add(1)
	}
	hook := r.hook.Swap(nil)
	if hook == nil {
		return nil
	}
	r.fired.Add(1)
	r.inHook.Store(true)
	defer r.inHook.Store(false)
	return (*hook)()
}

// requireRace checks the hook fired once and the request under test reached
// Store.Write exactly attempts times: 1 when the competing commit makes the
// retry refuse during preparation, 2 when it is reauthorized and commits.
func (r *claimRace) requireRace(t *testing.T, attempts int32) {
	t.Helper()
	if r.fired.Load() != 1 || r.attempts.Load() != attempts {
		t.Fatalf("interleave hook fired %d times and the request wrote %d times; want 1 and %d", r.fired.Load(), r.attempts.Load(), attempts)
	}
}

// Each wrapper embeds the concrete real store, so every optional driver
// capability (checkpoints, guarded checkpoints, response finalization, ...)
// is promoted unchanged; only Write is intercepted.
type memoryClaimRace struct {
	*memory.Store
	race *claimRace
}

func (s *memoryClaimRace) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if err := s.race.beforeWrite(); err != nil {
		return nil, err
	}
	return s.Store.Write(ctx, lease)
}

type sqliteClaimRace struct {
	*sqlite.Store
	race *claimRace
}

func (s *sqliteClaimRace) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if err := s.race.beforeWrite(); err != nil {
		return nil, err
	}
	return s.Store.Write(ctx, lease)
}

type postgresClaimRace struct {
	*postgres.Store
	race *claimRace
}

func (s *postgresClaimRace) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if err := s.race.beforeWrite(); err != nil {
		return nil, err
	}
	return s.Store.Write(ctx, lease)
}

func (r *claimRace) wrap(t *testing.T, store driver.Store) driver.Store {
	t.Helper()
	switch real := store.(type) {
	case *memory.Store:
		return &memoryClaimRace{Store: real, race: r}
	case *sqlite.Store:
		return &sqliteClaimRace{Store: real, race: r}
	case *postgres.Store:
		return &postgresClaimRace{Store: real, race: r}
	default:
		t.Fatalf("claim interleaving needs a real store, got %T", store)
		return nil
	}
}

type interleaveFixture struct {
	*quietFixture
	race *claimRace
}

const secondSponsor = "user:second"

// Two sponsored agents with independent permission paths to one project:
// A (agent:worker) through user:operator's team membership, B (agent:second)
// through user:second's direct project grant.
func newInterleaveFixture(t *testing.T, engine string, dsn func(*testing.T) string) *interleaveFixture {
	t.Helper()
	race := &claimRace{}
	f := &interleaveFixture{quietFixture: newQuietFixtureWrapped(t, engine, dsn, race.wrap), race: race}
	f.base.Grants = append(append([]authz.Grant(nil), f.base.Grants...), secondGrant())
	if err := f.facts.Replace(f.base); err != nil {
		t.Fatal(err)
	}
	return f
}

func secondGrant() authz.Grant {
	return authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: secondSponsor}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("project", model.Natural64(0))}, Role: "involved"}
}

func directOperatorGrant() authz.Grant {
	return authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("project", model.Natural64(0))}, Role: "involved"}
}

func agentA() model.PrincipalRef { return worker("user:operator") }

func agentB() model.PrincipalRef {
	b := worker(secondSponsor)
	b.ID = "agent:second"
	return b
}

// agentC shares agent A's sponsor and therefore exactly A's permission set,
// but is a distinct lease principal.
func agentC() model.PrincipalRef {
	c := worker("user:operator")
	c.ID = "agent:other"
	return c
}

// without returns grants minus every grant equal to one of drop.
func without(grants []authz.Grant, drop ...authz.Grant) []authz.Grant {
	out := make([]authz.Grant, 0, len(grants))
next:
	for _, grant := range grants {
		for _, d := range drop {
			if reflect.DeepEqual(grant, d) {
				continue next
			}
		}
		out = append(out, grant)
	}
	return out
}

func (f *interleaveFixture) replaceGrants(t *testing.T, grants []authz.Grant) {
	t.Helper()
	next := f.base
	next.Grants = grants
	if err := f.facts.Replace(next); err != nil {
		t.Fatal(err)
	}
}

// claimView is the durable claim plane for the seeded bead.
type claimView struct {
	Head     string
	Revision string
	Lease    *pipeline.ClaimLease
	Status   any
	Versions int
}

func (f *interleaveFixture) view(t *testing.T) claimView {
	t.Helper()
	state := f.state(t, agentA(), 0)
	doc, err := pipeline.DecodeDocument([]byte(state.Body))
	if err != nil {
		t.Fatal(err)
	}
	var versions struct{ Items []json.RawMessage }
	if err := json.Unmarshal([]byte(state.Versions), &versions); err != nil {
		t.Fatal(err)
	}
	return claimView{Head: state.Head, Revision: state.Revision, Lease: doc.Lease, Status: doc.Fields["status"], Versions: len(versions.Items)}
}

func (f *interleaveFixture) requireHolder(t *testing.T, got claimView, holder model.PrincipalRef) {
	t.Helper()
	if got.Lease == nil || !samePrincipal(t, got.Lease.Principal, holder) || got.Status != "in_progress" {
		t.Fatalf("claim holder = %+v status=%v; want %s in_progress", got.Lease, got.Status, holder.ID)
	}
	epoch, err := got.Lease.Epoch.Uint64()
	if err != nil || epoch != f.lease.Epoch() || !f.clock.Now().Before(got.Lease.ExpiresAt) {
		t.Fatalf("claim is not live under the current storage lease: %+v %v", got.Lease, err)
	}
}

func requireUnclaimed(t *testing.T, got claimView) {
	t.Helper()
	if got.Lease != nil || got.Status != "todo" {
		t.Fatalf("bead unexpectedly claimed: lease=%+v status=%v", got.Lease, got.Status)
	}
}

func requireCode(t *testing.T, got reply, code model.ErrorCode) {
	t.Helper()
	if got.Error == nil || got.Error.Data.Code != code || len(got.Result) != 0 || got.Meta.Replayed {
		t.Fatalf("want exactly %s without result or replay; error=%+v", code, got.Error)
	}
}

// requireLiveClaimBusy checks the public projection of the Writer's live-claim
// refusal. The wire carries only the catalog message, so callers prove the
// cause from the durable claim plane.
func requireLiveClaimBusy(t *testing.T, got reply) {
	t.Helper()
	requireCode(t, got, "busy")
	if !got.Error.Data.Retryable || len(got.Error.Data.Details) != 0 {
		t.Fatalf("live-claim refusal is not a retryable busy without details: %+v", got.Error.Data)
	}
}

// requireNoDisclosure checks a forbidden refusal carries no claim details.
func requireNoDisclosure(t *testing.T, got reply) {
	t.Helper()
	requireCode(t, got, "forbidden")
	if len(got.Error.Data.Details) != 0 {
		t.Fatalf("forbidden claim refusal disclosed details: %+v", got.Error.Data.Details)
	}
}

// leaseWrite is one public write form that passes the Writer's lease check.
type leaseWrite struct {
	method string
	params func() map[string]any
}

// claimStateWrites are every write form whose refusal depends on the bead's
// claim: claim, heartbeat and release, plus update and close directly and as
// tx.apply operations.
func claimStateWrites() []leaseWrite {
	ref := quietRef()
	return []leaseWrite{
		{"bead.claim", quietParams},
		{"bead.heartbeat", quietParams},
		{"bead.release", quietParams},
		{"bead.update", func() map[string]any {
			return map[string]any{"id": ref, "fields": map[string]any{"title": "interleaved update"}}
		}},
		{"bead.close", func() map[string]any { return map[string]any{"id": ref, "reason": "interleaved close"} }},
		{"tx.apply", func() map[string]any {
			return map[string]any{"changeSet": map[string]any{"ops": []any{map[string]any{"kind": "patch", "target": ref, "fields": map[string]any{"title": "batched update"}}}}}
		}},
		{"tx.apply", func() map[string]any {
			return map[string]any{"changeSet": map[string]any{"ops": []any{map[string]any{"kind": "close", "target": ref, "reason": "batched close"}}}}
		}},
	}
}

func (w leaseWrite) label(i int) string {
	return fmt.Sprintf("%s#%d", w.method, i)
}

func (f *interleaveFixture) requireNoReceipt(t *testing.T, by model.PrincipalRef, method string, key int) {
	t.Helper()
	if f.receipt(t, by, method, key) != nil {
		t.Fatalf("refused %s by %s stored a receipt", method, by.ID)
	}
}

func TestB05ClaimInterleavingAuthorizationAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var dsn func(*testing.T) string
			if engine == "postgres" {
				dsn = postgresDSNFactory(t)
			}
			for _, row := range []struct {
				name       string
				competitor func() model.PrincipalRef
			}{
				{"independent_grants", agentB},
				{"identical_grants_distinct_agents", agentC},
			} {
				t.Run("competing_claim_commits_first/"+row.name, func(t *testing.T) {
					interleavedCompetingClaim(t, newInterleaveFixture(t, engine, dsn), row.competitor())
				})
			}
			t.Run("claimer_revoked_before_commit", func(t *testing.T) {
				interleavedClaimerRevoked(t, newInterleaveFixture(t, engine, dsn))
			})
			t.Run("competitor_revoked_before_commit", func(t *testing.T) {
				interleavedCompetitorRevoked(t, newInterleaveFixture(t, engine, dsn))
			})
			t.Run("redundant_path_removed_before_commit", func(t *testing.T) {
				interleavedRedundantPathRemoved(t, newInterleaveFixture(t, engine, dsn))
			})
			t.Run("holder_loses_permission_after_claim", func(t *testing.T) {
				holderLosesPermission(t, newInterleaveFixture(t, engine, dsn))
			})
			t.Run("ungranted_agent_cannot_observe_claim_state", func(t *testing.T) {
				ungrantedAgentClaimStateHidden(t, newInterleaveFixture(t, engine, dsn))
			})
			t.Run("authorized_non_holder_keeps_lease_refusal", func(t *testing.T) {
				authorizedNonHolderRefusals(t, newInterleaveFixture(t, engine, dsn))
			})
		})
	}
}

// A is authorized and prepared against an unclaimed bead; the competitor's
// claim commits before A's write. Exactly one claim exists and A is refused
// without a receipt.
func interleavedCompetingClaim(t *testing.T, f *interleaveFixture, competitor model.PrincipalRef) {
	t.Helper()
	a := agentA()
	before := f.view(t)
	requireUnclaimed(t, before)
	f.race.arm(func() error {
		got := f.call(t, competitor, "bead.claim", quietParams(), 8101)
		if got.Error != nil {
			t.Errorf("competing claim by %s failed inside the interleave: %+v", competitor.ID, got.Error.Data)
		}
		return nil
	})
	got := f.call(t, a, "bead.claim", quietParams(), 8100)
	f.race.requireRace(t, 1)
	requireLiveClaimBusy(t, got)
	after := f.view(t)
	f.requireHolder(t, after, competitor)
	if after.Versions != before.Versions+1 {
		t.Fatalf("interleaved claims published %d bead versions; want exactly the winner's one", after.Versions-before.Versions)
	}
	f.requireNoReceipt(t, a, "bead.claim", 8100)
	if f.receipt(t, competitor, "bead.claim", 8101) == nil {
		t.Fatal("winning claim has no receipt")
	}
	// The refused key is not consumed: a retry re-evaluates against the
	// current live claim and is refused the same way.
	requireLiveClaimBusy(t, f.call(t, a, "bead.claim", quietParams(), 8100))
	if !reflect.DeepEqual(after, f.view(t)) {
		t.Fatal("retried losing claim changed the claim plane")
	}
	f.requireNoReceipt(t, a, "bead.claim", 8100)
}

// A's only permission path is removed after A's authorization and before its
// commit. The commit guard forces reauthorization under current facts: A is
// forbidden and nothing is published. B's independent path is unaffected.
func interleavedClaimerRevoked(t *testing.T, f *interleaveFixture) {
	t.Helper()
	a, b := agentA(), agentB()
	before := f.view(t)
	f.race.arm(func() error {
		f.replaceGrants(t, []authz.Grant{secondGrant()})
		return nil
	})
	got := f.call(t, a, "bead.claim", quietParams(), 8200)
	f.race.requireRace(t, 1)
	requireNoDisclosure(t, got)
	after := f.view(t)
	requireUnclaimed(t, after)
	if after.Versions != before.Versions || after.Revision != before.Revision || after.Head == before.Head {
		t.Fatalf("revoked claim published a bead version, or the competing ACL commit is missing: before=%+v after=%+v", before, after)
	}
	f.requireNoReceipt(t, a, "bead.claim", 8200)
	success(t, f.call(t, b, "bead.claim", quietParams(), 8201))
	f.requireHolder(t, f.view(t), b)
}

// Only the competitor's grant is removed in A's window. The ACL commit still
// invalidates A's pinned authorization, but A's own path is intact, so A is
// reauthorized and claims exactly once. The revoked competitor then learns
// nothing about A's claim: every claim-state write form is forbidden.
func interleavedCompetitorRevoked(t *testing.T, f *interleaveFixture) {
	t.Helper()
	a, b := agentA(), agentB()
	before := f.view(t)
	f.race.arm(func() error {
		f.replaceGrants(t, without(f.base.Grants, secondGrant()))
		return nil
	})
	got := f.call(t, a, "bead.claim", quietParams(), 8300)
	f.race.requireRace(t, 2)
	success(t, got)
	if got.Meta.Replayed {
		t.Fatal("reauthorized claim reported a replay")
	}
	after := f.view(t)
	f.requireHolder(t, after, a)
	if after.Versions != before.Versions+1 {
		t.Fatalf("reauthorized claim published %d bead versions; want 1", after.Versions-before.Versions)
	}
	if f.receipt(t, a, "bead.claim", 8300) == nil {
		t.Fatal("reauthorized claim has no receipt")
	}
	for i, write := range claimStateWrites() {
		key := 8310 + i
		got := f.call(t, b, write.method, write.params(), key)
		if got.Error == nil || got.Error.Data.Code != "forbidden" || len(got.Error.Data.Details) != 0 || len(got.Result) != 0 || got.Meta.Replayed {
			t.Fatalf("revoked competitor %s on a live claim was not a detail-free forbidden: %+v", write.label(i), got.Error)
		}
		f.requireNoReceipt(t, b, write.method, key)
	}
	if !reflect.DeepEqual(after, f.view(t)) {
		t.Fatal("revoked competitor changed A's claim")
	}
}

// A holds two permission paths (team membership and a direct project grant);
// one is removed in A's window. A still holds the other and claims once.
func interleavedRedundantPathRemoved(t *testing.T, f *interleaveFixture) {
	t.Helper()
	a := agentA()
	withBoth := append(append([]authz.Grant(nil), f.base.Grants...), directOperatorGrant())
	f.replaceGrants(t, withBoth)
	before := f.view(t)
	f.race.arm(func() error {
		f.replaceGrants(t, without(withBoth, directOperatorGrant()))
		return nil
	})
	got := f.call(t, a, "bead.claim", quietParams(), 8400)
	f.race.requireRace(t, 2)
	success(t, got)
	after := f.view(t)
	f.requireHolder(t, after, a)
	if after.Versions != before.Versions+1 || f.receipt(t, a, "bead.claim", 8400) == nil {
		t.Fatal("claim over the remaining path did not publish exactly one version and its receipt")
	}
}

// B wins the claim and then loses its permission. The live claim is not
// freed by that: A, still authorized, cannot take it, and B can no longer
// maintain or release it. Nothing changes until B's permission returns.
func holderLosesPermission(t *testing.T, f *interleaveFixture) {
	t.Helper()
	a, b := agentA(), agentB()
	success(t, f.call(t, b, "bead.claim", quietParams(), 8500))
	f.replaceGrants(t, without(f.base.Grants, secondGrant()))
	held := f.view(t)
	f.requireHolder(t, held, b)

	requireLiveClaimBusy(t, f.call(t, a, "bead.claim", quietParams(), 8501))
	f.requireNoReceipt(t, a, "bead.claim", 8501)
	for i, method := range []string{"bead.heartbeat", "bead.release"} {
		key := 8510 + i
		requireNoDisclosure(t, f.call(t, b, method, quietParams(), key))
		f.requireNoReceipt(t, b, method, key)
	}
	// A cannot maintain or release someone else's claim either.
	for i, method := range []string{"bead.heartbeat", "bead.release"} {
		key := 8520 + i
		requireCode(t, f.call(t, a, method, quietParams(), key), "lease_not_held")
		f.requireNoReceipt(t, a, method, key)
	}
	if !reflect.DeepEqual(held, f.view(t)) {
		t.Fatal("refusals around a permissionless holder changed the claim plane")
	}

	f.replaceGrants(t, f.base.Grants)
	_ = quietDeadline(t, f.call(t, b, "bead.heartbeat", quietParams(), 8530))
	restored := f.view(t)
	f.requireHolder(t, restored, b)
	if restored.Versions != held.Versions || restored.Revision != held.Revision {
		t.Fatal("heartbeat after restored permission was not quiet")
	}
}

// An agent whose sponsor holds no grant gets the same detail-free forbidden
// for every claim-state write form whether the bead is unclaimed, claimed by
// someone else, or claimed and then released: the claim plane is invisible
// to it and nothing it sends changes state or stores a receipt.
func ungrantedAgentClaimStateHidden(t *testing.T, f *interleaveFixture) {
	t.Helper()
	outsider := worker("user:outsider")
	outsider.ID = "agent:outsider"
	probe := func(phase string, base int) {
		t.Helper()
		before := f.view(t)
		for i, write := range claimStateWrites() {
			key := base + i
			got := f.call(t, outsider, write.method, write.params(), key)
			if got.Error == nil || got.Error.Data.Code != "forbidden" || len(got.Error.Data.Details) != 0 || len(got.Result) != 0 || got.Meta.Replayed {
				t.Fatalf("%s: ungranted %s was not a detail-free forbidden: %+v", phase, write.label(i), got.Error)
			}
			f.requireNoReceipt(t, outsider, write.method, key)
		}
		if !reflect.DeepEqual(before, f.view(t)) {
			t.Fatalf("%s: ungranted agent changed the claim plane", phase)
		}
	}
	requireUnclaimed(t, f.view(t))
	probe("unclaimed", 8600)
	a := agentA()
	success(t, f.call(t, a, "bead.claim", quietParams(), 8615))
	f.requireHolder(t, f.view(t), a)
	probe("claimed by another agent", 8620)
	success(t, f.call(t, a, "bead.release", quietParams(), 8635))
	requireUnclaimed(t, f.view(t))
	probe("released", 8640)
}

// interleaveCloseRules admits bead close and tx.apply modifies, which the fixed
// component leaves undecided for agents.
type interleaveCloseRules struct{}

func (interleaveCloseRules) Eval(_ context.Context, _ authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	decisions := make([]authz.Decision, len(requests))
	for i, request := range requests {
		decisions[i].Effect = authz.NoDecision
		if request.Action == authz.Modify && request.Resource.Kind == authz.Bead && (request.Method == "bead.close" || request.Method == "tx.apply") {
			decisions[i] = authz.Decision{Effect: authz.Allow, Reasons: []string{"test close grant"}}
		}
	}
	return decisions, nil
}

func (interleaveCloseRules) Compile(authz.ListRequest) (authz.FilterTemplate, error) {
	return authz.FilterTemplate{}, authz.ErrCheckOnly
}

// An authorized agent that is not the holder keeps the exact lease refusal
// for every claim-state write form, and that refusal still precedes a stale
// compare-and-set expectation.
func authorizedNonHolderRefusals(t *testing.T, f *interleaveFixture) {
	t.Helper()
	a, c := agentA(), agentC()
	// The fixed component permits agents claim, claimNext and update only;
	// admit close and tx.apply here so those forms have an authorized caller.
	authzrpc.AddRuleForTest(f.server, interleaveCloseRules{})
	success(t, f.call(t, a, "bead.claim", quietParams(), 8700))
	held := f.view(t)
	f.requireHolder(t, held, a)
	stale := model.PreconditionByRevision(quietRef(), model.Natural64(1))
	for i, write := range claimStateWrites() {
		want := model.ErrorCode("lease_not_held")
		if write.method == "bead.claim" {
			want = "busy"
		}
		for j, withStale := range []bool{false, true} {
			if withStale && (write.method == "bead.claim" || write.method == "bead.heartbeat" || write.method == "bead.release") {
				continue
			}
			key := 8710 + 2*i + j
			params := write.params()
			if withStale {
				if changeSet, ok := params["changeSet"].(map[string]any); ok {
					changeSet["expect"] = []model.Precondition{stale}
				} else {
					params["expect"] = []model.Precondition{stale}
				}
			}
			got := f.call(t, c, write.method, params, key)
			if got.Error == nil || got.Error.Data.Code != want || len(got.Result) != 0 || got.Meta.Replayed {
				t.Fatalf("authorized non-holder %s (stale CAS=%v): got %+v, want %s", write.label(i), withStale, got.Error, want)
			}
			f.requireNoReceipt(t, c, write.method, key)
		}
	}
	if !reflect.DeepEqual(held, f.view(t)) {
		t.Fatal("authorized non-holder refusals changed the claim plane")
	}
}
