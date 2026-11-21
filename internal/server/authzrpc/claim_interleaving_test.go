package authzrpc_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
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

// claimRace fires one armed hook at the next Store.Write.
type claimRace struct {
	hook  atomic.Pointer[func() error]
	fired atomic.Int32
}

func (r *claimRace) arm(hook func() error) { r.hook.Store(&hook) }

func (r *claimRace) beforeWrite() error {
	hook := r.hook.Swap(nil)
	if hook == nil {
		return nil
	}
	r.fired.Add(1)
	return (*hook)()
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
	if f.race.fired.Load() != 1 {
		t.Fatalf("interleave hook fired %d times; want 1", f.race.fired.Load())
	}
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
	if f.race.fired.Load() != 1 {
		t.Fatalf("interleave hook fired %d times; want 1", f.race.fired.Load())
	}
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
// nothing about A's claim: claim, heartbeat and release are all forbidden.
func interleavedCompetitorRevoked(t *testing.T, f *interleaveFixture) {
	t.Helper()
	a, b := agentA(), agentB()
	before := f.view(t)
	f.race.arm(func() error {
		f.replaceGrants(t, without(f.base.Grants, secondGrant()))
		return nil
	})
	got := f.call(t, a, "bead.claim", quietParams(), 8300)
	if f.race.fired.Load() != 1 {
		t.Fatalf("interleave hook fired %d times; want 1", f.race.fired.Load())
	}
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
	for i, method := range []string{"bead.claim", "bead.heartbeat", "bead.release"} {
		key := 8310 + i
		requireNoDisclosure(t, f.call(t, b, method, quietParams(), key))
		f.requireNoReceipt(t, b, method, key)
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
	if f.race.fired.Load() != 1 {
		t.Fatalf("interleave hook fired %d times; want 1", f.race.fired.Load())
	}
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
// for claim, heartbeat and release whether the bead is unclaimed, claimed by
// someone else, or claimed and then released: the claim plane is invisible
// to it and nothing it sends changes state or stores a receipt.
func ungrantedAgentClaimStateHidden(t *testing.T, f *interleaveFixture) {
	t.Helper()
	outsider := worker("user:outsider")
	outsider.ID = "agent:outsider"
	probe := func(phase string, base int) {
		t.Helper()
		before := f.view(t)
		for i, method := range []string{"bead.claim", "bead.heartbeat", "bead.release"} {
			key := base + i
			got := f.call(t, outsider, method, quietParams(), key)
			if got.Error == nil || got.Error.Data.Code != "forbidden" || len(got.Error.Data.Details) != 0 || len(got.Result) != 0 || got.Meta.Replayed {
				t.Fatalf("%s: ungranted %s was not a detail-free forbidden: %+v", phase, method, got.Error)
			}
			f.requireNoReceipt(t, outsider, method, key)
		}
		if !reflect.DeepEqual(before, f.view(t)) {
			t.Fatalf("%s: ungranted agent changed the claim plane", phase)
		}
	}
	requireUnclaimed(t, f.view(t))
	probe("unclaimed", 8600)
	a := agentA()
	success(t, f.call(t, a, "bead.claim", quietParams(), 8610))
	f.requireHolder(t, f.view(t), a)
	probe("claimed by another agent", 8620)
	success(t, f.call(t, a, "bead.release", quietParams(), 8630))
	requireUnclaimed(t, f.view(t))
	probe("released", 8640)
}
