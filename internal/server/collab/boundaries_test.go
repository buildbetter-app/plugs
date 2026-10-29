package collab_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/collab"
)

func domainCode(t *testing.T, err error, want string) {
	t.Helper()
	var e *model.Error
	if !errors.As(err, &e) || string(e.Data.Code) != want {
		t.Fatalf("want %s, got %v", want, err)
	}
}

func TestMissingCurrentPolicyCapabilityFailsClosedAtRealRPCBoundary(t *testing.T) {
	var installed atomic.Bool
	f := newFixture(t, func(c *collab.Config) { c.AuthorizationInstalled = installed.Load })
	id := f.open(t, alice, map[string]any{})
	agent := model.PrincipalRef{Kind: "agent", ID: "agent:a", Sponsor: &alice}
	code(t, f.call(t, agent, "session.open", map[string]any{}), "authz_not_installed")
	installed.Store(true)
	for _, test := range []struct {
		method string
		params map[string]any
	}{
		{"session.open", map[string]any{}},
		{"session.close", map[string]any{"sessionId": id}},
		{"presence.set", map[string]any{"sessionId": id}},
		{"presence.list", map[string]any{"project": "project-a"}},
	} {
		code(t, f.call(t, alice, test.method, test.params), "authz_unavailable")
	}
	installed.Store(false)
	if len(f.rows(t, alice, "project-a")) != 1 {
		t.Fatal("capability refusal changed session state")
	}
}

// These explicit port-contract fixtures never replace the actual service,
// pipeline, storage, registry or byte dispatcher. They certify host-port
// behavior only, not integration/acceptance of the fixed-permission plugin.
type controlledPolicy struct {
	admit func(context.Context, model.PrincipalRef, string) (bool, error)
	read  func(context.Context, model.PrincipalRef, service.Bead) (bool, error)
}

func (p controlledPolicy) AdmitPresence(ctx context.Context, by model.PrincipalRef, project string) (bool, error) {
	if p.admit != nil {
		return p.admit(ctx, by, project)
	}
	return project == "project-a", nil
}
func (p controlledPolicy) CanRead(ctx context.Context, by model.PrincipalRef, bead service.Bead) (bool, error) {
	if p.read != nil {
		return p.read(ctx, by, bead)
	}
	return false, nil
}

func TestReadPolicyPortCurrentDenialLeavesExactlyTheNoFocusRow(t *testing.T) {
	var installed atomic.Bool
	var failed atomic.Bool
	var reads atomic.Int32
	f := newFixture(t, func(c *collab.Config) {
		c.AuthorizationInstalled = installed.Load
		c.Policy = controlledPolicy{read: func(_ context.Context, by model.PrincipalRef, bead service.Bead) (bool, error) {
			reads.Add(1)
			if by.ID != bob.ID || bead.Project != "project-a" || bead.ID == "" {
				t.Error("policy did not receive actual viewer/service resource")
			}
			if failed.Load() {
				return false, errors.New("private backend details")
			}
			return false, nil
		}}
	})
	bead := f.create(t, "project-a")
	id := f.open(t, alice, map[string]any{})
	baseline := f.rows(t, bob, "project-a")
	result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "focus": "bead/" + bead}))
	installed.Store(true)
	rows := f.rows(t, bob, "project-a")
	if fmt.Sprint(rows) != fmt.Sprint(baseline) || reads.Load() != 1 || strings.Contains(fmt.Sprint(rows), bead) {
		t.Fatal("denied focus left a marker", rows, baseline)
	}
	failed.Store(true)
	response := f.call(t, bob, "presence.list", map[string]any{"project": "project-a"})
	code(t, response, "authz_unavailable")
	if response["result"] != nil || strings.Contains(fmt.Sprint(response), "private backend") {
		t.Fatal("policy failure leaked details or partial rows", response)
	}
	code(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-b"}), "forbidden")
}

func TestReadPolicyPortSponsoredRowsAreCompactAndPrincipalIsFrozen(t *testing.T) {
	f := newFixture(t, func(c *collab.Config) {
		c.AuthorizationInstalled = func() bool { return true }
		c.Policy = controlledPolicy{}
	})
	var first string
	for i := range 40 {
		sponsor := alice
		by := model.PrincipalRef{Kind: "agent", ID: fmt.Sprintf("agent:worker-%d", i), Sponsor: &sponsor, Caveats: map[string]any{"resources": []any{"bead/private-caveat"}}}
		id := f.open(t, by, map[string]any{})
		if i == 0 {
			first = id
		}
		by.Sponsor.ID = "user:mutated"
		by.Caveats["resources"].([]any)[0] = "bead/changed"
		code(t, f.call(t, by, "session.close", map[string]any{"sessionId": id}), "not_found")
	}
	rows := f.rows(t, bob, "project-a")
	if len(rows) != 1 || rows[0].(map[string]any)["onBehalfOf"] != alice.ID {
		t.Fatal(rows)
	}
	if strings.Contains(fmt.Sprint(rows), "private-caveat") || strings.Contains(fmt.Sprint(rows), "mutated") {
		t.Fatal("public presence leaked caveats or retained caller aliases", rows)
	}
	by := model.PrincipalRef{Kind: "agent", ID: "agent:worker-0", Sponsor: &alice, Caveats: map[string]any{"resources": []any{"bead/private-caveat"}}}
	code(t, f.call(t, by, "presence.set", map[string]any{"sessionId": first, "onBehalfOf": bob.ID}), "forbidden")
	result(t, f.call(t, by, "session.close", map[string]any{"sessionId": first}))
}

func TestReadPolicyPortCancellationDuringActualServiceBackedSetPublishesNothing(t *testing.T) {
	entered := make(chan struct{}, 1)
	f := newFixture(t, func(c *collab.Config) {
		c.AuthorizationInstalled = func() bool { return true }
		c.Policy = controlledPolicy{read: func(ctx context.Context, _ model.PrincipalRef, _ service.Bead) (bool, error) {
			entered <- struct{}{}
			<-ctx.Done()
			return false, ctx.Err()
		}}
	})
	bead := f.create(t, "project-a")
	id := f.open(t, alice, map[string]any{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	focus := "bead/" + bead
	go func() {
		_, err := f.manager.Set(ctx, collab.Caller{Principal: alice}, collab.SetRequest{SessionID: id, Focus: &focus})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("real resource read never reached policy port")
	}
	cancel()
	select {
	case err := <-done:
		domainCode(t, err, "busy")
	case <-time.After(2 * time.Second):
		t.Fatal("set ignored cancellation")
	}
	rows := f.rows(t, bob, "project-a")
	if len(rows) != 1 || rows[0].(map[string]any)["focus"] != nil {
		t.Fatal("canceled set published focus", rows)
	}
	f.clock.advance(collab.SessionTTL)
	if len(f.rows(t, bob, "project-a")) != 0 {
		t.Fatal("canceled set renewed expiry")
	}
}

func TestConcurrentQuotaAdmissionNeverOverbooksAndCloseCancelsSession(t *testing.T) {
	f := newFixture(t, func(c *collab.Config) { c.Limits.PerPrincipal = 8 })
	var wg sync.WaitGroup
	ids := make(chan string, 40)
	errors := make(chan error, 40)
	for range 40 {
		wg.Go(func() {
			v, err := f.manager.Open(context.Background(), collab.Caller{Principal: alice}, "project-a", nil)
			if err != nil {
				errors <- err
			} else {
				ids <- v.SessionID
			}
		})
	}
	wg.Wait()
	close(ids)
	close(errors)
	if len(ids) != 8 || len(errors) != 32 {
		t.Fatalf("quota admitted %d, refused %d", len(ids), len(errors))
	}
	for err := range errors {
		domainCode(t, err, "too_many_sessions")
	}
	for id := range ids {
		lifetime, err := f.manager.SessionContext(context.Background(), collab.Caller{Principal: alice}, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.manager.CloseSession(context.Background(), collab.Caller{Principal: alice}, id); err != nil {
			t.Fatal(err)
		}
		if lifetime.Err() == nil {
			t.Fatal("explicit close did not cancel session context")
		}
	}
	if len(f.rows(t, alice, "project-a")) != 0 {
		t.Fatal("concurrent quota cleanup retained sessions")
	}
}

func TestReadPolicyPortQuotaUsesIdentityRatherThanCaveatsAndSponsorRatherThanWorker(t *testing.T) {
	f := newFixture(t, func(c *collab.Config) {
		c.AuthorizationInstalled = func() bool { return true }
		c.Policy = controlledPolicy{}
		c.Limits = collab.Limits{PerPrincipal: 2, PerSponsor: 3}
	})
	for i := range 2 {
		by := model.PrincipalRef{Kind: "agent", ID: "agent:one", Sponsor: &alice, Caveats: map[string]any{"generation": i}}
		f.open(t, by, map[string]any{})
	}
	by := model.PrincipalRef{Kind: "agent", ID: "agent:one", Sponsor: &alice, Caveats: map[string]any{"generation": 2}}
	code(t, f.call(t, by, "session.open", map[string]any{}), "too_many_sessions")
	by.ID = "agent:two"
	f.open(t, by, map[string]any{})
	by.ID = "agent:three"
	code(t, f.call(t, by, "session.open", map[string]any{}), "too_many_sessions")
	by.Sponsor = &bob
	f.open(t, by, map[string]any{})
}

func TestIdleReaperCancelsWithoutAnotherRequestAndCanceledRequestsDoNotOpen(t *testing.T) {
	f := newFixture(t, func(c *collab.Config) { c.Clock = time.Now; c.IdleTTL = 100 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.manager.Open(ctx, collab.Caller{Principal: alice}, "project-a", nil)
	domainCode(t, err, "busy")
	id := f.open(t, alice, map[string]any{})
	lifetime, err := f.manager.SessionContext(context.Background(), collab.Caller{Principal: alice}, id)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-lifetime.Done():
	case <-time.After(time.Second):
		t.Fatal("idle expiry needed another request to reclaim session")
	}
	if len(f.rows(t, alice, "project-a")) != 0 {
		t.Fatal("reaper retained expired row")
	}
}

func TestConfigurationRequiresActualCapabilitiesAndRespectsHardCeilings(t *testing.T) {
	f := newFixture(t, nil)
	for _, mutate := range []func(*collab.Config){
		func(c *collab.Config) { c.Service = nil },
		func(c *collab.Config) { c.Lease = nil },
		func(c *collab.Config) { c.AuthorizationInstalled = nil },
		func(c *collab.Config) { c.IdleTTL = collab.SessionTTL + time.Second },
		func(c *collab.Config) { c.Limits.Sessions = collab.MaxSessions + 1 },
		func(c *collab.Config) { c.Limits.PerPrincipal = -1 },
		func(c *collab.Config) { c.Limits.PerSponsor = collab.MaxSessionsPerSponsor + 1 },
	} {
		config := f.config
		mutate(&config)
		manager, err := collab.New(context.Background(), config)
		if err == nil {
			_ = manager.Close()
			t.Fatal("invalid configuration accepted")
		}
	}
}

func TestRealLeaseTakeoverAndServerCancellationClearPresence(t *testing.T) {
	f := newFixture(t, nil)
	bead := f.create(t, "project-a")
	id := f.open(t, alice, map[string]any{})
	lifetime, err := f.manager.SessionContext(context.Background(), collab.Caller{Principal: alice}, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	code(t, f.call(t, alice, "presence.list", map[string]any{"project": "project-a"}), "busy")
	if lifetime.Err() == nil {
		t.Fatal("lease loss left session lifetime active")
	}
	lease, err := f.store.Lease(context.Background(), "collab-takeover", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	config := f.config
	config.Lease = lease
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, lease, pipeline.WithOntology(ontology), pipeline.WithClock(f.clock.now))
	config.Service = service.New(f.store, lease, writer, service.WithOntology(ontology), service.WithClock(f.clock.now))
	ctx, cancel := context.WithCancel(context.Background())
	manager, err := collab.New(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	rows, err := manager.List(context.Background(), collab.Caller{Principal: alice}, "project-a")
	if err != nil || len(rows) != 0 {
		t.Fatalf("takeover restored ephemeral sessions: %v, %v", rows, err)
	}
	domainCode(t, manager.CloseSession(context.Background(), collab.Caller{Principal: alice}, id), "not_found")
	session, err := manager.Open(context.Background(), collab.Caller{Principal: alice}, "project-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, err = manager.SessionContext(context.Background(), collab.Caller{Principal: alice}, session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	focus := "bead/" + bead
	if _, err := manager.Set(context.Background(), collab.Caller{Principal: alice}, collab.SetRequest{SessionID: session.SessionID, Focus: &focus}); err != nil {
		t.Fatal("real takeover service could not read persisted resource", err)
	}
	cancel()
	_, err = manager.List(context.Background(), collab.Caller{Principal: alice}, "project-a")
	domainCode(t, err, "busy")
	if lifetime.Err() == nil {
		t.Fatal("server cancellation did not clear session")
	}
}

func TestReadPolicyPortRevokedSessionIsNotFoundAndCannotMoveProjects(t *testing.T) {
	var allowed atomic.Bool
	allowed.Store(true)
	f := newFixture(t, func(c *collab.Config) {
		c.AuthorizationInstalled = func() bool { return true }
		c.Policy = controlledPolicy{admit: func(_ context.Context, _ model.PrincipalRef, project string) (bool, error) {
			return project == "project-b" || allowed.Load(), nil
		}}
	})
	id := f.open(t, alice, map[string]any{})
	lifetime, err := f.manager.SessionContext(context.Background(), collab.Caller{Principal: alice}, id)
	if err != nil {
		t.Fatal(err)
	}
	before := durableState(t, f)
	f.clock.advance(4 * time.Minute)
	allowed.Store(false)
	for _, params := range []map[string]any{
		{"sessionId": id},
		{"sessionId": id, "_meta": map[string]any{"project": "project-b"}},
	} {
		code(t, f.call(t, alice, "presence.set", params), "not_found")
	}
	code(t, f.call(t, alice, "session.close", map[string]any{"sessionId": id}), "not_found")
	_, err = f.manager.SessionContext(context.Background(), collab.Caller{Principal: alice}, id)
	domainCode(t, err, "not_found")
	allowed.Store(true)
	if len(f.rows(t, bob, "project-a")) != 1 || len(f.rows(t, bob, "project-b")) != 0 {
		t.Fatal("denied update moved or closed the session")
	}
	f.clock.advance(time.Minute)
	if len(f.rows(t, bob, "project-a")) != 0 || lifetime.Err() == nil {
		t.Fatal("denied update renewed expiry")
	}
	if string(before) != string(durableState(t, f)) {
		t.Fatal("denied session operations changed durable state")
	}
}

type sessionAdmissionBarrierKey struct{}

func TestSessionAdmissionConcurrentSetPreservesLifecycleAndAdmission(t *testing.T) {
	for _, operation := range []string{"close", "context"} {
		for _, scenario := range []struct {
			name, project, want string
			deny                bool
		}{
			{"same-project", "project-a", "busy", false},
			{"moved-project-denied", "project-b", "busy", true},
			{"revoked-project", "project-a", "not_found", true},
		} {
			t.Run(operation+"/"+scenario.name, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				var denied atomic.Bool
				f := newFixture(t, func(c *collab.Config) {
					c.Limits.PerPrincipal = 1
					c.AuthorizationInstalled = func() bool { return true }
					c.Policy = controlledPolicy{admit: func(ctx context.Context, by model.PrincipalRef, project string) (bool, error) {
						if ctx.Value(sessionAdmissionBarrierKey{}) == true {
							close(entered)
							select {
							case <-release:
							case <-ctx.Done():
								return false, ctx.Err()
							}
						}
						return !(denied.Load() && by.ID == alice.ID && project == scenario.project), nil
					}}
				})
				id := f.open(t, alice, map[string]any{})
				caller := collab.Caller{Principal: alice}
				existing, err := f.manager.SessionContext(context.Background(), caller, id)
				if err != nil {
					t.Fatal(err)
				}
				before := durableState(t, f)
				run := func(ctx context.Context) (context.Context, error) {
					if operation == "close" {
						return nil, f.manager.CloseSession(ctx, caller, id)
					}
					return f.manager.SessionContext(ctx, caller, id)
				}
				type outcome struct {
					ctx context.Context
					err error
				}
				done := make(chan outcome, 1)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go func() {
					lifetime, err := run(context.WithValue(ctx, sessionAdmissionBarrierKey{}, true))
					done <- outcome{lifetime, err}
				}()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("session operation did not reach the admission barrier")
				}
				// The real RPC update completes after the operation captured its
				// entry and before its final lock. No scheduling sleep is involved.
				result(t, f.call(t, alice, "presence.set", map[string]any{"sessionId": id, "_meta": map[string]any{"project": scenario.project}}))
				denied.Store(scenario.deny)
				unblock()
				select {
				case got := <-done:
					domainCode(t, got.err, scenario.want)
					var e *model.Error
					if !errors.As(got.err, &e) || e.Data.Retryable != (scenario.want == "busy") || got.ctx != nil {
						t.Fatalf("wrong retry/cancellation result: %+v", got)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("session operation did not leave the admission barrier")
				}
				if existing.Err() != nil || len(f.rows(t, bob, scenario.project)) != 1 {
					t.Fatal("refused operation canceled or deleted the live session")
				}
				if scenario.deny {
					lifetime, err := run(context.Background())
					domainCode(t, err, "not_found")
					if lifetime != nil || existing.Err() != nil || len(f.rows(t, bob, scenario.project)) != 1 {
						t.Fatal("retry bypassed current project denial")
					}
					denied.Store(false)
				}
				retried, err := run(context.Background())
				if err != nil {
					t.Fatal("authorized retry failed", err)
				}
				if operation == "context" {
					if retried != existing || retried.Err() != nil {
						t.Fatal("retry did not return the original live session context")
					}
					result(t, f.call(t, alice, "session.close", map[string]any{"sessionId": id}))
				}
				if existing.Err() == nil || len(f.rows(t, bob, scenario.project)) != 0 {
					t.Fatal("successful close did not cancel and delete the session")
				}
				code(t, f.call(t, alice, "session.close", map[string]any{"sessionId": id}), "not_found")
				_, err = f.manager.SessionContext(context.Background(), caller, id)
				domainCode(t, err, "not_found")
				f.open(t, alice, map[string]any{}) // Closed session released its quota.
				if string(before) != string(durableState(t, f)) {
					t.Fatal("session overlap changed durable state")
				}
			})
		}
	}
}
