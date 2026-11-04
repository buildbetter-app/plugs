package s1b05b08

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These probes retain the actual store/reader/lease implementations. They
// observe reads and inject failures or a competing real commit at existing
// seams; no cycle producer, authority result or storage data is substituted.
type cycleReadProbeStore struct {
	driver.Store
	mu                   sync.Mutex
	reads, writes        int
	queryViews, getViews []driver.View
	queryErr, getErr     error
	failRead             int
	afterQuery           func()
	afterClose           func(int)
}

func (s *cycleReadProbeStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	s.mu.Lock()
	s.reads++
	number, fail := s.reads, s.failRead
	s.mu.Unlock()
	if number == fail {
		return nil, errors.New("injected final snapshot read failure")
	}
	reader, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &cycleReadProbe{Reader: reader, owner: s, number: number}, nil
}

func (s *cycleReadProbeStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return s.Store.Write(ctx, lease)
}

type cycleReadProbe struct {
	driver.Reader
	owner  *cycleReadProbeStore
	number int
}

func (r *cycleReadProbe) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.owner.mu.Lock()
	r.owner.queryViews = append(r.owner.queryViews, r.Snapshot())
	err, after := r.owner.queryErr, r.owner.afterQuery
	r.owner.mu.Unlock()
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	page, err := r.Reader.Query(ctx, q)
	if err == nil && after != nil {
		after()
	}
	return page, err
}

func (r *cycleReadProbe) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	r.owner.mu.Lock()
	r.owner.getViews = append(r.owner.getViews, r.Snapshot())
	err := r.owner.getErr
	r.owner.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return r.Reader.Get(ctx, refs)
}

func (r *cycleReadProbe) Close() error {
	err := r.Reader.Close()
	if r.owner.afterClose != nil {
		r.owner.afterClose(r.number)
	}
	return err
}

type cyclePolicyProbe struct {
	authz.RuleSet
	mu         sync.Mutex
	methods    []string
	fail, deny bool
}

func (p *cyclePolicyProbe) Eval(ctx context.Context, s authz.Slice, reqs []authz.CheckRequest) ([]authz.Decision, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, req := range reqs {
		p.methods = append(p.methods, req.Method)
		if req.Method != "bead.cycles" || req.Action != authz.Read || req.Resource.Kind != authz.Bead {
			return nil, errors.New("cycle request changed its current method/action/resource")
		}
	}
	if p.fail {
		return nil, errors.New("injected installed policy failure")
	}
	if p.deny {
		out := make([]authz.Decision, len(reqs))
		for i := range out {
			out[i].Effect = authz.Deny
		}
		return out, nil
	}
	return p.RuleSet.Eval(ctx, s, reqs)
}

func cycleWithReadProbe(t *testing.T, engine string) (*cycleRPCFixture, *cycleReadProbeStore) {
	t.Helper()
	var probe *cycleReadProbeStore
	f := newCycleRPCFixtureConfigured(t, engine, nil, func(f *cycleRPCFixture, c *authzrpc.Config) {
		probe = &cycleReadProbeStore{Store: f.store}
		f.store, c.Store = probe, probe
	})
	return f, probe
}

func cycleNoGraphWrites(t *testing.T, f *cycleRPCFixture, probe *cycleReadProbeStore, before driver.Meta, externalWrites int) {
	t.Helper()
	after, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if probe.writes != externalWrites || !reflect.DeepEqual(after, before) {
		t.Fatalf("report mutated store: writes=%d external=%d before=%+v after=%+v", probe.writes, externalWrites, before, after)
	}
}

func TestB06CycleReportUsesCurrentExactMethodAndOneContentSnapshot(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f, probe := cycleWithReadProbe(t, engine)
			policy := &cyclePolicyProbe{RuleSet: f.app.Chain.Rules[0]}
			// The composition and checker share the installed rule slot. Wrap
			// that existing slot without replacing the actual fixed policy.
			f.app.Chain.Rules[0] = policy
			before, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			// Project caveats exercise the real member lookup on this same
			// authority reader; an unscoped user permit needs no member Get.
			user := model.PrincipalRef{Kind: "user", ID: "user:current-cycle-reader", Caveats: map[string]any{"projects": []model.Ref{
				f.project, model.RefByID("secret-project", model.Natural64(0)),
			}}}
			got, rpcErr := f.call(t, user, "bead.cycles", nil)
			if rpcErr != nil || got.CycleCount.String() != "3" {
				t.Fatalf("report=%+v error=%+v", got, rpcErr)
			}
			if probe.reads != 2 || len(probe.queryViews) != 1 || len(probe.getViews) == 0 || len(policy.methods) == 0 {
				t.Fatalf("snapshot/method evidence missing: reads=%d queries=%d gets=%d methods=%v", probe.reads, len(probe.queryViews), len(probe.getViews), policy.methods)
			}
			for _, view := range append(append([]driver.View{}, probe.queryViews...), probe.getViews...) {
				if view.At == nil || view.At.String() != before.Head.String() || view.Incarnation != before.Incarnation {
					t.Fatalf("content/authority used another snapshot: %+v", view)
				}
			}
			// A second request sees the changed installed policy. A former
			// successful response is never an authorization grant.
			policy.deny = true
			denied, rpcErr := f.call(t, user, "bead.cycles", nil)
			if rpcErr != nil || denied.CycleCount.String() != "0" || len(denied.Items) != 0 || denied.Truncated {
				t.Fatalf("current denial leaked cycles: %+v error=%+v", denied, rpcErr)
			}
			policy.deny = false
			policy.fail = true
			_, rpcErr = f.call(t, user, "bead.cycles", nil)
			if rpcErr == nil || rpcErr.Data.Code != "authz_unavailable" {
				t.Fatalf("policy failure became hidden success: %+v", rpcErr)
			}
			issued, err := f.keys.Issue(user, time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if !f.keys.Revoke(issued.ID) {
				t.Fatal("could not revoke fixture credential")
			}
			reads := probe.reads
			_, rpcErr = f.callToken(t, issued.Token, "bead.cycles", nil)
			if rpcErr == nil || rpcErr.Data.Code != "unauthenticated" || probe.reads != reads {
				t.Fatalf("revoked credential reached report: error=%+v reads=%d -> %d", rpcErr, reads, probe.reads)
			}
			cycleNoGraphWrites(t, f, probe, before, 0)
		})
	}
}

type cycleFailedFacts struct{ authzrpc.FactsSource }

func (s cycleFailedFacts) Load(context.Context, authzrpc.Snapshot) (authzfixed.Facts, error) {
	return authzfixed.Facts{}, errors.New("injected current authority source failure")
}

func TestB06CycleReportOperationalErrorsNeverBecomePartialSuccess(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, stage := range []string{"query", "member", "authority", "final-read"} {
				t.Run(stage, func(t *testing.T) {
					var probe *cycleReadProbeStore
					f := newCycleRPCFixtureConfigured(t, engine, nil, func(f *cycleRPCFixture, c *authzrpc.Config) {
						probe = &cycleReadProbeStore{Store: f.store}
						f.store, c.Store = probe, probe
						switch stage {
						case "query":
							probe.queryErr = errors.New("injected content query failure")
						case "member":
							probe.getErr = errors.New("injected member visibility read failure")
						case "authority":
							c.Facts = cycleFailedFacts{c.Facts}
						case "final-read":
							probe.failRead = 2
						}
					})
					before, err := f.store.Meta(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					by := model.PrincipalRef{Kind: "user", ID: "user:error-cycle-reader", Caveats: map[string]any{"projects": []model.Ref{f.project}}}
					_, rpcErr := f.call(t, by, "bead.cycles", nil)
					want := model.ErrorCode("authz_unavailable")
					if stage == "query" || stage == "final-read" {
						want = "internal_error"
					}
					if rpcErr == nil || rpcErr.Data.Code != want {
						t.Fatalf("%s error became hidden/empty success: %+v want=%s", stage, rpcErr, want)
					}
					cycleNoGraphWrites(t, f, probe, before, 0)
				})
			}
		})
	}
}

func TestB06CycleReportFencesCompetingAuthorityAndGraphCommits(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, mutation := range []string{"authority", "graph"} {
				t.Run(mutation, func(t *testing.T) {
					var probe *cycleReadProbeStore
					var facts *authzrpc.MutableFacts
					f := newCycleRPCFixtureConfigured(t, engine, nil, func(f *cycleRPCFixture, c *authzrpc.Config) {
						probe = &cycleReadProbeStore{Store: f.store}
						f.store, c.Store = probe, probe
						if mutation == "authority" {
							var err error
							facts, err = authzrpc.NewMutableFacts(f.facts)
							if err != nil {
								t.Fatal(err)
							}
							c.Facts = facts
						}
					})
					// Binding MutableFacts may publish its initialization.
					// Count only transactions after request preparation.
					probe.writes = 0
					var externalErr error
					var committed driver.Meta
					probe.afterQuery = func() {
						if mutation == "authority" {
							next := f.facts
							next.Projects = append([]authzfixed.Project{}, next.Projects...)
							next.Projects[0].Active = false
							externalErr = facts.Replace(next)
						} else {
							seedCycleRPCRows(t, f.store, f.lease, []cycleSeedRow{{id: "competing-row", project: "visible"}})
						}
						if externalErr == nil {
							committed, externalErr = f.store.Meta(context.Background())
						}
					}
					_, rpcErr := f.call(t, model.PrincipalRef{Kind: "user", ID: "user:racing-cycle-reader"}, "bead.cycles", nil)
					if externalErr != nil {
						t.Fatal(externalErr)
					}
					if rpcErr == nil || rpcErr.Data.Code != "authz_unavailable" || len(probe.queryViews) != 1 || probe.queryViews[0].At.String() == committed.Head.String() {
						t.Fatalf("%s changed during report without final refusal: error=%+v queries=%+v committed=%+v", mutation, rpcErr, probe.queryViews, committed)
					}
					cycleNoGraphWrites(t, f, probe, committed, 1)
				})
			}
		})
	}
}

func TestB06CycleReportFencesLeaseLossAtFinalReaderClose(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f, probe := cycleWithReadProbe(t, engine)
			before, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var releaseErr error
			released := false
			probe.afterClose = func(reader int) {
				if reader == 2 {
					releaseErr = f.lease.Release(context.Background())
					released = true
				}
			}
			_, rpcErr := f.call(t, model.PrincipalRef{Kind: "user", ID: "user:lease-cycle-reader"}, "bead.cycles", nil)
			if releaseErr != nil {
				t.Fatal(releaseErr)
			}
			if !released || rpcErr == nil || rpcErr.Data.Code != "busy" {
				t.Fatalf("lease lost at final reader close published success: released=%v error=%+v", released, rpcErr)
			}
			select {
			case <-f.lease.Lost():
			default:
				t.Fatal("real lease did not signal loss")
			}
			cycleNoGraphWrites(t, f, probe, before, 0)
		})
	}
}

func TestB06DirectCycleReportPreservesExplicitNoPluginSemantics(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f, probe := cycleWithReadProbe(t, engine)
			before, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			domain := service.New(f.store, f.lease, pipeline.New(f.store, f.lease))
			user := model.PrincipalRef{Kind: "user", ID: "user:direct-cycle-reader"}
			got, err := domain.Cycles(context.Background(), user, service.CyclesRequest{Project: "visible"})
			if err != nil || got.CycleCount.String() != "3" {
				t.Fatalf("explicit no-plugin read=%+v error=%v", got, err)
			}
			guarded := service.New(f.store, f.lease, pipeline.New(f.store, f.lease), service.WithAuthorizationInstalled(func() bool { return true }))
			reads := probe.reads
			_, err = guarded.Cycles(context.Background(), user, service.CyclesRequest{Project: "visible"})
			var failure *model.Error
			if !errors.As(err, &failure) || failure.Data.Code != "authz_unavailable" || probe.reads != reads {
				t.Fatalf("installed auth without adapter touched storage/returned success: error=%v reads=%d -> %d", err, reads, probe.reads)
			}
			cycleNoGraphWrites(t, f, probe, before, 0)
		})
	}
}
