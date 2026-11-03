package authzrpc

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
)

type workflowClaimNextFacts struct {
	FactsSource
	failure error
	onLoad  func() error
}

func (s *workflowClaimNextFacts) Load(ctx context.Context, snapshot Snapshot) (authzfixed.Facts, error) {
	if s.failure != nil {
		return authzfixed.Facts{}, s.failure
	}
	facts, err := s.FactsSource.Load(ctx, snapshot)
	if err == nil && s.onLoad != nil {
		err = s.onLoad()
	}
	return facts, err
}

func workflowClaimNextDenied(t *testing.T, got workflowClaimNextReply, code model.ErrorCode) {
	t.Helper()
	if got.Error == nil || got.Error.Data.Code != code || len(got.Result) != 0 || got.Meta.Replayed {
		t.Fatalf("denied claim disclosed a result or used the wrong error: %+v result=%s", got.Error, got.Result)
	}
}

func (f *workflowClaimNextFixture) revoke(t *testing.T) {
	t.Helper()
	f.facts.Grants = nil
	if err := f.source.Replace(f.facts); err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowClaimNextCurrentProjectPolicy(t *testing.T) {
	for _, phase := range []string{"live", "leased-replay", "empty-replay", "terminal-replay"} {
		for _, denial := range []string{"revoked-grant", "inactive-project", "facts-unavailable"} {
			t.Run(phase+"/"+denial, func(t *testing.T) {
				f := newWorkflowClaimNextFixture(t)
				ctx := context.Background()
				var ref model.Ref
				if phase == "empty-replay" {
					// Retain only a provider receipt, so this exercises the guarded
					// provider-replay path rather than the ordinary Writer replay.
					meta := workflowClaimNextMeta("bead.claimNext", 101)
					var err error
					meta.ClaimNextIdentity, err = service.ClaimNextRequestIdentity(service.ClaimNextRequest{Project: "project"})
					if err != nil {
						t.Fatal(err)
					}
					if _, found, replayed, err := f.provider.ClaimNext(ctx, f.by, "project", nil, meta); err != nil || found || replayed {
						t.Fatalf("provider empty receipt: found=%v replayed=%v err=%v", found, replayed, err)
					}
				} else {
					ref = model.RefByID(f.invoke(t, 100, 1)[0].ID, model.Natural64(0))
					if phase != "live" {
						workflowClaimNextCommit(t, f.call(t, nil, 101))
					}
					if phase == "terminal-replay" {
						if _, err := f.domain.CloseWithOutcome(ctx, f.by, service.CloseRequest{ID: ref}, workflowClaimNextMeta("bead.close", 102)); err != nil {
							t.Fatal(err)
						}
					}
				}
				want := model.ErrorCode("forbidden")
				switch denial {
				case "revoked-grant":
					f.revoke(t)
				case "inactive-project":
					f.facts.Projects[0].Active = false
					if err := f.source.Replace(f.facts); err != nil {
						t.Fatal(err)
					}
				case "facts-unavailable":
					f.app.Checker.source = &workflowClaimNextFacts{FactsSource: f.source, failure: errors.New("facts unavailable")}
					want = "authz_unavailable"
				}
				workflowClaimNextDenied(t, f.call(t, nil, 101), want)
				if phase == "live" {
					item, err := f.provider.Get(ctx, f.by, ref, workflowClaimNextMeta("bead.get", 103))
					if err != nil || item.Claim != nil || item.Status != "open" {
						t.Fatalf("denied live selection changed its workflow item: %+v err=%v", item, err)
					}
				}
			})
		}
	}
}

func TestWorkflowClaimNextRevocationAfterQueueSnapshot(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	ref := model.RefByID(f.invoke(t, 110, 1)[0].ID, model.Natural64(0))
	page := f.app.Service.claimNextPage
	selected := false
	f.app.Service.claimNextPage = func(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
		out, err := page(ctx, by, req)
		if err == nil && len(out.Items) == 1 && out.Items[0].ID == ref.ID {
			selected = true
			f.revoke(t)
		}
		return out, err
	}
	workflowClaimNextDenied(t, f.call(t, nil, 111), "forbidden")
	item, err := f.provider.Get(context.Background(), f.by, ref, workflowClaimNextMeta("bead.get", 112))
	if !selected || err != nil || item.Claim != nil || item.Status != "open" {
		t.Fatalf("revocation after live selection was not checked before mutation: selected=%v item=%+v err=%v", selected, item, err)
	}
}

func TestWorkflowClaimNextPreflightAuthorityFence(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	ref := model.RefByID(f.invoke(t, 120, 1)[0].ID, model.Natural64(0))
	revoked := false
	f.app.Checker.source = &workflowClaimNextFacts{FactsSource: f.source, onLoad: func() error {
		if !revoked {
			revoked = true
			f.facts.Grants = nil
			return f.source.Replace(f.facts)
		}
		return nil
	}}
	workflowClaimNextDenied(t, f.call(t, nil, 121), "authz_unavailable")
	item, err := f.provider.Get(context.Background(), f.by, ref, workflowClaimNextMeta("bead.get", 122))
	if !revoked || err != nil || item.Claim != nil {
		t.Fatalf("changed preflight authority reached provider mutation: revoked=%v item=%+v err=%v", revoked, item, err)
	}
}

// This delegates to the real provider-backed service, then changes current
// policy before the guarded adapter can publish its outcome. It establishes
// response fencing, not atomic ACL/checkpoint mutation or rollback.
type workflowClaimNextRevokingDelegate struct {
	*service.Service
	afterClaim  func()
	afterReplay func()
}

func (d *workflowClaimNextRevokingDelegate) ClaimWithOutcome(ctx context.Context, by model.PrincipalRef, req service.ClaimRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	out, err := d.Service.ClaimWithOutcome(ctx, by, req, meta)
	if err == nil && d.afterClaim != nil {
		d.afterClaim()
	}
	return out, err
}

func (d *workflowClaimNextRevokingDelegate) ReplayWorkflowClaimNext(ctx context.Context, by model.PrincipalRef, req service.ClaimNextRequest, meta model.WriteMeta) (*port.WriteOutcome, bool, error) {
	out, replayed, err := d.Service.ReplayWorkflowClaimNext(ctx, by, req, meta)
	if err == nil && replayed && d.afterReplay != nil {
		d.afterReplay()
	}
	return out, replayed, err
}

func TestWorkflowClaimNextFinalAuthorityFence(t *testing.T) {
	for _, phase := range []string{"live", "provider-replay", "late-fallback"} {
		t.Run(phase, func(t *testing.T) {
			f := newWorkflowClaimNextFixture(t)
			d := &workflowClaimNextRevokingDelegate{Service: f.domain}
			f.app.Service.delegate = d
			revoked := false
			revoke := func() { revoked = true; f.revoke(t) }
			if phase == "late-fallback" {
				page := f.app.Service.claimNextPage
				passes := 0
				f.app.Service.claimNextPage = func(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
					out, err := page(ctx, by, req)
					if err == nil && len(out.Items) == 0 && out.Next == nil {
						passes++
						if passes == 2 {
							f.invoke(t, 130, 1)
						}
					}
					return out, err
				}
				commit := f.app.Service.commitEmpty
				f.app.Service.commitEmpty = func(ctx context.Context, by model.PrincipalRef, req service.ClaimNextRequest, meta model.WriteMeta, probe port.ClaimNextReadyProbe) (port.WriteOutcome, error) {
					out, err := commit(ctx, by, req, meta, probe)
					if err == nil && len(out.Commit.Results) == 1 {
						revoke()
					}
					return out, err
				}
			} else {
				f.invoke(t, 130, 1)
				if phase == "provider-replay" {
					workflowClaimNextCommit(t, f.call(t, nil, 131))
					// Revoke after preflight's final snapshot has loaded. The
					// separate response fence must reload current authority.
					d.afterReplay = func() {
						loads := 0
						f.app.Checker.source = &workflowClaimNextFacts{FactsSource: f.source, onLoad: func() error {
							loads++
							if loads == 2 {
								revoke()
							}
							return nil
						}}
					}
				} else {
					d.afterClaim = revoke
				}
			}
			workflowClaimNextDenied(t, f.call(t, nil, 131), "authz_unavailable")
			if !revoked {
				t.Fatal("test never changed authority at the provider result boundary")
			}
		})
	}
}

func TestWorkflowClaimNextCoreNullSurvivesLaterWorkflow(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	ctx := context.Background()
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithAuthorizer(f.app.Checker), pipeline.WithEventContext(f.app.Checker))
	core := service.New(f.store, f.lease, writer, service.WithOntology(ontology))
	out, err := core.ClaimNextWithOutcome(ctx, f.by, service.ClaimNextRequest{Project: "project"}, workflowClaimNextMeta("bead.claimNext", 141))
	if err != nil || out == nil || !out.NullResult || out.Replayed {
		t.Fatalf("core-only null receipt: %+v err=%v", out, err)
	}
	ref := model.RefByID(f.invoke(t, 142, 1)[0].ID, model.Natural64(0))
	replay := f.call(t, nil, 141)
	if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(replay.Result, []byte("null")) {
		t.Fatalf("retained core null changed after workflow installation: %+v result=%s", replay.Error, replay.Result)
	}
	item, err := f.provider.Get(ctx, f.by, ref, workflowClaimNextMeta("bead.get", 143))
	if err != nil || item.Claim != nil || item.Status != "open" {
		t.Fatalf("core null replay selected a later workflow candidate: %+v err=%v", item, err)
	}
	workflowClaimNextDenied(t, f.call(t, &service.Filter{Types: []string{"task"}}, 141), "idempotency_key_reused")
	workflowClaimNextCommit(t, f.call(t, nil, 144))
}

type workflowClaimNextCheckedRules struct {
	authz.RuleSet
	checks []authz.CheckRequest
}

func (r *workflowClaimNextCheckedRules) Eval(ctx context.Context, facts authz.Slice, reqs []authz.CheckRequest) ([]authz.Decision, error) {
	r.checks = append(r.checks, reqs...)
	return r.RuleSet.Eval(ctx, facts, reqs)
}

func TestWorkflowClaimNextUsesCurrentImmutableProjectResource(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	f.invoke(t, 150, 1)
	rules := &workflowClaimNextCheckedRules{RuleSet: f.app.Checker.chain.Rules[0]}
	f.app.Checker.chain.Rules = []authz.RuleSet{rules}
	first := f.call(t, nil, 151)
	workflowClaimNextCommit(t, first)
	replay := f.call(t, nil, 151)
	if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
		t.Fatalf("authorized current-policy replay changed: %+v result=%s", replay.Error, replay.Result)
	}
	if len(rules.checks) != 2 {
		t.Fatalf("live selection and replay must each check current policy: %+v", rules.checks)
	}
	for _, req := range rules.checks {
		if req.Action != authz.Modify || req.Resource.Kind != authz.Project || req.Resource.Ref.ID != "project:immutable-claimnext" || req.Method != "" {
			t.Fatalf("claim used the wrong current project-policy resource: %+v", req)
		}
	}
}

func TestWorkflowClaimCurrentProjectDenialPrecedesMutation(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	ref := model.RefByID(f.invoke(t, 160, 1)[0].ID, model.Natural64(0))
	meta := workflowClaimNextMeta("bead.claim", 161)
	meta.Project = "unregistered-project"
	_, err := f.app.Service.ClaimWithOutcome(context.Background(), f.by, service.ClaimRequest{ID: ref}, meta)
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "forbidden" {
		t.Fatalf("unknown current project did not fail closed: %v", err)
	}
	f.revoke(t)
	meta.Project = "project"
	_, err = f.app.Service.ClaimWithOutcome(context.Background(), f.by, service.ClaimRequest{ID: ref}, meta)
	if !errors.As(err, &typed) || typed.Data.Code != "forbidden" {
		t.Fatalf("direct workflow claim bypassed current project policy: %v", err)
	}
	item, err := f.provider.Get(context.Background(), f.by, ref, workflowClaimNextMeta("bead.get", 162))
	if err != nil || item.Claim != nil || item.Status != "open" {
		t.Fatalf("denied direct workflow claim mutated provider state: %+v err=%v", item, err)
	}
}
