package authzrpc

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// This test policy supplies the explicit Read/Project permit missing from the
// adopted CP7 rules. It uses real current registry/grant facts, not provider
// ownership or a request-supplied project ID, to grant either workflow read.
type workflowReadRules struct {
	namespace authz.Namespace
	failure   error
	onEval    func() error
	checks    []authz.CheckRequest
}

func (r *workflowReadRules) Compile(req authz.ListRequest) (authz.FilterTemplate, error) {
	permit := authz.FilterExpr{Op: authz.False}
	if req.Action == authz.Read && req.ResourceKind == authz.Project && (req.Method == "bead.get" || req.Method == "bead.ready") {
		permit = authz.And(
			authz.FilterExpr{Op: authz.PrincipalKind, Kind: "user"},
			authz.FilterExpr{Op: authz.ActiveProject},
			authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "involved"},
		)
	}
	return authz.FilterTemplate{Namespace: r.namespace, Request: req, Permit: permit, Forbid: authz.FilterExpr{Op: authz.False}}, nil
}

func (r *workflowReadRules) Eval(ctx context.Context, facts authz.Slice, reqs []authz.CheckRequest) ([]authz.Decision, error) {
	r.checks = append(r.checks, reqs...)
	if r.failure != nil {
		return nil, r.failure
	}
	if r.onEval != nil {
		if err := r.onEval(); err != nil {
			return nil, err
		}
	}
	out := make([]authz.Decision, len(reqs))
	for i, req := range reqs {
		template, err := r.Compile(authz.ListRequest{Caller: req.Caller, Action: req.Action, ResourceKind: req.Resource.Kind, Method: req.Method})
		if err != nil {
			return nil, err
		}
		allowed, err := template.Match(ctx, facts, req.Resource)
		if err != nil {
			return nil, err
		}
		out[i].Effect = authz.NoDecision
		if allowed {
			out[i].Effect = authz.Allow
		}
	}
	return out, nil
}

type workflowReadDelegate struct {
	rpc.Service
	getCalls int
	lastGet  service.GetRequest
	item     service.Bead
	page     service.BeadPage
	onGet    func() error
}

func (d *workflowReadDelegate) Get(_ context.Context, _ model.PrincipalRef, req service.GetRequest) (service.GetResult, error) {
	d.getCalls++
	d.lastGet = req
	if d.onGet != nil {
		if err := d.onGet(); err != nil {
			return service.GetResult{}, err
		}
	}
	return service.GetResult{Bead: d.item}, nil
}

func (d *workflowReadDelegate) Ready(context.Context, model.PrincipalRef, service.ReadyRequest) (service.BeadPage, error) {
	return d.page, nil
}

type workflowReadFixture struct {
	guarded  *GuardedService
	delegate *workflowReadDelegate
	rules    *workflowReadRules
	facts    authzfixed.Facts
	source   *MutableFacts
	by       model.PrincipalRef
}

func newWorkflowReadFixture(t *testing.T, engine string) *workflowReadFixture {
	t.Helper()
	ctx := context.Background()
	var store driver.Store
	if engine == "sqlite" {
		var err error
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "workflow-read.db")})
		if err != nil {
			t.Fatal(err)
		}
	} else {
		store = memory.New()
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "workflow-read-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "workflow-read-test", Authority: model.RefByID("authority", model.Natural64(0))}
	by := model.PrincipalRef{Kind: "user", ID: "workflow-reader"}
	alpha := model.RefByID("project:immutable-alpha", model.Natural64(0))
	facts := authzfixed.Facts{
		Namespace: ns,
		Projects: []authzfixed.Project{
			{Ref: alpha, Name: "alpha", InstallationID: ns.InstallationID, Active: true},
			{Ref: model.RefByID("project:immutable-beta", model.Natural64(0)), Name: "beta", InstallationID: ns.InstallationID, Active: true},
		},
		Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: by.Kind, ID: by.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: alpha}, Role: "involved"}},
	}
	source, err := NewMutableFacts(facts)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.bind(ctx, store, lease, ns); err != nil {
		t.Fatal(err)
	}
	rules := &workflowReadRules{namespace: ns}
	checker := &Checker{chain: authz.Chain{Namespace: ns, Rules: []authz.RuleSet{rules}}, source: source, caller: DefaultCallerSource(model.Natural64(1)), store: store, lease: lease}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	view := reader.Snapshot()
	_ = reader.Close()
	item := service.Bead{ID: "workflow:item/first", Kind: "workflow.item", Plane: "ephemeral", Project: "alpha"}
	delegate := &workflowReadDelegate{item: item, page: service.BeadPage{Items: []service.Bead{item}, AsOf: service.AsOf{Revision: *view.At, Incarnation: view.Incarnation}}}
	return &workflowReadFixture{guarded: &GuardedService{delegate: delegate, checker: checker, lease: lease}, delegate: delegate, rules: rules, facts: facts, source: source, by: by}
}

func workflowReadError(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	var value *model.Error
	if !errors.As(err, &value) || value.Data.Code != code {
		t.Fatalf("error=%v, want %s", err, code)
	}
}

func TestWorkflowGetProjectAuthorization(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newWorkflowReadFixture(t, engine)
			ctx := context.Background()
			req := service.GetRequest{ID: model.RefByID(f.delegate.item.ID, model.Natural64(0)), Project: "alpha", Agent: "same-agent"}
			got, err := f.guarded.Get(ctx, f.by, req)
			if err != nil || got.Bead.ID != req.ID.ID || f.delegate.lastGet.Project != "alpha" || f.delegate.lastGet.Agent != req.Agent {
				t.Fatalf("authorized Get=%+v err=%v delegate=%+v", got, err, f.delegate.lastGet)
			}
			if len(f.rules.checks) != 1 || f.rules.checks[0].Resource.Kind != authz.Project || f.rules.checks[0].Resource.Ref.ID != "project:immutable-alpha" || f.rules.checks[0].Method != "bead.get" {
				t.Fatalf("wrong project policy request: %+v", f.rules.checks)
			}
			for _, project := range []string{"", "unknown", "beta"} {
				req.Project = project
				_, err := f.guarded.Get(ctx, f.by, req)
				workflowReadError(t, err, "not_found")
			}
			if f.delegate.getCalls != 1 {
				t.Fatalf("denied project loaded payload: calls=%d", f.delegate.getCalls)
			}
			req.Project = "alpha"
			f.delegate.item.Project = "beta"
			_, err = f.guarded.Get(ctx, f.by, req)
			workflowReadError(t, err, "not_found")
		})
	}
}

func TestWorkflowReadyProjectAuthorizationBeforePagination(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newWorkflowReadFixture(t, engine)
			foreign := f.delegate.item
			foreign.ID, foreign.Project = "workflow:item/foreign", "beta"
			second := f.delegate.item
			second.ID = "workflow:item/second"
			f.delegate.page.Items = []service.Bead{foreign, f.delegate.item, second}
			req := service.ReadyRequest{Project: "alpha", Limit: 1}
			first, err := f.guarded.Ready(context.Background(), f.by, req)
			if err != nil || len(first.Items) != 1 || first.Items[0].ID != f.delegate.item.ID || first.Next == nil || first.Next.Offset != 1 {
				t.Fatalf("first authorized page=%+v err=%v", first, err)
			}
			if len(f.rules.checks) != 1 || f.rules.checks[0].Resource.Ref.ID != "project:immutable-alpha" || f.rules.checks[0].Method != "bead.ready" {
				t.Fatalf("wrong ready policy request: %+v", f.rules.checks)
			}
			req.Position = first.Next
			last, err := f.guarded.Ready(context.Background(), f.by, req)
			if err != nil || len(last.Items) != 1 || last.Items[0].ID != second.ID || last.Next != nil {
				t.Fatalf("last authorized page=%+v err=%v", last, err)
			}
			for _, project := range []string{"", "unknown", "beta"} {
				page, err := f.guarded.Ready(context.Background(), f.by, service.ReadyRequest{Project: project, Limit: 1})
				if err != nil || len(page.Items) != 0 || page.Next != nil {
					t.Fatalf("denied project %q leaked page=%+v err=%v", project, page, err)
				}
			}
			f.facts.Grants = nil
			if err := f.source.Replace(f.facts); err != nil {
				t.Fatal(err)
			}
			_, err = f.guarded.Ready(context.Background(), f.by, req)
			workflowReadError(t, err, "cursor_invalid")
			page, err := f.guarded.Ready(context.Background(), f.by, service.ReadyRequest{Project: "alpha", Limit: 1})
			if err != nil || len(page.Items) != 0 || page.Next != nil {
				t.Fatalf("revoked project leaked page=%+v err=%v", page, err)
			}
		})
	}
}

func TestWorkflowReadPolicyFailureAndRevocation(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		for _, scenario := range []string{"policy-failure", "final-fence", "fixed-policy-denies"} {
			t.Run(engine+"/"+scenario, func(t *testing.T) {
				f := newWorkflowReadFixture(t, engine)
				req := service.GetRequest{ID: model.RefByID(f.delegate.item.ID, model.Natural64(0)), Project: "alpha"}
				switch scenario {
				case "policy-failure":
					f.rules.failure = errors.New("policy unavailable")
					_, err := f.guarded.Get(context.Background(), f.by, req)
					workflowReadError(t, err, "authz_unavailable")
					if f.delegate.getCalls != 0 {
						t.Fatal("unavailable policy loaded workflow payload")
					}
					_, err = f.guarded.Ready(context.Background(), f.by, service.ReadyRequest{Project: "alpha"})
					workflowReadError(t, err, "authz_unavailable")
				case "final-fence":
					f.delegate.onGet = func() error {
						f.facts.Grants = nil
						return f.source.Replace(f.facts)
					}
					_, err := f.guarded.Get(context.Background(), f.by, req)
					workflowReadError(t, err, "authz_unavailable")
					if f.delegate.getCalls != 1 {
						t.Fatal("test did not revoke during workflow payload load")
					}
					_, err = f.guarded.Get(context.Background(), f.by, req)
					workflowReadError(t, err, "not_found")
					if f.delegate.getCalls != 1 {
						t.Fatal("revoked policy loaded workflow payload again")
					}
				case "fixed-policy-denies":
					fixed, err := authzfixed.New(f.facts.Namespace)
					if err != nil {
						t.Fatal(err)
					}
					f.guarded.checker.chain.Rules = []authz.RuleSet{fixed}
					_, err = f.guarded.Get(context.Background(), f.by, req)
					workflowReadError(t, err, "not_found")
					page, err := f.guarded.Ready(context.Background(), f.by, service.ReadyRequest{Project: "alpha"})
					if err != nil || len(page.Items) != 0 || f.delegate.getCalls != 0 {
						t.Fatalf("fixed policy acquired a workflow permit: page=%+v calls=%d err=%v", page, f.delegate.getCalls, err)
					}
				}
			})
		}
		t.Run(engine+"/ready-final-fence", func(t *testing.T) {
			f := newWorkflowReadFixture(t, engine)
			f.rules.onEval = func() error {
				f.facts.Grants = nil
				return f.source.Replace(f.facts)
			}
			_, err := f.guarded.Ready(context.Background(), f.by, service.ReadyRequest{Project: "alpha"})
			workflowReadError(t, err, "authz_unavailable")
		})
	}
}
