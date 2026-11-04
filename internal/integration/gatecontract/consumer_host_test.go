package gatecontract_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This declared, test-only host policy is deliberately separate from CP7.
// It permits a finite method set only for a User with an explicit current
// project grant. There is no wildcard grant, injected RPC identity or policy
// override in the production fixed host.
const consumerPolicyProfile = "b14-consumer-host-test-policy/v1"

type consumerPolicy struct {
	namespace authz.Namespace
	grants    map[string]map[string]bool
	stages    []port.AuthorizationStage
}

func (p *consumerPolicy) Compile(req authz.ListRequest) (authz.FilterTemplate, error) {
	permit := authz.FilterExpr{Op: authz.False}
	read := req.Action == authz.Read && (req.Method == "bead.get" || req.Method == "bead.ready" || req.Method == "history.get" || req.Method == "view.explain")
	write := req.Action == authz.Modify && (req.Method == "bead.create" || req.Method == "bead.link" || req.Method == "bead.unlink" || req.Method == "bead.close" || req.Method == "bead.cancel" || req.Method == "bead.reopen" || req.Method == "bead.update" || req.Method == "tx.apply")
	if read || write {
		permit = authz.And(authz.FilterExpr{Op: authz.PrincipalKind, Kind: "user"}, authz.FilterExpr{Op: authz.ActiveProject}, authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "involved"})
	}
	template := authz.FilterTemplate{Namespace: p.namespace, Request: req, Permit: permit, Forbid: authz.FilterExpr{Op: authz.False}}
	return template, template.Validate()
}

func (p *consumerPolicy) Eval(ctx context.Context, s authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	decisions := make([]authz.Decision, len(requests))
	for i, request := range requests {
		template, err := p.Compile(authz.ListRequest{Caller: request.Caller, Action: request.Action, ResourceKind: request.Resource.Kind, Method: request.Method})
		if err != nil {
			return nil, err
		}
		allowed, err := template.Match(ctx, s, request.Resource)
		if err != nil {
			return nil, err
		}
		decisions[i].Effect = authz.NoDecision
		if allowed {
			decisions[i] = authz.Decision{Effect: authz.Allow, Reasons: []string{consumerPolicyProfile}}
		}
	}
	return decisions, nil
}

type consumerSlice struct {
	policy *consumerPolicy
	reader authzrpc.Snapshot
	caller authz.Caller
}

func (s *consumerSlice) Namespace() authz.Namespace { return s.policy.namespace }
func (s *consumerSlice) Record(ctx context.Context, dependency authz.Dependency) error {
	switch dependency.Kind {
	case "record":
		_, err := s.reader.Get(ctx, []model.Ref{model.RefByID(dependency.Key, model.Natural64(0))})
		return err
	case "sentinel":
		key, err := driver.NewSentinel(dependency.Key)
		if err != nil {
			return err
		}
		_, err = s.reader.Observe(ctx, []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: key}})
		return err
	case "read_fact":
		key, err := driver.NewReadFact(dependency.Key)
		if err != nil {
			return err
		}
		_, err = s.reader.Observe(ctx, []driver.LockKey{{Kind: driver.LockReadFact, Fact: key}})
		return err
	default:
		return fmt.Errorf("unsupported test-policy dependency %q", dependency.Kind)
	}
}
func (s *consumerSlice) CheckContext(_ context.Context, caller authz.Caller) (bool, error) {
	return reflect.DeepEqual(s.caller, caller) && caller.Principal.Kind == "user", nil
}
func (s *consumerSlice) HasGrant(_ context.Context, subject authz.SubjectRef, scope authz.Scope, role string) (bool, error) {
	return subject.Kind == "user" && scope.Kind == authz.ProjectScope && role == "involved" && s.policy.grants[subject.ID][scope.Ref.ID], nil
}
func (*consumerSlice) Groups(context.Context, authz.SubjectRef) ([]model.Ref, error) { return nil, nil }
func (*consumerSlice) TeamActive(context.Context, model.Ref) (bool, error)           { return false, nil }
func (*consumerSlice) ProjectActive(_ context.Context, ref model.Ref) (bool, error) {
	return ref.ID == "local" || ref.ID == "hidden" || ref.ID == "children", nil
}
func (s *consumerSlice) ProjectForBead(ctx context.Context, ref model.Ref) (authz.ProjectBinding, error) {
	rows, err := s.reader.Get(ctx, []model.Ref{ref})
	if err != nil || len(rows) == 0 {
		return authz.ProjectBinding{}, err
	}
	if len(rows) != 1 || rows[0].Kind != "bead" {
		return authz.ProjectBinding{}, fmt.Errorf("invalid test-policy bead")
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		return authz.ProjectBinding{}, err
	}
	return authz.ProjectBinding{Name: doc.Project, Ref: model.RefByID(doc.Project, model.Natural64(0)), Found: doc.Project != ""}, nil
}

func (p *consumerPolicy) check(ctx context.Context, reader authzrpc.Snapshot, by model.PrincipalRef, method string, action authz.Action, resources ...authz.Resource) error {
	caller, err := authzrpc.DefaultCallerSource(model.Natural64(1))(ctx, by)
	if err != nil {
		return err
	}
	requests := make([]authz.CheckRequest, len(resources))
	for i, resource := range resources {
		requests[i] = authz.CheckRequest{Caller: caller, Action: action, Resource: resource, Method: method}
	}
	chain := authz.Chain{Namespace: p.namespace, Rules: []authz.RuleSet{p}}
	decisions, err := chain.Check(ctx, &consumerSlice{p, reader, caller}, requests)
	if err != nil {
		return err
	}
	for _, decision := range decisions {
		if decision.Effect != authz.Allow {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
	}
	return nil
}

func (p *consumerPolicy) Authorize(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, reader port.SnapshotReader) error {
	p.stages = append(p.stages, stage)
	resources := make([]authz.Resource, 0, len(proposal.Changes)*2)
	for _, change := range proposal.Changes {
		if change.Op.Kind == model.OpCreate {
			var doc pipeline.Document
			if err := json.Unmarshal(change.After, &doc); err != nil || doc.Project == "" {
				return fmt.Errorf("test-policy create missing assigned project")
			}
			resources = append(resources, authz.Resource{Kind: authz.Project, Ref: model.RefByID(doc.Project, model.Natural64(0))})
		} else {
			resources = append(resources, authz.Resource{Kind: authz.Bead, Ref: change.Resource.Ref})
		}
		if change.Op.Link != nil {
			resources = append(resources, authz.Resource{Kind: authz.Bead, Ref: change.Op.Link.To})
		}
	}
	for _, expect := range proposal.Expect {
		resources = append(resources, authz.Resource{Kind: authz.Bead, Ref: expect.Ref})
	}
	if len(resources) == 0 {
		return fmt.Errorf("test-policy write missing resources")
	}
	return p.check(ctx, reader, proposal.Principal, proposal.Meta.Method, authz.Modify, resources...)
}

type consumerHost struct {
	f        *fixture
	policy   *consumerPolicy
	token    string
	creator  string
	source   service.OntologySource
	sourceID model.Ref
}

func newConsumerHost(t *testing.T, f *fixture) *consumerHost {
	t.Helper()
	child := ontology.DefaultDocument()
	child.Statuses["accepted"] = ontology.Status{Name: "Accepted child", Category: ontology.Completed, Outcome: ontology.Success}
	workflow := child.Workflows["issue"]
	workflow.Statuses = append(workflow.Statuses, "accepted")
	workflow.Slots[ontology.Close] = "accepted"
	for i := range workflow.Transitions {
		if workflow.Transitions[i].To == "done" {
			workflow.Transitions[i].To = "accepted"
		}
	}
	child.Workflows["issue"] = workflow
	m := f.meta()
	h := &consumerHost{f: f, sourceID: model.RefByID("ontology/b14-consumer-test", model.Natural64(0)),
		source: service.OntologySource{Base: ontology.DefaultDocument(), Overlays: map[string]ontology.Document{"children": child}},
		policy: &consumerPolicy{namespace: authz.Namespace{StoreID: string(m.StoreID), Incarnation: m.Incarnation, InstallationID: consumerPolicyProfile, Authority: model.RefByID("authority", model.Natural64(0))}, grants: map[string]map[string]bool{
			"user:operator": {"local": true, "children": true},
			"user:creator":  {"local": true, "children": true, "hidden": true},
		}},
	}
	// Only host configuration is seeded. Every bead, parent, status transition,
	// link, metadata change and receipt below is produced by authenticated RPC.
	body, err := service.MarshalOntologySource(h.source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: consumerPolicyProfile}}, []conformance.FixtureChange{{Ref: h.sourceID, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	h.bind()
	t.Logf("authenticated policy profile: %s; public writes only; own-project child close slot=accepted", consumerPolicyProfile)
	return h
}

type consumerReads struct {
	*service.Service
	host *consumerHost
}

func (h *consumerHost) read(ctx context.Context, by model.PrincipalRef, method string, ref model.Ref) error {
	m, err := h.f.store.Meta(ctx)
	if err != nil {
		return err
	}
	reader, err := h.f.store.Read(ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		return err
	}
	defer reader.Close()
	return h.policy.check(ctx, reader, by, method, authz.Read, authz.Resource{Kind: authz.Bead, Ref: ref})
}

func (s *consumerReads) Get(ctx context.Context, by model.PrincipalRef, req service.GetRequest) (service.GetResult, error) {
	if err := s.host.read(ctx, by, "bead.get", req.ID); err != nil {
		return service.GetResult{}, err
	}
	return s.Service.Get(ctx, by, req)
}
func (s *consumerReads) HistoryGet(ctx context.Context, by model.PrincipalRef, req service.HistoryVersionRequest) (service.HistoryVersion, error) {
	if err := s.host.read(ctx, by, "history.get", req.Ref); err != nil {
		return service.HistoryVersion{}, err
	}
	return s.Service.HistoryGet(ctx, by, req)
}
func (s *consumerReads) Ready(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
	page, err := s.Service.Ready(ctx, by, req)
	if err != nil {
		return page, err
	}
	items := make([]service.Bead, 0, len(page.Items))
	for _, bead := range page.Items {
		if err := s.host.read(ctx, by, "bead.ready", model.RefByID(bead.ID, model.Natural64(0))); err != nil {
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "forbidden" {
				return service.BeadPage{}, err
			}
			continue
		}
		items = append(items, bead)
	}
	page.Items = items
	return page, nil
}

// The saved view definition is trusted test host configuration. Its resource
// content and blocker verdicts always come from the actual pinned store reader.
func (h *consumerHost) GetView(ctx context.Context, by model.PrincipalRef, id string) (viewops.Definition, error) {
	if id != "b14-consumer" {
		return viewops.Definition{}, fmt.Errorf("unknown test view")
	}
	return viewops.Definition{Filter: &viewops.Filter{}}, nil
}

func (h *consumerHost) bind() {
	f := h.f
	f.t.Helper()
	snapshot, problems := ontology.Compile(h.source.Base, h.source.Overlays, h.source.Plugins)
	if len(problems) != 0 {
		f.t.Fatalf("host test ontology: %+v", problems)
	}
	o := service.NewOntology(snapshot.Effective)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithAuthorizer(h.policy))
	domain := service.New(f.store, f.lease, writer, service.WithOntology(o))
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err = rpc.BindService(registry, &consumerReads{domain, h}); err != nil {
		f.t.Fatal(err)
	}
	views, err := viewops.New(f.store, f.lease, viewops.Config{Views: h, OntologyRevision: model.Natural64(1), PolicyVersion: consumerPolicyProfile,
		AuthorizeRead: func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
			return h.read(ctx, by, "view.explain", resource.Ref)
		},
		DirectBlockers: func(ctx context.Context, reader driver.Reader, dependent model.Ref, limit int) (viewops.DirectBlockerVerdict, error) {
			verdict, err := service.ReadDirectBlockers(ctx, reader, service.DirectBlockerRequest{Dependent: dependent, Ontology: service.StaticOntologySource(h.sourceID), Limit: limit})
			out := viewops.DirectBlockerVerdict{Edges: make([]viewops.DirectBlockerEdge, 0, len(verdict.Edges))}
			for _, edge := range verdict.Edges {
				out.Edges = append(out.Edges, viewops.DirectBlockerEdge{Blocker: edge.Blocker, TargetKind: edge.TargetKind, Kind: edge.Kind, State: viewops.DirectBlockerState(edge.State), Reason: edge.Reason})
			}
			return out, err
		},
	})
	if err != nil {
		f.t.Fatal(err)
	}
	if err = viewrpc.Bind(registry, views); err != nil {
		f.t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "local", Incarnation: f.meta().Incarnation, AuthMode: "authenticated"})
	if err != nil {
		f.t.Fatal(err)
	}
	keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	provider, err := authn.New(authn.Config{Provider: keys, AuthorizationInstalled: func() bool { return true }})
	if err != nil {
		f.t.Fatal(err)
	}
	f.principal = model.PrincipalRef{Kind: "user", ID: "user:operator"}
	for _, principal := range []model.PrincipalRef{f.principal, {Kind: "user", ID: "user:creator"}} {
		issued, err := keys.Issue(principal, time.Now().Add(time.Hour))
		if err != nil {
			f.t.Fatal(err)
		}
		if principal.ID == f.principal.ID {
			h.token = issued.Token
		} else {
			h.creator = issued.Token
		}
	}
	handler, err := httpjson.New(server, provider, httpjson.Config{})
	if err != nil {
		f.t.Fatal(err)
	}
	transport := httptest.NewServer(handler)
	f.t.Cleanup(transport.Close)
	f.roundTrip = func(ctx context.Context, raw []byte) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, transport.URL+"/rpc/v1", bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Plug-RPC", "1")
		req.Header.Set("Content-Type", "application/json")
		if h.token != "" {
			req.Header.Set("Authorization", "Bearer "+h.token)
		}
		response, err := transport.Client().Do(req)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		return io.ReadAll(response.Body)
	}
}
