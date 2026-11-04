package authzrpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

// This recorder forwards the real maintained capability. It counts actual
// source hydration and raw queries at the complete guarded request boundary.
type readyRequestStore struct {
	driver.Store
	enabled                                 bool
	gets                                    map[string]int
	queries                                 int
	plans                                   []driver.ReadyOrderPlan
	afterSelect                             func()
	freshEnabled                            bool
	beforeWrite                             func()
	freshError                              error
	afterFresh                              func()
	freshCalls, foundFresh, terminalCommits int
}

func (s *readyRequestStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &readyRequestReader{Reader: r, store: s}, nil
}

type readyRequestReader struct {
	driver.Reader
	store *readyRequestStore
}

func (r *readyRequestReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	for _, ref := range refs {
		r.store.gets[ref.ID]++
	}
	return r.Reader.Get(ctx, refs)
}
func (r *readyRequestReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.store.queries++
	return r.Reader.Query(ctx, q)
}
func (r *readyRequestReader) SelectReadyOrder(ctx context.Context, p driver.ReadyOrderPlan) (driver.ReadyOrderResult, error) {
	if !r.store.enabled {
		return driver.ReadyOrderResult{}, driver.ErrUnsupported
	}
	r.store.plans = append(r.store.plans, p)
	result, err := r.Reader.(driver.ReadyOrderReader).SelectReadyOrder(ctx, p)
	if err == nil && r.store.afterSelect != nil {
		f := r.store.afterSelect
		r.store.afterSelect = nil
		f()
	}
	return result, err
}
func (s *readyRequestStore) reset() { s.gets = map[string]int{}; s.queries = 0; s.plans = nil }

type readyRequestFixture struct {
	store  *readyRequestStore
	app    *Composition
	source *service.Service
	by     model.PrincipalRef
	refs   []model.Ref
	parent model.Ref
	write  int
}

func (f *readyRequestFixture) meta() model.WriteMeta {
	f.write++
	return model.WriteMeta{Project: "local", Agent: "w10", AgentLabel: "w10", IdempotencyKey: fmt.Sprintf("%064x", f.write)}
}

func newReadyRequestFixture(t *testing.T, count int) *readyRequestFixture {
	t.Helper()
	ctx := context.Background()
	s := &readyRequestStore{Store: memory.New(), enabled: true, gets: map[string]int{}}
	t.Cleanup(func() { _ = s.Close() })
	lease, err := s.Lease(ctx, "w10-request", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	o := service.NewOntology(nil)
	next := 0
	w := pipeline.New(s, lease, pipeline.WithOntology(o), pipeline.WithIDGenerator(func() (string, error) { id := fmt.Sprintf("pl-w10-%04d", next); next++; return id, nil }))
	f := &readyRequestFixture{store: s, source: service.New(s, lease, w, service.WithOntology(o)), by: model.PrincipalRef{Kind: "user", ID: "user:reader"}}
	// Publish every source through the real Writer before installing the read
	// boundary. The parent is deliberately outside the caller's read scope.
	m := f.meta()
	m.Project = "secret"
	parent, err := f.source.Create(ctx, f.by, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "hidden parent", "priority": 0}}, m)
	if err != nil {
		t.Fatal(err)
	}
	f.parent = parent.Results[0].Ref
	for start := 0; start < count; start += 100 {
		ops := []model.Op{}
		for i := start; i < min(count, start+100); i++ {
			ops = append(ops, model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Type: "issue", Parent: &f.parent, Fields: map[string]any{"title": fmt.Sprintf("child%d", i), "priority": 0}}})
		}
		commit, err := f.source.Apply(ctx, f.by, model.ChangeSet{Ops: ops}, f.meta())
		if err != nil {
			t.Fatal(err)
		}
		for _, result := range commit.Results {
			f.refs = append(f.refs, result.Ref)
		}
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	project := model.RefByID("local", model.Natural64(0))
	// Fixed user reads are otherwise unrestricted. The existing project caveat
	// makes the shared secret-project parent unreadable without adding policy.
	f.by.Caveats = map[string]any{"projects": []model.Ref{project}}
	facts := authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: project, Name: "local", InstallationID: "fixed", Active: true}}, Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: f.by.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"}}}
	f.app, err = New(ctx, Config{Store: s, Lease: lease, Provider: authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }}), Facts: StaticFacts{Facts: facts}, Namespace: ns, DefaultProject: "local"})
	if err != nil {
		t.Fatal(err)
	}
	s.reset()
	return f
}

func TestCompiledReadyPage1000HydratesOnlyEmittedBodies(t *testing.T) {
	f := newReadyRequestFixture(t, 1001)
	ctx := context.Background()
	page, err := f.app.Service.Ready(ctx, f.by, service.ReadyRequest{Project: "local", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1000 || page.Next == nil || page.Next.Offset != 1000 || !strings.HasPrefix(page.ReadyMembership, "v2:") {
		t.Fatalf("page: %+v", page)
	}
	for i, item := range page.Items {
		if item.ID != f.refs[i].ID || item.Parent != "" || !item.ParentHidden || f.store.gets[item.ID] != 1 {
			t.Fatalf("emitted source/parent%d: %+v reads%d", i, item, f.store.gets[item.ID])
		}
	}
	if f.store.gets[f.refs[1000].ID] != 0 || f.store.gets[f.parent.ID] != 1 || f.store.queries != 0 || len(f.store.plans) != 2 {
		t.Fatalf("payload work: tail%d parent%d queries%d plans%d", f.store.gets[f.refs[1000].ID], f.store.gets[f.parent.ID], f.store.queries, len(f.store.plans))
	}
	t.Logf("READY_REQUEST page=1000 emitted_body_reads=1000 distinct_parent_reads=1 lookahead_body_reads=0 raw_queries=0 commitment=%s", page.ReadyMembership)
	// An unavailable accelerator retains the same v2 cursor and exact result.
	f.store.enabled = false
	tail, err := f.app.Service.Ready(ctx, f.by, service.ReadyRequest{Project: "local", Limit: 1000, Position: page.Next})
	if err != nil || len(tail.Items) != 1 || tail.Items[0].ID != f.refs[1000].ID || tail.ReadyMembership != page.ReadyMembership {
		t.Fatalf("fallback continuation: %+v %v", tail, err)
	}
	// Legacy cursors still validate their original full-sequence SHA-256.
	all := append(append([]service.Bead{}, page.Items...), tail.Items...)
	legacy := *page.Next
	legacy.ReadyMembership = readyMembershipDigest(all)
	f.store.enabled = true
	old, err := f.app.Service.Ready(ctx, f.by, service.ReadyRequest{Project: "local", Limit: 1000, Position: &legacy})
	if err != nil || len(old.Items) != 1 || old.ReadyMembership != legacy.ReadyMembership {
		t.Fatalf("legacy continuation: %+v %v", old, err)
	}
	if _, err = f.source.Update(ctx, f.by, service.UpdateRequest{ID: f.refs[1000], Fields: map[string]any{"description": "authorized tail changed"}}, f.meta()); err != nil {
		t.Fatal(err)
	}
	_, err = f.app.Service.Ready(ctx, f.by, service.ReadyRequest{Project: "local", Limit: 1000, Position: page.Next})
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "cursor_invalid" {
		t.Fatalf("tail mutation did not invalidate complete sequence: %v", err)
	}
}

func TestCompiledClaimUsesRawPivotBeforeModifyAndRealWriter(t *testing.T) {
	f := newReadyRequestFixture(t, 31)
	ctx := context.Background()
	allowed := []model.Ref{f.refs[2], f.refs[13], f.refs[28]}
	by := model.PrincipalRef{Kind: "agent", ID: "agent:w10", Sponsor: &f.by, Caveats: map[string]any{"actions": []string{"modify"}, "refs": allowed}}
	req := service.ClaimNextRequest{Project: "local"}
	identity, err := service.ClaimNextRequestIdentity(req)
	if err != nil {
		t.Fatal(err)
	}
	meta := f.meta()
	seed := service.ClaimNextSelectionSeed(identity, meta.IdempotencyKey)
	f.store.enabled = false
	raw, err := f.source.ClaimNextReadyPage(ctx, f.by, service.ReadyRequest{Project: "local", Limit: 1000, ClaimNextSeed: seed})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{}
	for _, item := range raw.Items {
		for _, ref := range allowed {
			if item.ID == ref.ID {
				want = append(want, item.ID)
			}
		}
	}
	f.store.enabled = true
	f.store.reset()
	page, err := f.app.Service.compiledClaimPage(ctx, by, service.ReadyRequest{Project: "local", Limit: 1000, ClaimNextSeed: seed})
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, item := range page.Items {
		got = append(got, item.ID)
	}
	if !reflect.DeepEqual(got, want) || len(got) != 3 || f.store.queries != 0 {
		t.Fatalf("raw pivot/auth order: got%v want%v queries%d", got, want, f.store.queries)
	}
	for _, ref := range f.refs {
		expected := 0
		for _, id := range want {
			if id == ref.ID {
				expected = 1
			}
		}
		if f.store.gets[ref.ID] != expected {
			t.Fatalf("hidden candidate hydrated: %s reads%d", ref.ID, f.store.gets[ref.ID])
		}
	}
	out, err := f.app.Service.ClaimNextWithOutcome(ctx, by, req, meta)
	if err != nil || out == nil || out.NullResult || len(out.Commit.Results) != 1 || out.Commit.Results[0].Ref.ID != want[0] {
		t.Fatalf("real Writer claim: %+v %v want%s", out, err, want[0])
	}
	t.Logf("READY_RAW_PIVOT raw=31 authorized=3 selected=%s real_writer=true", want[0])
}

func TestCompiledReadyEmptyPageKeepsFinalAuthorityFence(t *testing.T) {
	f := newReadyRequestFixture(t, 1)
	// Even with no authorized refs, mutate the live admission receipt after
	// selection. The empty page must fail its final current authority check.
	epoch := model.Natural64(1)
	f.app.Checker.caller = func(ctx context.Context, p model.PrincipalRef) (authz.Caller, error) {
		return DefaultCallerSource(epoch)(ctx, p)
	}
	f.store.afterSelect = func() { epoch = model.Natural64(2) }
	by := model.PrincipalRef{Kind: "agent", ID: "agent:empty", Sponsor: &f.by, Caveats: map[string]any{"refs": []model.Ref{}}}
	_, err := f.app.Service.Ready(context.Background(), by, service.ReadyRequest{Project: "local", Limit: 1000})
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "authz_unavailable" {
		t.Fatalf("empty page bypassed final caller fence: %v", err)
	}
}
