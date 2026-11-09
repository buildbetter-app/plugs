package authzrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type workflowClaimNextFixture struct {
	store    driver.Store
	lease    driver.ActiveLease
	provider *workflowplugin.Provider
	domain   *service.Service
	app      *Composition
	keys     *authnapikey.Provider
	by       model.PrincipalRef
	facts    authzfixed.Facts
	source   *MutableFacts
	now      time.Time
}

func newWorkflowClaimNextFixture(t *testing.T) *workflowClaimNextFixture {
	t.Helper()
	ctx := context.Background()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "claimnext.db")})
	if err != nil {
		t.Fatal(err)
	}
	f := &workflowClaimNextFixture{store: store, now: time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)}
	t.Cleanup(func() { _ = store.Close() })
	f.lease, err = store.Lease(ctx, "workflow-claimnext", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.provider, err = workflowplugin.New(pluginhost.NewDurableState(store.(driver.CheckpointStore), f.lease, workflowplugin.Alias, "default"))
	if err != nil {
		t.Fatal(err)
	}
	f.provider.WithClock(func() time.Time { return f.now }).WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
	f.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	project := model.RefByID("project:immutable-claimnext", model.Natural64(0))
	facts := authzfixed.Facts{Namespace: ns,
		Projects: []authzfixed.Project{{Ref: project, Name: "project", InstallationID: "fixed", Active: true}},
		Grants:   []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:sponsor"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"}},
	}
	f.facts = facts
	f.source, err = NewMutableFacts(facts)
	if err != nil {
		t.Fatal(err)
	}
	f.app, err = New(ctx, Config{Store: store, Lease: f.lease, Provider: f.keys, Facts: f.source, Namespace: ns, DefaultProject: "project", Workflow: f.provider})
	if err != nil {
		t.Fatal(err)
	}
	f.domain = f.app.Service.delegate.(*service.Service)
	f.by = model.PrincipalRef{Kind: "agent", ID: "agent:workflow-owner", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:sponsor"}}
	return f
}

func workflowClaimNextMeta(method string, key int) model.WriteMeta {
	return model.WriteMeta{Method: method, Project: "project", Agent: "client-label", IdempotencyKey: fmt.Sprintf("%032x", key)}
}

func TestGuardedClaimNextSelectionBudgetPreservesRetryAndCaller(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		t.Run(fmt.Sprintf("inherited=%t", inherited), func(t *testing.T) {
			f := newWorkflowClaimNextFixture(t)
			items := f.invoke(t, 910, 1)
			before, err := json.Marshal(workflowWriteCheckpoint(t, f))
			if err != nil {
				t.Fatal(err)
			}
			caller, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			ctx := caller
			var shared time.Time
			if inherited {
				shared = time.Now().Add(time.Second)
				ctx = pipeline.WithPreparationDeadline(ctx, shared)
			}
			page := f.app.Service.claimNextPage
			var observed time.Time
			f.app.Service.claimNextPage = func(ctx context.Context, _ model.PrincipalRef, _ service.ReadyRequest) (service.BeadPage, error) {
				observed, _ = ctx.Deadline()
				<-ctx.Done()
				return service.BeadPage{}, ctx.Err()
			}
			meta := workflowClaimNextMeta("bead.claimNext", 911)
			req := service.ClaimNextRequest{Project: "project"}
			out, err := f.app.Service.ClaimNextWithOutcome(ctx, f.by, req, meta)
			var typed *model.Error
			after, marshalErr := json.Marshal(workflowWriteCheckpoint(t, f))
			callerDeadline, _ := caller.Deadline()
			if out != nil || !errors.As(err, &typed) || typed.Data.Code != "busy" || !typed.Data.Retryable || caller.Err() != nil || !observed.Before(callerDeadline) || marshalErr != nil || !bytes.Equal(before, after) {
				t.Fatalf("guarded selection lost bounded retry/conservation: outcome=%v error=%v caller=%v preparation=%s checkpointError=%v", out, err, caller.Err(), observed, marshalErr)
			}
			if inherited && !observed.Equal(shared) {
				t.Fatalf("guarded selection restarted inherited budget: got=%s want=%s", observed, shared)
			}
			f.app.Service.claimNextPage = page
			first, err := f.app.Service.ClaimNextWithOutcome(caller, f.by, req, meta)
			if err != nil || first == nil || first.Replayed || len(first.Commit.Results) != 1 || first.Commit.Results[0].Ref.ID != items[0].ID {
				t.Fatalf("busy response prevented same-key retry: outcome=%v error=%v", first, err)
			}
			replayed, err := f.app.Service.ClaimNextWithOutcome(caller, f.by, req, meta)
			if err != nil || replayed == nil || !replayed.Replayed || len(replayed.Commit.Results) != 1 || replayed.Commit.Results[0].Ref.ID != first.Commit.Results[0].Ref.ID {
				t.Fatalf("same-key retry lost its single winner: outcome=%v error=%v", replayed, err)
			}
		})
	}
}

func TestGuardedClaimNextPreparationDoesNotMaskCallerExpiry(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	f.app.Service.claimNextPage = func(ctx context.Context, _ model.PrincipalRef, _ service.ReadyRequest) (service.BeadPage, error) {
		<-ctx.Done()
		return service.BeadPage{}, ctx.Err()
	}
	caller, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	out, err := f.app.Service.ClaimNextWithOutcome(caller, f.by, service.ClaimNextRequest{Project: "project"}, workflowClaimNextMeta("bead.claimNext", 912))
	if out != nil || !errors.Is(err, context.DeadlineExceeded) || caller.Err() != context.DeadlineExceeded {
		t.Fatalf("preparation masked the caller's actual deadline: outcome=%v error=%v caller=%v", out, err, caller.Err())
	}
}

func (f *workflowClaimNextFixture) invoke(t *testing.T, key int, count int) []workflowplugin.ItemView {
	t.Helper()
	items := make([]workflowplugin.ChecklistItem, count)
	for i := range items {
		items[i] = workflowplugin.ChecklistItem{Label: fmt.Sprintf("step-%d", i), Title: fmt.Sprintf("private title %d", i), Type: "task"}
	}
	run, err := f.domain.WorkflowInvoke(context.Background(), f.by, workflowplugin.InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: items}, workflowClaimNextMeta("x.workflow.invoke", key))
	if err != nil {
		t.Fatal(err)
	}
	return run.Items
}

type workflowClaimNextReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func (f *workflowClaimNextFixture) call(t *testing.T, filter *service.Filter, key int) workflowClaimNextReply {
	t.Helper()
	issued, err := f.keys.Issue(f.by, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"_meta": map[string]any{"project": "project", "idempotencyKey": fmt.Sprintf("%032x", key), "agent": "client-label"}}
	if filter != nil {
		selector := map[string]any{}
		if filter.Types != nil {
			selector["types"] = filter.Types
		}
		if filter.Resources != nil {
			selector["resources"] = filter.Resources
		}
		params["filter"] = selector
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "bead.claimNext", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issued.Token)
	response := httptest.NewRecorder()
	f.app.Handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status=%d", response.Code)
	}
	var got workflowClaimNextReply
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func workflowClaimNextCommit(t *testing.T, got workflowClaimNextReply) model.Commit {
	t.Helper()
	if got.Error != nil || bytes.Equal(got.Result, []byte("null")) {
		t.Fatalf("expected a committed claim: %+v result=%s", got.Error, got.Result)
	}
	var commit model.Commit
	if err := json.Unmarshal(got.Result, &commit); err != nil {
		t.Fatal(err)
	}
	if len(commit.Results) != 1 || commit.LeaseTtlMs == nil || commit.LeaseTtlMs.String() != "1800000" || commit.LeaseExpiresAt == nil {
		t.Fatalf("claim result lost its identity or original lease: %+v", commit)
	}
	return commit
}

func TestWorkflowClaimNextGuardedRetryPrecedesNewCoreCandidate(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	items := f.invoke(t, 1, 1)
	first := f.call(t, &service.Filter{Types: []string{"task", "issue"}}, 2)
	commit := workflowClaimNextCommit(t, first)
	if first.Meta.Replayed || commit.Results[0].Ref.ID != items[0].ID {
		t.Fatal("first claim did not select the workflow item")
	}
	core := model.RefByID("00000000-0000-4000-8000-000000000001", model.Natural64(0))
	_, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: core, Kind: "bead", Op: model.OpCreate,
		Body: []byte(`{"id":"` + core.ID + `","type":"issue","project":"project","fields":{"title":"new core item","status":"todo","priority":0}}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	// The retained result stays byte-identical even after its lease expires;
	// the new ordinary ready candidate must not consume the retried selector.
	f.now = f.now.Add(2 * workflowplugin.ClaimTTL)
	retry := f.call(t, &service.Filter{Types: []string{"issue", "task", "task"}}, 2)
	workflowClaimNextCommit(t, retry)
	if !retry.Meta.Replayed || !bytes.Equal(first.Result, retry.Result) {
		t.Fatalf("response-loss retry changed its original result: first=%s retry=%s", first.Result, retry.Result)
	}
	current, err := f.domain.Get(context.Background(), f.by, service.GetRequest{ID: core})
	if err != nil || current.Lease != nil {
		t.Fatalf("workflow retry consumed the new ordinary candidate: lease=%+v err=%v", current.Lease, err)
	}
	changed := f.call(t, &service.Filter{Types: []string{"issue", "task"}, Resources: []string{items[0].ID}}, 2)
	if changed.Error == nil || changed.Error.Data.Code != "idempotency_key_reused" {
		t.Fatal("selector change did not retain its idempotency error")
	}
}

func TestWorkflowClaimNextEmptyFallbackReturnsLateCommittedItem(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	page := f.app.Service.claimNextPage
	passes := 0
	var items []workflowplugin.ItemView
	f.app.Service.claimNextPage = func(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
		result, err := page(ctx, by, req)
		if err == nil && len(result.Items) == 0 && result.Next == nil {
			passes++
			if passes == 2 {
				// Commit a real workflow after the final empty live snapshot and
				// before the provider's terminal selection seam.
				items = f.invoke(t, 10, 1)
			}
		}
		return result, err
	}
	first := f.call(t, nil, 11)
	commit := workflowClaimNextCommit(t, first)
	if passes != 2 || first.Meta.Replayed || commit.Results[0].Ref.ID != items[0].ID {
		t.Fatal("empty fallback discarded the newly committed workflow result")
	}
	retry := f.call(t, nil, 11)
	if !retry.Meta.Replayed || !bytes.Equal(first.Result, retry.Result) || passes != 2 {
		t.Fatal("empty fallback retry walked the queue or changed its result")
	}
}

func TestWorkflowClaimNextServiceTerminalReplayKeepsOriginalIdentity(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	items := f.invoke(t, 20, 2)
	req := service.ClaimNextRequest{Project: "project", Filter: &service.Filter{Types: []string{"task"}}}
	meta := workflowClaimNextMeta("bead.claimNext", 21)
	meta.Project = "stale-client-project"
	first, err := f.domain.ClaimNextWithOutcome(context.Background(), f.by, req, meta)
	if err != nil || first == nil || first.NullResult || first.Replayed || len(first.Commit.Results) != 1 {
		t.Fatalf("service selected claim: %+v err=%v", first, err)
	}
	for i, item := range items {
		_, err := f.domain.CloseWithOutcome(context.Background(), f.by, service.CloseRequest{ID: model.RefByID(item.ID, model.Natural64(0))}, workflowClaimNextMeta("bead.close", 22+i))
		if err != nil {
			t.Fatal(err)
		}
	}
	retry, err := f.domain.ClaimNextWithOutcome(context.Background(), f.by, req, meta)
	if err != nil || retry == nil || retry.NullResult || !retry.Replayed || len(retry.Commit.Results) != 1 {
		t.Fatalf("terminal service replay: %+v err=%v", retry, err)
	}
	if retry.Commit.Results[0].Ref != first.Commit.Results[0].Ref || retry.Commit.Results[0].Revision != first.Commit.Results[0].Revision || retry.Commit.LeaseExpiresAt != nil || retry.Commit.LeaseTtlMs != nil || len(retry.Commit.Results[0].Record) != 0 {
		t.Fatal("terminal replay changed the original identity or fabricated lease/payload fields")
	}
}

func TestWorkflowClaimNextServiceSelectedReplaysPriorEmpty(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	req := service.ClaimNextRequest{Project: "project"}
	meta := workflowClaimNextMeta("bead.claimNext", 30)
	var err error
	meta.ClaimNextIdentity, err = service.ClaimNextRequestIdentity(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, replayed, err := f.provider.ClaimNext(context.Background(), f.by, req.Project, nil, meta); err != nil || found || replayed {
		t.Fatalf("empty receipt: found=%v replayed=%v err=%v", found, replayed, err)
	}
	items := f.invoke(t, 31, 1)
	ref := model.RefByID(items[0].ID, model.Natural64(0))
	out, err := f.domain.ClaimWithOutcome(context.Background(), f.by, service.ClaimRequest{ID: ref}, meta)
	if err != nil || !out.Replayed || !out.NullResult || len(out.Commit.Results) != 0 {
		t.Fatalf("selected candidate overwrote a prior empty result: %+v err=%v", out, err)
	}
	item, err := f.provider.Get(context.Background(), f.by, ref, meta)
	if err != nil || item.Claim != nil || item.Status != "open" {
		t.Fatalf("prior empty replay claimed the later item: %+v err=%v", item, err)
	}
	changed := req
	changed.Filter = &service.Filter{Types: []string{"task"}}
	_, _, err = f.domain.ReplayWorkflowClaimNext(context.Background(), f.by, changed, meta)
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "idempotency_key_reused" {
		t.Fatalf("changed empty selector: %v", err)
	}
}

func TestWorkflowClaimNextResourcesDoNotFallBackToExcludedItem(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	items := f.invoke(t, 40, 2)
	req := service.ClaimNextRequest{Project: "project", Filter: &service.Filter{Resources: []string{"workflow:item/00000000000000000000000000000000"}}}
	got := f.call(t, req.Filter, 41)
	if got.Error != nil || !bytes.Equal(got.Result, []byte("null")) || got.Meta.Replayed {
		t.Fatalf("excluded resource unexpectedly selected a workflow item: %+v result=%s", got.Error, got.Result)
	}
	for _, item := range items {
		view, getErr := f.provider.Get(context.Background(), f.by, model.RefByID(item.ID, model.Natural64(0)), workflowClaimNextMeta("bead.get", 42))
		if getErr != nil {
			t.Fatal(getErr)
		}
		if view.Claim != nil {
			t.Fatalf("legacy terminal fallback claimed excluded item %s", item.ID)
		}
	}
}
