package s1b05b08

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// These are authored Plug assertions, not paired reference observations. The
// provider is always backed by the actual selected store; memory is a refusal
// case and is never given a persistence marker by the fixture.
type ephemeralCycleFixture struct {
	*cycleRPCFixture
	storage driver.Driver
	dsn     string
	by      model.PrincipalRef
	token   string
	domain  *service.Service
}

func newEphemeralCycleFixture(t *testing.T, engine string) *ephemeralCycleFixture {
	t.Helper()
	f := &ephemeralCycleFixture{cycleRPCFixture: &cycleRPCFixture{project: model.RefByID("visible-project", model.Natural64(0))}}
	f.by = model.PrincipalRef{Kind: "user", ID: "user:workflow-cycle-reader", Caveats: map[string]any{"projects": []model.Ref{f.project}}}
	f.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	issued, err := f.keys.Issue(f.by, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	f.token = issued.Token
	switch engine {
	case "memory":
		f.store = memory.New()
	case "sqlite":
		f.storage, f.dsn = sqlite.Driver{}, filepath.Join(t.TempDir(), "ephemeral-cycles.db")
	case "postgres":
		f.storage, f.dsn = postgresFixture(t)
	default:
		t.Fatalf("unknown engine %q", engine)
	}
	f.open(t)
	t.Cleanup(func() {
		_ = f.lease.Release(context.Background())
		_ = f.store.Close()
	})
	seedCycleRPCRows(t, f.store, f.lease, defaultCycleRPCRows())
	return f
}

func (f *ephemeralCycleFixture) open(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	var err error
	if f.storage != nil {
		f.store, err = f.storage.Open(ctx, driver.Config{DSN: f.dsn})
		if err != nil {
			t.Fatal(err)
		}
	}
	f.lease, err = f.store.Lease(ctx, "two-plane-cycles", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	f.facts = authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{
		{Ref: f.project, Name: "visible", InstallationID: "fixed", Active: true},
		{Ref: model.RefByID("secret-project", model.Natural64(0)), Name: "secret", InstallationID: "fixed", Active: true},
	}}
	config := authzrpc.Config{Store: f.store, Lease: f.lease, Provider: f.keys, Facts: authzrpc.StaticFacts{Facts: f.facts}, Namespace: ns, DefaultProject: "visible"}
	if capability, ok := f.store.(driver.PersistentCheckpointStore); ok && capability.Persistent() {
		provider, err := workflowplugin.New(pluginhost.NewDurableState(capability, f.lease, workflowplugin.Alias, "default"))
		if err != nil {
			t.Fatal(err)
		}
		config.Workflow = provider.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
	}
	f.app, err = authzrpc.New(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	// The direct service sets up only genuine supported workflow state.
	// Report requests use the unmodified fixed policy and real API admission.
	f.domain = service.New(f.store, f.lease, pipeline.New(f.store, f.lease), service.WithWorkflow(config.Workflow))
}

func (f *ephemeralCycleFixture) reopen(t *testing.T) {
	t.Helper()
	if f.storage == nil {
		t.Fatal("positive workflow restart requires a persistent store")
	}
	if err := f.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.open(t)
}

func (f *ephemeralCycleFixture) request(t *testing.T, method string, params map[string]any, operation string) envelope {
	t.Helper()
	copyParams := make(map[string]any, len(params)+1)
	for k, v := range params {
		copyParams[k] = v
	}
	metadata := map[string]any{"project": "visible"}
	if operation != "" {
		metadata["idempotencyKey"] = key("two-plane/" + operation)
	}
	copyParams["_meta"] = metadata
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": copyParams})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+f.token)
	response := httptest.NewRecorder()
	f.app.Handler.ServeHTTP(response, req)
	var got envelope
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &got) != nil {
		t.Fatalf("%s invalid HTTP/RPC response: status=%d body=%s", method, response.Code, response.Body.Bytes())
	}
	if got.Error != nil && len(got.Result) != 0 {
		t.Fatalf("%s refusal disclosed a result: %s", method, got.Result)
	}
	return got
}

func (f *ephemeralCycleFixture) invoke(t *testing.T, operation string, count int) []model.Ref {
	t.Helper()
	items := make([]workflowplugin.ChecklistItem, count)
	for i := range items {
		items[i] = workflowplugin.ChecklistItem{Label: model.Natural64(uint64(i)).String(), Title: "private ephemeral title", Type: "task"}
	}
	run, err := f.domain.WorkflowInvoke(context.Background(), f.by, workflowplugin.InvokeRequest{Template: "cycle-template", Project: "visible", Mode: "ephemeral", Checklist: items}, ephemeralCycleWriteMeta("x.workflow.invoke", operation))
	if err != nil || run.Plane != "ephemeral" || len(run.Items) != count {
		t.Fatalf("real persistent invocation failed: error=%v result=%+v", err, run)
	}
	refs := make([]model.Ref, len(run.Items))
	for i, item := range run.Items {
		if item.Plane != "ephemeral" || item.Plugin != workflowplugin.Alias || item.RunID == "" {
			t.Fatalf("provider identity lost: %+v", item)
		}
		refs[i] = model.RefByID(item.ID, model.Natural64(0))
	}
	return refs
}

func ephemeralCycleWriteMeta(method, operation string) model.WriteMeta {
	return model.WriteMeta{Method: method, Project: "visible", IdempotencyKey: key("two-plane/" + operation)}
}

func (f *ephemeralCycleFixture) report(t *testing.T, ephemeral, tracks bool, limit int) service.CyclesResult {
	t.Helper()
	got := f.request(t, "bead.cycles", map[string]any{"includeEphemeral": ephemeral, "includeTracks": tracks, "limit": limit}, "")
	var result service.CyclesResult
	if got.Error != nil || json.Unmarshal(got.Result, &result) != nil {
		t.Fatalf("supported cycle report refused: error=%+v result=%s", got.Error, got.Result)
	}
	return result
}

type ephemeralCycleState struct {
	Archive    []byte
	Checkpoint driver.Checkpoint
	Graph      []driver.LockState
}

func (f *ephemeralCycleFixture) state(t *testing.T) ephemeralCycleState {
	t.Helper()
	ctx := context.Background()
	archive, _, err := restore.Dump(ctx, f.store, "full")
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := f.store.(driver.CheckpointStore).LoadCheckpoint(ctx, "plugin/workflow/default/durable")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	graph, err := reader.Observe(ctx, []driver.LockKey{driver.GraphBlockingSentinelLock})
	if err != nil {
		t.Fatal(err)
	}
	return ephemeralCycleState{Archive: archive, Checkpoint: checkpoint, Graph: graph}
}

func TestB06MemoryEphemeralCycleCapabilityRefusalConservesBothPlanes(t *testing.T) {
	f := newEphemeralCycleFixture(t, "memory")
	before := f.state(t)
	got := f.request(t, "bead.cycles", map[string]any{"includeEphemeral": true}, "")
	if got.Error == nil || got.Error.Data.Code != "plugin_durable_state_unavailable" {
		t.Fatalf("memory capability error=%+v", got.Error)
	}
	direct, err := f.domain.Cycles(context.Background(), f.by, service.CyclesRequest{Project: "visible", IncludeEphemeral: true})
	var refused *model.Error
	if !errors.As(err, &refused) || refused.Data.Code != "plugin_durable_state_unavailable" || !reflect.DeepEqual(direct, service.CyclesResult{}) {
		t.Fatalf("direct memory capability refusal published a report: result=%+v error=%v", direct, err)
	}
	if after := f.state(t); !reflect.DeepEqual(before, after) {
		t.Fatal("memory capability refusal changed graph, history, receipts, checkpoint or sentinel")
	}
	invalid := f.request(t, "bead.cycles", map[string]any{"includeEphemeral": true, "limit": 1001}, "")
	if invalid.Error == nil || invalid.Error.Data.Code != "invalid_params" {
		t.Fatalf("malformed report lost validation precedence: %+v", invalid.Error)
	}
	if got := f.report(t, false, false, 10); got.CycleCount.String() != "3" || got.Truncated || len(got.Items) != 3 {
		t.Fatalf("refusal broke ordinary durable reporting: %+v", got)
	}
	if after := f.state(t); !reflect.DeepEqual(before, after) {
		t.Fatal("report/refusal published state")
	}
}

func TestB06CycleReportFencesContentReaderClose(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, guarded := range []bool{false, true} {
				for _, boundary := range []string{"lease-loss", "cancellation"} {
					t.Run(map[bool]string{false: "direct", true: "guarded"}[guarded]+"/"+boundary, func(t *testing.T) {
						f, probe := cycleWithReadProbe(t, engine)
						before, err := f.store.Meta(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						contentNumber, contentCloses := probe.reads+1, 0
						var releaseErr error
						probe.afterClose = func(number int) {
							if number != contentNumber {
								return
							}
							contentCloses++
							if boundary == "lease-loss" {
								releaseErr = f.lease.Release(context.Background())
							} else {
								cancel()
							}
						}
						by := model.PrincipalRef{Kind: "user", ID: "user:content-close-reader"}
						var result service.CyclesResult
						if guarded {
							result, err = f.app.Service.Cycles(ctx, by, service.CyclesRequest{Project: "visible"})
						} else {
							domain := service.New(f.store, f.lease, pipeline.New(f.store, f.lease))
							result, err = domain.Cycles(ctx, by, service.CyclesRequest{Project: "visible"})
						}
						if releaseErr != nil || contentCloses != 1 || !reflect.DeepEqual(result, service.CyclesResult{}) {
							t.Fatalf("content close published or was repeated: result=%+v closes=%d release=%v error=%v", result, contentCloses, releaseErr, err)
						}
						if boundary == "cancellation" {
							if !errors.Is(err, context.Canceled) {
								t.Fatalf("close cancellation was not preserved: %v", err)
							}
						} else {
							var refused *model.Error
							if !errors.As(err, &refused) || refused.Data.Code != "busy" {
								t.Fatalf("close lease loss was not fenced: %v", err)
							}
						}
						cycleNoGraphWrites(t, f, probe, before, 0)
					})
				}
			}
		})
	}
}

func TestB06CycleReportFencesAuthorityChangeAtContentReaderClose(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			var probe *cycleReadProbeStore
			var facts *authzrpc.MutableFacts
			f := newCycleRPCFixtureConfigured(t, engine, nil, func(f *cycleRPCFixture, config *authzrpc.Config) {
				var err error
				facts, err = authzrpc.NewMutableFacts(f.facts)
				if err != nil {
					t.Fatal(err)
				}
				probe = &cycleReadProbeStore{Store: f.store}
				f.store, config.Store, config.Facts = probe, probe, facts
			})
			// Initialization is outside the request; only the competing authority
			// commit is allowed after the real content reader has closed.
			probe.writes = 0
			contentNumber, contentCloses := probe.reads+1, 0
			var externalErr error
			var committed driver.Meta
			probe.afterClose = func(number int) {
				if number != contentNumber {
					return
				}
				contentCloses++
				next := f.facts
				next.Projects = append([]authzfixed.Project{}, next.Projects...)
				next.Projects[0].Active = false
				externalErr = facts.Replace(next)
				if externalErr == nil {
					committed, externalErr = f.store.Meta(context.Background())
				}
			}
			by := model.PrincipalRef{Kind: "user", ID: "user:content-close-authority", Caveats: map[string]any{"projects": []model.Ref{f.project}}}
			result, rpcErr := f.call(t, by, "bead.cycles", nil)
			if externalErr != nil || contentCloses != 1 || rpcErr == nil || rpcErr.Data.Code != "authz_unavailable" || !reflect.DeepEqual(result, service.CyclesResult{}) {
				t.Fatalf("authority changed at content close without refusal: result=%+v error=%+v closes=%d mutation=%v", result, rpcErr, contentCloses, externalErr)
			}
			cycleNoGraphWrites(t, f, probe, committed, 1)
		})
	}
}

type cycleCloseFailureStore struct {
	driver.Store
	reads, failAt, closes int
	failure               error
}

func (s *cycleCloseFailureStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	reader, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	s.reads++
	if s.reads == s.failAt {
		return &cycleCloseFailureReader{Reader: reader, owner: s}, nil
	}
	return reader, nil
}

type cycleCloseFailureReader struct {
	driver.Reader
	owner *cycleCloseFailureStore
}

func (r *cycleCloseFailureReader) Close() error {
	err := r.Reader.Close()
	r.owner.closes++
	if err != nil {
		return err
	}
	return r.owner.failure
}

func TestB06CycleContentCloseFailurePublishesNoReport(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, guarded := range []bool{false, true} {
				t.Run(map[bool]string{false: "direct", true: "guarded"}[guarded], func(t *testing.T) {
					var probe *cycleCloseFailureStore
					f := newCycleRPCFixtureConfigured(t, engine, nil, func(f *cycleRPCFixture, config *authzrpc.Config) {
						probe = &cycleCloseFailureStore{Store: f.store, failure: errors.New("injected error after real content close")}
						f.store, config.Store = probe, probe
					})
					before, _, err := restore.Dump(context.Background(), f.store, "full")
					if err != nil {
						t.Fatal(err)
					}
					probe.failAt = probe.reads + 1
					by := model.PrincipalRef{Kind: "user", ID: "user:close-error-reader"}
					var result service.CyclesResult
					if guarded {
						result, err = f.app.Service.Cycles(context.Background(), by, service.CyclesRequest{Project: "visible"})
						var refused *model.Error
						if !errors.As(err, &refused) || refused.Data.Code != "authz_unavailable" {
							t.Fatalf("guarded close failure was not refused: %v", err)
						}
					} else {
						domain := service.New(f.store, f.lease, pipeline.New(f.store, f.lease))
						result, err = domain.Cycles(context.Background(), by, service.CyclesRequest{Project: "visible"})
						if !errors.Is(err, probe.failure) {
							t.Fatalf("direct close failure was lost: %v", err)
						}
					}
					if probe.closes != 1 || !reflect.DeepEqual(result, service.CyclesResult{}) {
						t.Fatalf("failed content close published/repeated: result=%+v closes=%d", result, probe.closes)
					}
					after, _, err := restore.Dump(context.Background(), f.store, "full")
					if err != nil || !bytes.Equal(before, after) {
						t.Fatalf("close failure changed durable graph/history/receipt: %v", err)
					}
				})
			}
		})
	}
}
