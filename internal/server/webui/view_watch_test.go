package webui_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/server/webui"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

//go:embed view_watch_check.mjs
var viewWatchCheck []byte

// Capture only bytes actually written by the admitted adapter, preserving its
// real flush behavior. This records evidence, never substitutes a response.
type recordedStream struct {
	http.ResponseWriter
	bytes bytes.Buffer
}

func (w *recordedStream) Write(body []byte) (int, error) {
	n, err := w.ResponseWriter.Write(body)
	w.bytes.Write(body[:n])
	return n, err
}

func (w *recordedStream) Flush() { w.ResponseWriter.(http.Flusher).Flush() }

// This fixture-only host policy reads an explicit current stored readership
// grant on every check. It denies absent grants, other projects/principals and
// revoked grants, including for historical events. It is not AllowAll, a
// production policy adapter, or E3 installation qualification.
type browserReadPolicy struct {
	service *service.Service
	checks  atomic.Int64
}

func (p *browserReadPolicy) current(ctx context.Context, by model.PrincipalRef, id string) (service.Bead, bool, error) {
	row, err := p.service.Get(ctx, by, service.GetRequest{ID: model.RefByID(id, model.Natural64(0))})
	if err != nil {
		return service.Bead{}, false, err
	}
	p.checks.Add(1)
	readers, _ := row.Bead.Metadata["browserReaders"].([]any)
	for _, reader := range readers {
		if by.Kind == "anonymous" && by.ID == "anonymous" && reader == by.ID && (row.Bead.Project == "web" || row.Bead.Project == "config") {
			return row.Bead, true, nil
		}
	}
	return row.Bead, false, nil
}

func (p *browserReadPolicy) authorize(ctx context.Context, by model.PrincipalRef, ref model.ResourceRef, project string) error {
	bead, allowed, err := p.current(ctx, by, ref.Ref.ID)
	if err != nil {
		return err
	}
	if !allowed || project != bead.Project {
		return &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "no current stored browser readership grant"}}
	}
	return nil
}

func (p *browserReadPolicy) Read(ctx context.Context, by model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	id, ok := strings.CutPrefix(event.Resource, "bead/")
	if !ok {
		return coreevents.Visibility{}, nil
	}
	_, allowed, err := p.current(ctx, by, id)
	return coreevents.Visibility{Readable: allowed, Before: event.Before, After: event.After}, err
}

func (p *browserReadPolicy) ReadResource(ctx context.Context, by model.PrincipalRef, resource model.AuditResource) (coreevents.ResourceVisibility, error) {
	if resource.Kind != "bead" {
		return coreevents.ResourceVisibility{}, nil
	}
	_, allowed, err := p.current(ctx, by, resource.Ref.ID)
	return coreevents.ResourceVisibility{Readable: allowed}, err
}

func (p *browserReadPolicy) AdmitPresence(ctx context.Context, by model.PrincipalRef, project string) (bool, error) {
	_, allowed, err := p.current(ctx, by, "pl-browser-view")
	return allowed && project == "web", err
}

func (p *browserReadPolicy) CanRead(ctx context.Context, by model.PrincipalRef, bead service.Bead) (bool, error) {
	_, allowed, err := p.current(ctx, by, bead.ID)
	return allowed, err
}

// A deliberately test-only saved-view port resolves definitions from real
// Writer-persisted metadata through service.Get. Production has no admitted
// saved-view CRUD/resolver yet; this fixture does not install one there.
type storedBrowserViews struct {
	policy *browserReadPolicy
	held   atomic.Bool
	active atomic.Int64
}

func (v *storedBrowserViews) GetView(ctx context.Context, by model.PrincipalRef, id string) (viewops.Definition, error) {
	if id == "slow" && v.held.Load() {
		v.active.Add(1)
		defer v.active.Add(-1)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for v.held.Load() {
			select {
			case <-ctx.Done():
				return viewops.Definition{}, ctx.Err()
			case <-ticker.C:
			}
		}
	}
	bead, allowed, err := v.policy.current(ctx, by, "pl-browser-view")
	if err != nil {
		return viewops.Definition{}, err
	}
	if !allowed {
		return viewops.Definition{}, &model.Error{Data: model.ErrorData{Code: "forbidden"}}
	}
	value, exists := bead.Metadata[id]
	if !exists {
		return viewops.Definition{}, &model.Error{Data: model.ErrorData{Code: "not_found"}}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return viewops.Definition{}, err
	}
	var definition viewops.Definition
	err = json.Unmarshal(raw, &definition)
	return definition, err
}

func TestBrowserAdmittedViewWatchStoredRowsAndCleanup(t *testing.T) {
	runBrowserViewWatch(t, false)
}

func TestBrowserRealExplainAndPresenceUXGaps(t *testing.T) {
	runBrowserViewWatch(t, true)
}

func runBrowserViewWatch(t *testing.T, verifyUX bool) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "browser-view-watch", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(context.Background())
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if lease.Renew(ctx) != nil {
					return
				}
			}
		}
	}()
	ids := []string{"pl-browser-c", "pl-browser-a", "pl-browser-b", "pl-browser-secret", "pl-browser-foreign", "pl-browser-view"}
	var allocated atomic.Int64
	ontology := service.NewOntology(nil)
	options := []pipeline.Option{pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
		i := allocated.Add(1) - 1
		if i >= int64(len(ids)) {
			return "", fmt.Errorf("unexpected create")
		}
		return ids[i], nil
	})}
	if verifyUX {
		// Claims require committed slot provenance for actual event replay.
		// Use the admitted audit implementation; its missing source locator
		// explicitly refuses conditional-edge decisions, which this fixture
		// does not create. Keep the original legacy nil-plan journey above.
		options = append(options, pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)))
	}
	writer := pipeline.New(store, lease, options...)
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	policy := &browserReadPolicy{service: domain}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	var key atomic.Int64
	setMeta := func(id, project, name string, value any) error {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		_, err = writer.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Kind: model.OpSetMeta, Target: model.RefByID(id, model.Natural64(0)), Key: name, Value: raw}}}}, model.WriteMeta{Project: project, Method: "bead.apply", IdempotencyKey: fmt.Sprintf("%064x", key.Add(1))})
		return err
	}
	budget := model.Natural64(2)
	definition := viewops.Definition{Filter: &viewops.Filter{Resources: ids[:4]}, Sort: []string{"id"}, BudgetTokens: &budget}
	saved := &storedBrowserViews{policy: policy}
	viewStore := &pausedStore{Store: store}
	views, err := viewops.New(viewStore, lease, viewops.Config{Views: saved, AuthorizeRead: policy.authorize, OntologyRevision: model.Natural64(1), PolicyVersion: "stored-browser-grants-v1"})
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := collab.New(ctx, collab.Config{Service: domain, Lease: lease, AuthorizationInstalled: func() bool { return true }, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	defer sessions.Close()
	codec, err := serverevents.NewCursorCodec([]byte(strings.Repeat("browser-watch-key-", 3)))
	if err != nil {
		t.Fatal(err)
	}
	events := &serverevents.Service{Reader: &coreevents.Reader{Log: serverevents.Log{Store: store}, Materializer: serverevents.HistoryMaterializer{Store: store}, Authorization: policy}, Cursors: codec}
	watches, err := watch.New(ctx, watch.Config{Events: events, Coalesce: 5 * time.Millisecond, MaxLag: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer watches.Close()
	adapter, err := watchhttp.New(watches, watchhttp.Config{Views: views, Ontology: model.Natural64(1), PolicyVersion: "stored-browser-grants-v1"})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, bind := range []func() error{func() error { return rpc.BindService(registry, domain) }, func() error { return collab.Bind(registry, sessions) }, func() error { return viewrpc.Bind(registry, views) }, func() error { return adapter.Register(registry) }} {
		if err := bind(); err != nil {
			t.Fatal(err)
		}
	}
	meta, _ := store.Meta(ctx)
	dispatcher, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "web", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	bridge := func(ctx context.Context, by model.PrincipalRef, id, agent string) (context.Context, error) {
		lifetime, err := sessions.SessionContext(ctx, collab.Caller{Principal: by, Agent: agent}, id)
		if err != nil {
			return nil, err
		}
		return watchhttp.WithSession(ctx, id, lifetime), nil
	}
	transport, err := httpjson.New(dispatcher, admission, httpjson.Config{SessionBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(httpjson.Path, transport)
	var attached atomic.Int64
	var streamMu sync.Mutex
	var delivered []map[string]any
	mux.Handle(watchhttp.DefaultPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := admission.AdmitHTTP(r)
		if err != nil {
			http.Error(w, "admission refused", http.StatusForbidden)
			return
		}
		ctx, err := bridge(r.Context(), identity.Principal(), r.Header.Get("Plug-Session"), r.Header.Get("Plug-Agent"))
		if err != nil {
			http.Error(w, "session refused", http.StatusForbidden)
			return
		}
		attached.Add(1)
		defer attached.Add(-1)
		recorded := &recordedStream{ResponseWriter: w}
		adapter.ServeHTTP(recorded, r.WithContext(watchhttp.WithPrincipal(ctx, identity.Principal())))
		streamMu.Lock()
		delivered = append(delivered, map[string]any{"contentType": w.Header().Get("Content-Type"), "ndjson": recorded.bytes.String()})
		streamMu.Unlock()
	}))
	for _, mount := range []struct{ path, view string }{{"/ui/", "home"}, {"/empty/ui/", "empty"}, {"/unsupported/ui/", "unsupported"}, {"/oversize/ui/", "oversize"}, {"/missing/ui/", "missing"}, {"/inline/ui/", ""}, {"/slow/ui/", "slow"}} {
		config := webui.Config{MountPath: mount.path, Project: "web", MaxRows: 2, HomeViewID: mount.view, BlockedViewID: "home"}
		if mount.view != "" {
			config.WatchPath = watchhttp.DefaultPath
		}
		ui, err := webui.New(config)
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle(mount.path, ui)
	}
	mux.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		rows, err := sessions.List(r.Context(), collab.Caller{Principal: by}, "web")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		count := 0
		for _, row := range rows {
			count += watches.WatchCount(row.SessionID)
		}
		current, _ := store.Meta(ctx)
		_ = json.NewEncoder(w).Encode(map[string]any{"sessions": len(rows), "watches": count, "streams": attached.Load(), "policyChecks": policy.checks.Load(), "head": current.Head, "activeReads": viewStore.active.Load(), "cancelledReads": viewStore.cancelled.Load(), "openingReads": saved.active.Load()})
	})
	mux.HandleFunc("/fixture/pause-view", func(w http.ResponseWriter, r *http.Request) {
		viewStore.paused.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/resume-view", func(w http.ResponseWriter, r *http.Request) {
		viewStore.paused.Store(false)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/hold-open", func(w http.ResponseWriter, r *http.Request) {
		saved.held.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/release-open", func(w http.ResponseWriter, r *http.Request) {
		saved.held.Store(false)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/reset", func(w http.ResponseWriter, r *http.Request) {
		changed := definition
		changed.Filter = &viewops.Filter{Resources: ids[:1]}
		if err := setMeta("pl-browser-view", "config", "home", changed); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/restore", func(w http.ResponseWriter, r *http.Request) {
		if err := setMeta("pl-browser-view", "config", "home", definition); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/revoke", func(w http.ResponseWriter, r *http.Request) {
		if err := setMeta("pl-browser-a", "web", "browserReaders", []string{}); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	for i, id := range ids {
		project := "web"
		if id == "pl-browser-foreign" {
			project = "foreign"
		}
		if id == "pl-browser-view" {
			project = "config"
		}
		response := rpcRequest(t, server.Client(), server.URL+httpjson.Path, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": []string{"Stored C", "<img src=x onerror=alert(1)>", "Stored B", "SECRET DENIED", "FOREIGN DENIED", "Saved browser definitions"}[i]}, "_meta": map[string]any{"project": project, "idempotencyKey": fmt.Sprintf("%064x", 100+i)}})
		if response["error"] != nil {
			t.Fatalf("seed: %v", response)
		}
		if id != "pl-browser-secret" {
			if err := setMeta(id, project, "browserReaders", []string{"anonymous"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	empty := definition
	empty.Filter = &viewops.Filter{Resources: []string{"pl-browser-absent"}}
	unsupported := definition
	unsupported.Filter = &viewops.Filter{Query: "status=todo"}
	oversize := definition
	largeBudget := model.Natural64(3)
	oversize.BudgetTokens = &largeBudget
	for name, value := range map[string]viewops.Definition{"home": definition, "empty": empty, "unsupported": unsupported, "oversize": oversize, "slow": definition} {
		if err := setMeta("pl-browser-view", "config", name, value); err != nil {
			t.Fatal(err)
		}
	}
	evidence := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if evidence == "" {
		evidence = t.TempDir()
	}
	evidence = filepath.Join(evidence, "view-watch")
	mode, wantHead := "legacy", "22"
	if verifyUX {
		evidence += "-ux"
		mode, wantHead = "ux", "23"
	}
	if err := os.MkdirAll(evidence, 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "view-watch-check.mjs")
	if err := os.WriteFile(script, viewWatchCheck, 0o600); err != nil {
		t.Fatal(err)
	}
	commandCtx, stop := context.WithTimeout(ctx, 90*time.Second)
	defer stop()
	command := exec.CommandContext(commandCtx, node, script, server.URL, evidence, chrome, mode)
	output, err := command.CombinedOutput()
	fmt.Print(string(output))
	if err != nil {
		t.Fatalf("real view/watch browser journey: %v", err)
	}
	rows, err := sessions.List(ctx, collab.Caller{Principal: by}, "web")
	if err != nil || len(rows) != 0 || attached.Load() != 0 || policy.checks.Load() == 0 {
		t.Fatalf("retained sessions=%d streams=%d checks=%d err=%v", len(rows), attached.Load(), policy.checks.Load(), err)
	}
	current, err := store.Meta(ctx)
	if err != nil || current.Head.String() != wantHead {
		t.Fatalf("unexpected durable browser writes: head=%s err=%v", current.Head, err)
	}
	streamMu.Lock()
	raw, err := json.MarshalIndent(delivered, "", "  ")
	streamMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for _, eventType := range []string{"watch.delta", "watch.reset", "visibility.lost"} {
		if !bytes.Contains(raw, []byte(eventType)) {
			t.Fatalf("no delivered %s evidence", eventType)
		}
	}
	if err := os.WriteFile(filepath.Join(evidence, "served-watch-ndjson.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("PASS real_view_watch_browser current_stored_policy_checks=%d head=%s harness_writes=%s mode=%s no_retained_sessions_or_streams evidence=%s\n", policy.checks.Load(), wantHead, wantHead, mode, evidence)
}
