package composition

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authnoidc"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type compositionViews map[string]viewops.Definition

func (v compositionViews) GetView(_ context.Context, _ model.PrincipalRef, id string) (viewops.Definition, error) {
	definition, ok := v[id]
	if !ok {
		return viewops.Definition{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "saved view is absent"}}
	}
	return definition, nil
}

type compositionProjector struct{}

func (compositionProjector) Apply(context.Context, coreevents.ChangeDelivery) error { return nil }

type runtimeFixture struct {
	r         *Runtime
	server    *httpjson.Server
	done      chan error
	transport *client.HTTPTransport
	client    *client.Client
}

func testOIDCConfig() *authnoidc.Config {
	return &authnoidc.Config{
		Issuer:                "https://idp.invalid",
		ClientID:              "plug-test",
		RedirectURI:           "https://plug.invalid/callback",
		AuthorizationEndpoint: "https://idp.invalid/authorize",
		TokenEndpoint:         "https://idp.invalid/token",
		JWKSURL:               "https://idp.invalid/jwks",
		Users:                 map[string]string{"subject": "user:test"},
	}
}

func startRuntime(t *testing.T, config Config) *runtimeFixture {
	t.Helper()
	r, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	server, err := r.Listen(context.Background(), config.Address, nil)
	if err != nil {
		_ = r.Close(context.Background())
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	transport, err := client.NewHTTP(client.HTTPConfig{Server: "http://" + server.Addr().String()})
	if err != nil {
		_ = r.Close(context.Background())
		t.Fatal(err)
	}
	c, err := client.New(transport, config.Project, "composition-test")
	if err != nil {
		transport.Close()
		_ = r.Close(context.Background())
		t.Fatal(err)
	}
	f := &runtimeFixture{r: r, server: server, done: done, transport: transport, client: c}
	t.Cleanup(func() {
		f.stop(t)
	})
	return f
}

func (f *runtimeFixture) stop(t *testing.T) {
	t.Helper()
	if f.r == nil {
		return
	}
	f.transport.Close()
	if err := f.r.Close(context.Background()); err != nil {
		t.Error(err)
	}
	select {
	case err := <-f.done:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	case <-time.After(3 * time.Second):
		t.Error("composed HTTP server did not stop")
	}
	f.r = nil
}

func resultObject(t *testing.T, result client.Result) map[string]any {
	t.Helper()
	value, err := wire.DecodeJSON(result.JSON)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("result=%T", value)
	}
	return object
}

func TestComposedSharedServerServesRPCAndEmbeddedUI(t *testing.T) {
	f := startRuntime(t, Config{Driver: "memory", Project: "composition", Address: DefaultAddress, UI: true})
	created, err := f.client.Call(context.Background(), "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "composed"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	object := resultObject(t, created)
	results := object["results"].([]any)
	id := results[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
	if _, err := f.client.Call(context.Background(), "bead.update", map[string]any{
		"id": map[string]any{"id": id}, "fields": map[string]any{"title": "updated"},
	}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Call(context.Background(), "bead.close", map[string]any{
		"id": map[string]any{"id": id}, "reason": "done",
	}, ""); err != nil {
		t.Fatal(err)
	}
	response, err := http.Get("http://" + f.server.Addr().String() + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Security-Policy") == "" {
		t.Fatalf("UI response status=%d headers=%v", response.StatusCode, response.Header)
	}
}

func TestComposedRPCSessionBridgesWatchLifetime(t *testing.T) {
	f := startRuntime(t, Config{Driver: "memory", Project: "composition", Address: DefaultAddress})
	endpoint := "http://" + f.server.Addr().String() + httpjson.Path
	call := func(method string, params map[string]any, sessionID string) map[string]any {
		t.Helper()
		body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": method, "method": method, "params": params})
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Plug-RPC", "1")
		if sessionID != "" {
			request.Header.Set("Plug-Session", sessionID)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", method, response.StatusCode, data)
		}
		var envelope struct {
			Result map[string]any  `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			t.Fatalf("%s response=%s: %v", method, data, err)
		}
		if len(envelope.Error) != 0 {
			t.Fatalf("%s error=%s", method, envelope.Error)
		}
		return envelope.Result
	}

	session := call("session.open", map[string]any{"_meta": map[string]any{"project": "composition"}}, "")
	sessionID, ok := session["sessionId"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("session=%v", session)
	}
	subscription := call("event.subscribe", map[string]any{
		"filter": map[string]any{}, "_meta": map[string]any{"project": "composition"},
	}, sessionID)
	if id, ok := subscription["subscriptionId"].(string); !ok || id == "" {
		t.Fatalf("subscription=%v", subscription)
	}
	if got := f.r.Watches.WatchCount(sessionID); got != 1 {
		t.Fatalf("watch count=%d want 1", got)
	}
	call("session.close", map[string]any{
		"sessionId": sessionID, "_meta": map[string]any{"project": "composition"},
	}, sessionID)
	deadline := time.Now().Add(time.Second)
	for f.r.Watches.WatchCount(sessionID) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := f.r.Watches.WatchCount(sessionID); got != 0 {
		t.Fatalf("session close retained watch count=%d", got)
	}
}

func TestCompositionUsesDurableProjectionStatusForViewsAndWatches(t *testing.T) {
	views := compositionViews{"issues": {Filter: &viewops.Filter{Types: []string{"issue"}}}}
	f := startRuntime(t, Config{
		Driver:     "memory",
		Project:    "composition",
		Address:    DefaultAddress,
		ViewReader: views,
		ProjectionBindings: []coreevents.Binding{{
			ID:        "core",
			Class:     coreevents.Projection,
			Projector: compositionProjector{},
		}},
	})
	created, err := f.client.Call(context.Background(), "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "projection"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	viewResult, err := f.client.Call(context.Background(), "view.run", map[string]any{
		"definition": map[string]any{"filter": map[string]any{"types": []string{"issue"}}},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	projection := resultObject(t, viewResult)["projection"].(map[string]any)
	if projection["binding"] != "core" || projection["checkpoint"].(map[string]any)["revision"].(json.Number).String() != "0" {
		t.Fatalf("view reported fabricated projection readiness: %v", projection)
	}

	subscription, err := f.r.Watches.Open(f.r.ctx, watch.Request{
		SessionID: "projection-session", Method: watch.MethodViewWatch,
		Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, ViewID: "issues",
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := subscription.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if frame.Projection == nil || !frame.Projection.Position.Revision.IsZero() {
		t.Fatalf("watch omitted durable initial projection status: %+v", frame.Projection)
	}
	_ = subscription.Close()

	binding, ok := f.r.Projections.Get("core")
	if !ok {
		t.Fatal("composition lost registered projection")
	}
	if _, err := f.r.Dispatcher.Step(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	viewResult, err = f.client.Call(context.Background(), "view.run", map[string]any{
		"definition": map[string]any{"filter": map[string]any{"types": []string{"issue"}}},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	projection = resultObject(t, viewResult)["projection"].(map[string]any)
	if projection["checkpoint"].(map[string]any)["revision"].(json.Number).String() != "1" {
		t.Fatalf("view did not consume checkpoint-backed adapter after delivery: %v", projection)
	}
	_ = created
}

func TestSQLiteCompositionPersistsAcrossRuntimeRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.db")
	config := Config{Driver: "sqlite", DSN: path, Project: "durable", Address: DefaultAddress}
	first := startRuntime(t, config)
	created, err := first.client.Call(context.Background(), "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "durable"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	id := resultObject(t, created)["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
	if _, err := first.client.Call(context.Background(), "bead.update", map[string]any{
		"id": map[string]any{"id": id}, "fields": map[string]any{"title": "survives"},
	}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := first.client.Call(context.Background(), "bead.close", map[string]any{
		"id": map[string]any{"id": id}, "reason": "done",
	}, ""); err != nil {
		t.Fatal(err)
	}
	first.stop(t)

	second := startRuntime(t, config)
	got, err := second.client.Call(context.Background(), "bead.get", map[string]any{"id": map[string]any{"id": id}}, "")
	if err != nil {
		t.Fatal(err)
	}
	value := resultObject(t, got)
	beadValue := value["bead"].(map[string]any)
	if beadValue["status"] != "done" || beadValue["fields"].(map[string]any)["title"] != "survives" {
		t.Fatalf("restart lost durable state: %+v", value)
	}
}

func TestMemoryCompositionIsExplicitlyEphemeral(t *testing.T) {
	config := Config{Driver: "memory", Project: "ephemeral", Address: DefaultAddress}
	first := startRuntime(t, config)
	created, err := first.client.Call(context.Background(), "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "gone"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	id := resultObject(t, created)["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
	first.stop(t)
	second := startRuntime(t, config)
	_, err = second.client.Call(context.Background(), "bead.get", map[string]any{"id": map[string]any{"id": id}}, "")
	var remote *wire.Error
	if !errors.As(err, &remote) || remote.Data.Code != "not_found" {
		t.Fatalf("memory restart error=%v", err)
	}
}

func TestOIDCCompositionInjectsSelectedDurableStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.db")
	var injected driver.CoreSessionStore
	r, err := Open(context.Background(), Config{
		Driver: "sqlite", DSN: path, Address: DefaultAddress, OIDC: testOIDCConfig(),
		OIDCFactory: func(config authnoidc.Config) (*authnoidc.Provider, error) {
			injected = config.SessionStore
			return authnoidc.New(config)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	selected, ok := r.Store.(driver.CoreSessionStore)
	if injected == nil || !ok || injected != selected {
		t.Fatalf("OIDC provider did not receive the selected durable store: injected=%T runtime=%T", injected, r.Store)
	}
	if r.OIDC == nil {
		t.Fatal("composition did not retain the constructed OIDC provider")
	}
	if _, err := r.Authenticator.Admit(context.Background(), ""); !errors.Is(err, authn.ErrUnauthenticated) {
		t.Fatalf("empty credential error=%v, want unauthenticated", err)
	}
}

func TestOIDCCompositionRefusesUnsupportedStoreAndClosesIt(t *testing.T) {
	base := memory.New()
	_, err := Open(context.Background(), Config{
		Driver: "memory", Address: DefaultAddress, OIDC: testOIDCConfig(),
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) {
			return base, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "durable") {
		t.Fatalf("unsupported durable store error=%v", err)
	}
	if _, metaErr := base.Meta(context.Background()); metaErr == nil {
		t.Fatal("unsupported OIDC composition left its store open")
	}
}

type failingMetaStore struct{ driver.Store }

func (s failingMetaStore) Meta(context.Context) (driver.Meta, error) {
	return driver.Meta{}, errors.New("metadata unavailable")
}

func TestFailedCompositionClosesStoreAndLease(t *testing.T) {
	base := memory.New()
	_, err := Open(context.Background(), Config{
		Driver: "memory", Address: DefaultAddress,
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) {
			return failingMetaStore{Store: base}, nil
		},
	})
	if err == nil {
		t.Fatal("composition unexpectedly succeeded")
	}
	if _, metaErr := base.Meta(context.Background()); metaErr == nil {
		t.Fatal("failed composition left its store open")
	}
}

func TestCompositionRefusesInvalidStorageAndListenerBeforeOpening(t *testing.T) {
	opened := false
	factory := func(context.Context, string, driver.Config) (driver.Store, error) {
		opened = true
		return memory.New(), nil
	}
	for _, config := range []Config{
		{Driver: "memory", DSN: "secret", Address: DefaultAddress},
		{Driver: "sqlite", Address: DefaultAddress},
		{Driver: "postgres", Address: DefaultAddress},
		{Driver: "unknown", Address: DefaultAddress},
		{Driver: "memory", Address: "0.0.0.0:0"},
	} {
		_, err := Open(context.Background(), Config{Driver: config.Driver, DSN: config.DSN, Address: config.Address, StoreFactory: factory})
		if err == nil {
			t.Fatalf("accepted invalid config=%+v", config)
		}
	}
	if opened {
		t.Fatal("invalid composition opened a store")
	}
}

func TestRuntimeCannotListenAfterClose(t *testing.T) {
	r, err := Open(context.Background(), Config{Driver: "memory", Address: DefaultAddress})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Listen(context.Background(), DefaultAddress, nil); err == nil {
		t.Fatal("closed runtime accepted a new listener")
	}
}
