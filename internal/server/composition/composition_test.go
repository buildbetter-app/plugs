package composition

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type runtimeFixture struct {
	r         *Runtime
	server    *httpjson.Server
	done      chan error
	transport *client.HTTPTransport
	client    *client.Client
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
