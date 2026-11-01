package transports_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/server/transport/stdio"
	"github.com/buildbetter-app/plugs/internal/server/transport/websocket"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

var transports = []string{"http", "websocket", "stdio", "mcp"}

type envelope struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
	Meta   struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

type fixture struct {
	store        *memory.Store
	rpc          *rpc.Server
	auth         *authn.Authenticator
	keys         *authnapikey.Provider
	principal    model.PrincipalRef
	token, keyID string
	url, session string
	client       *http.Client
	validators   atomic.Int64
	posts        atomic.Int64
	targetCalls  atomic.Int64
}

func newFixture(t *testing.T, authenticated bool) *fixture {
	t.Helper()
	f := &fixture{store: memory.New(), principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}}
	t.Cleanup(func() { _ = f.store.Close() })
	ctx := context.Background()
	lease, err := f.store.Lease(ctx, "i3-transport-fixture", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	mode, authConfig := "anonymous", authn.Config{}
	if authenticated {
		mode = "authenticated"
		f.principal = model.PrincipalRef{Kind: "user", ID: "user:i3-fixture"}
		f.keys = authnapikey.New(authnapikey.Config{})
		issued, err := f.keys.Issue(f.principal, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		f.token, f.keyID = issued.Token, issued.ID
		authConfig.Provider = f.keys
	}
	f.auth, err = authn.New(authConfig)
	if err != nil {
		t.Fatal(err)
	}
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.targetCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(target.Close)
	manifest, err := pluginapi.DecodeManifest([]byte(`{"manifest_version":1,"plugins":[{"id":"app.example.transportprobe","alias":"transportprobe","version":"1.0.0","plugin_api":"1.0","runtime":"native","provides":[{"kind":"subscriber","name":"inspect","phases":["validate","post"],"reads":["fields.title"]}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	native := sdk.Native{Instance: "default", Manifest: manifest.Plugins[0], New: func(host pluginapi.Host) (sdk.Registration, error) {
		return sdk.Registration{
			Validators: map[string]pluginapi.Validator{"inspect": sdk.ValidateFunc(func(ctx context.Context, change pluginapi.ProposedChange, _ pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
				if change.Principal.Kind != f.principal.Kind || change.Principal.ID != f.principal.ID {
					return pluginapi.Verdict{}, errors.New("native validator lost admitted principal")
				}
				f.validators.Add(1)
				if err := deniedEgress(host, target.URL, pluginhost.ErrHotEgress, "synchronous pre call"); err != nil {
					return pluginapi.Verdict{}, err
				}
				return pluginapi.Verdict{Allow: true}, nil
			})},
			Posts: map[string]pluginapi.PostSubscriber{"inspect": sdk.AfterFunc(func(_ context.Context, change pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
				if change.Principal.Kind != f.principal.Kind || change.Principal.ID != f.principal.ID {
					return errors.New("native post callback lost committed principal")
				}
				f.posts.Add(1)
				detail := "anonymous mode"
				if authenticated {
					detail = "private or non-public target"
				}
				return deniedEgress(host, target.URL, pluginhost.ErrEgress, detail)
			})},
		}, nil
	}}
	// This is a declared, disposable loopback target. Anonymous mode must
	// refuse on authentication posture; authenticated mode still refuses the IP.
	native.Manifest.Requires.Egress = []string{strings.TrimPrefix(target.URL, "https://")}
	host, err := pluginhost.New(pluginhost.Config{Natives: []sdk.Native{native}, AuthenticationConfigured: authenticated, Bindings: []pluginhost.Binding{
		{ID: "i3-validate", Alias: "transportprobe", Instance: "default", Subscriber: "inspect", Phase: pluginapi.PhaseValidate, Reads: []string{"fields.title"}},
		{ID: "i3-post", Alias: "transportprobe", Instance: "default", Subscriber: "inspect", Phase: pluginapi.PhasePost, Reads: []string{"fields.title"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, lease, pipeline.WithOntology(ontology), pipeline.WithValidators(host.Validators()...), pipeline.WithPostSubscribers(host.Posts()...))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, service.New(f.store, lease, writer, service.WithOntology(ontology))); err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindAdmin(registry, restore.NewManager(f.store, lease, nil)); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.rpc, err = rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "i3", AuthMode: mode, OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	httpHandler, err := httpjson.New(f.rpc, f.auth, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	wsHandler, err := websocket.New(f.rpc, f.auth, websocket.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wsHandler.Close() })
	mcpHandler, err := mcp.New(f.rpc, f.auth, mcp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mcpHandler.Close() })
	mux := http.NewServeMux()
	mux.Handle("/rpc/v1", httpHandler)
	mux.Handle(websocket.Path, wsHandler)
	mux.Handle(mcp.Path, mcpHandler)
	server, err := httpHandler.ListenHandler(ctx, "127.0.0.1:0", nil, mux)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("server shutdown: %v", err)
		}
	})
	f.url = "http://" + server.Addr().String()
	f.client = &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
	t.Cleanup(f.client.CloseIdleConnections)
	f.initializeMCP(t)
	return f
}

func (f *fixture) initializeMCP(t *testing.T) {
	t.Helper()
	status, header, body := f.post(t, mcp.Path, []byte(`{"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"i3-fixture","version":"1"}}}`), nil)
	if status != http.StatusOK || header.Get("MCP-Session-Id") == "" {
		t.Fatalf("MCP initialize: status=%d body=%s", status, body)
	}
	f.session = header.Get("MCP-Session-Id")
}

func deniedEgress(host pluginapi.Host, target, wantCode, detail string) error {
	response, err := host.HTTP().Get(target)
	if response != nil {
		response.Body.Close()
	}
	var diagnostic *pluginhost.Error
	if !errors.As(err, &diagnostic) || diagnostic.Code != wantCode || !strings.Contains(diagnostic.Detail, detail) {
		return fmt.Errorf("host egress: got %v, want %s (%s)", err, wantCode, detail)
	}
	return nil
}

func (f *fixture) post(t *testing.T, path string, body []byte, change func(*http.Request)) (int, http.Header, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, f.url+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Origin", f.url)
	if f.token != "" {
		request.Header.Set("Authorization", "Bearer "+f.token)
	}
	if path == mcp.Path && f.session != "" {
		request.Header.Set("MCP-Session-Id", f.session)
	}
	if change != nil {
		change(request)
	}
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	if f.token != "" && bytes.Contains(data, []byte(f.token)) {
		t.Fatal("credential leaked into transport response")
	}
	return response.StatusCode, response.Header, data
}

func (f *fixture) call(t *testing.T, transport string, request []byte) envelope {
	t.Helper()
	var response []byte
	switch transport {
	case "http", "mcp":
		path, body := "/rpc/v1", request
		if transport == "mcp" {
			path, body = mcp.Path, mcpRequest(t, request)
		}
		status, _, data := f.post(t, path, body, nil)
		if status != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", transport, status, data)
		}
		response = data
	case "websocket":
		connection, reader, status := f.connect(t, nil)
		defer connection.Close()
		if status != http.StatusSwitchingProtocols {
			t.Fatalf("WebSocket handshake: %d", status)
		}
		writeFrame(t, connection, request)
		response = readFrame(t, reader)
	case "stdio":
		identity, err := f.auth.Admit(context.Background(), f.token)
		if err != nil {
			t.Fatal(err)
		}
		ctx := rpc.WithPrincipal(authn.WithIdentity(context.Background(), identity), identity.Principal())
		transport, err := stdio.New(f.rpc, stdio.Config{})
		if err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		if err := transport.Serve(ctx, bytes.NewReader(append(append([]byte(nil), request...), '\n')), &output); err != nil {
			t.Fatal(err)
		}
		response = bytes.TrimSuffix(output.Bytes(), []byte{'\n'})
	default:
		t.Fatalf("unknown test transport %q", transport)
	}
	var got envelope
	if err := json.Unmarshal(response, &got); err != nil {
		t.Fatal(err)
	}
	if transport == "mcp" && len(got.Error) == 0 {
		var tool struct {
			Content []struct{ Type, Text string }
			IsError bool
			Meta    struct{ Replayed bool } `json:"_meta"`
		}
		if err := json.Unmarshal(got.Result, &tool); err != nil || len(tool.Content) != 1 || tool.Content[0].Type != "text" {
			t.Fatalf("MCP tool envelope: %s (%v)", response, err)
		}
		got.Meta.Replayed = tool.Meta.Replayed
		got.Result = nil
		if tool.IsError {
			got.Error = json.RawMessage(tool.Content[0].Text)
		} else {
			got.Result = json.RawMessage(tool.Content[0].Text)
		}
	}
	return got
}
