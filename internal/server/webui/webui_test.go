package webui_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/webui"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

func TestEmbeddedUIHasExplicitSecurityBoundaryAndStates(t *testing.T) {
	h, err := webui.New(webui.Config{Project: "web", MaxRows: 25, HomeViewID: "stored-home", WatchPath: "/watch/v1"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)

	response, err := server.Client().Get(server.URL + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(response.Header.Get("Content-Security-Policy"), "default-src 'self'") || !strings.Contains(response.Header.Get("Content-Security-Policy"), "frame-src 'self'") {
		t.Fatalf("status=%d CSP=%q", response.StatusCode, response.Header.Get("Content-Security-Policy"))
	}
	for _, header := range []string{"Cross-Origin-Resource-Policy", "X-Content-Type-Options", "X-Frame-Options", "Referrer-Policy"} {
		if response.Header.Get(header) == "" {
			t.Fatalf("missing security header %s", header)
		}
	}
	page := string(body)
	for _, want := range []string{"What changed", "Why blocked", "Explain an item", "Presence", "EPIC PROGRESS / COUNTS", "Epic child counts are unavailable on this server.", "./app.js", "./styles.css"} {
		if !strings.Contains(page, want) {
			t.Fatalf("embedded page missing %q", want)
		}
	}
	if strings.Contains(page, "<script>") || strings.Contains(page, "https://") {
		t.Fatal("page contains an inline script or remote runtime")
	}

	configResponse, err := server.Client().Get(server.URL + "/ui/config.json")
	if err != nil {
		t.Fatal(err)
	}
	configBody, err := io.ReadAll(configResponse.Body)
	configResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(configBody, &config); err != nil {
		t.Fatal(err)
	}
	if config["rpcPath"] != "/rpc/v1" || config["project"] != "web" || config["maxRows"] != float64(25) || config["homeViewId"] != "stored-home" || config["watchPath"] != "/watch/v1" {
		t.Fatalf("unexpected browser composition: %v", config)
	}

	jsResponse, err := server.Client().Get(server.URL + "/ui/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js, err := io.ReadAll(jsResponse.Body)
	jsResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"history.query", "bead.ready", "view.run", "presence.list", "No direct blocker reason was supplied", "Anonymous principal · client label unavailable", "Request cancelled; nothing changed.", "credentials: \"omit\"", "textContent", "event.source !== frame.contentWindow", "postMessage(response, \"*\")", "callTimes.length >= 20"} {
		if !bytes.Contains(js, []byte(want)) {
			t.Fatalf("UI behavior missing %q", want)
		}
	}
	frameHost, err := server.Client().Get(server.URL + "/ui/frame-host.js")
	if err != nil {
		t.Fatal(err)
	}
	frameHostJS, err := io.ReadAll(frameHost.Body)
	frameHost.Body.Close()
	if err != nil || !bytes.Contains(frameHostJS, []byte(`setAttribute("sandbox", "allow-scripts")`)) || bytes.Contains(frameHostJS, []byte("allow-same-origin")) {
		t.Fatal("trusted frame host did not preserve the opaque plugin sandbox", err)
	}
	if bytes.Contains(js, []byte("innerHTML")) || bytes.Contains(js, []byte("makeCorpus")) || bytes.Contains(js, []byte("cdn")) || bytes.Contains(js, []byte("allow-same-origin")) {
		t.Fatal("UI uses unsafe HTML injection or fixture/CDN data")
	}
}

func TestHandlerRejectsWritesAndTraversal(t *testing.T) {
	h, err := webui.New(webui.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, target string
		status         int
	}{
		{http.MethodPost, "/ui/", http.StatusMethodNotAllowed},
		{http.MethodOptions, "/ui/", http.StatusMethodNotAllowed},
		{http.MethodGet, "/ui/secret", http.StatusNotFound},
		{http.MethodGet, "/ui/../app.js", http.StatusNotFound},
		{http.MethodGet, "/ui/%61pp.js", http.StatusNotFound},
		{http.MethodGet, "/outside/app.js", http.StatusNotFound},
	} {
		t.Run(tc.method+tc.target, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, loopbackRequest(t, tc.method, tc.target))
			if recorder.Code != tc.status {
				t.Fatalf("status=%d want=%d", recorder.Code, tc.status)
			}
			if tc.status == http.StatusMethodNotAllowed && recorder.Header().Get("Allow") != "GET, HEAD" {
				t.Fatal("missing read-only method declaration")
			}
		})
	}
	for _, config := range []webui.Config{
		{MountPath: "ui/"}, {MountPath: "//remote/"}, {MountPath: "/ui/../other/"},
		{MountPath: "/ui/\x01/"}, {RPCPath: "/rpc/v1/"}, {RPCPath: "https://remote/rpc"},
		{RPCPath: "/rpc/%76"}, {Project: " web"}, {Project: "web\n"},
		{MaxRows: -1}, {MaxRows: 101}, {BlockedViewID: "bad\n"},
		{BlockedViewDefinition: json.RawMessage(`[]`)},
		{BlockedViewDefinition: json.RawMessage(`null`)},
		{HomeViewID: "bad\n"}, {WatchPath: "/watch/v1"},
		{HomeViewID: "home", WatchPath: "https://other.test/watch"},
		{HomeViewID: "home", WatchPath: "//other.test/watch"},
		{HomeViewID: "home", WatchPath: "/watch/../v1"},
		{HomeViewID: "home", WatchPath: "/watch/%76"},
	} {
		if _, err := webui.New(config); err == nil {
			t.Fatalf("invalid composition accepted: %+v", config)
		}
	}
}

func TestAnonymousAdmissionRefusesRemoteCredentialsAndCrossOrigin(t *testing.T) {
	h, err := webui.New(webui.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*http.Request)
	}{
		{"remote_host", func(r *http.Request) { r.Host = "example.test" }},
		{"remote_peer", func(r *http.Request) { r.RemoteAddr = "192.0.2.1:43210" }},
		{"public_listener", func(r *http.Request) {
			*r = *r.WithContext(context.WithValue(r.Context(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.IPv4zero, Port: 80}))
		}},
		{"cross_origin", func(r *http.Request) { r.Header.Set("Origin", "http://other.test") }},
		{"null_origin", func(r *http.Request) { r.Header.Set("Origin", "null") }},
		{"duplicate_origin", func(r *http.Request) {
			r.Header.Add("Origin", "http://127.0.0.1")
			r.Header.Add("Origin", "http://127.0.0.1")
		}},
		{"cookie", func(r *http.Request) { r.Header.Set("Cookie", "session=unsupported") }},
		{"authorization", func(r *http.Request) { r.Header.Set("Authorization", "Bearer unsupported") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := loopbackRequest(t, http.MethodGet, "/ui/config.json")
			tc.change(r)
			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, r)
			if recorder.Code != http.StatusForbidden || recorder.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Fatalf("unsupported admission status=%d headers=%v", recorder.Code, recorder.Header())
			}
			if r.Header.Get("Plug-RPC") != "" {
				t.Fatal("static admission mutated the incoming RPC sentinel")
			}
		})
	}
	r := loopbackRequest(t, http.MethodGet, "/ui/")
	r.Header.Set("Origin", "http://127.0.0.1")
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, r)
	if recorder.Code != http.StatusOK || r.Header.Get("Plug-RPC") != "" {
		t.Fatal("same-origin static navigation must work without a caller RPC sentinel")
	}
}

func TestExplicitMountAndHEADServeOnlyEmbeddedAssets(t *testing.T) {
	h, err := webui.New(webui.Config{MountPath: "/work/ui/", RPCPath: "/local/rpc", Project: "web"})
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range []string{"", "index.html", "styles.css", "app.js", "favicon.svg", "config.json", "missing"} {
		t.Run(asset, func(t *testing.T) {
			get, head := httptest.NewRecorder(), httptest.NewRecorder()
			h.ServeHTTP(get, loopbackRequest(t, http.MethodGet, "/work/ui/"+asset))
			h.ServeHTTP(head, loopbackRequest(t, http.MethodHead, "/work/ui/"+asset))
			if head.Code != get.Code || head.Body.Len() != 0 || head.Header().Get("Content-Length") != get.Header().Get("Content-Length") {
				t.Fatalf("HEAD disagrees with GET: get=%d head=%d body=%q", get.Code, head.Code, head.Body.String())
			}
			if asset == "config.json" && !strings.Contains(get.Body.String(), `"rpcPath":"/local/rpc"`) {
				t.Fatal("explicit RPC composition lost")
			}
		})
	}
	redirect := httptest.NewRecorder()
	h.ServeHTTP(redirect, loopbackRequest(t, http.MethodGet, "/work/ui"))
	if redirect.Code != http.StatusPermanentRedirect || redirect.Header().Get("Location") != "/work/ui/" {
		t.Fatal("mount root must redirect to relative-asset base")
	}
}

func loopbackRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, "http://127.0.0.1"+target, nil)
	r.RemoteAddr = "127.0.0.1:43210"
	return r
}

func TestComposedHTTPServerShowsStoredRowsAtAdmittedRPCBoundary(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "webui-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "web", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	rpcHandler, err := httpjson.New(server, auth, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ui, err := webui.New(webui.Config{Project: "web", MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/rpc/v1", rpcHandler)
	mux.Handle("/ui/", ui)
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)

	create := rpcRequest(t, httpServer.Client(), httpServer.URL+"/rpc/v1", "bead.create", map[string]any{
		"type":   "issue",
		"fields": map[string]any{"title": "Stored through the real RPC boundary"},
		"_meta":  map[string]any{"project": "web", "idempotencyKey": strings.Repeat("a", 64)},
	})
	if create["error"] != nil {
		t.Fatalf("real create failed: %v", create)
	}
	ready := rpcRequest(t, httpServer.Client(), httpServer.URL+"/rpc/v1", "bead.ready", map[string]any{
		"limit": 10,
		"_meta": map[string]any{"project": "web"},
	})
	result, ok := ready["result"].(map[string]any)
	if !ok {
		t.Fatalf("ready did not return result: %v", ready)
	}
	items, ok := result["items"].([]any)
	if !ok || len(items) != 1 || items[0].(map[string]any)["fields"].(map[string]any)["title"] != "Stored through the real RPC boundary" {
		t.Fatalf("stored row did not cross admitted RPC boundary: %v", result)
	}
	page, err := httpServer.Client().Get(httpServer.URL + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	pageBody, err := io.ReadAll(page.Body)
	page.Body.Close()
	if err != nil || page.StatusCode != http.StatusOK {
		t.Fatalf("UI page unavailable status=%d err=%v", page.StatusCode, err)
	}
	if !bytes.Contains(pageBody, []byte("What changed")) || !bytes.Contains(pageBody, []byte("since I looked")) {
		t.Fatal("composed UI lost the home focal copy")
	}
}

func rpcRequest(t *testing.T, client *http.Client, endpoint, method string, params map[string]any) map[string]any {
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
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var value map[string]any
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}
