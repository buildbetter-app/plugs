package webui_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/server/webui"
)

func TestBridgeAllowsOnlyGrantedProjectScopedCalls(t *testing.T) {
	bridge, err := webui.NewBridge(webui.BridgeGrant{Project: "web", Methods: []string{"bead.get", "tx.apply"}})
	if err != nil {
		t.Fatal(err)
	}
	call, err := bridge.Authorize([]byte(`{"jsonrpc":"2.0","id":"frame-1","method":"bead.get","params":{"_meta":{"project":"web"},"id":"pl-a"}}`))
	if err != nil || call.Method != "bead.get" {
		t.Fatalf("granted call refused: call=%+v err=%v", call, err)
	}
	for name, raw := range map[string]string{
		"unlisted":      `{"jsonrpc":"2.0","id":1,"method":"bead.ready","params":{"_meta":{"project":"web"}}}`,
		"cross_scope":   `{"jsonrpc":"2.0","id":1,"method":"bead.get","params":{"_meta":{"project":"other"}}}`,
		"missing_scope": `{"jsonrpc":"2.0","id":1,"method":"bead.get","params":{}}`,
		"credential":    `{"jsonrpc":"2.0","id":1,"method":"bead.get","params":{"_meta":{"project":"web"},"token":"secret"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := bridge.Authorize([]byte(raw))
			var refusal *webui.BridgeError
			if !errors.As(err, &refusal) {
				t.Fatalf("got %v, want typed bridge refusal", err)
			}
			if refusal.Code == "" {
				t.Fatal("bridge refusal lost its code")
			}
		})
	}
	if !bridge.Allows("bead.get") || bridge.Allows("bead.ready") {
		t.Fatal("bridge allowlist is not exact")
	}
	pluginCall, err := webui.NewBridge(webui.BridgeGrant{Project: "web", Methods: []string{"plug_call"}})
	if err != nil {
		t.Fatalf("explicit plug_call grant rejected: %v", err)
	}
	if _, err := pluginCall.Authorize([]byte(`{"jsonrpc":"2.0","id":2,"method":"plug_call","params":{"_meta":{"project":"web"}}}`)); err != nil {
		t.Fatalf("explicit plug_call grant refused: %v", err)
	}
}

func TestBridgeRefusesD16CredentialsAndEgressCapabilities(t *testing.T) {
	for _, method := range []string{"plugin.plan", "plugin.apply", "ontology.apply", "network.fetch", "http.request", "secret.get"} {
		t.Run(method, func(t *testing.T) {
			_, err := webui.NewBridge(webui.BridgeGrant{Project: "web", Methods: []string{method}})
			var refusal *webui.BridgeError
			if !errors.As(err, &refusal) || refusal.Code != "invalid_grant" {
				t.Fatalf("grant accepted %q: err=%v", method, err)
			}
		})
	}
	bridge, err := webui.NewBridge(webui.BridgeGrant{Project: "web", Methods: []string{"plug.call"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = bridge.Authorize([]byte(`{"jsonrpc":"2.0","id":1,"method":"plug.call","params":{"_meta":{"project":"web"},"url":"https://outside.example"}}`))
	var refusal *webui.BridgeError
	if !errors.As(err, &refusal) || refusal.Code != "bridge_field_denied" {
		t.Fatalf("egress field escaped bridge: err=%v", err)
	}
}

func TestEmbeddedFrameUsesOpaqueNoEgressCSPAndHostBinding(t *testing.T) {
	h, err := webui.New(webui.Config{
		Project: "web",
		Frames: []webui.FrameBundle{{
			ID:    "linear",
			Title: "Linear panel",
			Grant: webui.BridgeGrant{Methods: []string{"bead.get"}},
			Assets: map[string][]byte{
				"index.html": []byte(`<!doctype html><script src="./frame.js"></script>`),
				"frame.js":   []byte(`window.parent.postMessage({jsonrpc:"2.0",id:"ready",method:"bead.get",params:{}},"*");`),
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	request, err := http.NewRequest(http.MethodGet, server.URL+"/ui/frames/linear/index.html", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", "null")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("frame response status=%d err=%v", response.StatusCode, err)
	}
	csp := response.Header.Get("Content-Security-Policy")
	for _, want := range []string{"sandbox allow-scripts", "default-src 'none'", "connect-src 'none'", "form-action 'none'", "frame-ancestors 'self'"} {
		if !strings.Contains(csp, want) {
			t.Fatalf("frame CSP missing %q: %s", want, csp)
		}
	}
	if strings.Contains(csp, "allow-same-origin") || response.Header.Get("X-Frame-Options") != "SAMEORIGIN" {
		t.Fatalf("frame is not opaque/host-bound: CSP=%q XFO=%q", csp, response.Header.Get("X-Frame-Options"))
	}
	if !strings.Contains(string(body), "frame.js") {
		t.Fatal("embedded frame lost its asset reference")
	}
	duplicateOrigin, err := http.NewRequest(http.MethodGet, server.URL+"/ui/frames/linear/index.html", nil)
	if err != nil {
		t.Fatal(err)
	}
	duplicateOrigin.Header.Add("Origin", "null")
	duplicateOrigin.Header.Add("Origin", "http://other.example")
	duplicateResponse, err := server.Client().Do(duplicateOrigin)
	if err != nil {
		t.Fatal(err)
	}
	duplicateResponse.Body.Close()
	if duplicateResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("duplicate opaque origins were accepted: %d", duplicateResponse.StatusCode)
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
	var config struct {
		Frames []struct {
			Src     string   `json:"src"`
			Methods []string `json:"methods"`
		} `json:"frames"`
	}
	if err := json.Unmarshal(configBody, &config); err != nil || len(config.Frames) != 1 || config.Frames[0].Src != "/ui/frames/linear/index.html" || len(config.Frames[0].Methods) != 1 {
		t.Fatalf("frame descriptor is not embedded safely: %s err=%v", configBody, err)
	}
}

func TestFrameBundleRejectsCredentialOrNetworkCode(t *testing.T) {
	for name, source := range map[string][]byte{
		"fetch":  []byte(`<script>fetch("https://outside.example")</script>`),
		"cookie": []byte(`<script>document.cookie</script>`),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := webui.New(webui.Config{Frames: []webui.FrameBundle{{
				ID: "unsafe", Grant: webui.BridgeGrant{Project: "default"},
				Assets: map[string][]byte{"index.html": source},
			}}})
			if err == nil || !strings.Contains(err.Error(), "credentials or network egress") {
				t.Fatalf("unsafe frame accepted: %v", err)
			}
		})
	}
}
