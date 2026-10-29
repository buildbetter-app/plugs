package webacceptance_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestSharedServerHTTPAssetsSecurityAndRouteOwnership(t *testing.T) {
	f := newFixture(t, sqliteBackend)
	defer f.stop()

	for _, tc := range []struct {
		asset       string
		contentType string
	}{
		{"", "text/html; charset=utf-8"},
		{"index.html", "text/html; charset=utf-8"},
		{"app.js", "text/javascript; charset=utf-8"},
		{"styles.css", "text/css; charset=utf-8"},
		{"favicon.svg", "image/svg+xml"},
		{"config.json", "application/json; charset=utf-8"},
	} {
		t.Run(tc.asset, func(t *testing.T) {
			response, err := f.client.Get(f.uiEndpoint + tc.asset)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != tc.contentType {
				t.Fatalf("asset %q status=%d type=%q", tc.asset, response.StatusCode, response.Header.Get("Content-Type"))
			}
			if len(body) == 0 || response.Header.Get("Cache-Control") != "no-store" {
				t.Fatalf("asset %q was not served as a bounded uncached response", tc.asset)
			}
			for _, header := range []string{
				"Content-Security-Policy", "Cross-Origin-Opener-Policy", "Cross-Origin-Resource-Policy",
				"Permissions-Policy", "Referrer-Policy", "X-Content-Type-Options", "X-Frame-Options",
			} {
				if response.Header.Get(header) == "" {
					t.Fatalf("asset %q missing %s", tc.asset, header)
				}
			}
			if tc.asset == "" && (!strings.Contains(string(body), "What changed") || strings.Contains(string(body), "https://")) {
				t.Fatal("embedded home was not the local no-CDN page")
			}
		})
	}

	post, err := http.NewRequest(http.MethodPost, f.uiEndpoint, bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	postResponse, err := f.client.Do(post)
	if err != nil {
		t.Fatal(err)
	}
	postResponse.Body.Close()
	if postResponse.StatusCode != http.StatusMethodNotAllowed || postResponse.Header.Get("Allow") != "GET, HEAD" {
		t.Fatalf("UI write surface status=%d allow=%q", postResponse.StatusCode, postResponse.Header.Get("Allow"))
	}

	for _, tc := range []struct {
		name   string
		header string
		value  string
	}{
		{"cookie", "Cookie", "plug-session=unsupported"},
		{"authorization", "Authorization", "Bearer unsupported"},
		{"cross-origin", "Origin", "http://attacker.invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, f.uiEndpoint+"config.json", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set(tc.header, tc.value)
			response, err := f.client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusForbidden || response.Header.Get("Access-Control-Allow-Origin") != "" {
				t.Fatalf("unsupported browser admission status=%d cors=%q", response.StatusCode, response.Header.Get("Access-Control-Allow-Origin"))
			}
		})
	}

	rpcGet, err := f.client.Get(f.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	rpcGet.Body.Close()
	if rpcGet.StatusCode != http.StatusMethodNotAllowed || rpcGet.Header.Get("Allow") != http.MethodPost {
		t.Fatalf("RPC route was not owned by admitted HTTP transport: status=%d allow=%q", rpcGet.StatusCode, rpcGet.Header.Get("Allow"))
	}
	missing, err := f.client.Get(f.server.URL + "/ui/does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown UI asset status=%d", missing.StatusCode)
	}
}

func TestSharedServerRPCBackedRowsAndEmptyState(t *testing.T) {
	for _, backend := range []backend{sqliteBackend, postgresBackend} {
		t.Run(backend.String(), func(t *testing.T) {
			f := newFixture(t, backend)
			defer f.stop()
			_, first := f.create("row from the real RPC", "writer")
			if first.Fields["title"] != "row from the real RPC" {
				t.Fatalf("create did not return stored row: %+v", first)
			}
			var ready struct {
				Items []bead `json:"items"`
				AsOf  asOf   `json:"asOf"`
			}
			f.success("bead.ready", map[string]any{"limit": 2, "_meta": map[string]any{"project": project}}, &ready)
			if len(ready.Items) != 1 || ready.Items[0].Fields["title"] != "row from the real RPC" || ready.AsOf.Incarnation == "" {
				t.Fatalf("ready did not expose the actual row: %+v", ready)
			}
			var history beadPage
			f.success("history.query", map[string]any{
				"asOf": ready.AsOf, "limit": 2,
				"_meta": map[string]any{"project": project},
			}, &history)
			if len(history.Items) != 1 || history.Items[0].Fields["title"] != "row from the real RPC" || history.AsOf != ready.AsOf {
				t.Fatalf("history did not retain the actual pinned row: %+v", history)
			}
			empty, err := f.client.Get(f.server.URL + "/empty/ui/")
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(empty.Body)
			empty.Body.Close()
			if err != nil || empty.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("What changed")) {
				t.Fatalf("empty UI route unavailable status=%d err=%v", empty.StatusCode, err)
			}
			var emptyReady struct {
				Items []bead `json:"items"`
			}
			f.success("bead.ready", map[string]any{"limit": 2, "_meta": map[string]any{"project": "empty"}}, &emptyReady)
			if len(emptyReady.Items) != 0 {
				t.Fatalf("empty project returned synthetic rows: %+v", emptyReady.Items)
			}
			// The catalog remains the server's source of truth; the UI does not add
			// a write handler or pretend that optional methods exist.
			var methods struct {
				Methods []string `json:"methods"`
			}
			f.success("rpc.methods", map[string]any{}, &methods)
			encoded, _ := json.Marshal(methods.Methods)
			if !bytes.Contains(encoded, []byte("bead.ready")) || !bytes.Contains(encoded, []byte("history.query")) {
				t.Fatalf("real RPC catalog omitted required read methods: %s", encoded)
			}
		})
	}
}
