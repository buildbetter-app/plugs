package rehearsal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
)

type precisionHTTPFunc func(*http.Request) (*http.Response, error)

func (f precisionHTTPFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type precisionProvider struct{}

func (precisionProvider) Authenticate(_ context.Context, token string) (model.PrincipalRef, error) {
	if token != "public-precision-fixture" {
		return model.PrincipalRef{}, authn.ErrUnauthenticated
	}
	return model.PrincipalRef{Kind: "service", ID: "precision-fixture"}, nil
}

// Deliver the adapter's generated HTTP request to the real MCP handler without
// a listener, store or external resource. The canonical RPC handler below is
// reached only after MCP decoding, tool projection and catalog validation.
func precisionTransport(handler http.Handler) *mcpTransport {
	return &mcpTransport{
		endpoint: "http://localhost/mcp", token: "public-precision-fixture",
		http: &http.Client{Transport: precisionHTTPFunc(func(r *http.Request) (*http.Response, error) {
			r = r.Clone(r.Context())
			r.RemoteAddr = "127.0.0.1:32001"
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			return recorder.Result(), nil
		})},
	}
}

func precisionMCP(t *testing.T, handler rpc.Handler) *mcpTransport {
	t.Helper()
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("bead.update", handler); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "public-precision", AuthMode: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{Provider: precisionProvider{}})
	if err != nil {
		t.Fatal(err)
	}
	mcpHandler, err := mcp.New(server, authenticator, mcp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mcpHandler.Close() })
	return precisionTransport(mcpHandler)
}

func precisionObject(t *testing.T, raw string) map[string]any {
	t.Helper()
	value, err := wire.DecodeJSON([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected object: %s", raw)
	}
	return object
}

func TestMCPRoundTripDeliversExactIntegersToCanonicalHandler(t *testing.T) {
	for _, tc := range []struct{ name, revision, fields string }{
		{"ordinary", "7", `{"title":"public fixture","count":7}`},
		{"above_2pow53", "9007199254740993", `{"nested":{"values":[9007199254740993,-9007199254740993,0,true,null,"9007199254740993"]}}`},
		{"above_uint64", "18446744073709551617", `{"values":[18446744073709551617,-18446744073709551617]}`},
		{"symbolic_exponent", "1e1000000000", `{"values":[1e1000000000,-1e1000000000,9007199254740993.0,9007199254740993e0]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := `{"id":{"id":"public-bead"},"fields":` + tc.fields + `,"expect":[{"ref":{"id":"public-bead"},"revision":` + tc.revision + `}]}`
			want := precisionObject(t, params)
			want["_meta"] = precisionObject(t, `{"project":"public-project","agent":"precision-caller","idempotencyKey":"11111111111111111111111111111111"}`)
			calls := 0
			transport := precisionMCP(t, func(_ context.Context, call rpc.Call) (any, error) {
				calls++
				got := precisionObject(t, string(call.Params))
				// Compare lossless values and the exact revision token independently
				// of the adapter's decoder; a rounded but successful write must fail.
				revision := got["expect"].([]any)[0].(map[string]any)["revision"]
				if number, ok := revision.(json.Number); !ok || string(number) != tc.revision || !reflect.DeepEqual(got, want) {
					t.Errorf("receiving handler changed params: want %s; got %s", params, call.Params)
				}
				if call.Principal.Kind != "service" || call.Principal.ID != "precision-fixture" {
					t.Errorf("receiving handler lost admitted identity: %+v", call.Principal)
				}
				return rpc.Outcome{Result: json.RawMessage(`{"revision":2,"results":[],"warnings":[]}`), Replayed: true}, nil
			})
			caller, err := client.New(transport, "public-project", "precision-caller")
			if err != nil {
				t.Fatal(err)
			}
			result, err := caller.Call(context.Background(), "bead.update", precisionObject(t, params), "11111111111111111111111111111111")
			if err != nil || calls != 1 || !result.Replayed || result.IdempotencyKey != "11111111111111111111111111111111" {
				t.Fatalf("ordinary canonical completion: calls=%d replayed=%t key=%q err=%v", calls, result.Replayed, result.IdempotencyKey, err)
			}
			if got := precisionObject(t, string(result.JSON)); !reflect.DeepEqual(got, precisionObject(t, `{"revision":2,"results":[],"warnings":[]}`)) {
				t.Fatalf("canonical result changed: %s", result.JSON)
			}
		})
	}
}

func TestMCPRoundTripRetainsCanonicalTypedError(t *testing.T) {
	calls := 0
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	transport := precisionMCP(t, func(context.Context, rpc.Call) (any, error) {
		calls++
		return nil, catalog.NewError("not_found", "public missing bead", nil)
	})
	caller, err := client.New(transport, "public-project", "precision-caller")
	if err != nil {
		t.Fatal(err)
	}
	_, err = caller.Call(context.Background(), "bead.update", precisionObject(t, `{"id":{"id":"missing"},"fields":{}}`), "11111111111111111111111111111111")
	var remote *wire.Error
	if calls != 1 || !errors.As(err, &remote) || remote.Data.Code != "not_found" || remote.Data.Retryable || catalog.ValidateError(remote) != nil {
		t.Fatalf("typed error changed: calls=%d err=%v", calls, err)
	}
}

func TestMCPRoundTripRejectsInvalidCallerBeforeExchange(t *testing.T) {
	for _, raw := range []string{
		`{"jsonrpc":"2.0","id":"x","method":"bead.update","params":`,
		`{"jsonrpc":"2.0","id":"x","method":"bead.update","params":{}} {}`,
		`{"jsonrpc":"2.0","id":"x","method":"bead.update","params":[]}`,
		`{"jsonrpc":"2.0","id":"x","method":1,"params":{}}`,
		`{"jsonrpc":"2.0","id":"x","method":"bead.update","params":{"fields":{"n":1,"n":2}}}`,
		`{"jsonrpc":"2.0","id":"x","method":"bead.update","params":{},"extra":true}`,
	} {
		t.Run(raw, func(t *testing.T) {
			calls := 0
			transport := precisionTransport(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
			_, err := transport.RoundTrip(context.Background(), []byte(raw))
			if err == nil || err.Error() != "invalid caller bytes" || calls != 0 {
				t.Fatalf("invalid caller escaped: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestMCPRoundTripRetainsFramingRefusals(t *testing.T) {
	for _, body := range []string{
		`{`,
		`{"jsonrpc":"1.0","id":"x","result":{"content":[{"type":"text","text":"{}"}]}}`,
		`{"jsonrpc":"2.0","id":"other","result":{"content":[{"type":"text","text":"{}"}]}}`,
		`{"jsonrpc":"2.0","id":"x","result":{"content":[]}}`,
		`{"jsonrpc":"2.0","id":"x","result":{"content":[{"type":"image","text":"{}"}]}}`,
	} {
		t.Run(body, func(t *testing.T) {
			transport := precisionTransport(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
			_, err := transport.RoundTrip(context.Background(), []byte(`{"jsonrpc":"2.0","id":"x","method":"bead.update","params":{}}`))
			if err == nil || err.Error() != "MCP fixture returned invalid framing" {
				t.Fatalf("invalid framing accepted: %s err=%v", body, err)
			}
		})
	}
}
