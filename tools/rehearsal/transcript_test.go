package rehearsal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

type transcriptHTTPFunc func(*http.Request) (*http.Response, error)

func (f transcriptHTTPFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Exercise the actual MCP adapter without a listener, store, service or egress.
// Both integers are legal public wire revisions above float64's exact range.
func TestTranscriptMCPAdapterPreservesRequestAndResponseNumbers(t *testing.T) {
	request := []byte(`{"jsonrpc":"2.0","id":"public-1","method":"bead.get","params":{"id":{"id":"public-bead"},"asOf":{"revision":9007199254740993}}}`)
	result := `{"bead":{"id":"public-bead","type":"issue","revision":9007199254740993,"fields":{"title":"Public fixture"}},"attachments":[]}`
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	desc, _ := catalog.Descriptor("bead.get")
	if err := catalog.ValidateResult(desc, []byte(result)); err != nil {
		t.Fatal(err)
	}
	var sent []byte
	calls := 0
	adapter := &mcpTransport{
		endpoint: "https://public-fixture.invalid/mcp",
		http: &http.Client{Transport: transcriptHTTPFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			sent, err = io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			body, err := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": "public-1",
				"result": map[string]any{"content": []any{map[string]any{"type": "text", "text": result}}},
			})
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}, err
		})},
	}
	response, err := adapter.RoundTrip(context.Background(), request)
	if err != nil || calls != 1 {
		t.Fatalf("in-memory adapter calls=%d: %v", calls, err)
	}
	want, err := wire.DecodeJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.ValidateParams(desc, mustTranscriptJSON(t, want.(map[string]any)["params"])); err != nil {
		t.Fatal(err)
	}
	got, err := wire.DecodeJSON(sent)
	if err != nil {
		t.Fatal(err)
	}
	envelope := got.(map[string]any)
	arguments := envelope["params"].(map[string]any)["arguments"].(map[string]any)
	reconstructed := map[string]any{
		"jsonrpc": envelope["jsonrpc"], "id": envelope["id"],
		"method": arguments["method"], "params": arguments["params"],
	}
	if !reflect.DeepEqual(want, reconstructed) {
		t.Fatalf("actual MCP request changed public transcript: want %s; got %s", request, mustTranscriptJSON(t, reconstructed))
	}
	wantResponse, err := wire.DecodeJSON([]byte(`{"jsonrpc":"2.0","id":"public-1","result":` + result + `}`))
	if err != nil {
		t.Fatal(err)
	}
	gotResponse, err := wire.DecodeJSON(response)
	if err != nil || !reflect.DeepEqual(wantResponse, gotResponse) {
		t.Fatalf("actual MCP response changed public transcript: %v", err)
	}
}

func mustTranscriptJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
