package rehearsal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

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

// Hand-authored public fixtures include full result content, array order,
// optional nulls, a typed refusal and a catalog-read plan with warnings.
func publicTranscript() []TranscriptExchange {
	return []TranscriptExchange{
		{
			Request:  json.RawMessage(`{"jsonrpc":"2.0","id":"public-get","method":"bead.get","params":{"id":{"id":"public-bead"},"asOf":{"revision":9007199254740993}}}`),
			Response: json.RawMessage(`{"jsonrpc":"2.0","id":"public-get","result":{"bead":{"id":"public-bead","type":"issue","revision":9007199254740993,"fields":{"title":"Public fixture"}},"attachments":[]}}`),
		},
		{
			Request:  json.RawMessage(`{"jsonrpc":"2.0","id":"public-list","method":"memory.list","params":{"limit":2}}`),
			Response: json.RawMessage(`{"jsonrpc":"2.0","id":"public-list","result":{"items":[{"id":"memory-a","key":"alpha","content":"Public A","revision":1,"metadata":{"optional":null}},{"id":"memory-b","key":"beta","content":"Public B","revision":2}]}}`),
		},
		{
			Request:  json.RawMessage(`{"jsonrpc":"2.0","id":"public-error","method":"memory.get","params":{"id":"public-missing"}}`),
			Response: json.RawMessage(`{"jsonrpc":"2.0","id":"public-error","error":{"code":-32000,"message":"resource not found","data":{"code":"not_found","retryable":false,"hint":"Public missing memory","details":{"resource":"public-missing"}}}}`),
		},
		{
			Request:  json.RawMessage(`{"jsonrpc":"2.0","id":"public-plan","method":"ontology.plan","params":{"document":{}}}`),
			Response: json.RawMessage(`{"jsonrpc":"2.0","id":"public-plan","result":{"planDigest":"public-plan","baseRevision":3,"changes":{},"violations":[],"warnings":[{"code":"ontology_drifted","message":"Public warning"}],"requiresHuman":false}}`),
		},
	}
}

func TestTranscriptPublicControlAcceptsObjectOrderAndRetainsInputs(t *testing.T) {
	reference, shadow := publicTranscript(), publicTranscript()
	shadow[0].Request = json.RawMessage(` {"params":{"asOf":{"revision":9007199254740993},"id":{"id":"public-bead"}},"method":"bead.get","id":"public-get","jsonrpc":"2.0"} `)
	shadow[0].Response = json.RawMessage(`{"result":{"attachments":[],"bead":{"fields":{"title":"Public fixture"},"revision":9007199254740993,"type":"issue","id":"public-bead"}},"id":"public-get","jsonrpc":"2.0"}`)
	beforeReference, beforeShadow := mustTranscriptJSON(t, reference), mustTranscriptJSON(t, shadow)
	if err := ComparePublicTranscripts(reference, shadow); err != nil {
		t.Fatalf("valid independent public control refused: %v", err)
	}
	if !bytes.Equal(beforeReference, mustTranscriptJSON(t, reference)) || !bytes.Equal(beforeShadow, mustTranscriptJSON(t, shadow)) {
		t.Fatal("comparison changed its inputs")
	}
}

func TestTranscriptRejectsPayloadDriftWithIdenticalAggregateMetrics(t *testing.T) {
	metrics, err := Summarize([]Sample{{Method: "bead.get", Latency: time.Millisecond, Completed: true}}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if issues := Compare(metrics, metrics, map[string]time.Duration{"bead.get": time.Second}); len(issues) != 0 {
		t.Fatal(issues)
	}
	reference, shadow := publicTranscript()[:1], publicTranscript()[:1]
	shadow[0].Response = bytes.Replace(shadow[0].Response, []byte("Public fixture"), []byte("Different public content"), 1)
	if err := ComparePublicTranscripts(reference, shadow); err == nil || !strings.Contains(err.Error(), "response differs") {
		t.Fatalf("aggregate-equivalent payload drift accepted: %v", err)
	}
}

func TestTranscriptPreservesNumbersWarningsArraysNullsAndTypedErrors(t *testing.T) {
	for _, test := range []struct {
		name, part, before, after string
		index                     int
	}{
		{"request-large-revision", "request", "9007199254740993", "9007199254740992", 0},
		{"result-large-revision", "response", "9007199254740993", "9007199254740992", 0},
		{"request-reference", "request", "public-bead", "another-public-bead", 0},
		{"request-meta-presence", "request", `"limit":2`, `"limit":2,"_meta":{"dryRun":false}`, 1},
		{"null-member-presence", "response", `"optional":null`, ``, 1},
		{"warning-removed", "response", `,"warnings":[{"code":"ontology_drifted","message":"Public warning"}]`, ``, 3},
		{"warning-message", "response", "Public warning", "Different public warning", 3},
		{"error-hint", "response", "Public missing memory", "Another public hint", 2},
		{"error-details", "response", `"resource":"public-missing"`, `"resource":"public-other"`, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			reference, shadow := publicTranscript(), publicTranscript()
			raw := &shadow[test.index].Response
			if test.part == "request" {
				raw = &shadow[test.index].Request
			}
			if !bytes.Contains(*raw, []byte(test.before)) {
				t.Fatal("drift fixture did not match")
			}
			*raw = bytes.Replace(*raw, []byte(test.before), []byte(test.after), 1)
			// Both independent streams must remain valid; refusal must be for
			// actual drift rather than an invalid mutated fixture.
			if err := ComparePublicTranscripts(shadow, shadow); err != nil {
				t.Fatalf("drift control is not valid: %v", err)
			}
			if err := ComparePublicTranscripts(reference, shadow); err == nil {
				t.Fatal("public transcript drift accepted")
			}
		})
	}
	reference, shadow := publicTranscript(), publicTranscript()
	shadow[0], shadow[1] = shadow[1], shadow[0]
	if err := ComparePublicTranscripts(reference, shadow); err == nil {
		t.Fatal("exchange reordering accepted")
	}
	shadow = publicTranscript()
	shadow[1].Response = json.RawMessage(`{"jsonrpc":"2.0","id":"public-list","result":{"items":[{"id":"memory-b","key":"beta","content":"Public B","revision":2},{"id":"memory-a","key":"alpha","content":"Public A","revision":1,"metadata":{"optional":null}}]}}`)
	if err := ComparePublicTranscripts(shadow, shadow); err != nil {
		t.Fatal(err)
	}
	if err := ComparePublicTranscripts(reference, shadow); err == nil {
		t.Fatal("result array reordering accepted")
	}
}

func TestTranscriptReadProfileUsesCatalogEffectsIncludingAuthnWithoutReplayKey(t *testing.T) {
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ method, params string }{
		{"bead.comment", `{"id":{"id":"public-bead"},"body":"Public comment","_meta":{"idempotencyKey":"0123456789abcdef0123456789abcdef","dryRun":true}}`},
		{"plugin.plan", `{"manifest":{},"_meta":{"idempotencyKey":"0123456789abcdef0123456789abcdef"}}`},
		{"authn.authenticate", `{"credential":"public-invalid-fixture"}`},
		{"admin.restore.begin", `{"manifest":{},"humanPresence":{"mode":"presence_check","challenge":"public","proof":"public-invalid-fixture"},"_meta":{"idempotencyKey":"0123456789abcdef0123456789abcdef"}}`},
	} {
		t.Run(test.method, func(t *testing.T) {
			desc, known := catalog.Descriptor(test.method)
			if !known || catalog.ValidateParams(desc, []byte(test.params)) != nil {
				t.Fatal("effect fixture is not a valid public request")
			}
			stream := publicTranscript()[2:3]
			stream[0].Request = mustTranscriptJSON(t, map[string]any{"jsonrpc": "2.0", "id": "public-error", "method": test.method, "params": json.RawMessage(test.params)})
			if err := ComparePublicTranscripts(stream, stream); err == nil || !strings.Contains(err.Error(), "read-only") {
				t.Fatalf("unexpected non-read call admitted: %v", err)
			}
			if err := ComparePublicTranscripts(publicTranscript(), append(publicTranscript(), stream...)); err == nil {
				t.Fatal("extra unexpected non-read call admitted")
			}
		})
	}
}

func TestTranscriptRejectsIdenticalMalformedFramesWithoutEchoingPayloads(t *testing.T) {
	for _, test := range []struct{ name, part, raw string }{
		{"batch", "request", `[]`},
		{"notification", "request", `{"jsonrpc":"2.0","method":"memory.list","params":{}}`},
		{"wrong-version", "request", `{"jsonrpc":"1.0","id":"public-list","method":"memory.list","params":{}}`},
		{"invalid-id", "request", `{"jsonrpc":"2.0","id":true,"method":"memory.list","params":{}}`},
		{"unknown-method", "request", `{"jsonrpc":"2.0","id":"public-list","method":"public-do-not-echo","params":{}}`},
		{"unknown-flag", "request", `{"jsonrpc":"2.0","id":"public-list","method":"memory.list","params":{"public-do-not-echo":1}}`},
		{"unknown-member", "request", `{"jsonrpc":"2.0","id":"public-list","method":"memory.list","params":{},"public-do-not-echo":true}`},
		{"duplicate", "request", `{"jsonrpc":"2.0","id":"public-list","method":"memory.list","params":{"limit":1,"limit":2}}`},
		{"trailing", "request", `{"jsonrpc":"2.0","id":"public-list","method":"memory.list","params":{}} {}`},
		{"wrong-response-id", "response", `{"jsonrpc":"2.0","id":"other","result":{"items":[]}}`},
		{"wrong-response-version", "response", `{"jsonrpc":"1.0","id":"public-list","result":{"items":[]}}`},
		{"both-result-error", "response", `{"jsonrpc":"2.0","id":"public-list","result":{"items":[]},"error":{}}`},
		{"missing-outcome", "response", `{"jsonrpc":"2.0","id":"public-list"}`},
		{"invalid-result", "response", `{"jsonrpc":"2.0","id":"public-list","result":{"items":null}}`},
		{"unknown-response-member", "response", `{"jsonrpc":"2.0","id":"public-list","result":{"items":[]},"public-do-not-echo":true}`},
		{"read-replay-meta", "response", `{"jsonrpc":"2.0","id":"public-list","result":{"items":[]},"_meta":{"replayed":true}}`},
		{"error-numeric-code", "response", `{"jsonrpc":"2.0","id":"public-list","error":{"code":-32015,"message":"public-do-not-echo","data":{"code":"not_found","retryable":false}}}`},
		{"error-retryability", "response", `{"jsonrpc":"2.0","id":"public-list","error":{"code":-32000,"message":"resource not found","data":{"code":"not_found","retryable":true}}}`},
		{"error-unknown-member", "response", `{"jsonrpc":"2.0","id":"public-list","error":{"code":-32000,"message":"resource not found","data":{"code":"not_found","retryable":false},"public-do-not-echo":true}}`},
		{"error-unknown-code", "response", `{"jsonrpc":"2.0","id":"public-list","error":{"code":-32000,"message":"public-do-not-echo","data":{"code":"public-do-not-echo","retryable":false}}}`},
		{"error-missing-message", "response", `{"jsonrpc":"2.0","id":"public-list","error":{"code":-32000,"data":{"code":"not_found","retryable":false}}}`},
		{"error-null-hint", "response", `{"jsonrpc":"2.0","id":"public-list","error":{"code":-32000,"message":"resource not found","data":{"code":"not_found","retryable":false,"hint":null}}}`},
		{"error-missing-retryability", "response", `{"jsonrpc":"2.0","id":"public-list","error":{"code":-32000,"message":"resource not found","data":{"code":"not_found"}}}`},
		{"response-duplicate", "response", `{"jsonrpc":"2.0","id":"public-list","result":{"items":[]},"result":{"items":[]}}`},
		{"response-trailing", "response", `{"jsonrpc":"2.0","id":"public-list","result":{"items":[]}} {}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := publicTranscript()[1:2]
			if test.part == "request" {
				stream[0].Request = json.RawMessage(test.raw)
			} else {
				stream[0].Response = json.RawMessage(test.raw)
			}
			err := ComparePublicTranscripts(stream, stream)
			if err == nil {
				t.Fatal("identical invalid transcript accepted")
			}
			if len(err.Error()) > 160 || strings.Contains(err.Error(), "public-do-not-echo") {
				t.Fatal("comparison diagnostic disclosed input")
			}
			if err := ComparePublicTranscripts(publicTranscript()[1:2], stream); err == nil {
				t.Fatal("invalid shadow accepted")
			}
			if err := ComparePublicTranscripts(stream, publicTranscript()[1:2]); err == nil {
				t.Fatal("invalid reference accepted")
			}
		})
	}
}

func TestTranscriptRequiresNonemptyCompleteBoundedStreams(t *testing.T) {
	valid := publicTranscript()
	if ComparePublicTranscripts(nil, nil) == nil || ComparePublicTranscripts(valid, valid[:1]) == nil {
		t.Fatal("empty or incomplete stream accepted")
	}
	for _, part := range []string{"request", "response"} {
		stream := publicTranscript()[:1]
		if part == "request" {
			stream[0].Request = json.RawMessage(strings.Repeat(" ", (1<<20)+1))
		} else {
			stream[0].Response = json.RawMessage(strings.Repeat(" ", (1<<20)+1))
		}
		if err := ComparePublicTranscripts(stream, stream); err == nil {
			t.Fatal("oversized public frame accepted")
		}
	}
	tooMany := make([]TranscriptExchange, 257)
	if err := ComparePublicTranscripts(tooMany, tooMany); err == nil {
		t.Fatal("unbounded stream accepted")
	}
	for _, part := range []string{"request", "response"} {
		stream := publicTranscript()[:1]
		if part == "request" {
			stream[0].Request = nil
		} else {
			stream[0].Response = nil
		}
		if err := ComparePublicTranscripts(stream, stream); err == nil {
			t.Fatal("missing request or response accepted")
		}
	}
	large := publicTranscript()[1]
	large.Response = bytes.Replace(large.Response, []byte("Public A"), bytes.Repeat([]byte("x"), 1000000), 1)
	largeStream := make([]TranscriptExchange, 9)
	for i := range largeStream {
		largeStream[i] = large
	}
	if err := ComparePublicTranscripts(largeStream, largeStream); err == nil || !strings.Contains(err.Error(), "stream exceeds") {
		t.Fatalf("oversized complete stream accepted: %v", err)
	}
}
