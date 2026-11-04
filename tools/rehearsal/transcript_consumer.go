package rehearsal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/client"
)

// publicCapture keeps actual client transport frames in memory for one owned,
// quiescent fixture. It refuses non-read traffic before the underlying transport
// sees it; the sampled factory workload uses its ordinary clients instead.
type publicCapture struct {
	next      client.Transport
	catalog   *wire.Catalog
	exchanges []TranscriptExchange
	bytes     int
}

func (c *publicCapture) RoundTrip(ctx context.Context, request []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(request) == 0 || len(request) > 1<<20 || len(c.exchanges) == 256 || c.bytes+len(request) > 8<<20 {
		return nil, fmt.Errorf("public caller request exceeds transcript bound")
	}
	if err := c.guardRead(request); err != nil {
		return nil, err
	}
	capturedRequest := append(json.RawMessage(nil), request...)
	response, err := c.next.RoundTrip(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(response) == 0 || len(response) > 1<<20 || c.bytes+len(request)+len(response) > 8<<20 {
		return nil, fmt.Errorf("public caller response exceeds transcript bound")
	}
	exchange := TranscriptExchange{
		Request:  capturedRequest,
		Response: append(json.RawMessage(nil), response...),
	}
	if _, err := validatePublicExchange(c.catalog, exchange); err != nil {
		return nil, fmt.Errorf("public caller response invalid: %w", err)
	}
	c.exchanges = append(c.exchanges, exchange)
	c.bytes += len(request) + len(response)
	return response, nil
}

func (c *publicCapture) guardRead(request []byte) error {
	decoded, err := wire.DecodeJSON(request)
	if err != nil {
		return fmt.Errorf("public caller request invalid")
	}
	frame, ok := decoded.(map[string]any)
	if !ok || frame["jsonrpc"] != "2.0" {
		return fmt.Errorf("public caller requires a single JSON-RPC request")
	}
	for member := range frame {
		if member != "jsonrpc" && member != "id" && member != "method" && member != "params" {
			return fmt.Errorf("public caller request has an unknown member")
		}
	}
	id, present := frame["id"]
	if !present {
		return fmt.Errorf("public caller request ID required")
	}
	switch id.(type) {
	case string, json.Number, nil:
	default:
		return fmt.Errorf("public caller request ID invalid")
	}
	method, _ := frame["method"].(string)
	desc, known := c.catalog.Descriptor(method)
	if !known || desc.Effect != "read" || desc.Idempotency != "none" {
		return fmt.Errorf("public caller catalog read-only call required")
	}
	params := []byte(`{}`)
	if value, present := frame["params"]; present {
		params, err = json.Marshal(value)
		if err != nil {
			return fmt.Errorf("public caller request parameters invalid")
		}
	}
	if c.catalog.ValidateParams(desc, params) != nil {
		return fmt.Errorf("public caller request parameters invalid")
	}
	return nil
}

// compareOwnedPublicCalls compares independent HTTP and MCP frames from the
// same restored store. Restore parity and lifecycle writes have separate checks.
func compareOwnedPublicCalls(ctx context.Context, f *fixture, beadID string) (int, error) {
	return compareOwnedPublicCallsWithControl(ctx, f, beadID, f.transport, 3)
}

// The control arguments let a fixture exercise actual response drift and a
// missing captured call without fabricating either side's JSON-RPC frames.
func compareOwnedPublicCallsWithControl(ctx context.Context, f *fixture, beadID string, httpTransport client.Transport, shadowCallCount int) (int, error) {
	if beadID == "" {
		return 0, fmt.Errorf("public caller bead ID required")
	}
	if httpTransport == nil || shadowCallCount < 0 || shadowCallCount > 3 {
		return 0, fmt.Errorf("public caller capture control invalid")
	}
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		return 0, err
	}
	mcpTransport, closeMCP, err := openMCPTransport(ctx, f)
	if err != nil {
		return 0, err
	}
	defer closeMCP()
	reference := &publicCapture{next: httpTransport, catalog: catalog}
	shadow := &publicCapture{next: mcpTransport, catalog: catalog}
	const agent = "public-transcript-control"
	httpCaller, err := client.New(reference, "rehearsal", agent)
	if err != nil {
		return 0, err
	}
	mcpCaller, err := client.New(shadow, "rehearsal", agent)
	if err != nil {
		return 0, err
	}
	for index, call := range []struct {
		method string
		params map[string]any
	}{
		{"agent.brief", map[string]any{"budgetTokens": 500}},
		{"memory.list", map[string]any{"limit": 10}},
		{"bead.get", map[string]any{"id": map[string]any{"id": beadID}}},
	} {
		if _, err := httpCaller.Call(ctx, call.method, call.params, ""); err != nil {
			return 0, fmt.Errorf("public HTTP control failed: %w", err)
		}
		if index < shadowCallCount {
			if _, err := mcpCaller.Call(ctx, call.method, call.params, ""); err != nil {
				return 0, fmt.Errorf("public MCP control failed: %w", err)
			}
		}
	}
	if err := ComparePublicTranscripts(reference.exchanges, shadow.exchanges); err != nil {
		return 0, fmt.Errorf("owned public caller transcript differs: %w", err)
	}
	return len(reference.exchanges), nil
}
