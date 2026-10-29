// Package client carries canonical JSON-RPC bytes over a supplied transport.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

type Transport interface {
	RoundTrip(context.Context, []byte) ([]byte, error)
}

type Client struct {
	transport Transport
	catalog   *wire.Catalog
	project   string
	agent     string
	sequence  atomic.Uint64
}

func New(transport Transport, project, agent string) (*Client, error) {
	if transport == nil {
		return nil, fmt.Errorf("client requires a transport")
	}
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		return nil, err
	}
	return &Client{transport: transport, catalog: catalog, project: project, agent: agent}, nil
}

// Result retains the complete result, including every warning. IdempotencyKey
// is supplied even after a lost response so a caller can explicitly retry the
// same operation. Call never retries a write using a new key.
type Result struct {
	JSON           json.RawMessage
	IdempotencyKey string
}

func (c *Client) Call(ctx context.Context, method string, params map[string]any, key string) (Result, error) {
	var result Result
	if err := ctx.Err(); err != nil {
		return result, err
	}
	desc, ok := c.catalog.Descriptor(method)
	if !ok {
		return result, c.catalog.NewError("method_not_found", "", nil)
	}
	if _, present := params["_meta"]; present {
		return result, fmt.Errorf("pass metadata through client options, not params._meta")
	}
	if desc.Idempotency == "required" {
		if key == "" {
			var random [32]byte
			if _, err := rand.Read(random[:]); err != nil {
				return result, err
			}
			key = hex.EncodeToString(random[:])
		}
		if err := wire.ValidateIdempotencyKey(key); err != nil {
			return result, err
		}
	} else if key != "" {
		return result, fmt.Errorf("idempotency key is only valid for a write")
	}
	result.IdempotencyKey = key
	copy := make(map[string]any, len(params)+1)
	for k, v := range params {
		copy[k] = v
	}
	copy["_meta"] = wire.Meta{Project: c.project, Agent: c.agent, IdempotencyKey: key}
	paramsJSON, err := json.Marshal(copy)
	if err != nil {
		return result, err
	}
	if err := c.catalog.ValidateParams(desc, paramsJSON); err != nil {
		return result, err
	}
	id := fmt.Sprintf("plug-%d", c.sequence.Add(1))
	request, err := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      string          `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}{"2.0", id, method, paramsJSON})
	if err != nil {
		return result, err
	}
	response, err := c.transport.RoundTrip(ctx, request)
	if err != nil {
		return result, err
	}
	value, err := wire.DecodeJSON(response)
	if err != nil {
		return result, fmt.Errorf("invalid RPC response: %w", err)
	}
	envelope, ok := value.(map[string]any)
	if !ok || envelope["jsonrpc"] != "2.0" || envelope["id"] != id {
		return result, fmt.Errorf("RPC response version or ID does not match request")
	}
	_, hasResult := envelope["result"]
	_, hasError := envelope["error"]
	if hasResult == hasError {
		return result, fmt.Errorf("RPC response must contain exactly one of result or error")
	}
	for k := range envelope {
		if k != "jsonrpc" && k != "id" && k != "result" && k != "error" {
			return result, fmt.Errorf("unknown RPC response member %q", k)
		}
	}
	if hasError {
		raw, err := json.Marshal(envelope["error"])
		if err != nil {
			return result, err
		}
		var remote wire.Error
		if err := wire.DecodeInto(raw, &remote); err != nil {
			return result, fmt.Errorf("invalid RPC error: %w", err)
		}
		canonical := c.catalog.NewError(remote.Data.Code, "", nil)
		if canonical.Data.Code != remote.Data.Code || canonical.Code != remote.Code || canonical.Data.Retryable != remote.Data.Retryable {
			return result, fmt.Errorf("RPC error violates catalog")
		}
		if err := c.catalog.ValidateError(&remote); err != nil {
			return result, fmt.Errorf("RPC error data violates contract: %w", err)
		}
		return result, &remote
	}
	result.JSON, err = json.Marshal(envelope["result"])
	if err != nil {
		return result, err
	}
	if err := c.catalog.ValidateResult(desc, result.JSON); err != nil {
		return result, fmt.Errorf("RPC result violates contract: %w", err)
	}
	return result, nil
}
