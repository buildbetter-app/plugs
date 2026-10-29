package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Transport interface {
	RoundTrip(context.Context, []byte) ([]byte, error)
}

type Frame struct {
	At       time.Time       `json:"at"`
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
}

// Trace is a sequential scenario runner. It records real request/response
// bytes after validation; concurrent races use separate traces/transports.
type Trace struct {
	transport Transport
	clock     func() time.Time
	next      uint64
	frames    []Frame
}

func NewTrace(transport Transport, clock func() time.Time) (*Trace, error) {
	if transport == nil || clock == nil {
		return nil, fmt.Errorf("trace requires a transport and explicit clock")
	}
	return &Trace{transport: transport, clock: clock}, nil
}

// Call returns the complete envelope, including typed protocol refusals. A Go
// error means transport or envelope failure, never a disguised null success.
func (t *Trace) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if method == "" {
		return nil, fmt.Errorf("method is required")
	}
	t.next++
	id := fmt.Sprint(t.next)
	request, err := json.Marshal(struct {
		Version string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{"2.0", t.next, method, params})
	if err != nil {
		return nil, err
	}
	request, err = CanonicalJSON(request)
	if err != nil {
		return nil, err
	}
	at := t.clock()
	response, err := t.transport.RoundTrip(ctx, append([]byte(nil), request...))
	if err != nil {
		return nil, fmt.Errorf("%s transport: %w", method, err)
	}
	value, err := decode(response)
	if err != nil {
		return nil, fmt.Errorf("%s response: %w", method, err)
	}
	envelope, ok := value.(map[string]any)
	if !ok || envelope["jsonrpc"] != "2.0" || envelope["id"] != json.Number(id) {
		return nil, fmt.Errorf("%s response has wrong version or request ID", method)
	}
	_, result := envelope["result"]
	failure, hasError := envelope["error"]
	if result == hasError {
		return nil, fmt.Errorf("%s response must contain exactly one of result or error", method)
	}
	if hasError {
		errorObject, ok := failure.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s response error must be an object", method)
		}
		code, number := errorObject["code"].(json.Number)
		_, integerError := code.Int64()
		_, message := errorObject["message"].(string)
		if !number || integerError != nil || !message {
			return nil, fmt.Errorf("%s response error needs an integer code and message", method)
		}
	}
	retained := append(json.RawMessage(nil), response...)
	t.frames = append(t.frames, Frame{At: at, Request: request, Response: retained})
	return append(json.RawMessage(nil), retained...), nil
}

func (t *Trace) Frames() []Frame {
	frames := make([]Frame, len(t.frames))
	for i, f := range t.frames {
		frames[i] = Frame{At: f.At, Request: append(json.RawMessage(nil), f.Request...), Response: append(json.RawMessage(nil), f.Response...)}
	}
	return frames
}
