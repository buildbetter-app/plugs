// Package inprocess carries the exact JSON-RPC bytes without a listener or UI.
package inprocess

import "context"

type Dispatcher interface {
	Dispatch(context.Context, []byte) []byte
}
type Transport struct{ server Dispatcher }

func New(server Dispatcher) *Transport { return &Transport{server: server} }

// RoundTrip returns nil for notifications. Protocol failures are JSON-RPC
// response bytes; transport failures (such as cancellation) are Go errors.
func (t *Transport) RoundTrip(ctx context.Context, request []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return t.server.Dispatch(ctx, request), nil
}
