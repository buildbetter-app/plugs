# stdio JSON-RPC transport

`New(dispatcher, Config{})` creates a bounded newline-delimited JSON-RPC
adapter over an existing canonical dispatcher. Each input line is one request
frame; a final frame may end at EOF. The request bytes are passed to
`Dispatch` unchanged apart from the framing newline, and each non-notification
response is written unchanged followed by one newline.

The default frame limit is 1 MiB and the default cooperative dispatch timeout
is 30 seconds. Oversize frames are discarded through their newline and receive
the shared `invalid_request` JSON-RPC envelope when the dispatcher implements
`TransportError`. Malformed JSON is handled by the dispatcher itself, so it
uses the same `parse_error` response as in-process dispatch. Clean EOF returns
nil. Cancellation closes an input that implements `io.Closer` and never starts
a detached dispatch goroutine.

This package owns no listener, store, authentication provider, or UI. A
composition root supplies any authenticated context before calling `Serve`.
