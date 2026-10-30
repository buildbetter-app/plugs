package stdio

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Dispatcher is the canonical JSON-RPC boundary. A stdio transport never
// binds methods or translates parameters; it passes each complete frame to
// Dispatch unchanged.
type Dispatcher interface {
	Dispatch(context.Context, []byte) []byte
}

// ErrorEncoder is implemented by rpc.Server. It lets framing failures use the
// same closed error catalog and trace-ID envelope as ordinary RPC failures.
type ErrorEncoder interface {
	TransportError(error) []byte
}

// Transport owns one stdio stream. A Transport is intentionally a stream
// adapter, not a listener or server composition root: it never opens a
// socket, mounts a UI, or creates a store.
type Transport struct {
	dispatcher Dispatcher
	config     Config
}

// New validates and constructs a stdio adapter over an existing dispatcher.
func New(dispatcher Dispatcher, config Config) (*Transport, error) {
	if dispatcher == nil {
		return nil, fmt.Errorf("stdio: dispatcher is required")
	}
	config, err := config.defaults()
	if err != nil {
		return nil, err
	}
	return &Transport{dispatcher: dispatcher, config: config}, nil
}

// Serve reads newline-delimited JSON-RPC frames until clean EOF. A response
// is written as the exact dispatcher bytes followed by one newline. A
// notification produces no output. Oversize frames are refused with a
// canonical invalid_request envelope and discarded through their newline so a
// later frame can still be served.
//
// If in is an io.Closer, cancellation closes it to interrupt a blocked read.
// Dispatch itself is synchronous and must honor the per-frame context; this
// avoids detached work that could commit after cancellation.
func (t *Transport) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	if ctx == nil {
		return &Error{Kind: ErrorCanceled, Cause: errors.New("nil context")}
	}
	if in == nil {
		return &Error{Kind: ErrorRead, Cause: errors.New("nil input")}
	}
	if out == nil {
		return &Error{Kind: ErrorWrite, Cause: errors.New("nil output")}
	}
	if err := ctx.Err(); err != nil {
		if closer, ok := in.(io.Closer); ok {
			_ = closer.Close()
		}
		return &Error{Kind: ErrorCanceled, Cause: err}
	}

	if closer, ok := in.(io.Closer); ok {
		stop := context.AfterFunc(ctx, func() { _ = closer.Close() })
		defer stop()
	}

	reader := bufio.NewReader(in)
	for {
		if err := ctx.Err(); err != nil {
			return &Error{Kind: ErrorCanceled, Cause: err}
		}
		frame, err := readFrame(reader, t.config.MaxFrameBytes)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, ErrFrameTooLarge) {
			framing := &Error{Kind: ErrorFrameTooLarge, Limit: t.config.MaxFrameBytes, Cause: err}
			if err := t.writeTransportError(ctx, out, framing); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				return &Error{Kind: ErrorCanceled, Cause: canceled}
			}
			if _, ok := err.(*Error); ok {
				return err
			}
			return &Error{Kind: ErrorRead, Cause: err}
		}

		callCtx := ctx
		cancel := func() {}
		if t.config.RequestTimeout > 0 {
			callCtx, cancel = context.WithTimeout(ctx, t.config.RequestTimeout)
		}
		response := t.dispatcher.Dispatch(callCtx, frame)
		cancel()
		if len(response) != 0 {
			if err := writeFrame(out, response); err != nil {
				return &Error{Kind: ErrorWrite, Cause: err}
			}
		}
		if err := ctx.Err(); err != nil {
			return &Error{Kind: ErrorCanceled, Cause: err}
		}
	}
}

// Run is an alias for Serve for callers that name long-lived stream loops as
// runners. It has identical EOF, cancellation and framing semantics.
func (t *Transport) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	return t.Serve(ctx, in, out)
}

func (t *Transport) writeTransportError(ctx context.Context, out io.Writer, framing *Error) error {
	if err := ctx.Err(); err != nil {
		return &Error{Kind: ErrorCanceled, Cause: err}
	}
	var payload []byte
	if encoder, ok := t.dispatcher.(ErrorEncoder); ok {
		payload = encoder.TransportError(framing.modelError())
	} else {
		payload = fallbackError(framing.modelError())
	}
	if len(payload) == 0 {
		return &Error{Kind: ErrorWrite, Cause: errors.New("dispatcher returned an empty transport error")}
	}
	if err := writeFrame(out, payload); err != nil {
		return &Error{Kind: ErrorWrite, Cause: err}
	}
	return nil
}

func (e *Error) modelError() error {
	if e != nil && e.Kind == ErrorFrameTooLarge {
		return &model.Error{Data: model.ErrorData{
			Code: model.ErrorCode("invalid_request"),
			Hint: "stdio frame exceeds the configured limit",
		}}
	}
	return &model.Error{Data: model.ErrorData{
		Code: model.ErrorCode("busy"),
		Hint: "stdio transport is unavailable",
	}}
}

// readFrame reads one NDJSON frame without ever retaining more than the
// configured payload limit. The delimiter is not part of the returned bytes;
// a final frame may be terminated by EOF instead of a newline.
func readFrame(reader *bufio.Reader, limit int) ([]byte, error) {
	var frame []byte
	over := false
	for {
		chunk, err := reader.ReadSlice('\n')
		payloadLen := len(chunk)
		if payloadLen > 0 && chunk[payloadLen-1] == '\n' {
			payloadLen--
		}
		if !over {
			if len(frame)+payloadLen > limit {
				over = true
				frame = nil
			} else {
				frame = append(frame, chunk...)
			}
		}

		switch {
		case err == nil:
			if over {
				return nil, ErrFrameTooLarge
			}
			return stripDelimiter(frame), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if over {
				return nil, ErrFrameTooLarge
			}
			if len(frame) == 0 {
				return nil, io.EOF
			}
			return stripDelimiter(frame), nil
		default:
			return nil, &Error{Kind: ErrorRead, Cause: err}
		}
	}
}

func stripDelimiter(frame []byte) []byte {
	if len(frame) > 0 && frame[len(frame)-1] == '\n' {
		frame = frame[:len(frame)-1]
		if len(frame) > 0 && frame[len(frame)-1] == '\r' {
			frame = frame[:len(frame)-1]
		}
	}
	return frame
}

func writeFrame(out io.Writer, payload []byte) error {
	frame := make([]byte, 0, len(payload)+1)
	frame = append(frame, payload...)
	frame = append(frame, '\n')
	for len(frame) > 0 {
		n, err := out.Write(frame)
		if n > 0 {
			frame = frame[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func fallbackError(err error) []byte {
	catalog, catalogErr := wire.DefaultCatalog()
	if catalogErr != nil {
		return []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid request","data":{"code":"invalid_request","retryable":false}}}`)
	}
	encoded := catalog.MapError(err)
	if encoded.Data.TraceID == "" {
		var random [16]byte
		if _, randomErr := rand.Read(random[:]); randomErr == nil {
			encoded.Data.TraceID = hex.EncodeToString(random[:])
		}
	}
	payload, marshalErr := json.Marshal(encoded)
	if marshalErr != nil {
		return []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"internal error","data":{"code":"internal_error","retryable":false}}}`)
	}
	response := make([]byte, 0, len(payload)+40)
	response = append(response, `{"jsonrpc":"2.0","id":null,"error":`...)
	response = append(response, payload...)
	return append(response, '}')
}
