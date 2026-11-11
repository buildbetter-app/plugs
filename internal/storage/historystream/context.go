package historystream

import (
	"context"
	"io"
)

// ContextReader checks cancellation at every bounded page, including readers
// used by decoding and permanent memory-engine output construction.
func ContextReader(ctx context.Context, input io.Reader) io.Reader {
	return contextReader{ctx, input}
}

type contextReader struct {
	ctx   context.Context
	input io.Reader
}

func (r contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(b) > PageBytes {
		b = b[:PageBytes]
	}
	n, err := r.input.Read(b)
	if r.ctx.Err() != nil {
		return n, r.ctx.Err()
	}
	return n, err
}
