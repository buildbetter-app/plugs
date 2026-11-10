package dumpformat

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
)

// The newline is included in this bound. It is a logical record bound, not a
// part or total stream limit. One record may span any number of parts.
const NativeDeltaRecordLimit = 128 << 20

type NativeDeltaRecordLimitError struct {
	MeasuredBytes uint64
	LimitBytes    uint64
}

func (e *NativeDeltaRecordLimitError) Error() string {
	return fmt.Sprintf("native delta logical record exceeds limit: measured_bytes=%d limit_bytes=%d", e.MeasuredBytes, e.LimitBytes)
}

type nativeDeltaRecordReader struct {
	ctx   context.Context
	input *bufio.Reader
	limit uint64
}

func newNativeDeltaRecordReader(ctx context.Context, input io.Reader) *nativeDeltaRecordReader {
	return &nativeDeltaRecordReader{ctx: ctx, input: bufio.NewReaderSize(input, 32<<10), limit: NativeDeltaRecordLimit}
}

func (r *nativeDeltaRecordReader) next() ([]byte, error) {
	var record []byte
	for {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		fragment, err := r.input.ReadSlice('\n')
		measured := uint64(len(record)) + uint64(len(fragment))
		// Check before copying a cross-part fragment into the record buffer.
		if measured > r.limit {
			return nil, &NativeDeltaRecordLimitError{measured, r.limit}
		}
		record = append(record, fragment...)
		switch {
		case err == nil:
			if len(record) == 1 {
				return nil, fmt.Errorf("empty native delta logical record")
			}
			return record, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(record) != 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, io.EOF
		default:
			return nil, err
		}
	}
}
