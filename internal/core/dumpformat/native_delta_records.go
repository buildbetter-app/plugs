package dumpformat

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The newline is included in this bound. It is a logical record bound, not a
// part or total stream limit. One record may span any number of parts.
const NativeDeltaRecordLimit = 128 << 20

type spooledCaptureKey struct{}

type NativeDeltaRecordLimitError struct {
	MeasuredBytes uint64
	LimitBytes    uint64
}

func (e *NativeDeltaRecordLimitError) Code() string { return "native_record_limit" }

func (e *NativeDeltaRecordLimitError) Error() string {
	return fmt.Sprintf("native delta logical record exceeds limit: measured_bytes=%d limit_bytes=%d", e.MeasuredBytes, e.LimitBytes)
}

type nativeDeltaRecordReader struct {
	ctx          context.Context
	input        *bufio.Reader
	bounded      bool
	limit        uint64
	index        uint64
	offset       uint64
	partBytes    uint64
	budget       *NativeDeltaBudget
	buffer       *NativeDeltaBudget
	record       *NativeDeltaBudget
	initialError error
}

func newNativeDeltaRecordReader(ctx context.Context, input io.Reader, budgets ...*NativeDeltaBudget) *nativeDeltaRecordReader {
	r := &nativeDeltaRecordReader{ctx: ctx, limit: NativeDeltaRecordLimit}
	if spooled, _ := ctx.Value(spooledCaptureKey{}).(bool); spooled {
		r.limit = driver.ReverseJSONElementBytes + 1 // includes the newline
		r.bounded = true
		if parts, ok := input.(*NativeDeltaPartReader); ok {
			r.partBytes = parts.payloadLimit
		}
	}
	if len(budgets) != 0 && budgets[0] != nil {
		r.budget = budgets[0]
		r.buffer = r.budget.Scope()
		if err := r.buffer.ChargeBytes("record_reader_buffer", 32<<10); err != nil {
			r.initialError = err
			return r
		}
	}
	r.input = bufio.NewReaderSize(input, 32<<10)
	return r
}

func (r *nativeDeltaRecordReader) Close() {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
	if r.buffer != nil {
		r.buffer.Release()
		r.buffer = nil
	}
}

func (r *nativeDeltaRecordReader) next() ([]byte, error) {
	if r.initialError != nil {
		return nil, r.initialError
	}
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
	if r.budget != nil {
		r.record = r.budget.Scope()
	}
	var record []byte
	for {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		fragment, err := r.input.ReadSlice('\n')
		measured := uint64(len(record)) + uint64(len(fragment))
		// Check before copying a cross-part fragment into the record buffer.
		if measured > r.limit {
			if r.bounded {
				return nil, r.locate(&driver.ReverseJSONLimitError{
					Resource: "value_bytes", Measured: r.limit, Limit: r.limit - 1,
				})
			}
			return nil, &NativeDeltaRecordLimitError{measured, r.limit}
		}
		if measured > uint64(cap(record)) {
			capacity := uint64(cap(record)) * 2
			if capacity < measured {
				capacity = measured
			}
			if capacity > r.limit {
				capacity = r.limit
			}
			if r.record != nil {
				if err := r.record.ChargeBytes("logical_record_buffer", capacity); err != nil {
					return nil, err
				}
			}
			next := make([]byte, len(record), int(capacity))
			copy(next, record)
			if r.record != nil && cap(record) > 0 {
				r.record.ReleaseBytes("logical_record_buffer", uint64(cap(record)))
			}
			record = next
		}
		record = append(record, fragment...)
		switch {
		case err == nil:
			if len(record) == 1 {
				return nil, fmt.Errorf("empty native delta logical record")
			}
			if r.bounded {
				if _, err := driver.ReverseJSONElementFootprint(record[:len(record)-1], 0); err != nil {
					return nil, r.locate(err)
				}
			}
			r.index++
			r.offset += uint64(len(record))
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

func (r *nativeDeltaRecordReader) locate(err error) error {
	member := "parts/<id>.part"
	if r.partBytes != 0 {
		member = fmt.Sprintf("parts/%020d.part", r.offset/r.partBytes+1)
	}
	return driver.LocateReverseJSONLimit(err, member, "$", r.index)
}
