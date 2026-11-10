package dumpformat

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestNativeDeltaLogicalRecordBoundAndTerminalNewline(t *testing.T) {
	for _, input := range []string{"{}\n", strings.Repeat("x", 100000) + "\n"} {
		r := newNativeDeltaRecordReader(context.Background(), strings.NewReader(input))
		r.limit = uint64(len(input))
		got, err := r.next()
		if err != nil || string(got) != input {
			t.Fatalf("bounded complete record differs: %v", err)
		}
		if _, err := r.next(); err != io.EOF {
			t.Fatalf("terminal EOF: %v", err)
		}
		r = newNativeDeltaRecordReader(context.Background(), strings.NewReader(input))
		r.limit = uint64(len(input) - 1)
		var limit *NativeDeltaRecordLimitError
		if _, err := r.next(); !errors.As(err, &limit) || limit.MeasuredBytes <= limit.LimitBytes {
			t.Fatalf("record bound not enforced: %v", err)
		}
	}
	r := newNativeDeltaRecordReader(context.Background(), strings.NewReader("{}"))
	if _, err := r.next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("unterminated record accepted: %v", err)
	}
}
