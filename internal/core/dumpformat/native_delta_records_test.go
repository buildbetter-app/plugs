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

func TestNativeDeltaReaderReservesGrowthBeforeReturningCandidate(t *testing.T) {
	input := strings.Repeat("x", 40000) + "\n"
	budget := NewNativeDeltaBudget(40000)
	owner := budget.Scope()
	reader := newNativeDeltaRecordReader(context.Background(), strings.NewReader(input), owner)
	var limit *NativeDeltaDecodedLimitError
	if raw, err := reader.next(); raw != nil || !errors.As(err, &limit) || limit.Phase != "logical_record_buffer" || limit.MeasuredBytes <= limit.LimitBytes {
		t.Fatal("record candidate escaped growth refusal", err)
	}
	reader.Close()
	owner.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("reader refusal leaked reservation")
	}
	budget = NewNativeDeltaBudget(1 << 20)
	owner = budget.Scope()
	reader = newNativeDeltaRecordReader(context.Background(), strings.NewReader(input), owner)
	if raw, err := reader.next(); err != nil || string(raw) != input {
		t.Fatal(err)
	}
	held := budget.Usage().CurrentAccountedDecodedBytes
	if held <= uint64(len(input)) {
		t.Fatal("buffer and record ownership were conflated")
	}
	if _, err := reader.next(); err != io.EOF {
		t.Fatal(err)
	}
	if budget.Usage().CurrentAccountedDecodedBytes >= held {
		t.Fatal("previous record did not release at next read")
	}
	reader.Close()
	owner.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("reader Close leaked reservation")
	}
}

func TestNativeDeltaBoundedReadAccountsActualGrowthAndReleasesRefusals(t *testing.T) {
	for _, ceiling := range []uint64{1, 600, 4096} {
		budget := NewNativeDeltaBudget(ceiling)
		scope := budget.Scope()
		raw, err := nativeDeltaReadBounded(strings.NewReader(strings.Repeat("x", 700)), 700, scope, "input")
		if ceiling < 4096 && err == nil {
			t.Fatal("unadmitted growth accepted")
		}
		if ceiling == 4096 && (err != nil || len(raw) != 700) {
			t.Fatal("bounded input differs", err)
		}
		scope.Release()
		if budget.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("failed read leaked parent reservation")
		}
	}
}
