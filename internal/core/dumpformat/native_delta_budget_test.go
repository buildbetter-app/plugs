package dumpformat

import (
	"errors"
	"math"
	"testing"
)

func TestNativeDeltaDecodedBudgetRefusesCandidateBeforeRetention(t *testing.T) {
	b := NewNativeDeltaBudget(10)
	if err := b.ChargeJSON("schema", map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	// Canonical JSON is {"a":1} followed by a newline: eight bytes.
	if err := b.ChargeBytes("head", 2); err != nil {
		t.Fatal(err)
	}
	err := b.ChargeBytes("batch", 1)
	var limit *NativeDeltaDecodedLimitError
	if !errors.As(err, &limit) || limit.MeasuredBytes != 11 || limit.LimitBytes != 10 || limit.Overflow {
		t.Fatalf("wrong typed refusal: %#v", err)
	}
	usage := b.Usage()
	if usage.PeakAccountedDecodedBytes != 10 || usage.Categories["batch"] != 0 {
		t.Fatalf("refused candidate retained: %+v", usage)
	}
	usage.Categories["head"] = 100
	if b.Usage().Categories["head"] != 2 {
		t.Fatal("usage aliases budget")
	}
}

func TestNativeDeltaDecodedBudgetOverflowIsExplicit(t *testing.T) {
	b := NewNativeDeltaBudget(math.MaxUint64)
	if err := b.ChargeBytes("first", math.MaxUint64-1); err != nil {
		t.Fatal(err)
	}
	var limit *NativeDeltaDecodedLimitError
	if err := b.ChargeBytes("next", 2); !errors.As(err, &limit) || !limit.Overflow || limit.MeasuredBytes != math.MaxUint64 {
		t.Fatalf("wrong overflow refusal: %#v", err)
	}
}
