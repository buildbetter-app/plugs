package driver

import (
	"errors"
	"math"
	"testing"
)

func TestAllocatorSequentialContinuation(t *testing.T) {
	value := OfflineAllocator{Prefix: "demo-app", Mode: AllocatorSequential, LastID: 8, Children: map[string]uint64{"demo-app-8": 3}}
	if got, err := value.NextID(); err != nil || got != "demo-app-9" {
		t.Fatalf("next top-level ID=%q err=%v", got, err)
	}
	if got, err := value.NextChildID("demo-app-8"); err != nil || got != "demo-app-8.4" {
		t.Fatalf("next child ID=%q err=%v", got, err)
	}
	value.LastID = math.MaxUint64
	if _, err := value.NextID(); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("exhausted top-level allocator error=%v", err)
	}
}

func TestAllocatorRejectsUnsupportedOrNoncanonicalState(t *testing.T) {
	cases := []OfflineAllocator{
		{Prefix: "demo", Mode: AllocatorHash, HashLength: 2},
		{Prefix: "demo", Mode: "unknown"},
		{Prefix: "demo/app", Mode: AllocatorSequential},
	}
	for _, value := range cases {
		if err := value.Validate(); err == nil {
			t.Fatalf("invalid allocator accepted: %+v", value)
		}
	}
	hash := OfflineAllocator{Prefix: "demo", Mode: AllocatorHash, HashLength: 8}
	if _, err := hash.NextID(); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("hash allocator minted an ID: %v", err)
	}
}

func TestAllocatorSetRejectsDuplicatePrefix(t *testing.T) {
	value := OfflineAllocator{Prefix: "demo", Mode: AllocatorSequential}
	if err := ValidateAllocatorSet([]OfflineAllocator{value, value}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate prefix error=%v", err)
	}
	if fact, err := AllocatorFact("demo-app"); err != nil || fact != ReadFact("allocator/demo-app") {
		t.Fatalf("allocator fact=%q err=%v", fact, err)
	}
}
