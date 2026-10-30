package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
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

func TestRandomAllocatorPreviewAndConsume(t *testing.T) {
	a := NewRandomAllocator("source-app")
	for _, parent := range []string{"", "source-app-legacy.2"} {
		var id string
		var err error
		if parent == "" {
			id, err = a.NextID()
		} else {
			id, err = a.NextChildID(parent)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = a.ValidateCandidate(parent, id); err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{id + "0", strings.ToUpper(id), "other-" + strings.Repeat("a", 32), parent} {
			if a.ValidateCandidate(parent, bad) == nil {
				t.Fatalf("accepted %q", bad)
			}
		}
	}
	b, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte("last_id")) || bytes.Contains(b, []byte("child_counters")) {
		t.Fatalf("random state contains source counters: %s", b)
	}
	var restored OfflineAllocator
	if err = json.Unmarshal(b, &restored); err != nil || restored.Validate() != nil {
		t.Fatalf("random round trip: %+v %v", restored, err)
	}
	a.LastID = 1
	if a.Validate() == nil {
		t.Fatal("random accepted sequential counters")
	}
}
