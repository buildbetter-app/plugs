package driver

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestCanonicalLockOrder(t *testing.T) {
	ref := model.RefByID("bead-a", model.Natural("1.0"))
	keys := []LockKey{
		{Kind: LockRecord, Ref: &ref},
		{Kind: LockReadFact, Fact: "predicate"},
		GraphBlockingSentinelLock,
		{Kind: LockDerived, Derived: &DerivedRowKey{Table: DerivedReadyState, Key: "bead-a"}},
	}
	want := []string{"0:id:bead-a:1", "1:predicate", "2:graph/blocking", "3:ready_state:bead-a"}
	requests := make(LockSet, len(keys))
	for i, key := range keys {
		encoded, err := EncodeLockKey(key)
		if err != nil || encoded != want[i] {
			t.Fatalf("key %d = %q, %v; want %q", i, encoded, err, want[i])
		}
		requests[i] = LockRequest{Key: key, Mode: LockUpdate}
	}
	if err := ValidateLockSet(requests); err != nil {
		t.Fatal(err)
	}
	if err := ValidateHeaderLocks(keys, requests); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLockSet(LockSet{requests[1], requests[0]}); err == nil {
		t.Fatal("unsorted locks accepted")
	}
	if err := ValidateLockSet(LockSet{requests[0], requests[0]}); err == nil {
		t.Fatal("duplicate locks accepted")
	}
	if err := ValidateHeaderLocks(keys[:3], requests); err == nil {
		t.Fatal("incomplete commit declaration accepted")
	}
}

func TestSentinelCensus(t *testing.T) {
	for _, raw := range []string{"bead/b-1", "graph/blocking", "ready/project/task", "closure/b-1", "grant/project", "ontology", "plugins"} {
		if _, err := NewSentinel(raw); err != nil {
			t.Errorf("canonical sentinel %q: %v", raw, err)
		}
	}
	for _, raw := range []string{"", "ready-set", "closure", "grant", "plugin", "bead/", "bead/a/b", "ready/a", "ready/a/b/c", "grant/ x", "ontology ", "plugins\x00"} {
		if _, err := NewSentinel(raw); err == nil {
			t.Errorf("invalid sentinel %q accepted", raw)
		}
	}
}
