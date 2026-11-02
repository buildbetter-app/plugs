package driver

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParentCounterConsumesFinalValue(t *testing.T) {
	for _, source := range []string{"retained", "observed"} {
		t.Run(source, func(t *testing.T) {
			counter := ParentCounter{Incarnation: "inc", Parent: "arbitrary"}
			observed := uint64(0)
			if source == "retained" {
				counter.LastChild = ^uint64(0) - 1
			} else {
				observed = ^uint64(0) - 1
			}
			expected, err := counter.Next(observed)
			if err != nil || expected != "arbitrary.18446744073709551615" {
				t.Fatalf("final valid preview=%q err=%v", expected, err)
			}
			consumed, err := counter.Consume(expected)
			if err != nil || consumed.LastChild != ^uint64(0) {
				t.Fatalf("final valid consumption=%+v err=%v", consumed, err)
			}
			if counter.LastChild == ^uint64(0) {
				t.Fatal("consumption mutated the input value")
			}
			if id, err := consumed.Next(0); id != "" || !errors.Is(err, ErrUnsupported) {
				t.Fatalf("terminal continuation wrapped: id=%q err=%v", id, err)
			}
			if _, err := consumed.Consume(expected); !errors.Is(err, ErrConflict) {
				t.Fatalf("terminal value consumed twice: %v", err)
			}
		})
	}
}

func TestParentCounterIdentityInventoryAndExhaustion(t *testing.T) {
	seen := map[string]bool{}
	for _, identity := range [][2]string{{"inc-a", "arbitrary"}, {"inc-b", "arbitrary"}, {"inc-a", "arbitrary.1"}, {"inc", "a-b"}} {
		key, err := ParentCounterKey(identity[0], identity[1])
		if err != nil || seen[key] {
			t.Fatalf("noninjective counter identity: %q %v", key, err)
		}
		seen[key] = true
		if _, err := AllocatorFact(key); err == nil {
			t.Fatal("parent counter key admitted as a project prefix")
		}
	}
	for _, parent := range []string{"", " x", "x/y", "x\x00y", string([]byte{0xff})} {
		if _, err := ParentCounterKey("inc", parent); err == nil {
			t.Fatalf("noncanonical parent %q accepted", parent)
		}
	}
	counter := ParentCounter{Incarnation: "inc", Parent: "arbitrary", LastChild: 8}
	if id, err := counter.Next(2); err != nil || id != "arbitrary.9" {
		t.Fatalf("retained high-water: %q %v", id, err)
	}
	if id, err := counter.Next(10); err != nil || id != "arbitrary.11" {
		t.Fatalf("observed repair: %q %v", id, err)
	}
	if _, err := counter.Next(^uint64(0)); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("observed overflow=%v", err)
	}
	counter.LastChild = ^uint64(0)
	if _, err := counter.Next(0); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("retained overflow=%v", err)
	}
	counter.LastChild = 8
	if _, err := counter.Consume("arbitrary.8"); !errors.Is(err, ErrConflict) {
		t.Fatalf("consumed counter reused: %v", err)
	}
	key, _ := ParentCounterKey(counter.Incarnation, counter.Parent)
	raw, _ := json.Marshal(counter)
	inventory := AllocatorInventory{}
	if err := inventory.AddRow(key, raw, counter.Incarnation); err != nil || len(inventory.Parents) != 1 || len(inventory.Projects) != 0 {
		t.Fatalf("typed parent inventory=%+v %v", inventory, err)
	}
	if err := inventory.AddRow(key, raw, "foreign-incarnation"); err == nil {
		t.Fatal("cross-incarnation counter admitted")
	}
	if err := inventory.AddRow("arbitrary", raw, counter.Incarnation); err == nil {
		t.Fatal("parent counter masqueraded as project")
	}
	profile, _ := json.Marshal(NewRandomAllocator("demo"))
	if err := inventory.AddRow(key, profile, counter.Incarnation); err == nil {
		t.Fatal("project profile masqueraded as parent counter")
	}
}
