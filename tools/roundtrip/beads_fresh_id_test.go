package main

import (
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestFreshNativeIndependentIDDerivation(t *testing.T) {
	facts := freshNativeFacts{Allocator: driver.OfflineAllocator{Prefix: "demo", Mode: "hash", HashLength: 8, MinHashLength: 3, MaxHashLength: 8, MaxCollisionProb: 0.25}, IDs: []string{"demo-parent"}, RootCount: 1}
	op := freshNativeOperation{Kind: "root", NativeActor: "user:example", Clock: time.Unix(1750000000, 123456789).UTC(), Fields: map[string]any{"title": "Example", "description": "Example description"}}
	// Fixed SHA-256/base36 vectors were calculated independently of either
	// the native or Plug minting implementation. No runtime case uses them.
	if id, err := freshDerivedID(facts, op); err != nil || id != "demo-vbq" {
		t.Fatalf("native hash vector: %s %v", id, err)
	}
	facts.IDs = append(facts.IDs, "demo-vbq")
	if id, err := freshDerivedID(facts, op); err != nil || id != "demo-x0o" {
		t.Fatalf("global occupancy collision: %s %v", id, err)
	}
	facts.RootCount = 1000
	if id, err := freshDerivedID(facts, op); err != nil || id != "demo-030pk" {
		t.Fatalf("adaptive threshold: %s %v", id, err)
	}
	op.Kind, op.Parent = "child", "demo-parent"
	facts.Allocator.Children = map[string]uint64{"demo-parent": 3}
	facts.IDs = append(facts.IDs, "demo-parent.5", "demo-parent.99.1")
	if id, err := freshDerivedID(facts, op); err != nil || id != "demo-parent.6" {
		t.Fatalf("reconciled direct-child counter: %s %v", id, err)
	}
	facts.Allocator.Children[op.Parent] = ^uint64(0)
	if _, err := freshDerivedID(facts, op); err == nil {
		t.Fatal("exhausted native child counter accepted")
	}
}
