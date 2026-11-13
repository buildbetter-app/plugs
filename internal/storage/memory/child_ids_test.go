package memory

import (
	"context"
	"fmt"
	"testing"
)

func TestNumericChildIndexKeepsSnapshotsAndLogarithmicHeight(t *testing.T) {
	var root *childIDNode
	var saved *childIDNode
	for i := 1; i <= 2500; i++ {
		key := fmt.Sprintf("parent\x00%020d\x00key-%d", i, i)
		root = childInsert(root, key, key)
		if i == 500 {
			saved = root
		}
		if root.height > 18 {
			t.Fatalf("2500-key AVL height=%d", root.height)
		}
	}
	for i := 2500; i > 500; i-- {
		root = childRemove(root, fmt.Sprintf("parent\x00%020d\x00key-%d", i, i))
	}
	for _, r := range []*childIDNode{root, saved} {
		n, err := childMaximum(context.Background(), r, "parent", func(string) bool { return true })
		if err != nil || n != 500 {
			t.Fatalf("persistent maximum=%d err=%v", n, err)
		}
	}
	// Updating either branch must leave the old snapshot untouched.
	root = childInsert(root, "parent\x0000000000000000000999\x00later", "later")
	if n, err := childMaximum(context.Background(), saved, "parent", func(string) bool { return true }); err != nil || n != 500 {
		t.Fatalf("saved snapshot was mutated: %d %v", n, err)
	}
}
