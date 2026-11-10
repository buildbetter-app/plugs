package main

import (
	"os"
	"path/filepath"
	"testing"
)

// These are refusal-only filesystem probes. They supply no successful Beads
// executable, native capability, inventory census or runtime evidence.
func TestAutoInventoryRefusesUnsafeProducerOutput(t *testing.T) {
	for _, kind := range []string{"missing", "symlink", "directory", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			calls := 0
			path, err := captureAutoInventory(root, func(args ...string) ([]byte, error) {
				calls++
				want := filepath.Join(root, "beads-generated-inventory.json")
				if len(args) != 7 || args[6] != want {
					t.Fatalf("capture destination=%q", args)
				}
				switch kind {
				case "symlink":
					if err := os.Symlink("absent", want); err != nil {
						t.Fatal(err)
					}
				case "directory":
					if err := os.Mkdir(want, 0700); err != nil {
						t.Fatal(err)
					}
				case "oversized":
					if err := os.WriteFile(want, make([]byte, (1<<20)+1), 0600); err != nil {
						t.Fatal(err)
					}
				}
				return nil, nil
			})
			if err == nil || path != "" || calls != 1 {
				t.Fatalf("unsafe producer output accepted: path=%q calls=%d err=%v", path, calls, err)
			}
			if _, err := os.Lstat(filepath.Join(root, "beads-inventory.json")); !os.IsNotExist(err) {
				t.Fatalf("unsafe producer output published a copy: %v", err)
			}
		})
	}
}
