package lexicon

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestB24RecordedTimerReferencesAdmitOnlyExactBytes(t *testing.T) {
	if *undoSourceInventory == "" {
		t.Fatal("missing declared reference inventory")
	}
	name := filepath.Join(os.Getenv("TEST_SRCDIR"), filepath.FromSlash(*undoSourceInventory))
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var inventory struct{ Files []struct{ Path, File string } }
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Fatal(err)
	}
	policy := DefaultTreePolicy()
	references := b24RecordedTimerReferences()
	if len(references) != 68 {
		t.Fatal("recorded timer reference census changed")
	}
	seen := 0
	for _, entry := range inventory.Files {
		if !strings.HasPrefix(entry.Path, "spec/cardb24/testdata/recorded-timers/") {
			continue
		}
		seen++
		content, err := os.ReadFile(filepath.Join(filepath.Dir(name), filepath.FromSlash(entry.File)))
		if err != nil {
			t.Fatal(err)
		}
		t.Run(entry.Path, func(t *testing.T) {
			if disposition, err := policy.disposition(entry.Path, content); err != nil || disposition != dispositionReference {
				t.Fatalf("exact recorded input refused: %v", err)
			}
			changed := append(bytes.Clone(content), '\n')
			if _, err := policy.disposition(entry.Path, changed); err == nil {
				t.Fatal("changed recorded bytes were admitted")
			}
		})
	}
	if seen != len(references) {
		t.Fatalf("declared reference census=%d want=%d", seen, len(references))
	}
	if _, err := policy.disposition("spec/cardb24/testdata/recorded-timers/unlisted.json", nil); err == nil {
		t.Fatal("unlisted reference sibling was admitted")
	}
	if disposition, err := policy.disposition("spec/cardb24/recorded_timer_test.go", nil); err != nil || disposition != dispositionScan {
		t.Fatal("consumer validator stopped receiving ordinary vocabulary checks")
	}
}
