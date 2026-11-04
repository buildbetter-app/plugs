package s1b01b04

import (
	"os"
	"path/filepath"
	"testing"
)

// These ordinary tests exercise input/export failure boundaries only. They
// never start a native executable, server, or claim a reference recording.
func TestNativeFutureIdentityInputs(t *testing.T) {
	var inputs nativeInputs
	nativeDecode(t, nativeInputsBytes, &inputs)
	if len(inputs.Vectors) != 2 || inputs.Consumer != "9346ab3f384d9994dda3f447ec3ffa6148803919" ||
		inputs.AdoptedIndefiniteDecision != "b0d8be0f-3bde-4dbc-b4c7-51cb125c7511" {
		t.Fatal("future inputs lost the exact consumer or deviation binding")
	}
	seenIDs, databases, modes := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, vector := range inputs.Vectors {
		if vector.ID != "B01-parent-"+vector.Mode || len(vector.Runs) != 2 || modes[vector.Mode] || len(vector.SourceIDs) != 4 {
			t.Fatal("future inputs lost distinct vectors/twins/roles")
		}
		modes[vector.Mode] = true
		for _, role := range nativeRoles {
			id := vector.SourceIDs[role]
			if !nativeSafeSuffix(id) || id != "b01-"+vector.Mode+role || seenIDs[id] {
				t.Fatalf("refused or ambiguous native input identity %s", id)
			}
			seenIDs[id] = true
		}
		for _, run := range vector.Runs {
			if run.Database == "" || databases[run.Database] {
				t.Fatal("fresh stores must use distinct database names")
			}
			databases[run.Database] = true
		}
		if vector.Mode == "timed" && (vector.Until == nil || *vector.Until != "2099-01-01") ||
			vector.Mode == "indefinite" && vector.Until != nil {
			t.Fatal("timed/indefinite inputs are conflated")
		}
	}
	if !modes["timed"] || !modes["indefinite"] {
		t.Fatal("both native deferral modes are mandatory")
	}
	if inputs.ExternalHierarchyTarget != "external:b01-other:unresolved" ||
		inputs.ExternalHierarchyRefusal != "external capability dependencies cannot use parent-child edges" || len(inputs.HierarchyPhases) != 6 {
		t.Fatal("hierarchy continuation requires its exact external identity, refusal and six native phases")
	}
	for _, refused := range []string{"b01-timed-parent", "b01-indefinite-child", "b01-", "other-parent", "b01-a.b"} {
		if nativeSafeSuffix(refused) {
			t.Fatalf("old refused source identity was accepted: %s", refused)
		}
	}
}

func TestNativeOutputInventoryRetainsSidecarsEmptyAndAbsent(t *testing.T) {
	root := t.TempDir()
	sidecar := filepath.Join(root, "inventory.json.sources")
	if got := nativeOutputState(sidecar)["state"]; got != "absent" {
		t.Fatalf("absent sidecar state=%v", got)
	}
	if err := os.Mkdir(sidecar, 0o700); err != nil {
		t.Fatal(err)
	}
	if got := nativeOutputState(sidecar)["state"]; got != "present-empty" {
		t.Fatalf("empty sidecar state=%v", got)
	}
	config := []byte("original public config\n")
	nativeWrite(t, filepath.Join(sidecar, "config.json"), config)
	nativeWrite(t, filepath.Join(sidecar, "empty.json"), nil)
	// A non-readable/non-file poll path must be excluded by name. No stat or
	// target read is needed for a skipped link to an absent foreign path.
	if err := os.Symlink(filepath.Join(root, "absent-foreign"), filepath.Join(sidecar, "poll.log")); err != nil {
		t.Fatal(err)
	}
	entries, err := nativeInventory(root)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]map[string]any{}
	for _, entry := range entries {
		got[entry["path"].(string)] = entry
	}
	if len(got) != 3 || got["inventory.json.sources"]["type"] != "directory" ||
		got["inventory.json.sources/config.json"]["sha256"] != sha256Hex(config) ||
		got["inventory.json.sources/empty.json"]["sha256"] != sha256Hex(nil) {
		t.Fatalf("sidecar originals/types/hashes were lost: %+v", entries)
	}
	if got := nativeOutputState(filepath.Join(sidecar, "empty.json"))["state"]; got != "present-empty" {
		t.Fatalf("empty-file state=%v", got)
	}
}
