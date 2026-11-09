package s1b01b04

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeEvidenceArchiveRetainsOriginalTree(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "original")
	for _, dir := range []string{"sidecars", "empty-directory"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	want := map[string][]byte{
		"sidecars/observed.bin": {0, 1, 2, 255},
		"empty.json":            nil,
		"output-inventory.json": []byte("original inventory\n"),
		"SHA256SUMS":            []byte("original sums\n"),
	}
	for name, data := range want {
		nativeWrite(t, filepath.Join(root, name), data)
	}
	foreign := filepath.Join(base, "foreign.bin")
	nativeWrite(t, foreign, []byte("outside original\n"))
	if err := os.Symlink(foreign, filepath.Join(root, "foreign-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(base, "absent"), filepath.Join(root, "poll.log")); err != nil {
		t.Fatal(err)
	}
	inventory, err := nativeInventory(root)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := nativePackEvidence(root, inventory)
	if err != nil || !receipt.Verified || !receipt.OriginalRootRemoved || receipt.RegularFiles != len(want) {
		t.Fatalf("archive did not retain and verify originals: %+v %v", receipt, err)
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatalf("loose evidence remained after verified archive: %v", err)
	}
	if data, err := os.ReadFile(foreign); err != nil || string(data) != "outside original\n" {
		t.Fatalf("archiving followed or changed the outside link: %q %v", data, err)
	}
	file, err := os.Open(receipt.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader := tar.NewReader(file)
	seen := map[string]bool{}
	emptyDirectory, link := false, false
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "poll.log" || seen[header.Name] {
			t.Fatalf("archive admitted an excluded or duplicate path: %s", header.Name)
		}
		seen[header.Name] = true
		if data, exists := want[header.Name]; exists {
			actual, err := io.ReadAll(reader)
			if err != nil || header.Typeflag != tar.TypeReg || !bytes.Equal(actual, data) || header.Mode != 0o600 {
				t.Fatalf("archive changed original bytes/type/mode at %s", header.Name)
			}
			delete(want, header.Name)
		} else if header.Name == "empty-directory" {
			emptyDirectory = header.Typeflag == tar.TypeDir && header.Mode == 0o700
		} else if header.Name == "foreign-link" {
			link = header.Typeflag == tar.TypeSymlink && header.Linkname == foreign
		} else if header.Name != "sidecars" || header.Typeflag != tar.TypeDir {
			t.Fatalf("unexpected archive entry: %+v", header)
		}
	}
	if len(want) != 0 || !emptyDirectory || !link || len(seen) != receipt.Entries {
		t.Fatalf("archive omitted original entries: missing=%v directory=%t link=%t count=%d/%d", want, emptyDirectory, link, len(seen), receipt.Entries)
	}
	if digest, size, err := nativeDigestFile(receipt.Path); err != nil || digest != receipt.SHA256 || size != receipt.Bytes {
		t.Fatalf("archive receipt differs from its actual bytes: %v", err)
	}
}

func TestNativeEvidenceArchiveRefusesExistingDestination(t *testing.T) {
	root := filepath.Join(t.TempDir(), "original")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	nativeWrite(t, filepath.Join(root, "record.json"), []byte("original\n"))
	nativeWrite(t, root+".tar", []byte("existing destination\n"))
	inventory, err := nativeInventory(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nativePackEvidence(root, inventory); err == nil {
		t.Fatal("archive replaced an existing destination")
	}
	for name, want := range map[string]string{filepath.Join(root, "record.json"): "original\n", root + ".tar": "existing destination\n"} {
		if data, err := os.ReadFile(name); err != nil || string(data) != want {
			t.Fatalf("archive failure changed retained original %s: %q %v", name, data, err)
		}
	}
}

func TestNativeEvidenceArchiveRefusesChangedOriginal(t *testing.T) {
	root := filepath.Join(t.TempDir(), "original")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "record.json")
	nativeWrite(t, path, []byte("first original\n"))
	inventory, err := nativeInventory(root)
	if err != nil {
		t.Fatal(err)
	}
	nativeWrite(t, path, []byte("changed original\n"))
	if _, err := nativePackEvidence(root, inventory); err == nil {
		t.Fatal("archive admitted a source change after the final inventory")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "changed original\n" {
		t.Fatalf("failed archive removed or rewrote retained evidence: %q %v", data, err)
	}
	if _, err := os.Lstat(root + ".tar"); !os.IsNotExist(err) {
		t.Fatalf("failed inventory comparison published an archive: %v", err)
	}
}

func TestNativeEvidenceArchiveVerificationRefusesChangedMembers(t *testing.T) {
	for _, change := range []string{"payload", "missing", "extra", "duplicate", "path", "mode"} {
		t.Run(change, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "original")
			if err := os.Mkdir(root, 0o700); err != nil {
				t.Fatal(err)
			}
			nativeWrite(t, filepath.Join(root, "record.json"), []byte("original\n"))
			entries, err := nativeInventory(root)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.Create(root + ".tar")
			if err != nil {
				t.Fatal(err)
			}
			writer := tar.NewWriter(file)
			name, payload, mode := "record.json", "original\n", int64(0o600)
			if change == "path" {
				name = "../record.json"
			} else if change == "payload" {
				payload = "modified\n"
			} else if change == "mode" {
				mode = 0o644
			}
			count := 1
			if change == "missing" {
				count = 0
			} else if change == "extra" || change == "duplicate" {
				count = 2
			}
			for i := 0; i < count; i++ {
				if i == 1 && change == "extra" {
					name = "extra.json"
				}
				if err := writer.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: int64(len(payload)), Typeflag: tar.TypeReg}); err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(writer, payload); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if err := nativeVerifyEvidenceArchive(root+".tar", entries); err == nil {
				t.Fatal("archive verifier admitted changed original members")
			}
			if data, err := os.ReadFile(filepath.Join(root, "record.json")); err != nil || string(data) != "original\n" {
				t.Fatalf("verification changed the retained original: %q %v", data, err)
			}
		})
	}
}

// These ordinary tests exercise input/export failure boundaries only. They
// never start a native executable, server, or claim a reference recording.
func TestNativeFutureIdentityInputs(t *testing.T) {
	var inputs nativeInputs
	nativeDecode(t, nativeInputsBytes, &inputs)
	if len(inputs.Vectors) != 2 || inputs.Consumer != "7a43c3c625a5c08d3a4364a321cd47d6357ef30e" ||
		inputs.ConsumerTree != "be93a595dc4c44ef4d94d2333060788fd344dca0" ||
		inputs.AdoptedIndefiniteDecision != "b0d8be0f-3bde-4dbc-b4c7-51cb125c7511" {
		t.Fatal("future inputs lost the exact consumer or deviation binding")
	}
	seenIDs, databases, modes := map[string]bool{}, map[string]bool{}, map[string]bool{}
	caseNames := map[string]bool{}
	for _, vector := range inputs.Vectors {
		if vector.ID != "B01-parent-"+vector.Mode || len(vector.Runs) != 2 || modes[vector.Mode] || len(vector.SourceIDs) != 4 {
			t.Fatal("future inputs lost distinct vectors/twins/roles")
		}
		modes[vector.Mode] = true
		name := nativeVectorTestName(vector)
		if name == "" || caseNames[name] {
			t.Fatal("native vectors do not have two distinct top-level shard cases")
		}
		caseNames[name] = true
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
		inputs.ExternalHierarchyRefusal != "external capability dependencies cannot use parent-child edges" || len(inputs.HierarchyPhases) != 9 {
		t.Fatal("hierarchy continuation requires its exact external identity, refusal and nine native phases")
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
