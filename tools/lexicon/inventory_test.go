package lexicon

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestSourceManifestRequiresCompleteTree(t *testing.T) {
	for _, added := range []string{
		"README.go", ".hidden.md", "docs/guide.md", "new/main.go",
		"new/BUILD.bazel", "internal/nested/BUILD", "cmd/plug/nested/main.go",
		".hidden/BUILD.bazel", "tools/.hidden/readme.md", "internal/source_/note.md",
	} {
		t.Run(added, func(t *testing.T) {
			files := map[string][]byte{
				"BUILD.bazel":                       []byte("# root\n"),
				"internal/doc.go":                   []byte("package internal\n"),
				"tools/lexicon/source_manifest.txt": []byte("BUILD.bazel\ninternal/doc.go\ntools/lexicon/source_manifest.txt\n"),
			}
			if err := CheckSourceManifest(files); err != nil {
				t.Fatalf("complete inventory rejected: %v", err)
			}
			files[added] = []byte("new source\n")
			if err := CheckSourceManifest(files); err == nil || !strings.Contains(err.Error(), added) || !strings.Contains(err.Error(), "extra=") {
				t.Fatalf("unlisted path was not identified: %v", err)
			}
		})
	}
}

func TestSourceManifestRejectsMissingAndMalformedEntries(t *testing.T) {
	for name, manifest := range map[string]string{
		"missing":   "missing.md\ntools/lexicon/source_manifest.txt\n",
		"duplicate": "tools/lexicon/source_manifest.txt\ntools/lexicon/source_manifest.txt\n",
		"parent":    "../escape\n",
		"absolute":  "/escape\n",
		"unclean":   "dir/../escape\n",
		"windows":   "dir\\escape\n",
		"metadata":  ".git/config\n",
		"empty":     "",
	} {
		t.Run(name, func(t *testing.T) {
			files := map[string][]byte{"tools/lexicon/source_manifest.txt": []byte(manifest)}
			if err := CheckSourceManifest(files); err == nil {
				t.Fatal("invalid inventory was accepted")
			}
		})
	}
	if err := CheckSourceManifest(map[string][]byte{}); err == nil {
		t.Fatal("missing manifest was accepted")
	}
}

func writeSourceInventory(t *testing.T, files map[string][]byte) string {
	t.Helper()
	paths := []string{"tools/lexicon/source_manifest.txt"}
	for name := range files {
		paths = append(paths, name)
	}
	sort.Strings(paths)
	files["tools/lexicon/source_manifest.txt"] = []byte(strings.Join(paths, "\n") + "\n")
	return writeSourceInventoryFiles(t, files)
}

func writeSourceInventoryFiles(t *testing.T, files map[string][]byte) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "files"), 0o755); err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 0, len(files))
	for name := range files {
		paths = append(paths, name)
	}
	sort.Strings(paths)
	entries := make([]map[string]string, 0, len(paths))
	for i, name := range paths {
		file := "files/" + strconv.Itoa(i)
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(file)), files[name], 0o644); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, map[string]string{"path": name, "file": file})
	}
	content, err := json.Marshal(map[string]any{"version": 1, "files": entries})
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "source_inventory.json")
	if err := os.WriteFile(manifestPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return manifestPath
}

func fragmentedSourceManifest() map[string][]byte {
	return map[string][]byte{
		sourceManifestPath:                       []byte(sourceManifestDirectory + "/cardb20.txt\n" + sourceManifestDirectory + "/wire.txt\n" + sourceManifestPath + "\n"),
		sourceManifestDirectory + "/cardb20.txt": []byte("spec/cardb20/cases.go\n"),
		sourceManifestDirectory + "/wire.txt":    []byte("internal/api/wire/contract.go\n"),
		"spec/cardb20/cases.go":                  []byte("package cardb20\n"),
		"internal/api/wire/contract.go":          []byte("package wire\n"),
	}
}

func TestSourceManifestFragmentsRequireCompleteTree(t *testing.T) {
	for name, test := range map[string]struct {
		mutate func(map[string][]byte)
		want   string
	}{
		"complete": {func(map[string][]byte) {}, ""},
		"extra_source": {func(files map[string][]byte) {
			files["internal/api/wire/new/BUILD.bazel"] = []byte("# new package\n")
		}, "extra=[internal/api/wire/new/BUILD.bazel]"},
		"missing_source": {func(files map[string][]byte) {
			delete(files, "spec/cardb20/cases.go")
		}, "missing=[spec/cardb20/cases.go]"},
		"missing_fragment": {func(files map[string][]byte) {
			delete(files, sourceManifestDirectory+"/wire.txt")
		}, "missing=[" + sourceManifestDirectory + "/wire.txt]"},
		"unlisted_fragment": {func(files map[string][]byte) {
			files[sourceManifestDirectory+"/new.txt"] = []byte(sourceManifestDirectory + "/new.txt\n")
		}, "unlisted source inventory fragment"},
	} {
		t.Run(name, func(t *testing.T) {
			files := fragmentedSourceManifest()
			test.mutate(files)
			err := CheckSourceManifest(files)
			if test.want == "" {
				if err != nil {
					t.Fatalf("complete fragmented inventory rejected: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want %q", err, test.want)
			}
		})
	}
}

func TestSourceManifestFragmentsRejectGlobalDuplicates(t *testing.T) {
	for _, name := range []string{"base_fragment", "two_fragments", "same_fragment"} {
		t.Run(name, func(t *testing.T) {
			files := fragmentedSourceManifest()
			var target string
			switch name {
			case "base_fragment":
				target = sourceManifestPath
			case "two_fragments":
				target = sourceManifestDirectory + "/cardb20.txt"
			case "same_fragment":
				target = sourceManifestDirectory + "/wire.txt"
			}
			files[target] = append(files[target], []byte("internal/api/wire/contract.go\n")...)
			if err := CheckSourceManifest(files); err == nil || !strings.Contains(err.Error(), "more than once") {
				t.Fatalf("duplicate membership accepted: %v", err)
			}
		})
	}
}

func TestSourceManifestFragmentsRejectMalformedEntries(t *testing.T) {
	for name, content := range map[string]string{
		"empty": "", "utf8": "\xff\n", "parent": "../escape\n",
		"absolute": "/escape\n", "unclean": "dir/../escape\n", "windows": "dir\\escape\n",
		"metadata": ".git/config\n", "whitespace": " source.go\n", "control": "source\t.go\n",
	} {
		t.Run(name, func(t *testing.T) {
			files := fragmentedSourceManifest()
			files[sourceManifestDirectory+"/wire.txt"] = []byte(content)
			if err := CheckSourceManifest(files); err == nil || !strings.Contains(err.Error(), sourceManifestDirectory+"/wire.txt") {
				t.Fatalf("malformed fragment accepted: %v", err)
			}
		})
	}
	for _, name := range []string{"nested/area.txt", "area.json"} {
		t.Run(name, func(t *testing.T) {
			files := fragmentedSourceManifest()
			filename := sourceManifestDirectory + "/" + name
			files[filename] = []byte("source.go\n")
			files[sourceManifestPath] = append(files[sourceManifestPath], []byte(filename+"\n")...)
			if err := CheckSourceManifest(files); err == nil || !strings.Contains(err.Error(), "invalid source inventory fragment path") {
				t.Fatalf("invalid fragment path accepted: %v", err)
			}
		})
	}
}

func TestSourceInventoryScansFragmentMembers(t *testing.T) {
	files := fragmentedSourceManifest()
	identifier := string([]byte{98, 100})
	files[rulesDirectory+"/rules.toml"] = []byte("[[term]]\nmatch = [" + strconv.Quote(identifier) + "]\nclass = \"identifier\"\n")
	files[sourceManifestPath] = append(files[sourceManifestPath], []byte(rulesDirectory+"/rules.toml\n")...)
	for _, name := range []string{"internal/api/wire/contract.go", "spec/cardb20/cases.go"} {
		files[name] = []byte("package source\nconst " + identifier + " = 1\n")
	}
	findings, err := CheckSourceInventory(writeSourceInventoryFiles(t, files))
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, finding := range findings {
		paths = append(paths, finding.Path)
	}
	want := "internal/api/wire/contract.go,spec/cardb20/cases.go"
	if strings.Join(paths, ",") != want {
		t.Fatalf("fragment membership suppressed content checks: got %v, want %s", paths, want)
	}
}

func TestSourceInventoryScansListedPackagesDocsAndHiddenPaths(t *testing.T) {
	identifier := string([]byte{98, 100})
	productTerm := string([]byte{119, 105, 115, 112})
	config := "[[term]]\nmatch = [" + strconv.Quote(identifier) + "]\nclass = \"identifier\"\n" +
		"[[term]]\nmatch = [" + strconv.Quote(productTerm) + "]\nclass = \"product-term\"\n" +
		"[zones]\nprovenance = [\"NOTICE.md\"]\n"
	manifest := writeSourceInventory(t, map[string][]byte{
		rulesDirectory + "/rules.toml": []byte(config),
		"new/BUILD.bazel":              []byte("# package\n"),
		"new/main.go":                  []byte("package main\nconst inherited = " + strconv.Quote(identifier) + "\n"),
		"docs/nested/guide.md":         []byte(productTerm + "\n"),
		".hidden/note.md":              []byte(productTerm + "\n"),
		"internal/source_/note.md":     []byte(productTerm + "\n"),
	})
	findings, err := CheckSourceInventory(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, finding := range findings {
		paths = append(paths, finding.Path)
	}
	want := ".hidden/note.md,docs/nested/guide.md,internal/source_/note.md,new/main.go"
	if strings.Join(paths, ",") != want {
		t.Fatalf("got findings for %v, want %s", paths, want)
	}
}

func TestSourceInventoryFailsClosedOnInvalidMapping(t *testing.T) {
	for name, test := range map[string]struct{ content, want string }{
		"empty":       {`{"version":1,"files":[]}`, "no files"},
		"version":     {`{"version":2,"files":[]}`, "unsupported version"},
		"duplicate":   {`{"version":1,"files":[{"path":"a","file":"files/0"},{"path":"a","file":"files/1"}]}`, "duplicate source inventory mapping"},
		"alias":       {`{"version":1,"files":[{"path":"a","file":"files/0"},{"path":"b","file":"files/0"}]}`, "duplicate source inventory mapping"},
		"escape":      {`{"version":1,"files":[{"path":"a","file":"../escape"}]}`, "invalid source inventory mapping"},
		"source_path": {`{"version":1,"files":[{"path":"../escape","file":"files/0"}]}`, "invalid source inventory mapping"},
		"missing":     {`{"version":1,"files":[{"path":"a","file":"files/0"}]}`, "read source a"},
		"unknown":     {`{"version":1,"files":[],"ignored":"value"}`, "unknown field"},
		"trailing":    {`{"version":1,"files":[]} {}`, "trailing data"},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if name != "missing" {
				if err := os.Mkdir(filepath.Join(root, "files"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "files/0"), []byte("source\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			manifest := filepath.Join(root, "source_inventory.json")
			if err := os.WriteFile(manifest, []byte(test.content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := CheckSourceInventory(manifest); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want %q", err, test.want)
			}
		})
	}
}
