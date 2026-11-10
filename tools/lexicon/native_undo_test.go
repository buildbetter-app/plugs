package lexicon

import (
	"encoding/json"
	"flag"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var undoSourceInventory = flag.String("undo-source-inventory", "", "declared source inventory for native fixture boundary controls")

func undoBoundaryInputs(t *testing.T) map[string][]byte {
	t.Helper()
	if *undoSourceInventory == "" {
		t.Fatal("missing declared native fixture source inventory")
	}
	name := filepath.Join(os.Getenv("TEST_SRCDIR"), filepath.FromSlash(*undoSourceInventory))
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var inventory struct {
		Files []struct{ Path, File string }
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Fatal(err)
	}
	files := make(map[string][]byte)
	for _, entry := range inventory.Files {
		if len(undoLiteralBindings[entry.Path]) == 0 && !strings.HasPrefix(entry.Path, "internal/testutil/nativeschema/migrations/") && entry.Path != sourceManifestPath {
			continue
		}
		content, err := os.ReadFile(filepath.Join(filepath.Dir(name), filepath.FromSlash(entry.File)))
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Path] = content
	}
	return files
}

func TestUndoLiteralBindingsAdmitOnlyExactASTSites(t *testing.T) {
	files := undoBoundaryInputs(t)
	cfg, _ := checkedInExternalProtocolConfig(t)
	for path, bindings := range undoLiteralBindings {
		t.Run(path, func(t *testing.T) {
			content, ok := files[path]
			if !ok {
				t.Fatal("bound source missing from declared inputs")
			}
			expected := 0
			for _, binding := range bindings {
				expected += len(binding.literals)
			}
			offsets := undoLiteralOffsets(path, content)
			if len(offsets) != expected {
				t.Fatalf("actual AST admitted %d literals, want %d", len(offsets), expected)
			}
			findings, err := CheckFiles(map[string][]byte{path: content}, cfg)
			if err != nil || len(findings) != 0 {
				t.Fatalf("exact native fixture source refused: %#v %v", findings, err)
			}
			var first goStringLiteral
			for _, literal := range goStringLiterals(path, content) {
				if offsets[literal.start] {
					first = literal
					break
				}
			}
			if first.value == "" {
				t.Fatal("no actual bound string selected")
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, content, 0)
			if err != nil {
				t.Fatal(err)
			}
			var declaration string
			for _, node := range file.Decls {
				start, end := fset.Position(node.Pos()).Offset, fset.Position(node.End()).Offset
				if first.start >= start && first.end <= end {
					declaration = string(content[start:end])
				}
			}
			changed := string(content[:first.start]) + strconv.Quote(first.value+" changed") + string(content[first.end:])
			packageEnd := fset.Position(file.Name.End()).Offset
			imported := string(content[:packageEnd]) + "\nimport \"example.invalid/beads/adapter\"\n" + string(content[packageEnd:])
			for name, source := range map[string]string{
				"changed-value":         changed,
				"copied-diagnostic":     string(content) + "\nvar copiedBoundary = " + strconv.Quote(first.value),
				"duplicate-declaration": string(content) + "\n" + declaration,
				"identifier":            string(content) + "\nvar " + cfg.Terms[3].Matches[0] + " = 1\n",
				"import":                imported,
				"changed-context":       strings.Replace(string(content), bindings[0].name, bindings[0].name+"Changed", 1),
			} {
				t.Run(name, func(t *testing.T) {
					findings, err := CheckFiles(map[string][]byte{path: []byte(source)}, cfg)
					if err != nil || len(findings) == 0 {
						t.Fatalf("broadened native literal admission: %#v %v", findings, err)
					}
				})
			}
			for _, other := range []string{"other/fixture.go", strings.TrimSuffix(path, ".go") + "_sibling.go"} {
				findings, err := CheckFiles(map[string][]byte{other: content}, cfg)
				if err != nil || len(findings) == 0 {
					t.Fatalf("literal admitted at unrelated path %s: %#v %v", other, findings, err)
				}
			}
		})
	}
}

func TestUndoSQLReferenceCorpusIsExactAndClosed(t *testing.T) {
	files := undoBoundaryInputs(t)
	policy := DefaultTreePolicy()
	references := undoSQLReferences()
	if len(references) != 100 {
		t.Fatalf("native corpus has %d files, want 100", len(references))
	}
	paths := make([]string, 0, len(references))
	for path := range references {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			content, ok := files[path]
			if !ok {
				t.Fatal("immutable SQL source missing from declared inventory")
			}
			if disposition, err := policy.disposition(path, content); err != nil || disposition != dispositionReference {
				t.Fatalf("exact SQL refused: %v %v", disposition, err)
			}
			changed := append(append([]byte(nil), content...), '\n')
			if _, err := policy.disposition(path, changed); err == nil {
				t.Fatal("changed SQL bytes admitted")
			}
		})
	}
	for _, sibling := range []string{"new.up.sql", "nested/new.up.sql", "executable.go"} {
		if _, err := policy.disposition("internal/testutil/nativeschema/migrations/"+sibling, files[paths[0]]); err == nil {
			t.Fatalf("unlisted reference sibling admitted: %s", sibling)
		}
	}
}

func TestUndoReferenceManifestAdmitsOnlyDeclaredPaths(t *testing.T) {
	files := undoBoundaryInputs(t)
	cfg, _ := checkedInExternalProtocolConfig(t)
	content, ok := files[sourceManifestPath]
	if !ok {
		t.Fatal("source manifest absent from declared inventory")
	}
	findings, err := CheckFiles(map[string][]byte{sourceManifestPath: content}, cfg)
	if err != nil || len(findings) != 0 {
		t.Fatalf("canonical manifest entries refused: %#v %v", findings, err)
	}
	term := cfg.Terms[2].Matches[0]
	for name, body := range map[string]string{
		"new-entry":    string(content) + "\nother/" + term + ".go\n",
		"partial-line": "prefix/" + strings.TrimSpace(string(content)) + "/" + term,
	} {
		t.Run(name, func(t *testing.T) {
			findings, err := CheckFiles(map[string][]byte{sourceManifestPath: []byte(body)}, cfg)
			if err != nil || len(findings) == 0 {
				t.Fatalf("new manifest vocabulary admitted: %#v %v", findings, err)
			}
		})
	}
	findings, err = CheckFiles(map[string][]byte{"other/inventory.txt": content}, cfg)
	if err != nil || len(findings) == 0 {
		t.Fatalf("reference paths admitted outside the source manifest: %#v %v", findings, err)
	}
}
