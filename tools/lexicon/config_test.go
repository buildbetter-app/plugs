package lexicon

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Existing rejection matrices still mutate one complete document. Build that
// document from the real area files, preserving every original assertion.
func readConfigDocument(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return os.ReadFile(path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var data []byte
	for _, entry := range entries {
		part, err := os.ReadFile(filepath.Join(path, entry.Name()))
		if err != nil {
			return nil, err
		}
		data = append(data, part...)
		data = append(data, '\n')
	}
	return data, nil
}

func checkedInRuleFiles(t *testing.T) map[string][]byte {
	t.Helper()
	_, directory := checkedInExternalProtocolConfig(t)
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[rulesDirectory+"/"+entry.Name()] = data
	}
	return files
}

func TestAreaConfigDirectoryInventoryAndSingleDocumentAgree(t *testing.T) {
	directoryConfig, directory := checkedInExternalProtocolConfig(t)
	files := checkedInRuleFiles(t)
	inventoryConfig, err := configFromFiles(files)
	if err != nil || !reflect.DeepEqual(directoryConfig, inventoryConfig) {
		t.Fatalf("directory/inventory config mismatch: %v", err)
	}
	document, err := readConfigDocument(directory)
	if err != nil {
		t.Fatal(err)
	}
	single, err := parseConfigSources([]configSource{{name: "single.toml", data: document}}, true)
	if err != nil {
		t.Fatal(err)
	}
	directoryConfig.ruleSources = nil
	single.ruleSources = nil
	if !reflect.DeepEqual(directoryConfig, single) {
		t.Fatal("area loading changed the complete single-document rule data")
	}
	compiled := canonicalRules
	compiled.ruleSources = nil
	if !reflect.DeepEqual(directoryConfig, compiled) {
		t.Fatal("runtime rules differ from their compiled acceptance boundary")
	}
}

func TestAreaConfigSortsFilenamesAndCombinesScopedZones(t *testing.T) {
	files := map[string][]byte{
		rulesDirectory + "/90-last.toml":  []byte("[[term]]\nmatch = [\"last\"]\nclass = \"ban\"\n[zones]\nprovenance = [\"last/\"]\n"),
		rulesDirectory + "/10-first.toml": []byte("[[term]]\nmatch = [\"first\"]\nclass = \"ban\"\n[zones]\nprovenance = [\"first/\"]\n"),
	}
	cfg, err := configFromFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Terms) != 2 || cfg.Terms[0].Matches[0] != "first" || cfg.Terms[1].Matches[0] != "last" || !reflect.DeepEqual(cfg.Provenance, []string{"first/", "last/"}) {
		t.Fatalf("rules were not combined in filename order: %+v", cfg)
	}
}

func TestAreaConfigDoesNotContinueSectionsAcrossFiles(t *testing.T) {
	_, err := configFromFiles(map[string][]byte{
		rulesDirectory + "/10-first.toml": []byte("[[term]]\nmatch = [\"first\"]\nclass = \"ban\"\n[zones]\nprovenance = [\"NOTICE.md\"]\n"),
		rulesDirectory + "/90-last.toml":  []byte("provenance = [\"other/\"]\n"),
	})
	if err == nil || !strings.Contains(err.Error(), "outside a section") {
		t.Fatalf("cross-file section continuation was accepted: %v", err)
	}
}

func TestAreaConfigRejectsDuplicateCrossFileAllowances(t *testing.T) {
	files := checkedInRuleFiles(t)
	for _, area := range []string{"b13-native", "b21-b24-native", "b25-b28-public", "b01-native", "b03-native"} {
		t.Run(area, func(t *testing.T) {
			files[rulesDirectory+"/zz-duplicate.toml"] = files[rulesDirectory+"/"+area+".toml"]
			defer delete(files, rulesDirectory+"/zz-duplicate.toml")
			if _, err := configFromFiles(files); err == nil {
				t.Fatal("duplicate allowance in another area file was accepted")
			}
		})
	}
}

func TestAreaConfigRejectsMalformedRuleInputs(t *testing.T) {
	for _, name := range []string{"nested/extra.toml", "extra.txt", "../extra.toml"} {
		t.Run(name, func(t *testing.T) {
			files := checkedInRuleFiles(t)
			files[rulesDirectory+"/"+name] = []byte("# extra\n")
			if _, err := configFromFiles(files); err == nil {
				t.Fatal("invalid area rule path was accepted")
			}
		})
	}
	files := checkedInRuleFiles(t)
	files[rulesDirectory+"/invalid.toml"] = []byte{0xff}
	if _, err := configFromFiles(files); err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Fatalf("invalid rule encoding was accepted: %v", err)
	}
	if _, err := configFromFiles(map[string][]byte{}); err == nil {
		t.Fatal("missing rule files were accepted")
	}
}

func TestAreaConfigExemptsOnlyConsumedUnchangedRuleBytes(t *testing.T) {
	files := checkedInRuleFiles(t)
	cfg, err := configFromFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := CheckFiles(files, cfg)
	if err != nil || len(findings) != 0 {
		t.Fatalf("consumed configuration text was scanned as product text: %v, %v", findings, err)
	}
	extra := rulesDirectory + "/unconsumed.toml"
	files[extra] = []byte(string([]byte{119, 105, 115, 112}) + "\n")
	findings, err = CheckFiles(files, cfg)
	if err != nil || len(findings) != 1 || findings[0].Path != extra {
		t.Fatalf("unconsumed configuration-shaped path escaped scanning: %v, %v", findings, err)
	}
	delete(files, extra)
	for name, data := range files {
		files[name] = append(bytes.Clone(data), []byte("\n# changed\n")...)
		if _, err := CheckFiles(files, cfg); err == nil || !strings.Contains(err.Error(), "changed after loading") {
			t.Fatalf("changed rule bytes escaped the load binding: %v", err)
		}
		break
	}
}
