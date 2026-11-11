package lexicon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const sourceManifestPath = "tools/lexicon/source_manifest.txt"
const sourceManifestDirectory = "tools/lexicon/sources.d"

func validSourcePath(name string) bool {
	return name != "" && name != "." && name != ".." &&
		path.Clean(name) == name && !strings.HasPrefix(name, "/") &&
		!strings.HasPrefix(name, "../") && !strings.Contains(name, "\\") &&
		name != ".git" && !strings.HasPrefix(name, ".git/") && utf8.ValidString(name) &&
		strings.IndexFunc(name, unicode.IsControl) < 0 && strings.TrimSpace(name) == name
}

func addSourceManifest(expected map[string]struct{}, filename string, manifest []byte) error {
	if !utf8.Valid(manifest) {
		return fmt.Errorf("source inventory manifest %s is not valid UTF-8", filename)
	}
	entries := 0
	for i, name := range strings.Split(string(manifest), "\n") {
		if name == "" {
			continue
		}
		if !validSourcePath(name) {
			return fmt.Errorf("source inventory %s line %d is not a canonical source path: %q", filename, i+1, name)
		}
		if _, duplicate := expected[name]; duplicate {
			return fmt.Errorf("source inventory lists %q more than once", name)
		}
		expected[name] = struct{}{}
		entries++
	}
	if entries == 0 {
		return fmt.Errorf("source inventory manifest %s is empty", filename)
	}
	return nil
}

// CheckSourceManifest requires an exact reviewed inventory of repository files.
// The supplied map must come from the complete checkout inventory, not a Bazel
// package glob, since globs cannot see through new package boundaries. Area
// manifests extend membership only; all listed contents are still scanned.
func CheckSourceManifest(files map[string][]byte) error {
	manifest, ok := files[sourceManifestPath]
	if !ok {
		return fmt.Errorf("missing source inventory manifest %s", sourceManifestPath)
	}
	expected := make(map[string]struct{})
	if err := addSourceManifest(expected, sourceManifestPath, manifest); err != nil {
		return err
	}
	var fragments []string
	for name := range files {
		if !strings.HasPrefix(name, sourceManifestDirectory+"/") {
			continue
		}
		base := strings.TrimPrefix(name, sourceManifestDirectory+"/")
		if strings.Contains(base, "/") || !strings.HasSuffix(base, ".txt") || !validSourcePath(name) {
			return fmt.Errorf("invalid source inventory fragment path %q", name)
		}
		// The base manifest must explicitly register each fragment. A fragment
		// cannot admit itself or silently introduce another manifest.
		if _, ok := expected[name]; !ok {
			return fmt.Errorf("unlisted source inventory fragment %q in %s", name, sourceManifestPath)
		}
		fragments = append(fragments, name)
	}
	sort.Strings(fragments)
	for _, name := range fragments {
		if err := addSourceManifest(expected, name, files[name]); err != nil {
			return err
		}
	}
	var missing, extra []string
	for name := range expected {
		if _, ok := files[name]; !ok {
			missing = append(missing, name)
		}
	}
	for name := range files {
		if !validSourcePath(name) {
			return fmt.Errorf("invalid source inventory path %q", name)
		}
		if _, ok := expected[name]; !ok {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) != 0 || len(extra) != 0 {
		return fmt.Errorf("source inventory mismatch (missing=%v extra=%v); review intentional additions/removals in %s and follow tools/lexicon/README.md", missing, extra, sourceManifestPath)
	}
	return nil
}

// CheckSourceInventory reads only declared Bazel runfiles. The repository rule
// supplies every checkout source as a flat file artifact plus a path mapping,
// so neither new Bazel packages nor hidden directories escape the tree check.
func CheckSourceInventory(manifestPath string) ([]Finding, error) {
	manifest, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read source inventory: %w", err)
	}
	defer manifest.Close()
	var inventory struct {
		Version int `json:"version"`
		Files   []struct {
			Path string `json:"path"`
			File string `json:"file"`
		} `json:"files"`
	}
	decoder := json.NewDecoder(manifest)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&inventory); err != nil {
		return nil, fmt.Errorf("decode source inventory: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("source inventory has trailing data")
	}
	if inventory.Version != 1 || len(inventory.Files) == 0 {
		return nil, errors.New("source inventory has an unsupported version or no files")
	}
	files := make(map[string][]byte, len(inventory.Files))
	artifacts := make(map[string]bool, len(inventory.Files))
	for _, entry := range inventory.Files {
		if !validSourcePath(entry.Path) || !validSourcePath(entry.File) || !strings.HasPrefix(entry.File, "files/") || strings.Count(entry.File, "/") != 1 {
			return nil, fmt.Errorf("invalid source inventory mapping %q -> %q", entry.Path, entry.File)
		}
		if _, duplicate := files[entry.Path]; duplicate || artifacts[entry.File] {
			return nil, fmt.Errorf("duplicate source inventory mapping %q -> %q", entry.Path, entry.File)
		}
		filename := filepath.Join(filepath.Dir(manifestPath), filepath.FromSlash(entry.File))
		content, err := os.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("read source %s: %w", entry.Path, err)
		}
		files[entry.Path] = content
		artifacts[entry.File] = true
	}
	if err := CheckSourceManifest(files); err != nil {
		return nil, err
	}
	cfg, err := configFromFiles(files)
	if err != nil {
		return nil, err
	}
	return CheckFilesWithPolicy(files, cfg, DefaultTreePolicy())
}
