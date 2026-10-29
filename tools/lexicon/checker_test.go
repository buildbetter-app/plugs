package lexicon

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func testConfig() Config {
	return Config{
		Terms: []Term{
			{Matches: []string{string([]byte{103, 97, 115}) + " " + string([]byte{116, 111, 119, 110})}, Class: "ban"},
			{Matches: []string{string([]byte{119, 105, 115, 112})}, Class: "product-term"},
			{Matches: []string{string([]byte{115, 116, 101, 118, 101, 121, 101, 103, 103, 101}) + "/" + string([]byte{98, 101, 97, 100, 115})}, Class: "upstream-infra"},
			{Matches: []string{string([]byte{98, 100})}, Class: "identifier"},
			{Pattern: `[\x{2600}-\x{27BF}]`, Class: "ban", Allow: map[rune]bool{rune(0x2713): true}},
		},
		Provenance: []string{"NOTICE.md", "engdocs/design/"},
	}
}

func TestCheckFilesBlocksTermsAndImports(t *testing.T) {
	cfg := testConfig()
	files := map[string][]byte{
		"main.go":   []byte("package main\nimport \"github.com/buildbetter-app/" + string([]byte{98, 101, 97, 100, 115}) + "\"\n"),
		"README.md": []byte("a " + string([]byte{119, 105, 115, 112}) + " and " + string([]byte{115, 116, 101, 118, 101, 121, 101, 103, 103, 101, 47, 98, 101, 97, 100, 115})),
		"NOTICE.md": []byte("a " + string([]byte{119, 105, 115, 112}) + " and " + string([]byte{115, 116, 101, 118, 101, 121, 101, 103, 103, 101, 47, 98, 101, 97, 100, 115})),
	}
	findings, err := CheckFiles(files, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 4 {
		t.Fatalf("got %d findings, want four: %#v", len(findings), findings)
	}
	if err := CheckError(findings); err == nil || !strings.Contains(err.Error(), "import") {
		t.Fatalf("blocking error did not include import finding: %v", err)
	}
}

func TestCheckFilesAllowsProseAndSanctionedGlyph(t *testing.T) {
	cfg := testConfig()
	files := map[string][]byte{
		"README.md": []byte(string([]byte{98, 100}) + " is ordinary prose " + string([]rune{0x2713})),
		"main.go":   []byte("package main\nconst label = \"ok ✓\"\n"),
	}
	findings, err := CheckFiles(files, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("got unexpected findings: %#v", findings)
	}
}

func TestCheckFilesBlocksIdentifierAndUnsanctionedGlyph(t *testing.T) {
	cfg := testConfig()
	files := map[string][]byte{
		"main.go": []byte("package main\nvar " + string([]byte{98, 100}) + " = 1\nconst x = " + strconv.Quote(string([]rune{0x2603})) + "\n"),
	}
	findings, err := CheckFiles(files, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 2 {
		t.Fatalf("got %d findings, want two: %#v", len(findings), findings)
	}
}

func TestCheckFilesFailsClosedOnInvalidUTF8(t *testing.T) {
	if _, err := CheckFiles(map[string][]byte{"bad.txt": {0xff}}, testConfig()); err == nil {
		t.Fatal("invalid UTF-8 was accepted")
	}
}

func TestCheckTreeFailsClosedOutsideGit(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "engdocs/style")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "lexicon.toml"), []byte("[[term]]\nmatch = [\"x\"]\nclass = \"ban\"\n[zones]\nprovenance = [\"NOTICE.md\"]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckTree(root); err == nil {
		t.Fatal("a non-repository root was accepted")
	}
}

func TestPolicyProtectsDeclaredPathsAndRejectsUnknownProtectedBinary(t *testing.T) {
	content := []byte("legacy vocabulary")
	digest := sha256.Sum256(content)
	policy := TreePolicy{
		ProtectedPrefixes:       []string{".ci/"},
		StrictProtectedPrefixes: []string{".ci/"},
		ProtectedPaths:          map[string]struct{}{".ci/known.txt": {}},
		BinarySHA256:            map[string]string{".ci/known.bin": hex.EncodeToString(digest[:])},
	}
	cfg := testConfig()
	findings, err := CheckFilesWithPolicy(map[string][]byte{
		".ci/known.txt": []byte{119, 105, 115, 112},
	}, cfg, policy)
	if err != nil || len(findings) != 0 {
		t.Fatalf("declared protected text was not skipped: findings=%#v err=%v", findings, err)
	}
	if _, err := CheckFilesWithPolicy(map[string][]byte{
		".ci/unknown.bin": {0xff},
	}, cfg, policy); err == nil || !strings.Contains(err.Error(), "unlisted protected path") {
		t.Fatalf("unknown protected path did not fail closed: %v", err)
	}
	if _, err := CheckFilesWithPolicy(map[string][]byte{
		".ci/known.bin": []byte("different"),
	}, cfg, policy); err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("binary hash mismatch did not fail closed: %v", err)
	}
}

func TestPolicyAcceptsOnlyDeclaredBinaryBytes(t *testing.T) {
	content := []byte("legacy vocabulary")
	digest := sha256.Sum256(content)
	policy := TreePolicy{
		ProtectedPrefixes:       []string{".ci/"},
		StrictProtectedPrefixes: []string{".ci/"},
		ProtectedPaths:          map[string]struct{}{".ci/known.bin": {}},
		BinarySHA256:            map[string]string{".ci/known.bin": hex.EncodeToString(digest[:])},
	}
	findings, err := CheckFilesWithPolicy(map[string][]byte{".ci/known.bin": content}, testConfig(), policy)
	if err != nil || len(findings) != 0 {
		t.Fatalf("declared binary was not accepted: findings=%#v err=%v", findings, err)
	}
}

func TestPolicyRejectsInvalidUTF8OutsideDeclaredBinary(t *testing.T) {
	policy := TreePolicy{}
	if _, err := CheckFilesWithPolicy(map[string][]byte{"README.md": {0xff}}, testConfig(), policy); err == nil {
		t.Fatal("invalid UTF-8 outside a declared binary was accepted")
	}
}
