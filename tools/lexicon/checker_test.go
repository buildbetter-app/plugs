package lexicon

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestSplitWordsWithOffsetsPreservesBoundaries(t *testing.T) {
	for _, test := range []struct {
		text   string
		words  []string
		starts []int
	}{
		{"", nil, nil},
		{"HTTPServer fooBar2_ABC", []string{"http", "server", "foo", "bar2", "abc"}, []int{0, 4, 11, 14, 19}},
		{"fooBARBaz", []string{"foo", "bar", "baz"}, []int{0, 3, 6}},
		{"v2Token", []string{"v2token"}, []int{0}},
		{"éÉlan\n世界!尾", []string{"é", "élan", "世界", "尾"}, []int{0, 2, 8, 15}},
		{"İFoo caféHTTPServer123", []string{"i", "foo", "café", "http", "server123"}, []int{0, 2, 6, 11, 15}},
		{"... one\ttwo\n", []string{"one", "two"}, []int{4, 8}},
	} {
		t.Run(test.text, func(t *testing.T) {
			words, starts := splitWordsWithOffsets(test.text)
			if !reflect.DeepEqual(words, test.words) || !reflect.DeepEqual(starts, test.starts) {
				t.Fatalf("got words=%q starts=%v, want words=%q starts=%v", words, starts, test.words, test.starts)
			}
		})
	}
	text := "世界\nHTTPServer suffix"
	if at := phraseIndex(text, "http server"); at != len("世界\n") || lineAt(text, at) != 2 {
		t.Fatalf("phrase byte offset/line changed: %d", at)
	}
	for _, test := range []struct {
		text, phrase string
		want         int
	}{
		{"İstanbul", "istanbul", 0},
		{"ıstanbul", "istanbul", -1},
		{"Σ", "ς", 0},
		{"α\nfirst_second THIRD first.second", "first second", len("α\n")},
		{"left,\nright", "left right", 0},
	} {
		if got := phraseIndex(test.text, test.phrase); got != test.want {
			t.Fatalf("phraseIndex(%q, %q) = %d, want %d", test.text, test.phrase, got, test.want)
		}
	}
}

func TestPhraseIndexManyWordsAbsentAndLateMatch(t *testing.T) {
	prefix := strings.Repeat("alphaBeta 世界\n", 4096)
	text := prefix + "late_match late match"
	if at := phraseIndex(text, "missing pair"); at != -1 {
		t.Fatalf("absent phrase matched at %d", at)
	}
	if at := phraseIndex(text, "late match"); at != len(prefix) || lineAt(text, at) != 4097 {
		t.Fatalf("first late phrase has wrong byte offset/line: %d", at)
	}
}

// Growing-prefix conversions allocate quadratically even on this small fixture.
// A generous linear byte budget catches that regression without a wall-clock limit.
func TestSplitWordsWithOffsetsAllocationBudget(t *testing.T) {
	text := strings.Repeat("alphaBeta 世界 ", 1024)
	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			words, starts := splitWordsWithOffsets(text)
			if len(words) != 3*1024 || starts[len(starts)-1] != len(text)-len("世界 ") {
				b.Fatal("large input was not fully tokenized")
			}
		}
	})
	if result.N == 0 {
		t.Fatal("tokenization benchmark did not complete")
	}
	if allocated, budget := result.AllocedBytesPerOp(), int64(len(text)*64); allocated > budget {
		t.Fatalf("tokenizing %d bytes allocated %d bytes, linear budget %d", len(text), allocated, budget)
	}
}

func BenchmarkSplitWordsWithOffsetsLargeInventoryFile(b *testing.B) {
	text := strings.Repeat("alphaBeta 世界 ", 32*1024)
	b.SetBytes(int64(len(text)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		splitWordsWithOffsets(text)
	}
}

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

func checkedInExternalProtocolConfig(t *testing.T) (Config, string) {
	t.Helper()
	path := filepath.Join("..", "..", "engdocs", "style", "lexicon.toml")
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		path = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), "engdocs", "style", "lexicon.toml")
	}
	cfg, err := LoadConfig(path)
	if err != nil || cfg.ExternalProtocolString.Path != externalProtocolSource || cfg.ExternalProtocolString.Value == "" {
		t.Fatalf("checked-in external protocol allowance: %+v, %v", cfg.ExternalProtocolString, err)
	}
	return cfg, path
}

func TestExternalProtocolStringAllowsOnlyExactDecodedLiteral(t *testing.T) {
	cfg, _ := checkedInExternalProtocolConfig(t)
	value := cfg.ExternalProtocolString.Value
	for name, literal := range map[string]string{
		"quoted":  strconv.Quote(value),
		"raw":     "`" + value + "`",
		"escaped": `"\x42` + value[1:] + `"`,
	} {
		t.Run(name, func(t *testing.T) {
			source := "package recorder\nvar externalAssignment = " + literal + "\n"
			findings, err := CheckFiles(map[string][]byte{externalProtocolSource: []byte(source)}, cfg)
			if err != nil || len(findings) != 0 {
				t.Fatalf("exact decoded external assignment was rejected: %#v, %v", findings, err)
			}
		})
	}
}

func TestExternalProtocolStringKeepsOtherLexiconRefusals(t *testing.T) {
	cfg, _ := checkedInExternalProtocolConfig(t)
	value := cfg.ExternalProtocolString.Value
	assignment := strconv.Quote(value)
	productTerm := cfg.Terms[2].Matches[0]
	for _, test := range []struct {
		name, path, source, class string
	}{
		{"neighbor_path", "spec/s1-b25-b28/public_recording_other_test.go", "package recorder\nvar externalAssignment = " + assignment, "identifier"},
		{"wildcard_path", "spec/s1-b25-b28/*.go", "package recorder\nvar externalAssignment = " + assignment, "identifier"},
		{"different_value", externalProtocolSource, "package recorder\nvar externalAssignment = " + strconv.Quote(value+"-extra"), "identifier"},
		{"extra_term_in_literal", externalProtocolSource, "package recorder\nvar externalAssignment = " + strconv.Quote(value+" "+productTerm), "product-term"},
		{"other_term_in_file", externalProtocolSource, "package recorder\nvar externalAssignment = " + assignment + "\nvar other = " + strconv.Quote(productTerm), "product-term"},
		{"identifier", externalProtocolSource, "package recorder\nvar " + strings.SplitN(value, "=", 2)[0] + " = 1\nvar externalAssignment = " + assignment, "identifier"},
		{"import", externalProtocolSource, "package recorder\nimport " + assignment + "\nvar externalAssignment = " + assignment, "identifier"},
	} {
		t.Run(test.name, func(t *testing.T) {
			findings, err := CheckFiles(map[string][]byte{test.path: []byte(test.source)}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !hasFindingClass(findings, test.class) {
				t.Fatalf("%s did not retain %s refusal: %#v", test.name, test.class, findings)
			}
		})
	}
	without := cfg
	without.ExternalProtocolString = GoStringAllowance{}
	findings, err := CheckFiles(map[string][]byte{externalProtocolSource: []byte("package recorder\nvar externalAssignment = " + assignment)}, without)
	if err != nil || !hasFindingClass(findings, "identifier") {
		t.Fatalf("missing allowance admitted the external assignment: %#v, %v", findings, err)
	}
}

func hasFindingClass(findings []Finding, class string) bool {
	for _, finding := range findings {
		if finding.Class == class {
			return true
		}
	}
	return false
}

func TestExternalProtocolStringConfigurationRejectsBroadening(t *testing.T) {
	cfg, path := checkedInExternalProtocolConfig(t)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, config string }{
		{"neighbor_path", strings.Replace(string(original), cfg.ExternalProtocolString.Path, "spec/s1-b25-b28/public_recording_other_test.go", 1)},
		{"wildcard_path", strings.Replace(string(original), cfg.ExternalProtocolString.Path, "spec/s1-b25-b28/*.go", 1)},
		{"empty_value", strings.Replace(string(original), "value = "+strconv.Quote(cfg.ExternalProtocolString.Value), `value = ""`, 1)},
		{"duplicate_section", string(original) + "\n[external_protocol_string]\n"},
		{"duplicate_path", strings.Replace(string(original), "path = "+strconv.Quote(cfg.ExternalProtocolString.Path), "path = "+strconv.Quote(cfg.ExternalProtocolString.Path)+"\npath = "+strconv.Quote(cfg.ExternalProtocolString.Path), 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "lexicon.toml")
			if err := os.WriteFile(file, []byte(test.config), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(file); err == nil {
				t.Fatal("broadened or malformed external protocol allowance was accepted")
			}
		})
	}
}

func checkedInPhysicalProtocolConfig(t *testing.T) (Config, string) {
	t.Helper()
	cfg, path := checkedInExternalProtocolConfig(t)
	want := map[string]bool{
		"39d6e7ef1121f5ac563d8c278431a6a19f9d420534e9cbe38d2f9f41d9912502": true,
		"93602e05a1bff1bd127187c713caaeab338848eb4a6cba051123440a912cdf26": true,
		"91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75": true,
		"7853d42458d77d5bd188bd1a1bba51e87257cbe5ec12826168bc639f95bb0a72": true,
	}
	if len(cfg.ExternalProtocolString.PhysicalValues) != len(want) {
		t.Fatal("checked-in physical protocol literal set is incomplete")
	}
	for _, value := range cfg.ExternalProtocolString.PhysicalValues {
		digest := sha256.Sum256([]byte(value))
		key := hex.EncodeToString(digest[:])
		if !want[key] {
			t.Fatal("checked-in physical protocol literal changed or repeated")
		}
		delete(want, key)
	}
	return cfg, path
}

func escapedProtocolString(value string) string {
	var escaped strings.Builder
	escaped.WriteByte('"')
	for i := range value {
		fmt.Fprintf(&escaped, "\\x%02x", value[i])
	}
	escaped.WriteByte('"')
	return escaped.String()
}

func TestPhysicalProtocolStringsAllowOnlyExactDecodedLiterals(t *testing.T) {
	cfg, _ := checkedInPhysicalProtocolConfig(t)
	for i, value := range cfg.ExternalProtocolString.PhysicalValues {
		for name, literal := range map[string]string{
			"quoted": strconv.Quote(value), "raw": "`" + value + "`", "escaped": escapedProtocolString(value),
		} {
			t.Run(fmt.Sprintf("literal%d/%s", i, name), func(t *testing.T) {
				source := "package recorder\nvar externalAssignment = " + literal + "\n"
				findings, err := CheckFiles(map[string][]byte{externalProtocolSource: []byte(source)}, cfg)
				if err != nil || len(findings) != 0 {
					t.Fatalf("exact decoded physical literal refused: %#v, %v", findings, err)
				}
			})
		}
	}
}

func TestPhysicalProtocolStringsKeepOtherRefusals(t *testing.T) {
	cfg, _ := checkedInPhysicalProtocolConfig(t)
	productTerm := cfg.Terms[2].Matches[0]
	identifier := strings.SplitN(cfg.ExternalProtocolString.Value, "=", 2)[0]
	for i, value := range cfg.ExternalProtocolString.PhysicalValues {
		assignment := "var externalAssignment = " + strconv.Quote(value)
		for _, test := range []struct{ name, path, source string }{
			{"neighbor_path", "spec/s1-b25-b28/public_recording_other_test.go", "package recorder\n" + assignment},
			{"wildcard_path", "spec/s1-b25-b28/*.go", "package recorder\n" + assignment},
			{"wrong_path_raw", "other/recorder.go", "package recorder\nvar externalAssignment = `" + value + "`"},
			{"changed_case", externalProtocolSource, "package recorder\nvar externalAssignment = " + strconv.Quote(strings.ToUpper(value))},
			{"prefix", externalProtocolSource, "package recorder\nvar externalAssignment = " + strconv.Quote("other_"+value)},
			{"suffix", externalProtocolSource, "package recorder\nvar externalAssignment = " + strconv.Quote(value+"_other")},
			{"space", externalProtocolSource, "package recorder\nvar externalAssignment = " + strconv.Quote(value+" ")},
			{"changed_escaped", externalProtocolSource, "package recorder\nvar externalAssignment = " + escapedProtocolString(value+"_other")},
			{"extra_term", externalProtocolSource, "package recorder\nvar externalAssignment = " + strconv.Quote(value+" "+productTerm)},
			{"other_literal", externalProtocolSource, "package recorder\n" + assignment + "\nvar other = " + strconv.Quote(productTerm)},
			{"comment", externalProtocolSource, "package recorder\n// " + productTerm + "\n" + assignment},
			{"identifier", externalProtocolSource, "package recorder\nvar " + identifier + " = 1\n" + assignment},
			{"import", externalProtocolSource, "package recorder\nimport " + strconv.Quote(value) + "\n" + assignment},
			{"import_escaped", externalProtocolSource, "package recorder\nimport " + escapedProtocolString(value) + "\n" + assignment},
		} {
			t.Run(fmt.Sprintf("literal%d/%s", i, test.name), func(t *testing.T) {
				findings, err := CheckFiles(map[string][]byte{test.path: []byte(test.source)}, cfg)
				if err != nil || !hasFindingClass(findings, "identifier") && !hasFindingClass(findings, "product-term") {
					t.Fatalf("physical literal allowance lost ordinary refusal: %#v, %v", findings, err)
				}
			})
		}
		without := cfg
		without.ExternalProtocolString.PhysicalValues = nil
		findings, err := CheckFiles(map[string][]byte{externalProtocolSource: []byte("package recorder\n" + assignment)}, without)
		if err != nil || len(findings) == 0 {
			t.Fatalf("missing physical literal declaration passed: %#v, %v", findings, err)
		}
	}
}

func TestPhysicalProtocolConfigurationRejectsBroadening(t *testing.T) {
	cfg, path := checkedInPhysicalProtocolConfig(t)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	values := cfg.ExternalProtocolString.PhysicalValues
	line := func(values []string) string {
		var quoted []string
		for _, value := range values {
			quoted = append(quoted, strconv.Quote(value))
		}
		return "physical_values = [" + strings.Join(quoted, ", ") + "]"
	}
	complete := line(values)
	if !strings.Contains(string(original), complete) {
		t.Fatal("configuration literal set was not bound")
	}
	for _, test := range []struct{ name, replacement string }{
		{"empty", line(nil)},
		{"missing", line(values[:len(values)-1])},
		{"duplicate", line([]string{values[0], values[1], values[2], values[2]})},
		{"additional", line(append(append([]string{}, values...), "other"))},
		{"mutated", line([]string{values[0] + "_other", values[1], values[2], values[3]})},
		{"pattern", line([]string{"*" + values[0] + "*", values[1], values[2], values[3]})},
		{"wrong_type", "physical_values = " + strconv.Quote(values[0])},
		{"unknown_key", strings.Replace(complete, "physical_values", "physical_prefix", 1)},
		{"repeated_key", complete + "\n" + complete},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "lexicon.toml")
			if err := os.WriteFile(file, []byte(strings.Replace(string(original), complete, test.replacement, 1)), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(file); err == nil {
				t.Fatal("malformed or broadened physical allowance passed")
			}
		})
	}
	file := filepath.Join(t.TempDir(), "lexicon.toml")
	changedEnvironment := strings.Replace(string(original), "value = "+strconv.Quote(cfg.ExternalProtocolString.Value), "value = "+strconv.Quote(cfg.ExternalProtocolString.Value+"_other"), 1)
	if err := os.WriteFile(file, []byte(changedEnvironment), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(file); err == nil {
		t.Fatal("environment allowance stopped binding its exact existing literal")
	}
}

func TestPhysicalProtocolAllowancePreservesUnrelatedEscapedLiterals(t *testing.T) {
	cfg, _ := checkedInPhysicalProtocolConfig(t)
	baseline := cfg
	baseline.ExternalProtocolString.PhysicalValues = nil
	literals := map[string]string{
		"identifier": escapedProtocolString(cfg.ExternalProtocolString.Value),
		"product":    escapedProtocolString(cfg.Terms[2].Matches[0]),
		"unicode":    `"\U0001f600"`,
	}
	for i, value := range cfg.ExternalProtocolString.PhysicalValues {
		literals[fmt.Sprintf("physical%d", i)] = escapedProtocolString(value)
	}
	for name, literal := range literals {
		t.Run(name, func(t *testing.T) {
			files := map[string][]byte{"other/existing.go": []byte("package existing\nvar retained = " + literal)}
			before, beforeErr := CheckFiles(files, baseline)
			after, afterErr := CheckFiles(files, cfg)
			if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("physical protocol allowance changed unrelated scan: before=%#v/%v after=%#v/%v", before, beforeErr, after, afterErr)
			}
		})
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

func TestCheckFilesScansNewRootReadmeFiles(t *testing.T) {
	identifier := string([]byte{98, 100})
	productTerm := string([]byte{119, 105, 115, 112})
	files := map[string][]byte{
		"README.go": []byte("package readme\nconst inherited = " + strconv.Quote(identifier) + "\n"),
		"README.md": []byte("retained " + productTerm + " vocabulary\n"),
	}
	findings, err := CheckFilesWithPolicy(files, testConfig(), TreePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 2 {
		t.Fatalf("got %d findings, want both root files checked: %#v", len(findings), findings)
	}
	if findings[0].Path != "README.go" || findings[1].Path != "README.md" {
		t.Fatalf("root findings were not ordered by path: %#v", findings)
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

func TestPolicyAdmitsOnlyTheDeclaredImmutableReferenceBytes(t *testing.T) {
	content := []byte("reference " + string([]byte{119, 105, 115, 112}) + " " + string([]byte{115, 116, 101, 118, 101, 121, 101, 103, 103, 101, 47, 98, 101, 97, 100, 115}))
	digest := sha256.Sum256(content)
	policy := TreePolicy{
		ReferencePrefixes: []string{"spec/recorded/"},
		ReferenceSHA256: map[string]string{
			"spec/recorded/goldens.json": hex.EncodeToString(digest[:]),
		},
	}
	findings, err := CheckFilesWithPolicy(map[string][]byte{
		"spec/recorded/goldens.json": content,
	}, testConfig(), policy)
	if err != nil || len(findings) != 0 {
		t.Fatalf("declared immutable reference was not admitted: findings=%#v err=%v", findings, err)
	}
	if _, err := CheckFilesWithPolicy(map[string][]byte{
		"spec/recorded/goldens.json": append(append([]byte(nil), content...), '\n'),
	}, testConfig(), policy); err == nil || !strings.Contains(err.Error(), "immutable reference") {
		t.Fatalf("changed immutable reference was accepted: %v", err)
	}
	if _, err := CheckFilesWithPolicy(map[string][]byte{
		"spec/recorded/other.json": content,
	}, testConfig(), policy); err == nil || !strings.Contains(err.Error(), "unlisted immutable reference path") {
		t.Fatalf("unlisted immutable reference path was accepted: %v", err)
	}
}

func TestPolicyStillRejectsProductTermsOutsideImmutableReferences(t *testing.T) {
	content := []byte("ordinary " + string([]byte{119, 105, 115, 112}) + " vocabulary")
	policy := TreePolicy{
		ReferencePrefixes: []string{"spec/recorded/"},
		ReferenceSHA256:   map[string]string{"spec/recorded/goldens.json": ""},
	}
	findings, err := CheckFilesWithPolicy(map[string][]byte{
		"README.md": content,
	}, testConfig(), policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Path != "README.md" || findings[0].Class != "product-term" {
		t.Fatalf("ordinary product term was not rejected: %#v", findings)
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
