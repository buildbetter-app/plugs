package lexicon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestNativeHTTPListenerAdmitsOnlyExactParsingSite(t *testing.T) {
	cfg, _ := checkedInExternalProtocolConfig(t)
	identifier := string([]byte{98, 100})
	productTerm := string([]byte{119, 105, 115, 112})
	value := identifier + " serve: listening on http://"
	source := func(literal string) string {
		return "package recorder\nimport \"strings\"\nfunc (r *recorder) styleADomainCreate() { _, _ = strings.CutPrefix(line, " + literal + ") }\n"
	}
	for name, quote := range map[string]func(string) string{"quoted": strconv.Quote, "raw": func(s string) string { return "`" + s + "`" }, "escaped": escapedProtocolString} {
		t.Run(name, func(t *testing.T) {
			findings, err := CheckFiles(map[string][]byte{nativeHTTPDomainSource: []byte(source(quote(value)))}, cfg)
			if err != nil || len(findings) != 0 {
				t.Fatalf("exact listener parsing literal refused: %#v %v", findings, err)
			}
		})
	}
	good := source(strconv.Quote(value))
	for name, body := range map[string]string{
		"changed-value":    source(strconv.Quote(value + "other")),
		"other-method":     strings.Replace(good, "styleADomainCreate", "other", 1),
		"other-receiver":   strings.Replace(good, "*recorder", "*other", 1),
		"other-call":       strings.Replace(good, "strings.CutPrefix", "strings.TrimPrefix", 1),
		"copy":             good + "var diagnostic = " + strconv.Quote(value),
		"quoted-source":    "package recorder\nvar diagnostic = " + strconv.Quote(good),
		"import":           "package recorder\nimport " + strconv.Quote(value),
		"duplicate-method": good + strings.TrimPrefix(good, "package recorder\nimport \"strings\"\n"),
		"duplicate-call":   strings.Replace(good, " }", "; _, _ = strings.CutPrefix(line, "+strconv.Quote(value)+") }", 1),
		"closure":          strings.Replace(good, "_, _ = strings.CutPrefix", "f := func() { _, _ = strings.CutPrefix", 1) + "}\n",
		"other-identifier": good + "var " + identifier + " = 1",
		"other-term":       good + "var diagnostic = " + strconv.Quote(productTerm),
	} {
		t.Run(name, func(t *testing.T) {
			findings, err := CheckFiles(map[string][]byte{nativeHTTPDomainSource: []byte(body)}, cfg)
			if err != nil || len(findings) == 0 {
				t.Fatalf("broadened listener site accepted: %#v %v", findings, err)
			}
		})
	}
	for _, path := range []string{"spec/s1-b09-native/other_test.go", "internal/core/other.go"} {
		findings, err := CheckFiles(map[string][]byte{path: []byte(good)}, cfg)
		if err != nil || len(findings) == 0 {
			t.Fatalf("listener literal admitted at another path %s: %#v %v", path, findings, err)
		}
	}
}

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
	path := filepath.Join("..", "..", filepath.FromSlash(rulesDirectory))
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		path = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), filepath.FromSlash(rulesDirectory))
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

func checkedInNativeFieldConfig(t *testing.T) (Config, string) {
	t.Helper()
	cfg, path := checkedInNativeManifestConfig(t)
	want := map[string]struct {
		path, field string
		count       int
	}{
		"ready-types":              {b01NativeGoSource, "nativeExclusionTypes.return", 9},
		"custom-types":             {b01NativeGoSource, "recordNativeExclusions.c.run.types.custom", 5},
		"native-type-binding-json": {b01NativeJSONSource, "nativeTypeBoundary.defaultHidden", 7},
		"catalog-sql-tables":       {b03CatalogJSONSource, "sqlTables", 1},
	}
	if len(cfg.NativeFieldLiterals) != len(want) {
		t.Fatal("native field site set is incomplete")
	}
	for _, site := range cfg.NativeFieldLiterals {
		binding, ok := want[site.ID]
		if !ok || site.Path != binding.path || site.Field != binding.field || len(site.Values) != binding.count {
			t.Fatalf("unexpected native field site: %+v", site)
		}
		delete(want, site.ID)
		if site.ID == "catalog-sql-tables" && (!allowsPhysicalProtocolString(externalProtocolSource, site.Values[0], cfg.ExternalProtocolString) || len(strings.Fields(site.Values[0])) != 33) {
			t.Fatal("JSON catalog differs from the complete existing physical census")
		}
	}
	return cfg, path
}

func nativeFieldSite(t *testing.T, cfg Config, id string) NativeLiteralSite {
	t.Helper()
	for _, site := range cfg.NativeFieldLiterals {
		if site.ID == id {
			return site
		}
	}
	t.Fatalf("missing native field site %q", id)
	return NativeLiteralSite{}
}

func quotedNativeValues(values []string, quote func(string) string) string {
	var literals []string
	for _, value := range values {
		literals = append(literals, quote(value))
	}
	return strings.Join(literals, ", ")
}

func nativeFieldGoFixture(t *testing.T, cfg Config, quote func(string) string) string {
	t.Helper()
	ready := quotedNativeValues(nativeFieldSite(t, cfg, "ready-types").Values, quote)
	custom := quotedNativeValues(nativeFieldSite(t, cfg, "custom-types").Values, quote)
	return "package recorder\nfunc nativeExclusionTypes() []string { return []string{" + ready + "} }\n" +
		"func recordNativeExclusions() { c.run(" + custom + ") }\n"
}

func escapedNativeJSON(value string) string {
	var encoded strings.Builder
	encoded.WriteByte('"')
	for _, r := range value {
		fmt.Fprintf(&encoded, "\\u%04x", r)
	}
	encoded.WriteByte('"')
	return encoded.String()
}

func nativeFieldJSONFixture(site NativeLiteralSite, quote func(string) string) string {
	value := "[" + quotedNativeValues(site.Values, quote) + "]"
	if site.ID == "catalog-sql-tables" {
		return "{" + quote(site.Field) + ":" + quote(site.Values[0]) + "}"
	}
	return "{" + quote("nativeTypeBoundary") + ":{" + quote("defaultHidden") + ":" + value + "}}"
}

func requireNativeFieldRefusal(t *testing.T, cfg Config, path, source string) {
	t.Helper()
	findings, err := CheckFiles(map[string][]byte{path: []byte(source)}, cfg)
	if err != nil || len(findings) == 0 {
		t.Fatalf("native field lost refusal at %s: %#v, %v", path, findings, err)
	}
}

func TestNativeFieldProtocolAdmitsCompleteValuesOnlyAtRealSites(t *testing.T) {
	cfg, _ := checkedInNativeFieldConfig(t)
	for name, quote := range map[string]func(string) string{
		"quoted":  strconv.Quote,
		"raw":     func(value string) string { return "`" + value + "`" },
		"escaped": escapedProtocolString,
	} {
		t.Run("go/"+name, func(t *testing.T) {
			source := nativeFieldGoFixture(t, cfg, quote)
			findings, err := CheckFiles(map[string][]byte{b01NativeGoSource: []byte(source)}, cfg)
			if err != nil || len(findings) != 0 {
				t.Fatalf("actual complete Go vectors refused: %#v, %v", findings, err)
			}
		})
	}
	for _, id := range []string{"native-type-binding-json", "catalog-sql-tables"} {
		site := nativeFieldSite(t, cfg, id)
		for name, quote := range map[string]func(string) string{"quoted": strconv.Quote, "escaped_keys_and_values": escapedNativeJSON} {
			t.Run(id+"/"+name, func(t *testing.T) {
				source := nativeFieldJSONFixture(site, quote)
				findings, err := CheckFiles(map[string][]byte{site.Path: []byte(source)}, cfg)
				if err != nil || len(findings) != 0 {
					t.Fatalf("actual complete JSON field refused: %#v, %v", findings, err)
				}
				spaced := "{\n\"prefix\": \"é世界\",\n" + strings.ReplaceAll(source[1:], ":", ": \n ")
				findings, err = CheckFiles(map[string][]byte{site.Path: []byte(spaced)}, cfg)
				if err != nil || len(findings) != 0 {
					t.Fatalf("JSON field offsets changed with whitespace/UTF-8 prefix: %#v, %v", findings, err)
				}
			})
		}
	}
}

func TestNativeFieldGoProtocolRejectsContextCopiesAndChangedVectors(t *testing.T) {
	cfg, _ := checkedInNativeFieldConfig(t)
	source := nativeFieldGoFixture(t, cfg, strconv.Quote)
	ready := nativeFieldSite(t, cfg, "ready-types").Values
	custom := nativeFieldSite(t, cfg, "custom-types").Values
	readyList := quotedNativeValues(ready, strconv.Quote)
	customList := quotedNativeValues(custom, strconv.Quote)
	reordered := append([]string{}, ready...)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	product := cfg.Terms[2].Matches[0]
	for _, test := range []struct{ name, path, source string }{
		{"neighbor_path", "spec/s1-b01-b04/native_exclusion_other_test.go", source},
		{"wildcard_path", "spec/s1-b01-b04/*.go", source},
		{"other_protocol_path", externalProtocolSource, source},
		{"wrong_function", b01NativeGoSource, strings.Replace(source, "nativeExclusionTypes()", "otherTypes()", 1)},
		{"ready_parameter", b01NativeGoSource, strings.Replace(source, "nativeExclusionTypes()", "nativeExclusionTypes(x int)", 1)},
		{"ready_other_element_type", b01NativeGoSource, strings.Replace(source, "return []string", "return []any", 1)},
		{"ready_array_not_slice", b01NativeGoSource, strings.Replace(source, "return []string", "return [9]string", 1)},
		{"ready_nested_closure", b01NativeGoSource, strings.Replace(source, "return []string{"+readyList+"}", "f := func() []string { return []string{"+readyList+"} }; return f()", 1)},
		{"ready_duplicate_function", b01NativeGoSource, source + "func nativeExclusionTypes() []string { return []string{" + readyList + "} }"},
		{"ready_reordered", b01NativeGoSource, strings.Replace(source, readyList, quotedNativeValues(reordered, strconv.Quote), 1)},
		{"ready_missing_ordinary_value", b01NativeGoSource, strings.Replace(source, readyList, quotedNativeValues(ready[:len(ready)-1], strconv.Quote), 1)},
		{"ready_extra_value", b01NativeGoSource, strings.Replace(source, readyList, readyList+`, "extra"`, 1)},
		{"ready_duplicate_value", b01NativeGoSource, strings.Replace(source, readyList, readyList+", "+strconv.Quote(ready[0]), 1)},
		{"ready_extra_token", b01NativeGoSource, strings.Replace(source, strconv.Quote(product), strconv.Quote(product+" extra"), 1)},
		{"custom_wrong_function", b01NativeGoSource, strings.Replace(source, "recordNativeExclusions()", "otherRecorder()", 1)},
		{"custom_wrong_receiver", b01NativeGoSource, strings.Replace(source, "c.run(", "other.run(", 1)},
		{"custom_wrong_command", b01NativeGoSource, strings.Replace(source, strconv.Quote(custom[0]), strconv.Quote(custom[0]+"-extra"), 1)},
		{"custom_wrong_key", b01NativeGoSource, strings.Replace(source, strconv.Quote(custom[3]), strconv.Quote(custom[3]+".extra"), 1)},
		{"custom_extra_arg", b01NativeGoSource, strings.Replace(source, customList, customList+`, "extra"`, 1)},
		{"custom_extra_token", b01NativeGoSource, strings.Replace(source, strconv.Quote(custom[4]), strconv.Quote(custom[4]+",extra"), 1)},
		{"custom_nested_closure", b01NativeGoSource, strings.Replace(source, "c.run("+customList+")", "f := func() { c.run("+customList+") }; f()", 1)},
		{"custom_duplicate_call", b01NativeGoSource, strings.Replace(source, "c.run("+customList+")", "c.run("+customList+"); c.run("+customList+")", 1)},
		{"copied_literal", b01NativeGoSource, source + "var copy = " + strconv.Quote(product)},
		{"source_shaped_string", b01NativeGoSource, "package recorder\nvar copy = " + escapedProtocolString(source)},
		{"banned_comment", b01NativeGoSource, source + "// " + product},
		{"banned_identifier", b01NativeGoSource, source + "var " + ready[3] + " = 1"},
		{"plain_import", b01NativeGoSource, "package recorder\nimport " + strconv.Quote(product)},
		{"escaped_import", b01NativeGoSource, "package recorder\nimport " + escapedProtocolString(product)},
		{"invalid_go", b01NativeGoSource, "package recorder\nfunc ("},
	} {
		t.Run(test.name, func(t *testing.T) { requireNativeFieldRefusal(t, cfg, test.path, test.source) })
	}
}

func TestNativeFieldJSONRejectsCopiesDuplicateKeysAndChangedValues(t *testing.T) {
	cfg, _ := checkedInNativeFieldConfig(t)
	product := cfg.Terms[2].Matches[0]
	for _, id := range []string{"native-type-binding-json", "catalog-sql-tables"} {
		site := nativeFieldSite(t, cfg, id)
		source := nativeFieldJSONFixture(site, strconv.Quote)
		key := strings.Split(site.Field, ".")
		leaf := key[len(key)-1]
		quotedKey := strconv.Quote(leaf)
		value := "[" + quotedNativeValues(site.Values, strconv.Quote) + "]"
		if id == "catalog-sql-tables" {
			value = strconv.Quote(site.Values[0])
		}
		changed := site
		changed.Values = append([]string{}, site.Values...)
		changed.Values[0] += " extra"
		tests := []struct{ name, path, source string }{
			{"neighbor_path", site.Path + ".other.json", source},
			{"wrong_root_field", site.Path, strings.Replace(source, strconv.Quote(key[0]), strconv.Quote(key[0]+"Extra"), 1)},
			{"wrong_leaf_field", site.Path, strings.Replace(source, quotedKey, strconv.Quote(leaf+"Extra"), 1)},
			{"wrong_case", site.Path, strings.Replace(source, quotedKey, strconv.Quote(strings.ToUpper(leaf)), 1)},
			{"nested_copy", site.Path, `{"copy":` + source + `}`},
			{"extra_token", site.Path, nativeFieldJSONFixture(changed, strconv.Quote)},
			{"wrong_type", site.Path, strings.Replace(source, value, `{"copy":`+value+`}`, 1)},
			{"duplicate_leaf", site.Path, strings.Replace(source, quotedKey+":"+value, quotedKey+":"+value+","+quotedKey+":"+value, 1)},
			{"escaped_duplicate_leaf", site.Path, strings.Replace(source, quotedKey+":"+value, quotedKey+":"+value+","+escapedNativeJSON(leaf)+":"+value, 1)},
			{"copy_in_other_field", site.Path, source[:len(source)-1] + `,"copy":` + value + `}`},
			{"other_banned_value", site.Path, source[:len(source)-1] + `,"prose":` + strconv.Quote(product) + `}`},
			{"escaped_banned_value", site.Path, source[:len(source)-1] + `,"prose":` + escapedNativeJSON(product) + `}`},
			{"source_shaped_string", site.Path, `{"copy":` + escapedNativeJSON(source) + `}`},
			{"malformed_json", site.Path, `{"incomplete":`},
			{"extra_json_document", site.Path, source + `{}`},
			{"prose_file", "docs/native-census.md", source},
		}
		if id == "native-type-binding-json" {
			group := source[strings.Index(source, ":")+1 : len(source)-1]
			tests = append(tests,
				struct{ name, path, source string }{"duplicate_group", site.Path, source[:len(source)-1] + `,"nativeTypeBoundary":` + group + `}`},
				struct{ name, path, source string }{"escaped_duplicate_group", site.Path, source[:len(source)-1] + "," + escapedNativeJSON("nativeTypeBoundary") + ":" + group + `}`},
				struct{ name, path, source string }{"extra_element", site.Path, strings.Replace(source, value, value[:len(value)-1]+`, "extra"]`, 1)},
				struct{ name, path, source string }{"missing_element", site.Path, strings.Replace(source, value, "["+quotedNativeValues(site.Values[:len(site.Values)-1], strconv.Quote)+"]", 1)},
				struct{ name, path, source string }{"duplicate_element", site.Path, strings.Replace(source, value, value[:len(value)-1]+", "+strconv.Quote(site.Values[0])+"]", 1)},
			)
			reordered := site
			reordered.Values = append([]string{}, site.Values...)
			reordered.Values[0], reordered.Values[1] = reordered.Values[1], reordered.Values[0]
			tests = append(tests, struct{ name, path, source string }{"reordered_elements", site.Path, nativeFieldJSONFixture(reordered, strconv.Quote)})
		} else {
			tables := strings.Fields(site.Values[0])
			reordered := append([]string{}, tables...)
			reordered[0], reordered[1] = reordered[1], reordered[0]
			for name, replacement := range map[string]string{
				"reordered_tables": strconv.Quote(strings.Join(reordered, " ")),
				"missing_table":    strconv.Quote(strings.Join(tables[:len(tables)-1], " ")),
				"duplicate_table":  strconv.Quote(site.Values[0] + " " + tables[0]),
				"array_not_scalar": "[" + value + "]",
				"prose_not_census": strconv.Quote("The census is " + site.Values[0]),
			} {
				tests = append(tests, struct{ name, path, source string }{name, site.Path, strings.Replace(source, value, replacement, 1)})
			}
		}
		for _, test := range tests {
			t.Run(id+"/"+test.name, func(t *testing.T) { requireNativeFieldRefusal(t, cfg, test.path, test.source) })
		}
	}
}

func TestNativeFieldAllowanceRequiresUnmodifiedCompleteConfiguration(t *testing.T) {
	cfg, _ := checkedInNativeFieldConfig(t)
	for _, mutate := range []struct {
		name string
		edit func([]NativeLiteralSite) []NativeLiteralSite
	}{
		{"absent", func(sites []NativeLiteralSite) []NativeLiteralSite { return nil }},
		{"missing_site", func(sites []NativeLiteralSite) []NativeLiteralSite { return sites[:len(sites)-1] }},
		{"changed_value", func(sites []NativeLiteralSite) []NativeLiteralSite { sites[0].Values[0] += " extra"; return sites }},
		{"changed_path", func(sites []NativeLiteralSite) []NativeLiteralSite { sites[0].Path += ".other"; return sites }},
		{"changed_field", func(sites []NativeLiteralSite) []NativeLiteralSite { sites[0].Field += ".other"; return sites }},
		{"duplicate_site", func(sites []NativeLiteralSite) []NativeLiteralSite { sites[0] = sites[1]; return sites }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			changed := cfg
			changed.NativeFieldLiterals = nil
			for _, site := range cfg.NativeFieldLiterals {
				site.Values = append([]string{}, site.Values...)
				changed.NativeFieldLiterals = append(changed.NativeFieldLiterals, site)
			}
			changed.NativeFieldLiterals = mutate.edit(changed.NativeFieldLiterals)
			requireNativeFieldRefusal(t, changed, b01NativeGoSource, nativeFieldGoFixture(t, cfg, strconv.Quote))
			for _, id := range []string{"native-type-binding-json", "catalog-sql-tables"} {
				site := nativeFieldSite(t, cfg, id)
				requireNativeFieldRefusal(t, changed, site.Path, nativeFieldJSONFixture(site, strconv.Quote))
			}
		})
	}
}

func nativeFieldConfigSection(site NativeLiteralSite) string {
	return "[[native_field_literal]]\nid = " + strconv.Quote(site.ID) + "\npath = " + strconv.Quote(site.Path) +
		"\nfield = " + strconv.Quote(site.Field) + "\nvalues = [" + quotedNativeValues(site.Values, strconv.Quote) + "]"
}

func checkedInB03NativeGoConfig(t *testing.T) (Config, string) {
	t.Helper()
	cfg, path := checkedInNativeFieldConfig(t)
	want := map[string]string{
		"catalog-go-tables":   "b03NativeTables.strings.Fields.arg0",
		"catalog-go-artifact": "verifyB03PreparedArtifact.artifact.manifest.Artifacts.index",
	}
	if len(cfg.B03NativeGoLiterals) != len(want) {
		t.Fatal("B03 Go site set is incomplete")
	}
	for _, site := range cfg.B03NativeGoLiterals {
		field, ok := want[site.ID]
		if !ok || site.Path != b03CatalogGoSource || site.Field != field || len(site.Values) != 1 {
			t.Fatalf("unexpected B03 Go site: %+v", site)
		}
		delete(want, site.ID)
		if site.ID == "catalog-go-tables" && site.Values[0] != nativeFieldSite(t, cfg, "catalog-sql-tables").Values[0] {
			t.Fatal("Go census differs from the complete admitted JSON census")
		}
		if site.ID == "catalog-go-artifact" && !allowsNativeManifestString(nativeManifestProtocolSource, site.Values[0], cfg.NativeManifestStrings) {
			t.Fatal("Go artifact key differs from the accepted manifest literal")
		}
	}
	return cfg, path
}

func b03NativeGoFixture(t *testing.T, cfg Config, quote func(string) string) (source, census, artifact string) {
	t.Helper()
	for _, site := range cfg.B03NativeGoLiterals {
		switch site.ID {
		case "catalog-go-tables":
			census = site.Values[0]
		case "catalog-go-artifact":
			artifact = site.Values[0]
		}
	}
	if census == "" || artifact == "" {
		t.Fatal("missing B03 Go fixture values")
	}
	source = "package recorder\nimport \"strings\"\nvar b03NativeTables = strings.Fields(" + quote(census) + ")\n" +
		"func verifyB03PreparedArtifact() { artifact := manifest.Artifacts[" + quote(artifact) + "] }\n"
	return source, census, artifact
}

func TestB03NativeGoProtocolAdmitsOnlyActualASTSites(t *testing.T) {
	cfg, _ := checkedInB03NativeGoConfig(t)
	for name, quote := range map[string]func(string) string{
		"quoted":  strconv.Quote,
		"raw":     func(value string) string { return "`" + value + "`" },
		"escaped": escapedProtocolString,
	} {
		t.Run(name, func(t *testing.T) {
			source, _, _ := b03NativeGoFixture(t, cfg, quote)
			findings, err := CheckFiles(map[string][]byte{b03CatalogGoSource: []byte(source)}, cfg)
			if err != nil || len(findings) != 0 {
				t.Fatalf("actual B03 Go sites refused: %#v, %v", findings, err)
			}
		})
	}
}

func TestB03NativeGoProtocolRejectsCopiesAndChangedContexts(t *testing.T) {
	cfg, _ := checkedInB03NativeGoConfig(t)
	source, census, artifact := b03NativeGoFixture(t, cfg, strconv.Quote)
	call := "strings.Fields(" + strconv.Quote(census) + ")"
	lookup := "artifact := manifest.Artifacts[" + strconv.Quote(artifact) + "]"
	product := cfg.Terms[2].Matches[0]
	tables := strings.Fields(census)
	reordered := append([]string{}, tables...)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	for _, test := range []struct{ name, path, source string }{
		{"neighbor_path", b03CatalogGoSource + ".other.go", source},
		{"wildcard_path", "internal/integration/b03catalog/*.go", source},
		{"prose", "docs/catalog.md", source},
		{"wrong_global", b03CatalogGoSource, strings.Replace(source, "var b03NativeTables", "var otherTables", 1)},
		{"typed_global", b03CatalogGoSource, strings.Replace(source, "var b03NativeTables", "var b03NativeTables []string", 1)},
		{"const_not_var", b03CatalogGoSource, strings.Replace(source, "var b03NativeTables", "const b03NativeTables", 1)},
		{"local_copy", b03CatalogGoSource, strings.Replace(source, "var b03NativeTables = "+call, "func other() { b03NativeTables := "+call+" }", 1)},
		{"duplicate_global", b03CatalogGoSource, source + "var b03NativeTables = " + call},
		{"global_type_collision", b03CatalogGoSource, source + "type b03NativeTables int"},
		{"wrong_call_receiver", b03CatalogGoSource, strings.Replace(source, call, strings.Replace(call, "strings.", "other.", 1), 1)},
		{"wrong_call_member", b03CatalogGoSource, strings.Replace(source, call, strings.Replace(call, ".Fields(", ".Other(", 1), 1)},
		{"extra_call_argument", b03CatalogGoSource, strings.Replace(source, call, "strings.Fields("+strconv.Quote(census)+`, "extra")`, 1)},
		{"variadic_call", b03CatalogGoSource, strings.Replace(source, call, "strings.Fields("+strconv.Quote(census)+"...)", 1)},
		{"expression_not_literal", b03CatalogGoSource, strings.Replace(source, call, "strings.Fields("+strconv.Quote(census)+` + "")`, 1)},
		{"missing_table", b03CatalogGoSource, strings.Replace(source, strconv.Quote(census), strconv.Quote(strings.Join(tables[:len(tables)-1], " ")), 1)},
		{"reordered_tables", b03CatalogGoSource, strings.Replace(source, strconv.Quote(census), strconv.Quote(strings.Join(reordered, " ")), 1)},
		{"extra_table", b03CatalogGoSource, strings.Replace(source, strconv.Quote(census), strconv.Quote(census+" extra"), 1)},
		{"duplicate_table", b03CatalogGoSource, strings.Replace(source, strconv.Quote(census), strconv.Quote(census+" "+tables[0]), 1)},
		{"extra_census_space", b03CatalogGoSource, strings.Replace(source, strconv.Quote(census), strconv.Quote(census+" "), 1)},
		{"wrong_function", b03CatalogGoSource, strings.Replace(source, "func verifyB03PreparedArtifact()", "func otherVerifier()", 1)},
		{"method_not_function", b03CatalogGoSource, strings.Replace(source, "func verifyB03PreparedArtifact()", "func (r receiver) verifyB03PreparedArtifact()", 1)},
		{"wrong_assignment_name", b03CatalogGoSource, strings.Replace(source, lookup, strings.Replace(lookup, "artifact :=", "copy :=", 1), 1)},
		{"assignment_not_definition", b03CatalogGoSource, strings.Replace(source, lookup, strings.Replace(lookup, ":=", "=", 1), 1)},
		{"wrong_manifest", b03CatalogGoSource, strings.Replace(source, lookup, strings.Replace(lookup, "manifest.", "other.", 1), 1)},
		{"wrong_manifest_field", b03CatalogGoSource, strings.Replace(source, lookup, strings.Replace(lookup, ".Artifacts[", ".Other[", 1), 1)},
		{"nested_lookup", b03CatalogGoSource, strings.Replace(source, lookup, "f := func() { "+lookup+" }; f()", 1)},
		{"duplicate_lookup", b03CatalogGoSource, strings.Replace(source, lookup, lookup+"; "+lookup, 1)},
		{"duplicate_function", b03CatalogGoSource, source + "func verifyB03PreparedArtifact() { " + lookup + " }"},
		{"changed_artifact_key", b03CatalogGoSource, strings.Replace(source, strconv.Quote(artifact), strconv.Quote(artifact+"-extra"), 1)},
		{"artifact_copy", b03CatalogGoSource, source + "var copy = " + strconv.Quote(artifact)},
		{"census_copy", b03CatalogGoSource, source + "var copy = " + strconv.Quote(census)},
		{"quoted_source_copy", b03CatalogGoSource, "package recorder\nvar copy = " + escapedProtocolString(source)},
		{"product_comment", b03CatalogGoSource, source + "// " + product},
		{"identifier", b03CatalogGoSource, source + "var " + cfg.Terms[3].Matches[len(cfg.Terms[3].Matches)-1] + " = 1"},
		{"plain_import", b03CatalogGoSource, "package recorder\nimport " + strconv.Quote(artifact)},
		{"escaped_import", b03CatalogGoSource, "package recorder\nimport " + escapedProtocolString(artifact)},
		{"escaped_product_import", b03CatalogGoSource, "package recorder\nimport " + escapedProtocolString(product)},
	} {
		t.Run(test.name, func(t *testing.T) { requireNativeFieldRefusal(t, cfg, test.path, test.source) })
	}
}

func TestB03NativeGoProtocolPreservesOtherPathsAndCommentRules(t *testing.T) {
	cfg, _ := checkedInB03NativeGoConfig(t)
	baseline := cfg
	baseline.B03NativeGoLiterals = nil
	_, census, artifact := b03NativeGoFixture(t, cfg, strconv.Quote)
	for _, path := range []string{"other/existing.go", b01NativeGoSource, nativeManifestProtocolSource, externalProtocolSource} {
		for i, value := range []string{census, artifact} {
			for name, quote := range map[string]func(string) string{"quoted": strconv.Quote, "escaped": escapedProtocolString} {
				t.Run(fmt.Sprintf("%s/literal%d/%s", path, i, name), func(t *testing.T) {
					files := map[string][]byte{path: []byte("package recorder\nvar copy = " + quote(value))}
					before, beforeErr := CheckFiles(files, baseline)
					after, afterErr := CheckFiles(files, cfg)
					if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("B03 Go admission changed outside scan: before=%#v/%v after=%#v/%v", before, beforeErr, after, afterErr)
					}
				})
			}
		}
	}
	for i, value := range []string{census, artifact} {
		t.Run(fmt.Sprintf("comment%d", i), func(t *testing.T) {
			files := map[string][]byte{b03CatalogGoSource: []byte("package recorder\n// " + value)}
			before, beforeErr := CheckFiles(files, baseline)
			after, afterErr := CheckFiles(files, cfg)
			if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("B03 Go admission changed comment baseline: before=%#v/%v after=%#v/%v", before, beforeErr, after, afterErr)
			}
		})
	}
}

func b03NativeGoConfigSection(site NativeLiteralSite) string {
	return strings.Replace(nativeFieldConfigSection(site), "[[native_field_literal]]", "[[b03_native_go_literal]]", 1)
}

func TestB03NativeGoConfigurationRejectsBroadenedOrIncompleteSites(t *testing.T) {
	cfg, path := checkedInB03NativeGoConfig(t)
	original, err := readConfigDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, site := range cfg.B03NativeGoLiterals {
		section := b03NativeGoConfigSection(site)
		if !strings.Contains(string(original), section) {
			t.Fatalf("B03 Go site %s was not bound", site.ID)
		}
		idLine := "id = " + strconv.Quote(site.ID)
		pathLine := "path = " + strconv.Quote(site.Path)
		fieldLine := "field = " + strconv.Quote(site.Field)
		valueLine := "values = [" + strconv.Quote(site.Values[0]) + "]"
		for _, test := range []struct{ name, replacement string }{
			{"unknown_id", strings.Replace(section, idLine, `id = "other"`, 1)},
			{"wrong_path", strings.Replace(section, pathLine, "path = "+strconv.Quote(site.Path+".other"), 1)},
			{"wrong_field", strings.Replace(section, fieldLine, "field = "+strconv.Quote(site.Field+".other"), 1)},
			{"changed_value", strings.Replace(section, valueLine, "values = ["+strconv.Quote(site.Values[0]+" extra")+"]", 1)},
			{"extra_value", strings.Replace(section, valueLine, valueLine[:len(valueLine)-1]+`, "extra"]`, 1)},
			{"empty_values", strings.Replace(section, valueLine, "values = []", 1)},
			{"scalar_values", strings.Replace(section, valueLine, "values = "+strconv.Quote(site.Values[0]), 1)},
			{"array_path", strings.Replace(section, pathLine, "path = ["+strconv.Quote(site.Path)+"]", 1)},
			{"missing_id", strings.Replace(section, idLine+"\n", "", 1)},
			{"missing_path", strings.Replace(section, pathLine+"\n", "", 1)},
			{"missing_field", strings.Replace(section, fieldLine+"\n", "", 1)},
			{"missing_values", strings.Replace(section, valueLine, "", 1)},
			{"duplicate_field", strings.Replace(section, fieldLine, fieldLine+"\n"+fieldLine, 1)},
			{"unknown_field", section + "\nprefix = \"other\""},
			{"duplicate_section", section + "\n" + section},
			{"incomplete_set", ""},
			{"tuple_separator_injection", strings.Replace(section, fieldLine, "field = "+strconv.Quote(site.Field+"\x00extra"), 1)},
		} {
			t.Run(site.ID+"/"+test.name, func(t *testing.T) {
				file := filepath.Join(t.TempDir(), "lexicon.toml")
				content := strings.Replace(string(original), section, test.replacement, 1)
				if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := LoadConfig(file); err == nil {
					t.Fatal("broadened or incomplete B03 Go site accepted")
				}
			})
		}
	}
	without := string(original)
	for _, site := range cfg.B03NativeGoLiterals {
		without = strings.Replace(without, b03NativeGoConfigSection(site), "", 1)
	}
	file := filepath.Join(t.TempDir(), "lexicon.toml")
	if err := os.WriteFile(file, []byte(without), 0o600); err != nil {
		t.Fatal(err)
	}
	parsed, err := LoadConfig(file)
	if err != nil || len(parsed.B03NativeGoLiterals) != 0 {
		t.Fatalf("omitted optional B03 Go set should grant nothing: %+v, %v", parsed.B03NativeGoLiterals, err)
	}
	source, _, _ := b03NativeGoFixture(t, cfg, strconv.Quote)
	requireNativeFieldRefusal(t, parsed, b03CatalogGoSource, source)
}

func TestB03NativeGoAllowanceRejectsMutatedInMemorySites(t *testing.T) {
	cfg, _ := checkedInB03NativeGoConfig(t)
	source, _, _ := b03NativeGoFixture(t, cfg, strconv.Quote)
	for _, test := range []struct {
		name string
		edit func([]NativeLiteralSite) []NativeLiteralSite
	}{
		{"absent", func(sites []NativeLiteralSite) []NativeLiteralSite { return nil }},
		{"incomplete", func(sites []NativeLiteralSite) []NativeLiteralSite { return sites[:1] }},
		{"duplicate", func(sites []NativeLiteralSite) []NativeLiteralSite { sites[1] = sites[0]; return sites }},
		{"wrong_path", func(sites []NativeLiteralSite) []NativeLiteralSite { sites[0].Path += ".other"; return sites }},
		{"wrong_field", func(sites []NativeLiteralSite) []NativeLiteralSite { sites[0].Field += ".other"; return sites }},
		{"wrong_value", func(sites []NativeLiteralSite) []NativeLiteralSite { sites[0].Values[0] += " extra"; return sites }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := cfg
			changed.B03NativeGoLiterals = nil
			for _, site := range cfg.B03NativeGoLiterals {
				site.Values = append([]string{}, site.Values...)
				changed.B03NativeGoLiterals = append(changed.B03NativeGoLiterals, site)
			}
			changed.B03NativeGoLiterals = test.edit(changed.B03NativeGoLiterals)
			requireNativeFieldRefusal(t, changed, b03CatalogGoSource, source)
		})
	}
}

func TestNativeFieldConfigurationRejectsBroadeningAndAmbiguity(t *testing.T) {
	cfg, path := checkedInNativeFieldConfig(t)
	original, err := readConfigDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, site := range cfg.NativeFieldLiterals {
		section := nativeFieldConfigSection(site)
		if !strings.Contains(string(original), section) {
			t.Fatalf("checked-in native site %s was not bound", site.ID)
		}
		idLine := "id = " + strconv.Quote(site.ID)
		pathLine := "path = " + strconv.Quote(site.Path)
		fieldLine := "field = " + strconv.Quote(site.Field)
		valueLine := "values = [" + quotedNativeValues(site.Values, strconv.Quote) + "]"
		changed := site
		changed.Values = append([]string{}, site.Values...)
		changed.Values[0] += " extra"
		tests := []struct{ name, replacement string }{
			{"unknown_id", strings.Replace(section, idLine, `id = "unknown"`, 1)},
			{"missing_id", strings.Replace(section, idLine+"\n", "", 1)},
			{"missing_path", strings.Replace(section, pathLine+"\n", "", 1)},
			{"missing_field", strings.Replace(section, fieldLine+"\n", "", 1)},
			{"missing_values", strings.Replace(section, valueLine, "", 1)},
			{"neighbor_path", strings.Replace(section, pathLine, "path = "+strconv.Quote(site.Path+".other"), 1)},
			{"wildcard_path", strings.Replace(section, pathLine, `path = "spec/*"`, 1)},
			{"wrong_field", strings.Replace(section, fieldLine, "field = "+strconv.Quote(site.Field+".other"), 1)},
			{"unknown_key", section + "\nprefix = \"extra\""},
			{"duplicate_id_field", strings.Replace(section, idLine, idLine+"\n"+idLine, 1)},
			{"duplicate_path_field", strings.Replace(section, pathLine, pathLine+"\n"+pathLine, 1)},
			{"duplicate_value_field", strings.Replace(section, valueLine, valueLine+"\n"+valueLine, 1)},
			{"changed_vector", nativeFieldConfigSection(changed)},
			{"extra_element", strings.Replace(section, valueLine, valueLine[:len(valueLine)-1]+`, "extra"]`, 1)},
			{"scalar_values", strings.Replace(section, valueLine, "values = "+strconv.Quote(site.Values[0]), 1)},
			{"null_values", strings.Replace(section, valueLine, "values = null", 1)},
			{"null_element", strings.Replace(section, valueLine, "values = [null]", 1)},
			{"number_element", strings.Replace(section, valueLine, "values = [1]", 1)},
			{"empty_array", strings.Replace(section, valueLine, "values = []", 1)},
			{"malformed_array", strings.Replace(section, valueLine, "values = [", 1)},
			{"array_path", strings.Replace(section, pathLine, "path = ["+strconv.Quote(site.Path)+"]", 1)},
			{"tuple_separator_injection", strings.Replace(section, fieldLine, "field = "+strconv.Quote(site.Field+"\x00extra"), 1)},
			{"duplicate_section", section + "\n" + section},
			{"missing_section", ""},
		}
		if len(site.Values) > 1 {
			changed.Values = append([]string{}, site.Values...)
			changed.Values[0], changed.Values[1] = changed.Values[1], changed.Values[0]
			tests = append(tests, struct{ name, replacement string }{"reordered_vector", nativeFieldConfigSection(changed)})
			changed.Values = append([]string{}, site.Values[:len(site.Values)-1]...)
			tests = append(tests, struct{ name, replacement string }{"incomplete_vector", nativeFieldConfigSection(changed)})
		}
		for _, test := range tests {
			t.Run(site.ID+"/"+test.name, func(t *testing.T) {
				file := filepath.Join(t.TempDir(), "lexicon.toml")
				content := strings.Replace(string(original), section, test.replacement, 1)
				if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := LoadConfig(file); err == nil {
					t.Fatal("malformed or broadened native field configuration was accepted")
				}
			})
		}
	}
	without := string(original)
	for _, site := range cfg.NativeFieldLiterals {
		without = strings.Replace(without, nativeFieldConfigSection(site), "", 1)
	}
	file := filepath.Join(t.TempDir(), "lexicon.toml")
	if err := os.WriteFile(file, []byte(without), 0o600); err != nil {
		t.Fatal(err)
	}
	if parsed, err := LoadConfig(file); err != nil || len(parsed.NativeFieldLiterals) != 0 {
		t.Fatalf("omitted optional field section should grant nothing: %+v, %v", parsed.NativeFieldLiterals, err)
	}
}

func TestNativeFieldProtocolPreservesOutsidePathsAndCommentBaseline(t *testing.T) {
	cfg, _ := checkedInNativeFieldConfig(t)
	baseline := cfg
	baseline.NativeFieldLiterals = nil
	product := cfg.Terms[2].Matches[0]
	ready := nativeFieldSite(t, cfg, "ready-types").Values
	for _, path := range []string{"other/source.go", "docs/protocol.md", "other/inputs.json", externalProtocolSource, nativeManifestProtocolSource} {
		for name, source := range map[string]string{
			"quoted":             "package recorder\nvar copy = " + strconv.Quote(product),
			"escaped":            "package recorder\nvar copy = " + escapedProtocolString(product),
			"identifier_comment": "package recorder\n// " + ready[3],
			"product_comment":    "package recorder\n// " + product,
		} {
			t.Run(path+"/"+name, func(t *testing.T) {
				files := map[string][]byte{path: []byte(source)}
				before, beforeErr := CheckFiles(files, baseline)
				after, afterErr := CheckFiles(files, cfg)
				if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("native field policy changed outside baseline: before=%#v/%v after=%#v/%v", before, beforeErr, after, afterErr)
				}
			})
		}
	}
	for i, value := range ready {
		t.Run(fmt.Sprintf("native_comment%d", i), func(t *testing.T) {
			files := map[string][]byte{b01NativeGoSource: []byte("package recorder\n// " + value)}
			before, beforeErr := CheckFiles(files, baseline)
			after, afterErr := CheckFiles(files, cfg)
			if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("native field policy changed comment baseline: before=%#v/%v after=%#v/%v", before, beforeErr, after, afterErr)
			}
		})
	}
}

func TestExternalProtocolStringConfigurationRejectsBroadening(t *testing.T) {
	cfg, path := checkedInExternalProtocolConfig(t)
	original, err := readConfigDocument(path)
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

func checkedInSourcePlaneConstants(t *testing.T) (Config, string) {
	t.Helper()
	cfg, path := checkedInExternalProtocolConfig(t)
	want := map[string]struct{ name, valueDigest string }{
		sourcePlaneProtocolSource: {"sourcePlaneEphemeralDependencyTable", "a250fe4bfeccf35cd1f34c8eb7625ef05127294cdeb6884a04adea45796adebc"},
		sourcePlaneFixtureSource:  {"sourcePlaneFixtureActiveEventTable", "5f3c5fb3612b7ea7e7df0cf4f21c4ad123f1603b84d03ad8605fe4b8f67c5025"},
	}
	if len(cfg.SourcePlaneConstants) != len(want) {
		t.Fatal("source plane constant set is incomplete")
	}
	for _, a := range cfg.SourcePlaneConstants {
		binding, ok := want[a.Path]
		if !ok || a.Name != binding.name || protocolLiteralDigest(a.Value) != binding.valueDigest {
			t.Fatalf("source plane constant binding differs: %+v", a)
		}
		delete(want, a.Path)
	}
	return cfg, path
}

func sourcePlaneConstantFixture(a GoConstantAllowance, literal string) string {
	return "package driver\nconst " + a.Name + " = " + literal + "\n"
}

func TestSourcePlaneConstantsAdmitOnlyNamedDeclarations(t *testing.T) {
	cfg, _ := checkedInSourcePlaneConstants(t)
	for _, a := range cfg.SourcePlaneConstants {
		for name, quote := range map[string]func(string) string{
			"quoted": strconv.Quote, "raw": func(s string) string { return "`" + s + "`" }, "escaped": escapedProtocolString,
		} {
			t.Run(a.Name+"/"+name, func(t *testing.T) {
				source := sourcePlaneConstantFixture(a, quote(a.Value))
				for _, content := range []string{source, strings.Replace(source, "const "+a.Name+" = "+quote(a.Value), "const (\n"+a.Name+" = "+quote(a.Value)+"\n)", 1), strings.Replace(source, "const ", "// é世界\nconst ", 1) + "var reference = " + a.Name} {
					findings, err := CheckFiles(map[string][]byte{a.Path: []byte(content)}, cfg)
					if err != nil || len(findings) != 0 {
						t.Fatalf("named driver constant refused: %#v, %v", findings, err)
					}
				}
			})
		}
	}
}

func TestSourcePlaneConstantsKeepContextAndContentRefusals(t *testing.T) {
	cfg, _ := checkedInSourcePlaneConstants(t)
	for i, a := range cfg.SourcePlaneConstants {
		literal := strconv.Quote(a.Value)
		declaration := "const " + a.Name + " = " + literal
		source := sourcePlaneConstantFixture(a, literal)
		for _, test := range []struct{ name, path, source string }{
			{"neighbor", a.Path + ".other.go", source},
			{"wildcard", "internal/storage/driver/*.go", source},
			{"other_path", "internal/storage/driver/other.go", source},
			{"other_slot", cfg.SourcePlaneConstants[1-i].Path, source},
			{"prose", "docs/source-plane.md", a.Value},
			{"wrong_name", a.Path, strings.Replace(source, a.Name, "otherTable", 1)},
			{"variable", a.Path, strings.Replace(source, "const ", "var ", 1)},
			{"typed", a.Path, strings.Replace(source, a.Name+" =", a.Name+" string =", 1)},
			{"local", a.Path, "package driver\nfunc example() { " + declaration + " }"},
			{"closure", a.Path, "package driver\nvar example = func() { " + declaration + " }"},
			{"multiple_names", a.Path, strings.Replace(source, a.Name+" = "+literal, a.Name+", other = "+literal+`, "other"`, 1)},
			{"concatenation", a.Path, sourcePlaneConstantFixture(a, literal+` + ""`)},
			{"parenthesized", a.Path, sourcePlaneConstantFixture(a, "("+literal+")")},
			{"near_value", a.Path, sourcePlaneConstantFixture(a, strconv.Quote(a.Value+"-other"))},
			{"extra_product", a.Path, sourcePlaneConstantFixture(a, strconv.Quote(a.Value+" "+cfg.Terms[2].Matches[0]))},
			{"case_change", a.Path, sourcePlaneConstantFixture(a, strconv.Quote(strings.ToUpper(a.Value)))},
			{"duplicate_const", a.Path, source + declaration},
			{"duplicate_inherited", a.Path, "package driver\nconst (\n" + a.Name + " = " + literal + "\n" + a.Name + "\n)"},
			{"type_collision", a.Path, source + "type " + a.Name + " int"},
			{"function_collision", a.Path, source + "func " + a.Name + "() {}"},
			{"copy", a.Path, source + "var copy = " + literal},
			{"escaped_copy", a.Path, source + "var copy = " + escapedProtocolString(a.Value)},
			{"quoted_source", a.Path, "package driver\nvar copy = " + escapedProtocolString(source)},
			{"comment", a.Path, source + "// " + a.Value},
			{"block_comment", a.Path, source + "/* " + a.Value + " */"},
			{"other_product", a.Path, source + "var other = " + strconv.Quote(cfg.Terms[2].Matches[0])},
			{"identifier", a.Path, source + "var " + cfg.Terms[3].Matches[len(cfg.Terms[3].Matches)-1] + " = 1"},
			{"plain_import", a.Path, "package driver\nimport " + literal + "\n" + declaration},
			{"escaped_import", a.Path, "package driver\nimport " + escapedProtocolString(a.Value) + "\n" + declaration},
			{"malformed", a.Path, source + "func broken("},
		} {
			t.Run(a.Name+"/"+test.name, func(t *testing.T) {
				findings, err := CheckFiles(map[string][]byte{test.path: []byte(test.source)}, cfg)
				if err != nil || len(findings) == 0 {
					t.Fatalf("driver constant lost refusal: %#v, %v", findings, err)
				}
			})
		}
		t.Run(a.Name+"/split_initializer", func(t *testing.T) {
			split := strconv.Quote(a.Value[:2]) + " + " + strconv.Quote(a.Value[2:])
			findings, err := CheckFiles(map[string][]byte{a.Path: []byte(sourcePlaneConstantFixture(a, split))}, cfg)
			if err != nil || !hasFindingClass(findings, "invalid-protocol-literal") {
				t.Fatalf("split named initializer escaped the declaration contract: %#v/%v", findings, err)
			}
		})
	}
}

func sourcePlaneConfigSection(a GoConstantAllowance) string {
	return "[[source_plane_constant]]\npath = " + strconv.Quote(a.Path) + "\nname = " + strconv.Quote(a.Name) + "\nvalue = " + strconv.Quote(a.Value)
}

func TestSourcePlaneConstantConfigurationRejectsOverrides(t *testing.T) {
	cfg, path := checkedInSourcePlaneConstants(t)
	original, err := readConfigDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	without := string(original)
	for _, a := range cfg.SourcePlaneConstants {
		section := sourcePlaneConfigSection(a)
		if !strings.Contains(string(original), section) {
			t.Fatal("source plane config section not bound")
		}
		without = strings.Replace(without, section, "", 1)
		mutations := map[string]string{
			"empty_section": "[[source_plane_constant]]", "missing_site": "",
			"duplicate_section": section + "\n" + section, "unknown_key": section + "\nprefix = \"other\"",
		}
		for _, field := range []struct{ name, value string }{{"path", a.Path}, {"name", a.Name}, {"value", a.Value}} {
			line := field.name + " = " + strconv.Quote(field.value)
			for name, replacement := range map[string]string{
				"changed": field.name + " = " + strconv.Quote(field.value+".other"), "empty": field.name + ` = ""`, "missing": "",
				"duplicate": line + "\n" + line, "array": field.name + " = [" + strconv.Quote(field.value) + "]",
				"separator": field.name + " = " + strconv.Quote(field.value+"\x00other"),
			} {
				mutations[field.name+"/"+name] = strings.Replace(section, line, replacement, 1)
			}
		}
		for name, replacement := range mutations {
			t.Run(a.Name+"/"+name, func(t *testing.T) {
				file := filepath.Join(t.TempDir(), "lexicon.toml")
				if err := os.WriteFile(file, []byte(strings.Replace(string(original), section, replacement, 1)), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := LoadConfig(file); err == nil {
					t.Fatal("broadened or malformed driver constant binding accepted")
				}
			})
		}
	}
	file := filepath.Join(t.TempDir(), "lexicon.toml")
	if err := os.WriteFile(file, []byte(without), 0o600); err != nil {
		t.Fatal(err)
	}
	parsed, err := LoadConfig(file)
	if err != nil || len(parsed.SourcePlaneConstants) != 0 {
		t.Fatalf("absent optional set should grant nothing: %+v/%v", parsed, err)
	}
	for i, a := range cfg.SourcePlaneConstants {
		for name, edit := range map[string]func([]GoConstantAllowance) []GoConstantAllowance{
			"absent":     func(s []GoConstantAllowance) []GoConstantAllowance { return nil },
			"incomplete": func(s []GoConstantAllowance) []GoConstantAllowance { return s[:1] },
			"duplicate":  func(s []GoConstantAllowance) []GoConstantAllowance { return []GoConstantAllowance{a, a} },
			"path":       func(s []GoConstantAllowance) []GoConstantAllowance { s[i].Path += ".other"; return s },
			"name":       func(s []GoConstantAllowance) []GoConstantAllowance { s[i].Name += "Other"; return s },
			"value":      func(s []GoConstantAllowance) []GoConstantAllowance { s[i].Value += " other"; return s },
			"separator":  func(s []GoConstantAllowance) []GoConstantAllowance { s[i].Name += "\x00"; return s },
		} {
			t.Run(a.Name+"/in_memory/"+name, func(t *testing.T) {
				changed := cfg
				changed.SourcePlaneConstants = edit(append([]GoConstantAllowance{}, cfg.SourcePlaneConstants...))
				findings, err := CheckFiles(map[string][]byte{a.Path: []byte(sourcePlaneConstantFixture(a, strconv.Quote(a.Value)))}, changed)
				if err != nil || !hasFindingClass(findings, "product-term") {
					t.Fatalf("invalid in-memory binding admitted constant: %#v/%v", findings, err)
				}
			})
		}
	}
}

func TestSourcePlaneConstantsPreserveOtherPathBehavior(t *testing.T) {
	cfg, _ := checkedInSourcePlaneConstants(t)
	baseline := cfg
	baseline.SourcePlaneConstants = nil
	for _, a := range cfg.SourcePlaneConstants {
		for _, path := range []string{externalProtocolSource, "existing/other.go", "existing/notes.md"} {
			for name, quote := range map[string]func(string) string{"quoted": strconv.Quote, "escaped": escapedProtocolString} {
				t.Run(a.Name+"/"+path+"/"+name, func(t *testing.T) {
					files := map[string][]byte{path: []byte(sourcePlaneConstantFixture(a, quote(a.Value)))}
					before, beforeErr := CheckFiles(files, baseline)
					after, afterErr := CheckFiles(files, cfg)
					if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("unrelated path behavior changed: before=%#v/%v after=%#v/%v", before, beforeErr, after, afterErr)
					}
				})
			}
		}
	}
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
	original, err := readConfigDocument(path)
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

func checkedInNativeManifestConfig(t *testing.T) (Config, string) {
	t.Helper()
	cfg, path := checkedInPhysicalProtocolConfig(t)
	want := map[string]bool{
		"448bbf6c576972f4320475a0f2c84fc95020744752746eeb7bc440c488827ec7": true,
		"189e26badd76744c7e096b1d22c566d77a483176d37748a42b5c8ea3301c09ea": true,
		"8610e53a1800745f41cf493e7f8e05da1513988d330e48bffe2a65422300379e": true,
		"28eb85035ca7f533164539df882ca0584982aaad9f833d341983a6bff0fd0d33": true,
	}
	if cfg.NativeManifestStrings.Path != nativeManifestProtocolSource || len(cfg.NativeManifestStrings.Values) != len(want) {
		t.Fatal("checked-in native manifest path or set is incomplete")
	}
	for _, value := range cfg.NativeManifestStrings.Values {
		digest := sha256.Sum256([]byte(value))
		key := hex.EncodeToString(digest[:])
		if !want[key] {
			t.Fatal("checked-in native manifest literal changed or repeated")
		}
		delete(want, key)
	}
	return cfg, path
}

func TestNativeManifestProtocolAllowsOnlyExactDecodedLiterals(t *testing.T) {
	cfg, _ := checkedInNativeManifestConfig(t)
	for i, value := range cfg.NativeManifestStrings.Values {
		for name, literal := range map[string]string{
			"quoted": strconv.Quote(value), "raw": "`" + value + "`", "escaped": escapedProtocolString(value),
		} {
			t.Run(fmt.Sprintf("literal%d/%s", i, name), func(t *testing.T) {
				source := "package recorder\nvar acceptedWireValue = " + literal
				findings, err := CheckFiles(map[string][]byte{nativeManifestProtocolSource: []byte(source)}, cfg)
				if err != nil || len(findings) != 0 {
					t.Fatalf("exact native manifest literal refused: %#v, %v", findings, err)
				}
			})
		}
	}
}

func TestNativeManifestProtocolKeepsOtherRefusals(t *testing.T) {
	cfg, _ := checkedInNativeManifestConfig(t)
	identifier := cfg.Terms[3].Matches[len(cfg.Terms[3].Matches)-1]
	productTerm := cfg.Terms[2].Matches[0]
	for i, value := range cfg.NativeManifestStrings.Values {
		assignment := "var acceptedWireValue = " + strconv.Quote(value)
		for _, test := range []struct{ name, path, source string }{
			{"neighbor_path", "spec/s1-b13-native/recording_other_test.go", "package recorder\n" + assignment},
			{"wildcard_path", "spec/s1-b13-native/*.go", "package recorder\n" + assignment},
			{"other_recorder", externalProtocolSource, "package recorder\n" + assignment},
			{"wrong_path_raw", "other/recorder.go", "package recorder\nvar acceptedWireValue = `" + value + "`"},
			{"changed_case", nativeManifestProtocolSource, "package recorder\nvar acceptedWireValue = " + strconv.Quote(strings.ToUpper(value))},
			{"prefix", nativeManifestProtocolSource, "package recorder\nvar acceptedWireValue = " + strconv.Quote("other_"+value)},
			{"suffix", nativeManifestProtocolSource, "package recorder\nvar acceptedWireValue = " + strconv.Quote(value+"_other")},
			{"space", nativeManifestProtocolSource, "package recorder\nvar acceptedWireValue = " + strconv.Quote(value+" ")},
			{"changed_escaped", nativeManifestProtocolSource, "package recorder\nvar acceptedWireValue = " + escapedProtocolString(value+"_other")},
			{"extra_term", nativeManifestProtocolSource, "package recorder\nvar acceptedWireValue = " + strconv.Quote(value+" "+productTerm)},
			{"other_literal", nativeManifestProtocolSource, "package recorder\n" + assignment + "\nvar other = " + strconv.Quote(productTerm)},
			{"comment", nativeManifestProtocolSource, "package recorder\n// " + productTerm + "\n" + assignment},
			{"identifier", nativeManifestProtocolSource, "package recorder\nvar " + identifier + " = 1\n" + assignment},
			{"import", nativeManifestProtocolSource, "package recorder\nimport " + strconv.Quote(value) + "\n" + assignment},
			{"import_escaped", nativeManifestProtocolSource, "package recorder\nimport " + escapedProtocolString(value) + "\n" + assignment},
		} {
			t.Run(fmt.Sprintf("literal%d/%s", i, test.name), func(t *testing.T) {
				findings, err := CheckFiles(map[string][]byte{test.path: []byte(test.source)}, cfg)
				if err != nil || !hasFindingClass(findings, "identifier") && !hasFindingClass(findings, "product-term") {
					t.Fatalf("native manifest allowance lost ordinary refusal: %#v, %v", findings, err)
				}
			})
		}
		without := cfg
		without.NativeManifestStrings = GoStringSetAllowance{}
		findings, err := CheckFiles(map[string][]byte{nativeManifestProtocolSource: []byte("package recorder\n" + assignment)}, without)
		if err != nil || len(findings) == 0 {
			t.Fatalf("missing native manifest declaration passed: %#v, %v", findings, err)
		}
	}
}

func TestNativeManifestProtocolPreservesUnrelatedScan(t *testing.T) {
	cfg, _ := checkedInNativeManifestConfig(t)
	baseline := cfg
	baseline.NativeManifestStrings = GoStringSetAllowance{}
	literals := map[string]string{
		"product":     escapedProtocolString(cfg.Terms[2].Matches[0]),
		"unicode":     `"\U0001f600"`,
		"environment": escapedProtocolString(cfg.ExternalProtocolString.Value),
	}
	for i, value := range cfg.NativeManifestStrings.Values {
		literals[fmt.Sprintf("native%d", i)] = escapedProtocolString(value)
	}
	for _, path := range []string{"other/existing.go", "spec/s1-b13-native/recording_other_test.go", externalProtocolSource} {
		for name, literal := range literals {
			t.Run(path+"/"+name, func(t *testing.T) {
				files := map[string][]byte{path: []byte("package existing\nvar retained = " + literal)}
				before, beforeErr := CheckFiles(files, baseline)
				after, afterErr := CheckFiles(files, cfg)
				if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("native manifest extension changed unrelated scan: before=%#v/%v after=%#v/%v", before, beforeErr, after, afterErr)
				}
			})
		}
	}
	for i, value := range cfg.NativeManifestStrings.Values {
		t.Run(fmt.Sprintf("comment%d", i), func(t *testing.T) {
			files := map[string][]byte{nativeManifestProtocolSource: []byte("package existing\n// " + value)}
			before, beforeErr := CheckFiles(files, baseline)
			after, afterErr := CheckFiles(files, cfg)
			if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("native manifest extension changed comment scanning: before=%#v/%v after=%#v/%v", before, beforeErr, after, afterErr)
			}
		})
	}
}

func TestNativeManifestProtocolConfigurationRejectsBroadening(t *testing.T) {
	cfg, path := checkedInNativeManifestConfig(t)
	original, err := readConfigDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	values := cfg.NativeManifestStrings.Values
	line := func(values []string) string {
		var quoted []string
		for _, value := range values {
			quoted = append(quoted, strconv.Quote(value))
		}
		return "values = [" + strings.Join(quoted, ", ") + "]"
	}
	pathLine := "path = " + strconv.Quote(nativeManifestProtocolSource)
	valuesLine := line(values)
	section := "[native_manifest_strings]\n" + pathLine + "\n" + valuesLine
	if !strings.Contains(string(original), section) {
		t.Fatal("native manifest section was not bound")
	}
	for _, test := range []struct{ name, replacement string }{
		{"neighbor_path", strings.Replace(section, nativeManifestProtocolSource, "spec/s1-b13-native/recording_other_test.go", 1)},
		{"wildcard_path", strings.Replace(section, nativeManifestProtocolSource, "spec/s1-b13-native/*.go", 1)},
		{"empty_path", strings.Replace(section, pathLine, `path = ""`, 1)},
		{"path_array", strings.Replace(section, pathLine, "path = ["+strconv.Quote(nativeManifestProtocolSource)+"]", 1)},
		{"missing_path", strings.Replace(section, pathLine+"\n", "", 1)},
		{"missing_values", strings.Replace(section, "\n"+valuesLine, "", 1)},
		{"empty_values", strings.Replace(section, valuesLine, line(nil), 1)},
		{"missing_literal", strings.Replace(section, valuesLine, line(values[:3]), 1)},
		{"duplicate_literal", strings.Replace(section, valuesLine, line([]string{values[0], values[1], values[2], values[2]}), 1)},
		{"extra_literal", strings.Replace(section, valuesLine, line(append(append([]string{}, values...), "other")), 1)},
		{"mutated_literal", strings.Replace(section, valuesLine, line([]string{values[0] + "_other", values[1], values[2], values[3]}), 1)},
		{"literal_pattern", strings.Replace(section, valuesLine, line([]string{"*" + values[0] + "*", values[1], values[2], values[3]}), 1)},
		{"scalar_values", strings.Replace(section, valuesLine, "values = "+strconv.Quote(values[0]), 1)},
		{"unknown_key", section + "\npattern = \"*\""},
		{"duplicate_path", section + "\n" + pathLine},
		{"duplicate_values", section + "\n" + valuesLine},
		{"duplicate_section", section + "\n" + section},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "lexicon.toml")
			if err := os.WriteFile(file, []byte(strings.Replace(string(original), section, test.replacement, 1)), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(file); err == nil {
				t.Fatal("malformed or broadened native manifest configuration passed")
			}
		})
	}
}

func checkedInNativeControlConfig(t *testing.T) (Config, string) {
	t.Helper()
	path := filepath.Join("..", "..", filepath.FromSlash(rulesDirectory))
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		path = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), filepath.FromSlash(rulesDirectory))
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("checked-in native control config: %v", err)
	}
	if cfg.NativeRecordingLiterals.Path != nativeRecordingLiteralSource || len(cfg.NativeRecordingLiterals.Values) != len(nativeRecordingLiteralDigests) {
		t.Fatalf("recording allowance is incomplete: %+v", cfg.NativeRecordingLiterals)
	}
	if cfg.NativeViewStrings.Path != nativeViewProtocolSource || len(cfg.NativeViewStrings.Values) != len(nativeViewProtocolDigests) {
		t.Fatalf("view allowance is incomplete: %+v", cfg.NativeViewStrings)
	}
	return cfg, path
}

func escapedNativeLiteral(value string) string {
	return "\"\\x" + hex.EncodeToString([]byte(value[:1])) + value[1:] + "\""
}

func TestNativeControlLiteralsAllowOnlyExactDecodedValues(t *testing.T) {
	cfg, _ := checkedInNativeControlConfig(t)
	for _, allowance := range []NativeControlAllowance{cfg.NativeRecordingLiterals, cfg.NativeViewStrings} {
		for i, value := range allowance.Values {
			for name, literal := range map[string]string{
				"quoted":  strconv.Quote(value),
				"raw":     "`" + value + "`",
				"escaped": escapedNativeLiteral(value),
			} {
				t.Run(fmt.Sprintf("%s/value%d/%s", allowance.Path, i, name), func(t *testing.T) {
					findings, err := CheckFiles(map[string][]byte{allowance.Path: []byte("package recorder\nvar externalValue = " + literal)}, cfg)
					if err != nil || len(findings) != 0 {
						t.Fatalf("exact decoded native control literal was not admitted: %#v, %v", findings, err)
					}
				})
			}
		}
	}
}

func TestNativeControlLiteralsKeepOtherRefusals(t *testing.T) {
	cfg, _ := checkedInNativeControlConfig(t)
	productWord := string([]byte{119, 105, 115, 112})
	for _, allowance := range []NativeControlAllowance{cfg.NativeRecordingLiterals, cfg.NativeViewStrings} {
		for i, value := range allowance.Values {
			assignment := "var externalValue = " + strconv.Quote(value)
			for _, test := range []struct {
				name, path, source string
			}{
				{"neighbor_path", strings.TrimSuffix(allowance.Path, ".go") + "_other.go", "package recorder\n" + assignment},
				{"changed_value", allowance.Path, "package recorder\nvar externalValue = " + strconv.Quote(value+"_other")},
				{"changed_escaped", allowance.Path, "package recorder\nvar externalValue = " + escapedNativeLiteral(value+"_other")},
				{"comment", allowance.Path, "package recorder\n// " + productWord + " " + value},
				{"identifier", allowance.Path, "package recorder\nvar " + productWord + " = 1\n" + assignment},
				{"import", allowance.Path, "package recorder\nimport " + strconv.Quote(value) + "\n" + assignment},
				{"escaped_import", allowance.Path, "package recorder\nimport " + escapedNativeLiteral(value) + "\n" + assignment},
				{"structured_json", "spec/s1-b21-b24/native_testdata/protocol.json", "{\"value\":" + strconv.Quote(value) + ",\"other\":" + strconv.Quote(productWord) + "}"},
			} {
				t.Run(fmt.Sprintf("%s/value%d/%s", allowance.Path, i, test.name), func(t *testing.T) {
					findings, err := CheckFiles(map[string][]byte{test.path: []byte(test.source)}, cfg)
					if err != nil || len(findings) == 0 {
						t.Fatalf("native control refusal was lost: %#v, %v", findings, err)
					}
				})
			}
		}
	}
	without := cfg
	without.NativeRecordingLiterals = NativeControlAllowance{}
	without.NativeViewStrings = NativeControlAllowance{}
	for _, allowance := range []NativeControlAllowance{cfg.NativeRecordingLiterals, cfg.NativeViewStrings} {
		for i, value := range allowance.Values {
			source := []byte("package recorder\n// " + value)
			before, beforeErr := CheckFiles(map[string][]byte{allowance.Path: source}, without)
			after, afterErr := CheckFiles(map[string][]byte{allowance.Path: source}, cfg)
			if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("comment baseline changed for %s/value%d: before=%#v/%v after=%#v/%v", allowance.Path, i, before, beforeErr, after, afterErr)
			}
		}
	}
	for _, value := range cfg.NativeRecordingLiterals.Values {
		if allowsNativeControlLiteral(nativeViewProtocolSource, value, cfg) {
			continue
		}
		findings, err := CheckFiles(map[string][]byte{nativeViewProtocolSource: []byte("package recorder\nvar externalValue = " + strconv.Quote(value))}, cfg)
		if err != nil || len(findings) == 0 {
			t.Fatalf("recording-only literal leaked into the views allowance: %#v, %v", findings, err)
		}
	}
}

func TestNativeControlLiteralsPreserveOutsidePathBaseline(t *testing.T) {
	cfg, _ := checkedInNativeControlConfig(t)
	productValue := string([]byte{119, 105, 115, 112, 32, 111, 110, 108, 121})
	without := cfg
	without.NativeRecordingLiterals = NativeControlAllowance{}
	without.NativeViewStrings = NativeControlAllowance{}
	for _, path := range []string{
		"other/existing.go",
		"spec/s1-b21-b24/native_recording_other_test.go",
		"spec/s1-b13-native/recording_test.go",
		externalProtocolSource,
	} {
		files := map[string][]byte{path: []byte("package existing\nvar retained = " + escapedNativeLiteral(productValue))}
		before, beforeErr := CheckFiles(files, without)
		after, afterErr := CheckFiles(files, cfg)
		if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("allowance changed unrelated path %s: before=%#v/%v after=%#v/%v", path, before, beforeErr, after, afterErr)
		}
	}
}

func TestNativeControlLiteralConfigurationRejectsBroadening(t *testing.T) {
	cfg, path := checkedInNativeControlConfig(t)
	originalBytes, err := readConfigDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	original := string(originalBytes)
	for _, set := range []struct {
		name, section string
		allowance     NativeControlAllowance
	}{
		{"recording", "native_recording_literals", cfg.NativeRecordingLiterals},
		{"views", "native_view_strings", cfg.NativeViewStrings},
	} {
		section := "[" + set.section + "]\npath = " + strconv.Quote(set.allowance.Path) + "\nvalues = ["
		for i, value := range set.allowance.Values {
			if i != 0 {
				section += ", "
			}
			section += strconv.Quote(value)
		}
		section += "]"
		if !strings.Contains(original, section) {
			t.Fatalf("checked-in %s section was not found", set.name)
		}
		mutated := append([]string(nil), set.allowance.Values...)
		mutated[0] += "_other"
		duplicate := append([]string(nil), set.allowance.Values...)
		duplicate[len(duplicate)-1] = duplicate[0]
		line := func(values []string) string {
			quoted := make([]string, 0, len(values))
			for _, value := range values {
				quoted = append(quoted, strconv.Quote(value))
			}
			return "values = [" + strings.Join(quoted, ", ") + "]"
		}
		for _, test := range []struct {
			name, replacement string
		}{
			{"neighbor_path", strings.Replace(section, set.allowance.Path, strings.TrimSuffix(set.allowance.Path, ".go")+"_other.go", 1)},
			{"wildcard_path", strings.Replace(section, set.allowance.Path, strings.TrimSuffix(set.allowance.Path, ".go")+"/*.go", 1)},
			{"missing_path", strings.Replace(section, "path = "+strconv.Quote(set.allowance.Path)+"\n", "", 1)},
			{"missing_values", strings.Replace(section, "\n"+line(set.allowance.Values), "", 1)},
			{"missing_literal", strings.Replace(section, line(set.allowance.Values), line(set.allowance.Values[:len(set.allowance.Values)-1]), 1)},
			{"duplicate_literal", strings.Replace(section, line(set.allowance.Values), line(duplicate), 1)},
			{"extra_literal", strings.Replace(section, line(set.allowance.Values), line(append(append([]string(nil), set.allowance.Values...), "other")), 1)},
			{"mutated_literal", strings.Replace(section, line(set.allowance.Values), line(mutated), 1)},
			{"scalar_values", strings.Replace(section, line(set.allowance.Values), "values = "+strconv.Quote(set.allowance.Values[0]), 1)},
			{"unknown_key", section + "\npattern = \"*\""},
			{"duplicate_path", section + "\npath = " + strconv.Quote(set.allowance.Path)},
			{"duplicate_values", section + "\n" + line(set.allowance.Values)},
			{"duplicate_section", section + "\n" + section},
		} {
			t.Run(set.name+"/"+test.name, func(t *testing.T) {
				file := filepath.Join(t.TempDir(), "lexicon.toml")
				if err := os.WriteFile(file, []byte(strings.Replace(original, section, test.replacement, 1)), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := LoadConfig(file); err == nil {
					t.Fatal("broadened or malformed native control configuration was accepted")
				}
			})
		}
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
	path := filepath.Join(root, rulesDirectory)
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "rules.toml"), []byte("[[term]]\nmatch = [\"x\"]\nclass = \"ban\"\n[zones]\nprovenance = [\"NOTICE.md\"]\n"), 0o644); err != nil {
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

func TestDefaultPolicyBindsOriginalPublicReadbackReferences(t *testing.T) {
	for _, name := range []string{
		"tools/roundtrip/testdata/fresh-native/recipe-profile.json",
		"tools/roundtrip/testdata/fresh-native/beads-ef3f15.tar.gz",
		"tools/roundtrip/testdata/fresh-native/beads-ef3f15.git-tree",
		"tools/roundtrip/testdata/typed-reader/beads-e25cf4.tar.gz",
		"tools/roundtrip/testdata/typed-reader/beads-e25cf4.git-tree",
		"tools/roundtrip/testdata/readback-8f19/memory-complete-beads-raw-readback.tar",
		"tools/roundtrip/testdata/readback-8f19/memory-partial-beads-raw-readback.tar",
		"tools/roundtrip/testdata/readback-8f19/memory-complete-reverse-live.jsonl",
		"tools/roundtrip/testdata/readback-8f19/memory-partial-reverse-live.jsonl",
	} {
		t.Run(filepath.Base(name), func(t *testing.T) {
			file := filepath.Join("..", "..", filepath.FromSlash(name))
			if root := os.Getenv("TEST_SRCDIR"); root != "" {
				file = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), filepath.FromSlash(name))
			}
			content, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			cfg := testConfig()
			findings, err := CheckFilesWithPolicy(map[string][]byte{name: content}, cfg, DefaultTreePolicy())
			if err != nil || len(findings) != 0 {
				t.Fatalf("original declared bytes refused: %#v, %v", findings, err)
			}
			t.Run("changed-bytes", func(t *testing.T) {
				changed := append(append([]byte(nil), content...), '\n')
				if _, err := CheckFilesWithPolicy(map[string][]byte{name: changed}, cfg, DefaultTreePolicy()); err == nil || !strings.Contains(err.Error(), "SHA-256") {
					t.Fatalf("changed reference did not fail its hash binding: %v", err)
				}
			})
			t.Run("missing-binding", func(t *testing.T) {
				policy := DefaultTreePolicy()
				delete(policy.ReferenceSHA256, name)
				delete(policy.BinarySHA256, name)
				findings, err := CheckFilesWithPolicy(map[string][]byte{name: content}, cfg, policy)
				if err == nil && len(findings) == 0 {
					t.Fatal("reference without an exact hash binding was admitted")
				}
			})
			for _, adjacent := range []string{
				name + ".copy",
				filepath.ToSlash(filepath.Join(filepath.Dir(name), "adjacent"+filepath.Ext(name))),
				"tools/roundtrip/copied_reference.go",
			} {
				t.Run(adjacent, func(t *testing.T) {
					policy := DefaultTreePolicy()
					disposition, err := policy.disposition(adjacent, content)
					if err == nil && disposition != dispositionScan {
						t.Fatal("copied reference gained an immutable path exemption")
					}
					// Identifier terms apply to Go. A copied JSON profile must
					// scan normally, and still refuse a product term, rather
					// than inventing an identifier ban for JSON documents.
					if filepath.Base(name) == "recipe-profile.json" && filepath.Ext(adjacent) == ".json" {
						probe, err := json.Marshal(map[string]any{"reference": json.RawMessage(content), "scan_control": cfg.Terms[1].Matches[0]})
						if err != nil {
							t.Fatal(err)
						}
						findings, err := CheckFilesWithPolicy(map[string][]byte{adjacent: probe}, cfg, policy)
						if err != nil || len(findings) == 0 {
							t.Fatalf("copied JSON reference bypassed normal scanning: %#v %v", findings, err)
						}
						return
					}
					findings, err := CheckFilesWithPolicy(map[string][]byte{adjacent: content}, cfg, policy)
					if err == nil && len(findings) == 0 {
						t.Fatal("reference bytes gained admission at another path")
					}
				})
			}
		})
	}
}
