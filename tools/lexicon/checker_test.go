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

func TestNativeFieldConfigurationRejectsBroadeningAndAmbiguity(t *testing.T) {
	cfg, path := checkedInNativeFieldConfig(t)
	original, err := os.ReadFile(path)
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
	original, err := os.ReadFile(path)
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
