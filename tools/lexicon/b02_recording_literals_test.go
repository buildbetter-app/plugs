package lexicon

import (
	"strconv"
	"strings"
	"testing"
)

// This small parser fixture exercises selection independently of the frozen
// recorder body. The production wrapper has its own immutable binding; its
// actual source is exercised by the combined B02 tree gate.
func b02LiteralFixture(t *testing.T) (string, string, b02RecordingBinding) {
	t.Helper()
	cfg, _ := checkedInSourcePlaneConstants(t)
	var value string
	for _, a := range cfg.SourcePlaneConstants {
		if protocolLiteralDigest(a.Value) == "a250fe4bfeccf35cd1f34c8eb7625ef05127294cdeb6884a04adea45796adebc" {
			value = a.Value
		}
	}
	if value == "" {
		t.Fatal("canonical physical table binding is absent")
	}
	literal := strconv.Quote(value)
	body := "func b02ObserveNative() {\n" +
		"for _, table := range []string{\"issues\", \"wisps\", \"dependencies\", " + literal + "} { _ = table }\n" +
		"for _, table := range []string{\"dependencies\", " + literal + "} { _ = table }\n" +
		"if table == " + literal + " { checkEndpointPlane() }\n}"
	binding := b02RecordingBinding{contextSHA256: protocolLiteralDigest(body), sites: make([]b02RecordingSite, 3)}
	rest, offset := body, 0
	for i := range binding.sites {
		next := strings.Index(rest, literal)
		if next < 0 {
			t.Fatal("parser fixture lacks a selected site")
		}
		binding.sites[i] = b02RecordingSite{offset + next, protocolLiteralDigest(value)}
		offset += next + len(literal)
		rest = body[offset:]
	}
	return "package recording\n" + body + "\n", value, binding
}

func TestB02RecordingLiteralSelectionIsExactlyThreeSites(t *testing.T) {
	source, value, binding := b02LiteralFixture(t)
	for name, content := range map[string]string{
		"exact":                   source,
		"outside_context_spacing": "\n\n" + source + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			selected := selectB02RecordingLiterals(b02NativeRecordingSource, []byte(content), binding)
			if len(selected) != 3 {
				t.Fatalf("expected exactly three nodes, got %v", selected)
			}
			for offset := range selected {
				if !strings.HasPrefix(content[offset:], strconv.Quote(value)) {
					t.Fatal("selected a different AST node")
				}
			}
		})
	}
	if len(b02RecordingLiteralOffsets(b02NativeRecordingSource, []byte(source))) != 0 {
		t.Fatal("synthetic context must not replace the frozen production recorder")
	}
}

func TestB02RecordingLiteralSelectionRejectsChangedBindings(t *testing.T) {
	source, value, binding := b02LiteralFixture(t)
	literal := strconv.Quote(value)
	for _, test := range []struct{ name, path, source string }{
		{"wrong_path", "spec/cardb20/other_test.go", source},
		{"sibling_path", b02NativeRecordingSource + ".other.go", source},
		{"wildcard_path", "spec/cardb20/*.go", source},
		{"wrong_function", b02NativeRecordingSource, strings.Replace(source, "b02ObserveNative", "otherObserver", 1)},
		{"changed_context", b02NativeRecordingSource, strings.Replace(source, "checkEndpointPlane", "otherCheck", 1)},
		{"changed_value", b02NativeRecordingSource, strings.Replace(source, literal, strconv.Quote(value+"_other"), 1)},
		{"missing_site", b02NativeRecordingSource, strings.Replace(source, literal, `"other"`, 1)},
		{"extra_site_inside", b02NativeRecordingSource, strings.Replace(source, "checkEndpointPlane()", "checkEndpointPlane("+literal+")", 1)},
		{"split_value", b02NativeRecordingSource, strings.Replace(source, literal, literal+` + ""`, 1)},
		{"parenthesized_value", b02NativeRecordingSource, strings.Replace(source, literal, "("+literal+")", 1)},
		{"duplicate_function", b02NativeRecordingSource, source + strings.TrimPrefix(source, "package recording\n")},
		{"duplicate_variable", b02NativeRecordingSource, source + "var b02ObserveNative = 1\n"},
		{"duplicate_type", b02NativeRecordingSource, source + "type b02ObserveNative int\n"},
		{"duplicate_constant", b02NativeRecordingSource, source + "const b02ObserveNative = 1\n"},
		{"malformed", b02NativeRecordingSource, source + "func broken("},
		{"receiver", b02NativeRecordingSource, strings.Replace(source, "func b02ObserveNative", "func (r *recorder) b02ObserveNative", 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if selected := selectB02RecordingLiterals(test.path, []byte(test.source), binding); len(selected) != 0 {
				t.Fatalf("changed binding admitted nodes: %v", selected)
			}
		})
	}
	for _, field := range []string{"context_digest", "value_digest", "site_offset", "duplicate_offset"} {
		t.Run(field, func(t *testing.T) {
			changed := binding
			changed.sites = append([]b02RecordingSite(nil), binding.sites...)
			switch field {
			case "context_digest":
				changed.contextSHA256 = strings.Repeat("0", 64)
			case "value_digest":
				changed.sites[0].valueSHA256 = strings.Repeat("0", 64)
			case "site_offset":
				changed.sites[0].offset++
			case "duplicate_offset":
				changed.sites[0].offset = changed.sites[1].offset
			}
			if selected := selectB02RecordingLiterals(b02NativeRecordingSource, []byte(source), changed); len(selected) != 0 {
				t.Fatal("changed digest/offset admitted a node")
			}
		})
	}
}

func TestB02RecordingLiteralAdmissionKeepsOtherContentScanning(t *testing.T) {
	source, value, binding := b02LiteralFixture(t)
	literal := strconv.Quote(value)
	for name, content := range map[string]string{
		"sibling_literal":   source + "var copied = " + literal + "\n",
		"line_comment":      source + "// " + value + "\n",
		"block_comment":     source + "/* " + value + " */\n",
		"identifier":        source + "var " + value + " = 1\n",
		"adjacent_function": source + "func adjacent() { println(" + literal + ") }\n",
		"import":            strings.Replace(source, "package recording\n", "package recording\nimport "+literal+"\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			selected := selectB02RecordingLiterals(b02NativeRecordingSource, []byte(content), binding)
			if len(selected) != 3 {
				t.Fatal("outside content unexpectedly changed selection")
			}
			masked := []byte(content)
			for _, node := range goStringLiterals(b02NativeRecordingSource, masked) {
				if selected[node.start] {
					// Retain valid Go so an invalid-go finding cannot satisfy this
					// control instead of the unrelated content's actual violation.
					copy(masked[node.start:node.end], []byte(`""`+strings.Repeat(" ", node.end-node.start-2)))
				}
			}
			cfg, _ := checkedInSourcePlaneConstants(t)
			findings, err := CheckFiles(map[string][]byte{b02NativeRecordingSource: masked}, cfg)
			if err != nil || hasFindingClass(findings, "invalid-go") || !hasFindingClass(findings, "product-term") {
				t.Fatalf("unrelated physical term stopped scanning: %v, %v", findings, err)
			}
		})
	}
	cfg, _ := checkedInSourcePlaneConstants(t)
	for _, a := range cfg.SourcePlaneConstants {
		canonical := sourcePlaneConstantFixture(a, strconv.Quote(a.Value))
		findings, err := CheckFiles(map[string][]byte{a.Path: []byte(canonical)}, cfg)
		if err != nil || len(findings) != 0 {
			t.Fatalf("existing singleton changed: %v, %v", findings, err)
		}
		findings, err = CheckFiles(map[string][]byte{b02NativeRecordingSource: []byte(canonical)}, cfg)
		if err != nil || len(findings) == 0 {
			t.Fatal("singleton admission leaked to the new path")
		}
	}
}

func b02FiveLiteralFixture(t *testing.T) (string, string, string, b02RecordingBinding) {
	t.Helper()
	source, tableValue, binding := b02LiteralFixture(t)
	cfg, _ := checkedInSourcePlaneConstants(t)
	var columnValue string
	for _, value := range cfg.ExternalProtocolString.PhysicalValues {
		if protocolLiteralDigest(value) == "7853d42458d77d5bd188bd1a1bba51e87257cbe5ec12826168bc639f95bb0a72" {
			columnValue = value
		}
	}
	if columnValue == "" {
		t.Fatal("canonical physical column binding is absent")
	}
	literal := strconv.Quote(columnValue)
	source = strings.TrimSuffix(source, "}\n") +
		"for _, column := range []string{\"depends_on_issue_id\", " + literal + "} { _ = column }\n" +
		"if targetPlane == " + literal + " { checkTargetPlane() }\n}\n"
	start := strings.Index(source, "func b02ObserveNative")
	body := strings.TrimSuffix(source[start:], "\n")
	binding.contextSHA256 = protocolLiteralDigest(body)
	offset := 0
	for i := 0; i < 2; i++ {
		next := strings.Index(body[offset:], literal)
		if next < 0 {
			t.Fatal("parser fixture lacks a column node")
		}
		offset += next
		binding.sites = append(binding.sites, b02RecordingSite{offset, protocolLiteralDigest(columnValue)})
		offset += len(literal)
	}
	return source, tableValue, columnValue, binding
}

func TestB02RecordingLiteralSelectionIncludesExactlyTwoColumnSites(t *testing.T) {
	source, tableValue, columnValue, binding := b02FiveLiteralFixture(t)
	selected := selectB02RecordingLiterals(b02NativeRecordingSource, []byte(source), binding)
	if len(selected) != 5 {
		t.Fatalf("expected three table and two column nodes, got %v", selected)
	}
	counts := map[string]int{}
	for _, literal := range goStringLiterals(b02NativeRecordingSource, []byte(source)) {
		if selected[literal.start] {
			counts[literal.value]++
		}
	}
	if counts[tableValue] != 3 || counts[columnValue] != 2 || len(counts) != 2 {
		t.Fatalf("table and column bindings mixed or broadened: %v", counts)
	}
	if len(b02RecordingLiteralOffsets(b02NativeRecordingSource, []byte(source))) != 0 {
		t.Fatal("synthetic postimage must not replace the full frozen recorder")
	}
	literal := strconv.Quote(columnValue)
	for name, content := range map[string]string{
		"changed_column":             strings.Replace(source, literal, strconv.Quote(columnValue+"_other"), 1),
		"missing_column":             strings.Replace(source, literal, `"other"`, 1),
		"extra_column_inside":        strings.Replace(source, "checkTargetPlane()", "checkTargetPlane("+literal+")", 1),
		"table_value_in_column_site": strings.Replace(source, literal, strconv.Quote(tableValue), 1),
		"column_value_in_table_site": strings.Replace(source, strconv.Quote(tableValue), literal, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if got := selectB02RecordingLiterals(b02NativeRecordingSource, []byte(content), binding); len(got) != 0 {
				t.Fatalf("changed column context admitted %v", got)
			}
		})
	}
	for _, field := range []string{"column_digest", "column_offset", "column_table_offset_collision", "empty_digest", "negative_offset"} {
		t.Run(field, func(t *testing.T) {
			changed := binding
			changed.sites = append([]b02RecordingSite(nil), binding.sites...)
			switch field {
			case "column_digest":
				changed.sites[3].valueSHA256 = protocolLiteralDigest(tableValue)
			case "column_offset":
				changed.sites[3].offset++
			case "column_table_offset_collision":
				changed.sites[3].offset = changed.sites[0].offset
			case "empty_digest":
				changed.sites[3].valueSHA256 = ""
			case "negative_offset":
				changed.sites[3].offset = -1
			}
			if got := selectB02RecordingLiterals(b02NativeRecordingSource, []byte(source), changed); len(got) != 0 {
				t.Fatal("changed column binding admitted a node")
			}
		})
	}
}

func TestB02FiveLiteralAdmissionKeepsAdditionalColumnsAndContextsScanning(t *testing.T) {
	source, _, value, binding := b02FiveLiteralFixture(t)
	literal := strconv.Quote(value)
	for name, content := range map[string]string{
		"sixth_column_literal":    source + "var copiedColumn = " + literal + "\n",
		"column_comment":          source + "// " + value + "\n",
		"column_identifier":       source + "var " + value + " = 1\n",
		"adjacent_column_context": source + "func adjacent() { println(" + literal + ") }\n",
		"column_import":           strings.Replace(source, "package recording\n", "package recording\nimport "+literal+"\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			selected := selectB02RecordingLiterals(b02NativeRecordingSource, []byte(content), binding)
			if len(selected) != 5 {
				t.Fatal("outside content changed the five context-bound nodes")
			}
			masked := []byte(content)
			for _, node := range goStringLiterals(b02NativeRecordingSource, masked) {
				if selected[node.start] {
					copy(masked[node.start:node.end], []byte(`""`+strings.Repeat(" ", node.end-node.start-2)))
				}
			}
			cfg, _ := checkedInSourcePlaneConstants(t)
			findings, err := CheckFiles(map[string][]byte{b02NativeRecordingSource: masked}, cfg)
			if err != nil || hasFindingClass(findings, "invalid-go") || !hasFindingClass(findings, "product-term") {
				t.Fatalf("additional column content stopped scanning: %v, %v", findings, err)
			}
		})
	}
}
