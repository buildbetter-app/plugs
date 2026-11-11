package lexicon

import (
	"strconv"
	"strings"
	"testing"
)

func b07RepresentedValue(t *testing.T) (Config, string) {
	t.Helper()
	cfg, _ := checkedInPhysicalProtocolConfig(t)
	for _, value := range cfg.ExternalProtocolString.PhysicalValues {
		if protocolLiteralDigest(value) == "7853d42458d77d5bd188bd1a1bba51e87257cbe5ec12826168bc639f95bb0a72" {
			return cfg, value
		}
	}
	t.Fatal("canonical represented column binding is absent")
	return cfg, ""
}

// Parser controls use small contexts; they cannot replace either frozen
// production context. The combined consumer gate checks the actual source.
func b07RepresentedFixture(t *testing.T, path string) (string, string, b07RepresentedBinding) {
	t.Helper()
	_, value := b07RepresentedValue(t)
	keys := `"id", "issue_id", "depends_on_id", "depends_on_issue_id", ` + strconv.Quote(value) + `, "depends_on_external", "type", "created_at", "created_by"`
	var function, body string
	switch path {
	case b07RepresentedModelSource:
		function = "ValidateNativeExtensions"
		body = "func " + function + "(values map[string]string) error {\n" +
			"for key := range values { switch strings.TrimPrefix(key, \"beads.\") { case " + keys + ": return representedFieldError(key) } }\nreturn nil\n}"
	case b07RepresentedControlSource:
		function = "b07OpaquePublic"
		body = "func " + function + "() {\nfor _, field := range []string{" + keys + "} {\n" +
			"before := snapshot()\nrequireInvalidParams(field)\nrequireSameState(before)\n}\n}"
	default:
		t.Fatal("unknown parser fixture path")
	}
	binding := b07RepresentedBinding{
		path: path, function: function, contextSHA256: protocolLiteralDigest(body),
		valueSHA256: protocolLiteralDigest(value), offset: strings.Index(body, strconv.Quote(value)),
	}
	if binding.offset < 0 {
		t.Fatal("fixture lacks the represented column")
	}
	return "package control\n" + body + "\n", value, binding
}

func TestB07RepresentedLiteralSelectionIsOnePerContext(t *testing.T) {
	for _, path := range []string{b07RepresentedModelSource, b07RepresentedControlSource} {
		t.Run(path, func(t *testing.T) {
			source, value, binding := b07RepresentedFixture(t, path)
			for _, content := range []string{source, "\n\n" + source + "\n"} {
				selected := selectB07RepresentedLiteral(path, []byte(content), binding)
				if len(selected) != 1 {
					t.Fatalf("expected one node, got %v", selected)
				}
				for offset := range selected {
					if !strings.HasPrefix(content[offset:], strconv.Quote(value)) {
						t.Fatal("selected a different AST node")
					}
				}
			}
			if len(b07RepresentedLiteralOffsets(path, []byte(source))) != 0 {
				t.Fatal("synthetic context replaced an immutable production binding")
			}
		})
	}
}

func TestB07RepresentedLiteralSelectionRefusesChangedSource(t *testing.T) {
	for _, path := range []string{b07RepresentedModelSource, b07RepresentedControlSource} {
		t.Run(path, func(t *testing.T) {
			source, value, binding := b07RepresentedFixture(t, path)
			literal := strconv.Quote(value)
			for _, test := range []struct{ name, path, source string }{
				{"wrong_path", "other/opaque_test.go", source},
				{"sibling_path", path + ".other.go", source},
				{"wrong_function", path, strings.Replace(source, binding.function, "otherFunction", 1)},
				{"changed_neighbor", path, strings.Replace(source, `"depends_on_issue_id"`, `"other_field"`, 1)},
				{"changed_value", path, strings.Replace(source, literal, strconv.Quote(value+"_other"), 1)},
				{"missing_site", path, strings.Replace(source, literal, `"other"`, 1)},
				{"extra_site", path, strings.Replace(source, literal, literal+", "+literal, 1)},
				{"split_expression", path, strings.Replace(source, literal, literal+` + ""`, 1)},
				{"parenthesized", path, strings.Replace(source, literal, "("+literal+")", 1)},
				{"changed_context", path, strings.Replace(source, "\n}", "\nprintln(1)\n}", 1)},
				{"receiver", path, strings.Replace(source, "func "+binding.function, "func (r *reader) "+binding.function, 1)},
				{"duplicate_function", path, source + strings.TrimPrefix(source, "package control\n")},
				{"duplicate_variable", path, source + "var " + binding.function + " = 1\n"},
				{"duplicate_constant", path, source + "const " + binding.function + " = 1\n"},
				{"duplicate_type", path, source + "type " + binding.function + " int\n"},
				{"duplicate_method", path, source + "func (r *reader) " + binding.function + "() {}\n"},
				{"malformed", path, source + "func broken("},
			} {
				t.Run(test.name, func(t *testing.T) {
					if got := selectB07RepresentedLiteral(test.path, []byte(test.source), binding); len(got) != 0 {
						t.Fatalf("changed source admitted a node: %v", got)
					}
				})
			}
			for _, field := range []string{"context_digest", "value_digest", "empty_digest", "offset", "negative_offset", "function", "path"} {
				t.Run(field, func(t *testing.T) {
					changed := binding
					switch field {
					case "context_digest":
						changed.contextSHA256 = strings.Repeat("0", 64)
					case "value_digest":
						changed.valueSHA256 = strings.Repeat("0", 64)
					case "empty_digest":
						changed.valueSHA256 = ""
					case "offset":
						changed.offset++
					case "negative_offset":
						changed.offset = -1
					case "function":
						changed.function = "otherFunction"
					case "path":
						changed.path = "other/opaque_test.go"
					}
					if got := selectB07RepresentedLiteral(path, []byte(source), changed); len(got) != 0 {
						t.Fatal("changed binding admitted a node")
					}
				})
			}
		})
	}
}

func TestB07RepresentedLiteralSelectionKeepsUnrelatedContentScanning(t *testing.T) {
	for _, path := range []string{b07RepresentedModelSource, b07RepresentedControlSource} {
		t.Run(path, func(t *testing.T) {
			source, value, binding := b07RepresentedFixture(t, path)
			literal := strconv.Quote(value)
			for name, content := range map[string]string{
				"sibling_literal":   source + "var copied = " + literal + "\n",
				"line_comment":      source + "// " + value + "\n",
				"block_comment":     source + "/* " + value + " */\n",
				"identifier":        source + "var " + value + " = 1\n",
				"adjacent_function": source + "func adjacent() { println(" + literal + ") }\n",
				"import":            strings.Replace(source, "package control\n", "package control\nimport "+literal+"\n", 1),
			} {
				t.Run(name, func(t *testing.T) {
					selected := selectB07RepresentedLiteral(path, []byte(content), binding)
					if len(selected) != 1 {
						t.Fatal("outside content changed selection")
					}
					masked := []byte(content)
					for _, node := range goStringLiterals(path, masked) {
						if selected[node.start] {
							copy(masked[node.start:node.end], []byte(`""`+strings.Repeat(" ", node.end-node.start-2)))
						}
					}
					cfg, _ := b07RepresentedValue(t)
					findings, err := CheckFiles(map[string][]byte{path: masked}, cfg)
					if err != nil || hasFindingClass(findings, "invalid-go") || !hasFindingClass(findings, "product-term") {
						t.Fatalf("unrelated content stopped scanning: %v, %v", findings, err)
					}
				})
			}
		})
	}
}

// This reconstructs the actual model function byte for byte from its frozen
// source, with the field value read from the existing canonical protocol set.
// It exercises the production wrapper and scanning hooks without a new rule.
func b07FrozenRepresentedModel(t *testing.T) (Config, string, string) {
	t.Helper()
	cfg, value := b07RepresentedValue(t)
	body := "func ValidateNativeExtensions(values map[string]string) error {\n" +
		"\tfor key, token := range values {\n" +
		"\t\tif !strings.HasPrefix(key, \"beads.\") || len(key) == len(\"beads.\") || !utf8.ValidString(key) {\n" +
		"\t\t\treturn fmt.Errorf(\"nativeExtensions[%q] requires a nonempty beads.<field> key\", key)\n\t\t}\n" +
		"\t\tswitch strings.TrimPrefix(key, \"beads.\") {\n" +
		"\t\tcase \"id\", \"issue_id\", \"depends_on_id\", \"depends_on_issue_id\", " + strconv.Quote(value) + ", \"depends_on_external\", \"type\", \"created_at\", \"created_by\":\n" +
		"\t\t\treturn fmt.Errorf(\"nativeExtensions[%q] cannot override a represented edge field\", key)\n\t\t}\n" +
		"\t\tif !utf8.ValidString(token) || !json.Valid([]byte(token)) {\n" +
		"\t\t\treturn fmt.Errorf(\"nativeExtensions[%q] requires one valid UTF-8 JSON token\", key)\n\t\t}\n\t}\n\treturn nil\n}"
	if protocolLiteralDigest(body) != "dbfa30004d954e236d2440721260d0876f751058c5103b0c62cb49db4fde23d4" {
		t.Fatal("model fixture differs from the immutable refusal function")
	}
	return cfg, "package model\n\n" + body + "\n", value
}

func TestB07RepresentedProductionBindingAndResidualScanning(t *testing.T) {
	cfg, source, value := b07FrozenRepresentedModel(t)
	if selected := b07RepresentedLiteralOffsets(b07RepresentedModelSource, []byte(source)); len(selected) != 1 {
		t.Fatalf("actual model context was not selected: %v", selected)
	}
	findings, err := CheckFiles(map[string][]byte{b07RepresentedModelSource: []byte(source)}, cfg)
	if err != nil || len(findings) != 0 {
		t.Fatalf("actual represented-field context did not pass: %v, %v", findings, err)
	}
	for name, content := range map[string]string{
		"other_literal":     source + "var copied = " + strconv.Quote(value) + "\n",
		"comment":           source + "// " + value + "\n",
		"identifier":        source + "var " + value + " = 1\n",
		"adjacent_function": source + "func adjacent() { println(" + strconv.Quote(value) + ") }\n",
		"import":            strings.Replace(source, "package model\n", "package model\nimport "+strconv.Quote(value)+"\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if selected := b07RepresentedLiteralOffsets(b07RepresentedModelSource, []byte(content)); len(selected) != 1 {
				t.Fatal("unrelated source changed the bound context")
			}
			findings, err := CheckFiles(map[string][]byte{b07RepresentedModelSource: []byte(content)}, cfg)
			if err != nil || hasFindingClass(findings, "invalid-go") || !hasFindingClass(findings, "product-term") {
				t.Fatalf("production hook hid unrelated content: %v, %v", findings, err)
			}
		})
	}
	for _, path := range []string{b07RepresentedControlSource, "internal/core/model/other.go", "other/changes.go"} {
		t.Run(path, func(t *testing.T) {
			findings, err := CheckFiles(map[string][]byte{path: []byte(source)}, cfg)
			if err != nil || !hasFindingClass(findings, "product-term") || len(b07RepresentedLiteralOffsets(path, []byte(source))) != 0 {
				t.Fatalf("production binding leaked to another path: %v, %v", findings, err)
			}
		})
	}
}

func TestB07RepresentedAdmissionPreservesSourcePlaneSingleton(t *testing.T) {
	cfg, _ := checkedInSourcePlaneConstants(t)
	for _, allowance := range cfg.SourcePlaneConstants {
		canonical := sourcePlaneConstantFixture(allowance, strconv.Quote(allowance.Value))
		findings, err := CheckFiles(map[string][]byte{allowance.Path: []byte(canonical)}, cfg)
		if err != nil || len(findings) != 0 {
			t.Fatalf("existing singleton stopped passing: %v, %v", findings, err)
		}
		for _, path := range []string{b07RepresentedModelSource, b07RepresentedControlSource} {
			findings, err := CheckFiles(map[string][]byte{path: []byte(canonical)}, cfg)
			if err != nil || len(findings) == 0 {
				t.Fatal("singleton admission leaked to a represented-field path")
			}
		}
		duplicate := canonical + strings.TrimPrefix(canonical, "package driver\n")
		findings, err = CheckFiles(map[string][]byte{allowance.Path: []byte(duplicate)}, cfg)
		if err != nil || !hasFindingClass(findings, "invalid-protocol-literal") {
			t.Fatalf("existing duplicate singleton refusal changed: %v, %v", findings, err)
		}
	}
}

func TestB07RepresentedAdmissionRetainsIdentifierAndImportChecks(t *testing.T) {
	cfg, source, value := b07FrozenRepresentedModel(t)
	cfg.Terms = []Term{{Matches: []string{value}, Class: "identifier"}}
	findings, err := CheckFiles(map[string][]byte{b07RepresentedModelSource: []byte(source)}, cfg)
	if err != nil || len(findings) != 0 {
		t.Fatalf("bound node was not admitted by the existing identifier pass: %v, %v", findings, err)
	}
	for name, content := range map[string]string{
		"literal":    source + "var copied = " + strconv.Quote(value) + "\n",
		"identifier": source + "var " + value + " = 1\n",
		"import":     strings.Replace(source, "package model\n", "package model\nimport "+strconv.Quote(value)+"\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			findings, err := CheckFiles(map[string][]byte{b07RepresentedModelSource: []byte(content)}, cfg)
			if err != nil || hasFindingClass(findings, "invalid-go") || !hasFindingClass(findings, "identifier") {
				t.Fatalf("unrelated identifier/import check was bypassed: %v, %v", findings, err)
			}
		})
	}
}
