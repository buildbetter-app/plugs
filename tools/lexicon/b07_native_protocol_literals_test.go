package lexicon

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Use the actual consumer file supplied by the existing remote test wrapper.
// No normalized reconstruction or selector-only fixture can replace it.
func b07FrozenNativeProtocolFile(t *testing.T) (string, Config, []goStringLiteral) {
	t.Helper()
	root := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("the remote test wrapper must supply the source module root")
	}
	content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(b07NativeProtocolSource)))
	if err != nil {
		t.Fatal(err)
	}
	if len(content) != 31985 || protocolLiteralDigest(string(content)) != "2a65599399cc8c21926529ddfef12077bd9ff6692051ba1bd272e49a50417e8d" {
		t.Fatal("native control differs from the immutable consumer postimage")
	}
	var selected []goStringLiteral
	for _, offset := range []int{26504, 30383, 31330} {
		for _, literal := range goStringLiterals(b07NativeProtocolSource, content) {
			if literal.start == offset && !literal.imported {
				selected = append(selected, literal)
			}
		}
	}
	if len(selected) != 3 {
		t.Fatal("the complete immutable fixture lacks an expected nonimport node")
	}
	cfg, _ := checkedInExternalProtocolConfig(t)
	return string(content), cfg, selected
}

func b07NativeProtocolReplace(source string, start, end int, replacement string) string {
	return source[:start] + replacement + source[end:]
}

func TestB07NativeProtocolAdmitsExactlyThreeActualNodes(t *testing.T) {
	source, cfg, literals := b07FrozenNativeProtocolFile(t)
	selected := b07NativeProtocolLiteralOffsets(b07NativeProtocolSource, []byte(source))
	if len(selected) != 3 {
		t.Fatalf("expected exactly three actual nodes, got %v", selected)
	}
	for i, literal := range literals {
		site := b07NativeProtocolSites()[i]
		if !selected[literal.start] || protocolLiteralDigest(literal.value) != site.valueSHA256 || literal.start-site.contextStart != site.relative {
			t.Fatal("selected a different value or AST node")
		}
		if source[literal.start] != '"' || source[literal.end-1] != '"' {
			t.Fatal("frozen node delimiters changed")
		}
	}
	findings, err := CheckFiles(map[string][]byte{b07NativeProtocolSource: []byte(source)}, cfg)
	if err != nil || len(findings) != 0 {
		t.Fatalf("complete actual postimage retains findings: %v, %v", findings, err)
	}
	// Exercise both existing scanner passes against all three complete values.
	for _, class := range []string{"identifier", "product-term"} {
		t.Run(class, func(t *testing.T) {
			var values []string
			for _, literal := range literals {
				values = append(values, literal.value)
			}
			isolated := Config{Terms: []Term{{Class: class, Matches: values}}}
			findings, err := CheckFiles(map[string][]byte{b07NativeProtocolSource: []byte(source)}, isolated)
			if err != nil || len(findings) != 0 {
				t.Fatalf("existing scanner did not admit the three exact nodes: %v, %v", findings, err)
			}
		})
	}
}

func TestB07NativeProtocolRefusesChangedPathsAndDeclarations(t *testing.T) {
	source, _, _ := b07FrozenNativeProtocolFile(t)
	for _, path := range []string{"spec/cardb07/other_test.go", "other/native_test.go", b07NativeProtocolSource + ".other.go", "./" + b07NativeProtocolSource} {
		t.Run(path, func(t *testing.T) {
			if got := b07NativeProtocolLiteralOffsets(path, []byte(source)); len(got) != 0 {
				t.Fatalf("wrong path admitted nodes: %v", got)
			}
		})
	}
	for _, method := range []b07NativeProtocolSite{b07NativeProtocolSites()[0], b07NativeProtocolSites()[1]} {
		t.Run(method.method, func(t *testing.T) {
			body := source[method.contextStart:method.contextEnd]
			for name, changed := range map[string]string{
				"moved":              "\n" + source,
				"missing_method":     b07NativeProtocolReplace(source, method.contextStart, method.contextEnd, ""),
				"wrong_method":       b07NativeProtocolReplace(source, method.contextStart, method.contextEnd, strings.Replace(body, method.method, method.method+"Other", 1)),
				"wrong_receiver":     b07NativeProtocolReplace(source, method.contextStart, method.contextEnd, strings.Replace(body, "*b07Native", "*otherNative", 1)),
				"value_receiver":     b07NativeProtocolReplace(source, method.contextStart, method.contextEnd, strings.Replace(body, "*b07Native", "b07Native", 1)),
				"qualified_receiver": b07NativeProtocolReplace(source, method.contextStart, method.contextEnd, strings.Replace(body, "*b07Native", "*other.b07Native", 1)),
				"receiver_name":      b07NativeProtocolReplace(source, method.contextStart, method.contextEnd, strings.Replace(body, "(n *", "(x *", 1)),
				"no_receiver":        b07NativeProtocolReplace(source, method.contextStart, method.contextEnd, strings.Replace(body, "(n *b07Native) ", "", 1)),
				"changed_context":    b07NativeProtocolReplace(source, method.contextStart, method.contextEnd, strings.Replace(body, "\n", "\n\t// added context\n", 1)),
				"duplicate_method":   source + body + "\n",
				"other_type_method":  source + "func (n *otherNative) " + method.method + "() {}\n",
				"top_level_function": source + "func " + method.method + "() {}\n",
				"duplicate_variable": source + "var " + method.method + " = 1\n",
				"duplicate_constant": source + "const " + method.method + " = 1\n",
				"duplicate_type":     source + "type " + method.method + " int\n",
				"duplicate_receiver": source + "type b07Native struct{}\n",
				"missing_type":       strings.Replace(source, "type b07Native struct", "type otherNative struct", 1),
				"malformed":          source + "func broken(",
			} {
				t.Run(name, func(t *testing.T) {
					if got := b07NativeProtocolLiteralOffsets(b07NativeProtocolSource, []byte(changed)); len(got) != 0 {
						t.Fatalf("changed declaration admitted nodes: %v", got)
					}
				})
			}
		})
	}
}

func TestB07NativeProtocolRefusesChangedNodesAndBindings(t *testing.T) {
	source, _, literals := b07FrozenNativeProtocolFile(t)
	for i, literal := range literals {
		t.Run(strconv.Itoa(literal.start), func(t *testing.T) {
			quoted := strconv.Quote(literal.value)
			for name, replacement := range map[string]string{
				"changed_value": strconv.Quote(literal.value + "_other"),
				"missing_value": `"other"`,
				"duplicate":     quoted + ", " + quoted,
				"split":         quoted + ` + ""`,
				"parenthesized": "(" + quoted + ")",
				"raw_spelling":  "`" + literal.value + "`",
			} {
				t.Run(name, func(t *testing.T) {
					changed := b07NativeProtocolReplace(source, literal.start, literal.end, replacement)
					if got := b07NativeProtocolLiteralOffsets(b07NativeProtocolSource, []byte(changed)); len(got) != 0 {
						t.Fatalf("changed node admitted: %v", got)
					}
				})
			}
			t.Run("moved_site", func(t *testing.T) {
				changed := b07NativeProtocolReplace(source, literal.start, literal.end, `"other"`) + "var moved = " + quoted + "\n"
				if got := b07NativeProtocolLiteralOffsets(b07NativeProtocolSource, []byte(changed)); len(got) != 0 {
					t.Fatalf("moved literal inherited admission: %v", got)
				}
			})
			for _, field := range []string{"method", "context_digest", "value_digest", "empty_digest", "context_start", "context_end", "relative", "absolute", "negative", "duplicate_site", "missing_site"} {
				t.Run(field, func(t *testing.T) {
					sites := b07NativeProtocolSites()
					switch field {
					case "method":
						sites[i].method = "otherMethod"
					case "context_digest":
						sites[i].contextSHA256 = strings.Repeat("0", 64)
					case "value_digest":
						sites[i].valueSHA256 = strings.Repeat("0", 64)
					case "empty_digest":
						sites[i].valueSHA256 = ""
					case "context_start":
						sites[i].contextStart++
					case "context_end":
						sites[i].contextEnd--
					case "relative":
						sites[i].relative++
					case "absolute":
						sites[i].absolute++
					case "negative":
						sites[i].relative = -1
					case "duplicate_site":
						sites[i] = sites[(i+1)%len(sites)]
					case "missing_site":
						sites = sites[:2]
					}
					if got := selectB07NativeProtocolLiterals(b07NativeProtocolSource, []byte(source), sites); len(got) != 0 {
						t.Fatalf("changed binding admitted nodes: %v", got)
					}
				})
			}
		})
	}
}

func TestB07NativeProtocolScansResidualNodesAfterAdmission(t *testing.T) {
	source, cfg, _ := b07FrozenNativeProtocolFile(t)
	var residual string
	for _, term := range cfg.Terms {
		if term.Class == "product-term" && len(term.Matches) != 0 {
			residual = term.Matches[0]
			break
		}
	}
	if residual == "" {
		t.Fatal("canonical residual rule is absent")
	}
	for name, extra := range map[string]string{
		"literal":          "var copied = " + strconv.Quote(residual) + "\n",
		"line_comment":     "// " + residual + "\n",
		"block_comment":    "/* " + residual + " */\n",
		"identifier":       "var " + residual + " = 1\n",
		"adjacent_method":  "func (n *b07Native) adjacent() { println(" + strconv.Quote(residual) + ") }\n",
		"decoded_residual": "var copied = " + escapedProtocolString(residual) + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			content := source + extra
			if got := b07NativeProtocolLiteralOffsets(b07NativeProtocolSource, []byte(content)); len(got) != 3 {
				t.Fatalf("unrelated trailing content changed the three bound nodes: %v", got)
			}
			findings, err := CheckFiles(map[string][]byte{b07NativeProtocolSource: []byte(content)}, cfg)
			if err != nil || hasFindingClass(findings, "invalid-go") || !hasFindingClass(findings, "product-term") {
				t.Fatalf("residual content stopped scanning: %v, %v", findings, err)
			}
			found := false
			for _, finding := range findings {
				if finding.Class == "product-term" && finding.Term == residual && finding.Line == strings.Count(source, "\n")+1 {
					found = true
				}
			}
			if !found {
				t.Fatal("the exact residual node was hidden by an earlier finding")
			}
		})
	}
	for _, path := range []string{"spec/cardb07/other_test.go", "other/native_test.go"} {
		t.Run(path, func(t *testing.T) {
			findings, err := CheckFiles(map[string][]byte{path: []byte(source)}, cfg)
			if err != nil || !hasFindingClass(findings, "identifier") {
				t.Fatalf("another path stopped scanning: %v, %v", findings, err)
			}
		})
	}
}

func TestB07NativeProtocolCopiesAndImportsRemainRefused(t *testing.T) {
	source, cfg, literals := b07FrozenNativeProtocolFile(t)
	for _, literal := range literals {
		t.Run(strconv.Itoa(literal.start), func(t *testing.T) {
			content := source + "var copied = " + strconv.Quote(literal.value) + "\n"
			if got := b07NativeProtocolLiteralOffsets(b07NativeProtocolSource, []byte(content)); len(got) != 0 {
				t.Fatal("a duplicate complete value retained admission")
			}
			findings, err := CheckFiles(map[string][]byte{b07NativeProtocolSource: []byte(content)}, cfg)
			if err != nil || hasFindingClass(findings, "invalid-go") {
				t.Fatalf("duplicate fixture is invalid: %v, %v", findings, err)
			}
			found := false
			for _, finding := range findings {
				if finding.Class == "identifier" && finding.Line == strings.Count(source, "\n")+1 {
					found = true
				}
			}
			if !found {
				t.Fatal("the appended duplicate itself stopped scanning")
			}
		})
	}
	var importValue string
	for _, term := range cfg.Terms {
		if term.Class == "upstream-infra" {
			for _, value := range term.Matches {
				if strings.Contains(value, "/") {
					importValue = value
				}
			}
		}
	}
	if importValue == "" {
		t.Fatal("canonical legacy-module import control is absent")
	}
	content := strings.Replace(source, "import (", "import (\n"+strconv.Quote(importValue), 1)
	findings, err := CheckFiles(map[string][]byte{b07NativeProtocolSource: []byte(content)}, cfg)
	if err != nil || hasFindingClass(findings, "invalid-go") || !hasFindingClass(findings, "import") || len(b07NativeProtocolLiteralOffsets(b07NativeProtocolSource, []byte(content))) != 0 {
		t.Fatalf("import bypass or moved-source admission: %v, %v", findings, err)
	}
}

func TestB07NativeProtocolPreservesTwoSiteAndSingletonBoundaries(t *testing.T) {
	cfg, model, _ := b07FrozenRepresentedModel(t)
	if len(b07NativeProtocolLiteralOffsets(b07RepresentedModelSource, []byte(model))) != 0 || len(b07RepresentedLiteralOffsets(b07RepresentedModelSource, []byte(model))) != 1 {
		t.Fatal("the original represented-field matcher changed")
	}
	findings, err := CheckFiles(map[string][]byte{b07RepresentedModelSource: []byte(model)}, cfg)
	if err != nil || len(findings) != 0 {
		t.Fatalf("the original represented-field context changed: %v, %v", findings, err)
	}
	cfg, _ = checkedInSourcePlaneConstants(t)
	for _, allowance := range cfg.SourcePlaneConstants {
		singleton := sourcePlaneConstantFixture(allowance, strconv.Quote(allowance.Value))
		findings, err := CheckFiles(map[string][]byte{allowance.Path: []byte(singleton)}, cfg)
		if err != nil || len(findings) != 0 || len(b07NativeProtocolLiteralOffsets(allowance.Path, []byte(singleton))) != 0 {
			t.Fatalf("existing singleton changed: %v, %v", findings, err)
		}
		duplicate := singleton + strings.TrimPrefix(singleton, "package driver\n")
		findings, err = CheckFiles(map[string][]byte{allowance.Path: []byte(duplicate)}, cfg)
		if err != nil || !hasFindingClass(findings, "invalid-protocol-literal") {
			t.Fatalf("duplicate singleton refusal changed: %v, %v", findings, err)
		}
		findings, err = CheckFiles(map[string][]byte{b07NativeProtocolSource: []byte(singleton)}, cfg)
		if err != nil || len(findings) == 0 {
			t.Fatal("singleton admission leaked to the new path")
		}
	}
}
