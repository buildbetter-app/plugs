package lexicon

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func b07ActualEffectFile(t *testing.T) (string, Config, []goStringLiteral) {
	t.Helper()
	root := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) { t.Fatal("remote source module root is required") }
	content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(b07EffectSource)))
	if err != nil { t.Fatal(err) }
	if protocolLiteralDigest(string(content)) != "0aca280159f27d822cc71ee4c131814fe83a165803877f36121bda7106f363cc" { t.Fatal("actual consumer differs from the immutable reviewed postimage") }
	literals := []goStringLiteral{}
	for _, site := range b07EffectSites() {
		for _, literal := range goStringLiterals(b07EffectSource, content) {
			if literal.start == site.absolute && !literal.imported { literals = append(literals, literal) }
		}
	}
	if len(literals) != 6 { t.Fatal("actual consumer lacks one of the six nonimport nodes") }
	cfg, _ := checkedInExternalProtocolConfig(t)
	return string(content), cfg, literals
}

func TestB07EffectAdmitsExactlySixActualNodes(t *testing.T) {
	source, cfg, literals := b07ActualEffectFile(t)
	selected := b07EffectLiteralOffsets(b07EffectSource, []byte(source))
	if len(selected) != 6 { t.Fatalf("expected six exact AST nodes: %v", selected) }
	values := []string{}
	for i, literal := range literals {
		site := b07EffectSites()[i]
		if !selected[literal.start] || literal.start-248 != site.relative || protocolLiteralDigest(literal.value) != site.valueSHA256 || source[literal.start] != '"' || source[literal.end-1] != '"' { t.Fatal("selector bound the wrong node/value") }
		values = append(values, literal.value)
	}
	findings, err := CheckFiles(map[string][]byte{b07EffectSource: []byte(source)}, cfg)
	if err != nil || len(findings) != 0 { t.Fatalf("complete actual consumer retains findings: %v %v", findings, err) }
	for _, class := range []string{"identifier", "product-term"} {
		findings, err := CheckFiles(map[string][]byte{b07EffectSource: []byte(source)}, Config{Terms: []Term{{Class: class, Matches: values}}})
		if err != nil || len(findings) != 0 { t.Fatalf("%s pass failed exact-node admission: %v %v", class, findings, err) }
	}
}

func TestB07EffectRefusesChangedDeclarationAndBindings(t *testing.T) {
	source, _, literals := b07ActualEffectFile(t)
	for _, path := range []string{"other/native_state_test.go", "spec/cardb07effects/other_test.go", "./"+b07EffectSource, b07EffectSource+".other.go"} {
		if len(b07EffectLiteralOffsets(path, []byte(source))) != 0 { t.Fatal("admission leaked to another path", path) }
	}
	for name, changed := range map[string]string{
		"moved_block": strings.Replace(source, "const (", "\nconst (", 1),
		"variable": strings.Replace(source, "const (", "var (", 1),
		"package": strings.Replace(source, "package cardb20", "package other20", 1),
		"context": strings.Replace(source, "const (", "const (\n// adjacent", 1),
		"malformed": source+"func broken(",
	} {
		t.Run(name, func(t *testing.T) { if len(b07EffectLiteralOffsets(b07EffectSource, []byte(changed))) != 0 { t.Fatal("changed declaration retained admission") } })
	}
	for i, literal := range literals {
		site := b07EffectSites()[i]
		t.Run(site.name, func(t *testing.T) {
			for name, replacement := range map[string]string{
				"value": strconv.Quote(literal.value+"_other"),
				"raw": "`"+literal.value+"`",
				"concat": strconv.Quote(literal.value)+` + ""`,
				"parenthesized": "("+strconv.Quote(literal.value)+")",
				"duplicate": strconv.Quote(literal.value)+", "+strconv.Quote(literal.value),
			} {
				t.Run(name, func(t *testing.T) {
					changed := b07NativeProtocolReplace(source, literal.start, literal.end, replacement)
					if len(b07EffectLiteralOffsets(b07EffectSource, []byte(changed))) != 0 { t.Fatal("changed literal shape gained admission") }
				})
			}
			for name, changed := range map[string]string{
				"renamed": strings.Replace(source, site.name+" =", site.name+"Other =", 1),
				"typed": strings.Replace(source, site.name+" =", site.name+" string =", 1),
				"duplicate_name": source+"var "+site.name+" = 1\n",
				"duplicate_type": source+"type "+site.name+" int\n",
				"duplicate_function": source+"func "+site.name+"() {}\n",
				"copied": source+"var copied = "+strconv.Quote(literal.value)+"\n",
				"moved": b07NativeProtocolReplace(source, literal.start, literal.end, `"other"`)+"var moved = "+strconv.Quote(literal.value)+"\n",
			} {
				t.Run(name, func(t *testing.T) { if len(b07EffectLiteralOffsets(b07EffectSource, []byte(changed))) != 0 { t.Fatal("renamed/moved/duplicate node gained admission") } })
			}
			for _, field := range []string{"name", "digest", "absolute", "relative", "duplicate", "missing"} {
				t.Run("binding_"+field, func(t *testing.T) {
					sites := b07EffectSites()
					switch field {
					case "name": sites[i].name = "other"
					case "digest": sites[i].valueSHA256 = strings.Repeat("0",64)
					case "absolute": sites[i].absolute++
					case "relative": sites[i].relative++
					case "duplicate": sites[i] = sites[(i+1)%len(sites)]
					case "missing": sites = sites[:5]
					}
					if len(selectB07EffectLiterals(b07EffectSource, []byte(source), sites)) != 0 { t.Fatal("mutated binding gained admission") }
				})
			}
		})
	}
}

func TestB07EffectKeepsResidualsAndImportsVisible(t *testing.T) {
	source, cfg, literals := b07ActualEffectFile(t)
	for _, class := range []string{"identifier", "product-term"} {
		for _, literal := range literals {
			controls:=map[string]string{
				"copy": "var copied = "+strconv.Quote(literal.value)+"\n",
				"encoded": "var copied = "+escapedProtocolString(literal.value)+"\n",
				"identifier": "var "+literal.value+" = 1\n",
			}
			// Comments belong to the text scanner; the identifier pass scans
			// AST identifiers and BasicLits, preserving its existing policy.
			if class=="product-term" {controls["line_comment"]="// "+literal.value+"\n";controls["block_comment"]="/* "+literal.value+" */\n"}
			for name, extra := range controls {
				t.Run(class+"/"+strconv.Itoa(literal.start)+"/"+name,func(t *testing.T) {
					content := source+extra
					if strings.Contains(name,"comment") && len(b07EffectLiteralOffsets(b07EffectSource,[]byte(content))) != 6 {t.Fatal("comment altered the six original nodes")}
					findings,err:=CheckFiles(map[string][]byte{b07EffectSource:[]byte(content)},Config{Terms:[]Term{{Class:class,Matches:[]string{literal.value}}}})
					if err!=nil || hasFindingClass(findings,"invalid-go") {t.Fatalf("invalid residual control: %v %v",findings,err)}
					wantLines:=[]int{strings.Count(source,"\n")+1}
					if name=="copy" || name=="encoded" {
						if len(b07EffectLiteralOffsets(b07EffectSource,[]byte(content)))!=0 {t.Fatal("copied decoded value retained any admission")}
						// A duplicate revokes all six admissions. The shared
						// scanner then uses its existing raw-string fallback:
						// the original node is reported, with a second raw
						// identifier finding only for an unescaped copy.
						wantLines=[]int{literal.line}
						if class=="identifier" && name=="copy" {wantLines=append(wantLines,strings.Count(source,"\n")+1)}
					}
					if len(findings)!=len(wantLines) {t.Fatalf("residual count differs from existing scanner policy: %s/%s got %v want lines %v",class,name,findings,wantLines)}
					for i,finding:=range findings {if finding.Class!=class || finding.Line!=wantLines[i] || finding.Term!=literal.value {t.Fatalf("residual finding differs from existing scanner policy: got %v want lines %v",findings,wantLines)}}
				})
			}
		}
	}
	var module string
	for _,term:=range cfg.Terms {if term.Class=="upstream-infra" {for _,value:=range term.Matches {if strings.Contains(value,"/") {module=value}}}}
	if module=="" {t.Fatal("canonical import control missing")}
	content:=strings.Replace(source,"import (","import (\n"+strconv.Quote(module),1)
	findings,err:=CheckFiles(map[string][]byte{b07EffectSource:[]byte(content)},cfg)
	if err!=nil || hasFindingClass(findings,"invalid-go") || !hasFindingClass(findings,"import") || len(b07EffectLiteralOffsets(b07EffectSource,[]byte(content)))!=0 {t.Fatalf("import/moved-offset refusal changed: %v %v",findings,err)}
	old,_,_:=b07FrozenNativeProtocolFile(t)
	if len(b07NativeProtocolLiteralOffsets(b07NativeProtocolSource,[]byte(old)))!=3 || len(b07EffectLiteralOffsets(b07NativeProtocolSource,[]byte(old)))!=0 {t.Fatal("old exact contexts changed or new admission escaped")}
}
