// Package lexicon enforces the vocabulary boundary for the clean tree.
package lexicon

import (
	"bufio"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Term is one lexicon rule.
type Term struct {
	Matches []string
	Pattern string
	Class   string
	Allow   map[rune]bool
}

// GoStringAllowance admits one decoded external-protocol string at one path.
type GoStringAllowance struct {
	Path  string
	Value string
}

// Config is the small TOML subset used by the repository lexicon.
type Config struct {
	Terms                  []Term
	Provenance             []string
	ExternalProtocolString GoStringAllowance
}

const externalProtocolSource = "spec/s1-b25-b28/public_recording_test.go"

// Finding identifies one boundary violation.
type Finding struct {
	Path  string
	Line  int
	Class string
	Term  string
}

func (f Finding) String() string {
	return fmt.Sprintf("%s:%d: %s term %q", f.Path, f.Line, f.Class, f.Term)
}

// LoadConfig reads the checked-in lexicon without adding a parser dependency.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read lexicon: %w", err)
	}
	if !utf8.Valid(b) {
		return Config{}, errors.New("lexicon is not valid UTF-8")
	}
	var cfg Config
	section := ""
	var current *Term
	var externalSection, externalPath, externalValue bool
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(strings.SplitN(line, "#", 2)[0])
		if line == "" {
			continue
		}
		switch line {
		case "[[term]]":
			cfg.Terms = append(cfg.Terms, Term{})
			current = &cfg.Terms[len(cfg.Terms)-1]
			section = "term"
			continue
		case "[zones]":
			section = "zones"
			current = nil
			continue
		case "[external_protocol_string]":
			if externalSection {
				return Config{}, errors.New("duplicate external protocol string section")
			}
			externalSection = true
			section = "external_protocol_string"
			current = nil
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return Config{}, fmt.Errorf("invalid lexicon line %q", line)
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		values, err := quotedValues(value)
		if err != nil {
			return Config{}, fmt.Errorf("lexicon %s: %w", key, err)
		}
		if section == "zones" && key == "provenance" {
			cfg.Provenance = values
			continue
		}
		if section == "external_protocol_string" {
			if len(values) != 1 {
				return Config{}, errors.New("external protocol string fields require one value")
			}
			switch key {
			case "path":
				if externalPath {
					return Config{}, errors.New("duplicate external protocol string path")
				}
				externalPath = true
				cfg.ExternalProtocolString.Path = values[0]
			case "value":
				if externalValue {
					return Config{}, errors.New("duplicate external protocol string value")
				}
				externalValue = true
				cfg.ExternalProtocolString.Value = values[0]
			default:
				return Config{}, fmt.Errorf("unknown external protocol string key: %s", key)
			}
			continue
		}
		if current == nil {
			return Config{}, fmt.Errorf("lexicon key outside a section: %s", key)
		}
		switch key {
		case "match":
			current.Matches = values
		case "regex":
			if len(values) != 1 {
				return Config{}, errors.New("regex must have one value")
			}
			current.Pattern = values[0]
		case "class":
			if len(values) != 1 {
				return Config{}, errors.New("class must have one value")
			}
			current.Class = values[0]
		case "allow":
			current.Allow = make(map[rune]bool)
			for _, value := range values {
				for _, r := range value {
					current.Allow[r] = true
				}
			}
		default:
			return Config{}, fmt.Errorf("unknown lexicon key: %s", key)
		}
	}
	if len(cfg.Terms) == 0 || len(cfg.Provenance) == 0 {
		return Config{}, errors.New("lexicon has no terms or provenance zones")
	}
	for i := range cfg.Terms {
		if cfg.Terms[i].Class == "" || (len(cfg.Terms[i].Matches) == 0 && cfg.Terms[i].Pattern == "") {
			return Config{}, fmt.Errorf("lexicon term %d is incomplete", i)
		}
	}
	if externalSection && (!externalPath || !externalValue || cfg.ExternalProtocolString.Path != externalProtocolSource || cfg.ExternalProtocolString.Value == "") {
		return Config{}, errors.New("external protocol string must bind the exact source path and a nonempty literal")
	}
	return cfg, nil
}

func quotedValues(value string) ([]string, error) {
	if strings.HasPrefix(value, "\"") {
		unquoted, err := strconv.Unquote(value)
		if err != nil {
			return nil, err
		}
		return []string{unquoted}, nil
	}
	if !strings.HasPrefix(value, "[") || !strings.HasSuffix(value, "]") {
		return nil, errors.New("expected a quoted value or array")
	}
	var values []string
	for _, field := range strings.Split(strings.TrimSpace(value[1:len(value)-1]), ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		v, err := strconv.Unquote(field)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

// CheckFiles checks UTF-8 file contents keyed by repository-relative path.
// It scans every supplied path; callers that retain protected paths must
// filter them before calling this function.
func CheckFiles(files map[string][]byte, cfg Config) ([]Finding, error) {
	return checkFiles(files, cfg, nil)
}

// CheckFilesWithPolicy checks files using the checked-in tree policy. The
// policy is intentionally separate from the vocabulary config: retained CI
// and design assets have an explicit protected boundary while B1-owned files
// are scanned normally.
func CheckFilesWithPolicy(files map[string][]byte, cfg Config, policy TreePolicy) ([]Finding, error) {
	return checkFiles(files, cfg, &policy)
}

func checkFiles(files map[string][]byte, cfg Config, policy *TreePolicy) ([]Finding, error) {
	var findings []Finding
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		content := files[path]
		if policy != nil {
			disposition, err := policy.disposition(path, content)
			if err != nil {
				return nil, err
			}
			switch disposition {
			case dispositionReference:
				if !utf8.Valid(content) {
					return nil, fmt.Errorf("%s: immutable reference is not valid UTF-8", path)
				}
				continue
			case dispositionBinary:
				continue
			case dispositionProtected:
				if !utf8.Valid(content) {
					return nil, fmt.Errorf("%s: protected content is not valid UTF-8 and is not a declared binary", path)
				}
				continue
			}
		}
		if !utf8.Valid(content) {
			return nil, fmt.Errorf("%s: content is not valid UTF-8", path)
		}
		if path == "engdocs/style/lexicon.toml" {
			continue
		}
		provenance := isProvenance(path, cfg.Provenance)
		findings = append(findings, scanFile(path, content, cfg, provenance)...)
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Path != findings[j].Path {
			return findings[i].Path < findings[j].Path
		}
		if findings[i].Line != findings[j].Line {
			return findings[i].Line < findings[j].Line
		}
		return findings[i].Term < findings[j].Term
	})
	return findings, nil
}

// CheckTree checks tracked files in root using the explicit B1 policy. It does
// not consult environment variables: a newly tracked protected path or an
// unlisted binary fails closed.
func CheckTree(root string) ([]Finding, error) {
	if root == "" {
		return nil, errors.New("empty repository root")
	}
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("repository root is not a directory: %s", root)
	}
	cfg, err := LoadConfig(filepath.Join(root, "engdocs/style/lexicon.toml"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command("git", "-C", root, "ls-files", "-z")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list tracked files: %w", err)
	}
	if !utf8.Valid(out) {
		return nil, errors.New("tracked file list is not valid UTF-8")
	}
	files := make(map[string][]byte)
	for _, raw := range strings.Split(string(out), "\x00") {
		if raw == "" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(raw)))
		if err != nil {
			return nil, fmt.Errorf("read tracked file %s: %w", raw, err)
		}
		files[raw] = b
	}
	if err := CheckSourceManifest(files); err != nil {
		return nil, err
	}
	return CheckFilesWithPolicy(files, cfg, DefaultTreePolicy())
}

func isProvenance(path string, zones []string) bool {
	for _, zone := range zones {
		if path == zone || strings.HasSuffix(zone, "/") && strings.HasPrefix(path, zone) {
			return true
		}
	}
	return false
}

func scanFile(path string, content []byte, cfg Config, provenance bool) []Finding {
	text := string(content)
	var findings []Finding
	for _, term := range cfg.Terms {
		if provenance && term.Class != "upstream-infra" {
			continue
		}
		if term.Pattern != "" {
			re, err := regexp.Compile(term.Pattern)
			if err != nil {
				findings = append(findings, Finding{Path: path, Line: 1, Class: "invalid-lexicon", Term: term.Pattern})
				continue
			}
			for _, loc := range re.FindAllStringIndex(text, -1) {
				bad := false
				for _, r := range text[loc[0]:loc[1]] {
					if !term.Allow[r] {
						bad = true
					}
				}
				if bad {
					findings = append(findings, Finding{Path: path, Line: lineAt(text, loc[0]), Class: term.Class, Term: term.Pattern})
				}
			}
			continue
		}
		if term.Class == "identifier" {
			if filepath.Ext(path) != ".go" {
				continue
			}
			findings = append(findings, scanGoIdentifiers(path, content, term, cfg.ExternalProtocolString)...)
			continue
		}
		for _, match := range term.Matches {
			if at := phraseIndex(text, match); at >= 0 {
				findings = append(findings, Finding{Path: path, Line: lineAt(text, at), Class: term.Class, Term: match})
			}
		}
	}
	if filepath.Ext(path) == ".go" {
		findings = append(findings, scanGoImports(path, content)...)
	}
	return findings
}

func scanGoIdentifiers(path string, content []byte, term Term, allowance GoStringAllowance) []Finding {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, parser.ParseComments)
	if err != nil {
		return []Finding{{Path: path, Line: 1, Class: "invalid-go", Term: "parse"}}
	}
	imports := make(map[token.Pos]bool, len(file.Imports))
	for _, spec := range file.Imports {
		imports[spec.Path.Pos()] = true
	}
	var findings []Finding
	ast.Inspect(file, func(node ast.Node) bool {
		var text string
		switch n := node.(type) {
		case *ast.Ident:
			text = n.Name
		case *ast.BasicLit:
			if n.Kind == token.STRING {
				text = n.Value
				if path == externalProtocolSource {
					decoded, err := strconv.Unquote(n.Value)
					if err != nil {
						findings = append(findings, Finding{Path: path, Line: fset.Position(node.Pos()).Line, Class: "invalid-go", Term: "string"})
						return true
					}
					if !imports[n.Pos()] && allowance.Path == path && decoded == allowance.Value && decoded != "" {
						return true
					}
					// Decode every other string here so altered spellings cannot use
					// the allowance; keep the established scan elsewhere unchanged.
					text = decoded
				}
			}
		}
		if text != "" {
			for _, match := range term.Matches {
				if phraseIndex(text, match) >= 0 {
					findings = append(findings, Finding{Path: path, Line: fset.Position(node.Pos()).Line, Class: term.Class, Term: match})
					break
				}
			}
		}
		return true
	})
	return findings
}

func scanGoImports(path string, content []byte) []Finding {
	file, err := parser.ParseFile(token.NewFileSet(), path, content, 0)
	if err != nil {
		return []Finding{{Path: path, Line: 1, Class: "invalid-go", Term: "parse"}}
	}
	var findings []Finding
	for _, spec := range file.Imports {
		value, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			findings = append(findings, Finding{Path: path, Line: 1, Class: "invalid-import", Term: "parse"})
			continue
		}
		for _, segment := range strings.Split(value, "/") {
			if strings.EqualFold(segment, "beads") {
				findings = append(findings, Finding{Path: path, Line: 1, Class: "import", Term: "legacy-module"})
				break
			}
		}
	}
	return findings
}

func phraseIndex(text, phrase string) int {
	want := splitWords(phrase)
	if len(want) == 0 {
		return -1
	}
	words, starts := splitWordsWithOffsets(text)
	for i := 0; i+len(want) <= len(words); i++ {
		if strings.EqualFold(strings.Join(words[i:i+len(want)], " "), strings.Join(want, " ")) {
			return starts[i]
		}
	}
	return -1
}

func splitWords(text string) []string {
	words, _ := splitWordsWithOffsets(text)
	return words
}

func splitWordsWithOffsets(text string) ([]string, []int) {
	var words []string
	var starts []int
	start := -1
	startOffset := 0
	offset := 0
	runes := []rune(text)
	for i, r := range runes {
		// Account for each rune once instead of re-encoding the entire prefix
		// for every word. Keep rune indices for the existing boundary rules.
		byteStart := offset
		offset += utf8.RuneLen(r)
		boundary := !unicode.IsLetter(r) && !unicode.IsDigit(r)
		camelBoundary := i > 0 && unicode.IsUpper(r) && (unicode.IsLower(runes[i-1]) ||
			(unicode.IsUpper(runes[i-1]) && i+1 < len(runes) && unicode.IsLower(runes[i+1])))
		if camelBoundary {
			boundary = true
		}
		if boundary {
			if start >= 0 {
				words = append(words, strings.ToLower(string(runes[start:i])))
				starts = append(starts, startOffset)
				start = -1
			}
			if !camelBoundary {
				continue
			}
		}
		if start < 0 {
			start = i
			startOffset = byteStart
		}
	}
	if start >= 0 {
		words = append(words, strings.ToLower(string(runes[start:])))
		starts = append(starts, startOffset)
	}
	return words, starts
}

func lineAt(text string, offset int) int {
	return 1 + strings.Count(text[:offset], "\n")
}

// CheckError formats findings as a blocking error.
func CheckError(findings []Finding) error {
	if len(findings) == 0 {
		return nil
	}
	var b strings.Builder
	for _, finding := range findings {
		b.WriteString(finding.String())
		b.WriteByte('\n')
	}
	return errors.New(strings.TrimSpace(b.String()))
}

// ReadLines is kept tiny and useful to callers that need stable guard output.
func ReadLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	s := bufio.NewScanner(f)
	for s.Scan() {
		lines = append(lines, s.Text())
	}
	return lines, s.Err()
}
