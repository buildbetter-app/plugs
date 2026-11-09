// Package lexicon enforces the vocabulary boundary for the clean tree.
package lexicon

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
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

// GoStringAllowance admits exact decoded external-protocol strings at one path.
type GoStringAllowance struct {
	Path           string
	Value          string
	PhysicalValues []string
}

// GoStringSetAllowance binds a finite decoded protocol set at one source path.
type GoStringSetAllowance struct {
	Path   string
	Values []string
}

// NativeControlAllowance binds a complete finite set of decoded Go string
// literals to one exact native source path. No family or prefix matching is
// supported, and imports are never admitted by this allowance.
type NativeControlAllowance struct {
	Path   string
	Values []string
}

// NativeLiteralSite binds a complete decoded vector to one source field.
// Go sites require their actual function/argv context; JSON requires its field.
type NativeLiteralSite struct {
	ID, Path, Field string
	Values          []string
}

// Config is the small TOML subset used by the repository lexicon.
type Config struct {
	Terms                   []Term
	Provenance              []string
	ExternalProtocolString  GoStringAllowance
	NativeManifestStrings   GoStringSetAllowance
	NativeFieldLiterals     []NativeLiteralSite
	NativeRecordingLiterals NativeControlAllowance
	NativeViewStrings       NativeControlAllowance
}

const externalProtocolSource = "spec/s1-b25-b28/public_recording_test.go"
const nativeManifestProtocolSource = "spec/s1-b13-native/recording_test.go"
const b01NativeGoSource = "spec/s1-b01-b04/native_exclusion_test.go"
const b01NativeJSONSource = "spec/s1-b01-b04/native_source_bindings.json"
const b03CatalogJSONSource = "internal/integration/b03catalog/native_catalog_inputs.json"

const nativeRecordingLiteralSource = "spec/s1-b21-b24/native_recording_test.go"
const nativeViewProtocolSource = "spec/s1-b21-b24/native_views_test.go"

const externalEnvironmentDigest = "1b48d14732a1b4689f7af2d622235e53ddf1703e7043985592e66b0d1ecf4eff"

// These decoded-string digests bind the accepted physical catalog, its two
// journal/cursor names and one dependency column. Config cannot add a family
// of names or substitute another literal, even at the authorized source path.
var physicalProtocolDigests = map[string]bool{
	"39d6e7ef1121f5ac563d8c278431a6a19f9d420534e9cbe38d2f9f41d9912502": true,
	"93602e05a1bff1bd127187c713caaeab338848eb4a6cba051123440a912cdf26": true,
	"91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75": true,
	"7853d42458d77d5bd188bd1a1bba51e87257cbe5ec12826168bc639f95bb0a72": true,
}

// Accepted manifest keys, artifact/target and build package suffix are fixed
// wire values; this set cannot admit a package or command family.
var nativeManifestProtocolDigests = map[string]bool{
	"448bbf6c576972f4320475a0f2c84fc95020744752746eeb7bc440c488827ec7": true,
	"189e26badd76744c7e096b1d22c566d77a483176d37748a42b5c8ea3301c09ea": true,
	"8610e53a1800745f41cf493e7f8e05da1513988d330e48bffe2a65422300379e": true,
	"28eb85035ca7f533164539df882ca0584982aaad9f833d341983a6bff0fd0d33": true,
}

var nativeRecordingLiteralDigests = map[string]bool{
	"d56ebf9119cfd53f6690d8ec4111275a19c16b5c1c89123d3ff99499b81586cd": true,
	"91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75": true,
	"93602e05a1bff1bd127187c713caaeab338848eb4a6cba051123440a912cdf26": true,
	"a250fe4bfeccf35cd1f34c8eb7625ef05127294cdeb6884a04adea45796adebc": true,
	"bb4a56c556282d591f27cc54e1f06ac5a3de17e1aed41e0ac387b2b71fe30f91": true,
	"c7e69eb7c0bdafa652aa9539c60152fb468d0ac0ddc4f71bb90469ec8747caf8": true,
	"d29f73f45d1bb46a0ca4f641e919fcb6964b2aedeb3db5205118b50627c725fc": true,
}

var nativeViewProtocolDigests = map[string]bool{
	"91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75": true,
	"93602e05a1bff1bd127187c713caaeab338848eb4a6cba051123440a912cdf26": true,
}

func protocolLiteralDigest(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

func allowsPhysicalProtocolString(path, value string, allowance GoStringAllowance) bool {
	if path != externalProtocolSource || allowance.Path != path || !physicalProtocolDigests[protocolLiteralDigest(value)] {
		return false
	}
	for _, allowed := range allowance.PhysicalValues {
		if value == allowed {
			return true
		}
	}
	return false
}

func allowsNativeManifestString(path, value string, allowance GoStringSetAllowance) bool {
	if path != nativeManifestProtocolSource || allowance.Path != path || !nativeManifestProtocolDigests[protocolLiteralDigest(value)] {
		return false
	}
	for _, allowed := range allowance.Values {
		if value == allowed {
			return true
		}
	}
	return false
}

func nativeControlAllowance(path string, cfg Config) (NativeControlAllowance, map[string]bool, bool) {
	switch path {
	case nativeRecordingLiteralSource:
		return cfg.NativeRecordingLiterals, nativeRecordingLiteralDigests, true
	case nativeViewProtocolSource:
		return cfg.NativeViewStrings, nativeViewProtocolDigests, true
	default:
		return NativeControlAllowance{}, nil, false
	}
}

func allowsNativeControlLiteral(path, value string, cfg Config) bool {
	allowance, digests, ok := nativeControlAllowance(path, cfg)
	if !ok || allowance.Path != path || !digests[protocolLiteralDigest(value)] {
		return false
	}
	for _, allowed := range allowance.Values {
		if value == allowed {
			return true
		}
	}
	return false
}

func hasNativeControlAllowance(path string, cfg Config) bool {
	allowance, _, ok := nativeControlAllowance(path, cfg)
	return ok && allowance.Path == path && len(allowance.Values) != 0
}

func allowsConfiguredLiteral(path, value string, cfg Config) bool {
	return allowsPhysicalProtocolString(path, value, cfg.ExternalProtocolString) || allowsNativeControlLiteral(path, value, cfg)
}

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
	var externalSection, externalPath, externalValue, externalPhysical bool
	var manifestSection, manifestPath, manifestValues bool
	var recordingSection, recordingPath, recordingValues bool
	var viewSection, viewPath, viewValues bool
	var nativeSite *NativeLiteralSite
	var nativeFields map[string]bool
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
		case "[native_recording_literals]":
			if recordingSection {
				return Config{}, errors.New("duplicate native recording literal section")
			}
			recordingSection = true
			section = "native_recording_literals"
			current = nil
			continue
		case "[native_view_strings]":
			if viewSection {
				return Config{}, errors.New("duplicate native view string section")
			}
			viewSection = true
			section = "native_view_strings"
			current = nil
			continue
		case "[native_manifest_strings]":
			if manifestSection {
				return Config{}, errors.New("duplicate native manifest string section")
			}
			manifestSection = true
			section = "native_manifest_strings"
			current = nil
			continue
		case "[[native_field_literal]]":
			cfg.NativeFieldLiterals = append(cfg.NativeFieldLiterals, NativeLiteralSite{})
			nativeSite = &cfg.NativeFieldLiterals[len(cfg.NativeFieldLiterals)-1]
			nativeFields = make(map[string]bool)
			section, current = "native_field_literal", nil
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return Config{}, fmt.Errorf("invalid lexicon line %q", line)
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if section == "native_field_literal" {
			if nativeFields[key] {
				return Config{}, fmt.Errorf("duplicate native field literal field: %s", key)
			}
			nativeFields[key] = true
			switch key {
			case "id", "path", "field":
				decoded, err := strconv.Unquote(value)
				if err != nil || !strings.HasPrefix(value, "\"") {
					return Config{}, errors.New("native field literal fields require a quoted scalar")
				}
				switch key {
				case "id":
					nativeSite.ID = decoded
				case "path":
					nativeSite.Path = decoded
				case "field":
					nativeSite.Field = decoded
				}
			case "values":
				// JSON's quoted-array subset also preserves commas inside argv.
				if !strings.HasPrefix(value, "[") || json.Unmarshal([]byte(value), &nativeSite.Values) != nil || len(nativeSite.Values) == 0 {
					return Config{}, errors.New("native field literal values require a nonempty quoted array")
				}
			default:
				return Config{}, fmt.Errorf("unknown native field literal field: %s", key)
			}
			continue
		}
		values, err := quotedValues(value)
		if err != nil {
			return Config{}, fmt.Errorf("lexicon %s: %w", key, err)
		}
		if section == "zones" && key == "provenance" {
			cfg.Provenance = values
			continue
		}
		if section == "native_manifest_strings" {
			switch key {
			case "path":
				if manifestPath || !strings.HasPrefix(value, "\"") || len(values) != 1 || values[0] != nativeManifestProtocolSource {
					return Config{}, errors.New("native manifest strings require their exact source path")
				}
				manifestPath = true
				cfg.NativeManifestStrings.Path = values[0]
			case "values":
				if manifestValues || !strings.HasPrefix(value, "[") || len(values) != len(nativeManifestProtocolDigests) {
					return Config{}, errors.New("native manifest strings require one complete exact literal set")
				}
				manifestValues = true
				seen := make(map[string]bool)
				for _, value := range values {
					digest := protocolLiteralDigest(value)
					if !nativeManifestProtocolDigests[digest] || seen[digest] {
						return Config{}, errors.New("native manifest string is unlisted or duplicated")
					}
					seen[digest] = true
				}
				cfg.NativeManifestStrings.Values = values
			default:
				return Config{}, fmt.Errorf("unknown native manifest string key: %s", key)
			}
			continue
		}
		if section == "external_protocol_string" {
			if key != "physical_values" && len(values) != 1 {
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
			case "physical_values":
				if externalPhysical || len(values) != len(physicalProtocolDigests) {
					return Config{}, errors.New("physical protocol strings require one complete exact literal set")
				}
				externalPhysical = true
				seen := make(map[string]bool)
				for _, value := range values {
					digest := protocolLiteralDigest(value)
					if !physicalProtocolDigests[digest] || seen[digest] {
						return Config{}, errors.New("physical protocol string is unlisted or duplicated")
					}
					seen[digest] = true
				}
				cfg.ExternalProtocolString.PhysicalValues = values
			default:
				return Config{}, fmt.Errorf("unknown external protocol string key: %s", key)
			}
			continue
		}
		if section == "native_recording_literals" || section == "native_view_strings" {
			allowance := &cfg.NativeRecordingLiterals
			source := nativeRecordingLiteralSource
			digests := nativeRecordingLiteralDigests
			pathSeen, valuesSeen := &recordingPath, &recordingValues
			if section == "native_view_strings" {
				allowance = &cfg.NativeViewStrings
				source = nativeViewProtocolSource
				digests = nativeViewProtocolDigests
				pathSeen, valuesSeen = &viewPath, &viewValues
			}
			switch key {
			case "path":
				if *pathSeen || !strings.HasPrefix(value, "\"") || len(values) != 1 || values[0] != source {
					return Config{}, errors.New("native control literals require their exact source path")
				}
				*pathSeen = true
				allowance.Path = values[0]
			case "values":
				if *valuesSeen || !strings.HasPrefix(value, "[") || len(values) != len(digests) {
					return Config{}, errors.New("native control literals require one complete exact set")
				}
				*valuesSeen = true
				seen := make(map[string]bool)
				for _, literal := range values {
					digest := protocolLiteralDigest(literal)
					if !digests[digest] || seen[digest] {
						return Config{}, errors.New("native control literal is unlisted or duplicated")
					}
					seen[digest] = true
				}
				allowance.Values = values
			default:
				return Config{}, fmt.Errorf("unknown native control literal key: %s", key)
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
	if externalSection && (!externalPath || !externalValue || cfg.ExternalProtocolString.Path != externalProtocolSource || protocolLiteralDigest(cfg.ExternalProtocolString.Value) != externalEnvironmentDigest) {
		return Config{}, errors.New("external protocol string must bind the exact source path and environment literal")
	}
	if recordingSection && (!recordingPath || !recordingValues) || viewSection && (!viewPath || !viewValues) {
		return Config{}, errors.New("native control literals lack their exact path or complete set")
	}
	if manifestSection && (!manifestPath || !manifestValues) {
		return Config{}, errors.New("native manifest strings lack their exact path or literal set")
	}
	if len(cfg.NativeFieldLiterals) != 0 && !validNativeFieldLiterals(cfg.NativeFieldLiterals) {
		return Config{}, errors.New("native field literals require all four exact path/field/value sites")
	}
	return cfg, nil
}

// The fixed digests bind every field and the complete ordered decoded vector.
// Delimiters cannot be injected to repartition a tuple; config alone cannot
// introduce another source field or value. An absent/invalid set grants nothing.
func validNativeFieldLiterals(sites []NativeLiteralSite) bool {
	want := map[string]string{
		"ready-types":              "3519f1cfb20fa24cfca6c77869671a5d9a1411785d0d3081ed32d12b20d5f361",
		"custom-types":             "ec901e9a98f01ac8fd80e4d987682abcf07f06543bb41996e5f0196e0d495b3b",
		"native-type-binding-json": "704769d0bfe4c9bf675c4e547811c87d194f5a5789d89618911f496b48de9361",
		"catalog-sql-tables":       "abece64033a685f3ad2c353f60c392e56cae472cf16427fbb62c3427f2dc064c",
	}
	if len(sites) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(sites))
	for _, site := range sites {
		if seen[site.ID] {
			return false
		}
		fields := append([]string{site.Path, site.Field}, site.Values...)
		for _, field := range fields {
			if strings.ContainsRune(field, '\x00') {
				return false
			}
		}
		if protocolLiteralDigest(strings.Join(fields, "\x00")) != want[site.ID] {
			return false
		}
		seen[site.ID] = true
	}
	return true
}

func nativeStringExpressions(expressions []ast.Expr, values []string) []*ast.BasicLit {
	if len(expressions) != len(values) {
		return nil
	}
	var literals []*ast.BasicLit
	for i, expression := range expressions {
		literal, ok := expression.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return nil
		}
		decoded, err := strconv.Unquote(literal.Value)
		if err != nil || decoded != values[i] {
			return nil
		}
		literals = append(literals, literal)
	}
	return literals
}

func nativeFieldLiteralOffsets(path string, content []byte, sites []NativeLiteralSite) map[int]bool {
	if path != b01NativeGoSource && path != b01NativeJSONSource && path != b03CatalogJSONSource {
		return nil
	}
	if !validNativeFieldLiterals(sites) {
		return nil
	}
	if path == b01NativeJSONSource || path == b03CatalogJSONSource {
		return nativeJSONFieldOffsets(path, content, sites)
	}
	if path != b01NativeGoSource {
		return nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, 0)
	if err != nil {
		return nil
	}
	functions := make(map[string][]*ast.FuncDecl)
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			functions[function.Name.Name] = append(functions[function.Name.Name], function)
		}
	}
	offsets := make(map[int]bool)
	for _, site := range sites {
		if site.Path != path {
			continue
		}
		name, _, _ := strings.Cut(site.Field, ".")
		if len(functions[name]) != 1 {
			continue
		}
		function := functions[name][0]
		if function.Recv != nil || function.Body == nil {
			continue
		}
		var literals []*ast.BasicLit
		switch site.ID {
		case "ready-types":
			if function.Type.Params.NumFields() != 0 || len(function.Body.List) != 1 {
				continue
			}
			statement, ok := function.Body.List[0].(*ast.ReturnStmt)
			if !ok || len(statement.Results) != 1 {
				continue
			}
			vector, ok := statement.Results[0].(*ast.CompositeLit)
			if !ok {
				continue
			}
			array, ok := vector.Type.(*ast.ArrayType)
			if !ok || array.Len != nil {
				continue
			}
			element, ok := array.Elt.(*ast.Ident)
			if !ok || element.Name != "string" {
				continue
			}
			literals = nativeStringExpressions(vector.Elts, site.Values)
		case "custom-types":
			// Only the direct command statement in this function can bind argv.
			// A nested closure, quoted source or duplicate command grants nothing.
			matches := 0
			for _, statement := range function.Body.List {
				expression, ok := statement.(*ast.ExprStmt)
				if !ok {
					continue
				}
				call, ok := expression.X.(*ast.CallExpr)
				if !ok || call.Ellipsis.IsValid() {
					continue
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "run" {
					continue
				}
				receiver, ok := selector.X.(*ast.Ident)
				if !ok || receiver.Name != "c" {
					continue
				}
				if candidate := nativeStringExpressions(call.Args, site.Values); candidate != nil {
					matches++
					literals = candidate
				}
			}
			if matches != 1 {
				continue
			}
		}
		for _, literal := range literals {
			offsets[fset.Position(literal.Pos()).Offset] = true
		}
	}
	return offsets
}

// Decode keys before checking uniqueness. A source-shaped copy under another
// field, or any duplicate decoded object key, cannot select the admitted value.
func uniqueJSONField(text, key string) (json.RawMessage, int, bool) {
	decoder := json.NewDecoder(strings.NewReader(text))
	if opening, err := decoder.Token(); err != nil || opening != json.Delim('{') {
		return nil, 0, false
	}
	seen := make(map[string]bool)
	var selected json.RawMessage
	start := 0
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] {
			return nil, 0, false
		}
		seen[name] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, 0, false
		}
		if name == key {
			selected, start = value, int(decoder.InputOffset())-len(value)
		}
	}
	if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') {
		return nil, 0, false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, 0, false
	}
	return selected, start, selected != nil
}

func nativeJSONFieldOffsets(path string, content []byte, sites []NativeLiteralSite) map[int]bool {
	for _, site := range sites {
		if site.Path != path {
			continue
		}
		selected, start := json.RawMessage(content), 0
		for _, key := range strings.Split(site.Field, ".") {
			value, offset, ok := uniqueJSONField(string(selected), key)
			if !ok {
				return nil
			}
			selected, start = value, start+offset
		}
		var values []string
		if site.ID == "catalog-sql-tables" {
			var scalar string
			if json.Unmarshal(selected, &scalar) != nil {
				return nil
			}
			values = []string{scalar}
		} else if json.Unmarshal(selected, &values) != nil {
			return nil
		}
		if len(values) != len(site.Values) {
			return nil
		}
		for i := range values {
			if values[i] != site.Values[i] {
				return nil
			}
		}
		offsets := make(map[int]bool)
		for _, literal := range jsonStringLiterals(content) {
			if literal.start >= start && literal.end <= start+len(selected) {
				offsets[literal.start] = true
			}
		}
		return offsets
	}
	return nil
}

// Used only at the exact JSON protocol paths. The JSON decoder, rather than Go
// unquoting, handles JSON escapes; every non-admitted string still scans.
func jsonStringLiterals(content []byte) []goStringLiteral {
	if !json.Valid(content) {
		return nil
	}
	var literals []goStringLiteral
	for i := 0; i < len(content); i++ {
		if content[i] != '"' {
			continue
		}
		start := i
		for i++; i < len(content); i++ {
			if content[i] == '\\' {
				i++
			} else if content[i] == '"' {
				break
			}
		}
		var value string
		if json.Unmarshal(content[start:i+1], &value) == nil {
			literals = append(literals, goStringLiteral{start: start, end: i + 1, line: lineAt(string(content), start), value: value})
		}
	}
	return literals
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
	var literals []goStringLiteral
	if (path == b01NativeJSONSource || path == b03CatalogJSONSource) && !json.Valid(content) {
		return []Finding{{Path: path, Line: 1, Class: "invalid-json", Term: "parse"}}
	}
	fieldOffsets := nativeFieldLiteralOffsets(path, content, cfg.NativeFieldLiterals)
	if path == externalProtocolSource && len(cfg.ExternalProtocolString.PhysicalValues) != 0 || path == nativeManifestProtocolSource && len(cfg.NativeManifestStrings.Values) != 0 || path == b01NativeGoSource || path == b01NativeJSONSource || path == b03CatalogJSONSource || hasNativeControlAllowance(path, cfg) {
		masked := append([]byte(nil), content...)
		sourceLiterals := goStringLiterals(path, content)
		if path == b01NativeJSONSource || path == b03CatalogJSONSource {
			sourceLiterals = jsonStringLiterals(content)
		}
		for _, literal := range sourceLiterals {
			if !literal.imported && (fieldOffsets[literal.start] || allowsConfiguredLiteral(path, literal.value, cfg) || allowsNativeManifestString(path, literal.value, cfg.NativeManifestStrings)) {
				for i := literal.start; i < literal.end; i++ {
					if masked[i] != '\n' && masked[i] != '\r' {
						masked[i] = ' '
					}
				}
			} else {
				literals = append(literals, literal)
			}
		}
		text = string(masked)
	}
	var findings []Finding
	appendDecoded := func(literal goStringLiteral, term Term, match string) {
		finding := Finding{Path: path, Line: literal.line, Class: term.Class, Term: match}
		for _, previous := range findings {
			if previous == finding {
				return
			}
		}
		findings = append(findings, finding)
	}
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
			for _, literal := range literals {
				for _, match := range re.FindAllString(literal.value, -1) {
					for _, r := range match {
						if !term.Allow[r] {
							appendDecoded(literal, term, term.Pattern)
							break
						}
					}
				}
			}
			continue
		}
		if term.Class == "identifier" {
			if filepath.Ext(path) != ".go" {
				continue
			}
			findings = append(findings, scanGoIdentifiers(path, content, term, cfg)...)
			continue
		}
		for _, match := range term.Matches {
			if at := phraseIndex(text, match); at >= 0 {
				findings = append(findings, Finding{Path: path, Line: lineAt(text, at), Class: term.Class, Term: match})
			}
			// Decode non-admitted strings at these exact protocol paths too;
			// retain the established scan at every other source path.
			for _, literal := range literals {
				if phraseIndex(literal.value, match) >= 0 {
					appendDecoded(literal, term, match)
				}
			}
		}
	}
	if filepath.Ext(path) == ".go" {
		findings = append(findings, scanGoImports(path, content)...)
	}
	return findings
}

type goStringLiteral struct {
	start, end, line int
	value            string
	imported         bool
}

func goStringLiterals(path string, content []byte) []goStringLiteral {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, 0)
	if err != nil {
		return nil // The identifier scan retains the invalid-Go refusal.
	}
	imports := make(map[token.Pos]bool, len(file.Imports))
	for _, spec := range file.Imports {
		imports[spec.Path.Pos()] = true
	}
	var literals []goStringLiteral
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err == nil {
			position := fset.Position(literal.Pos())
			literals = append(literals, goStringLiteral{start: position.Offset, end: fset.Position(literal.End()).Offset, line: position.Line, value: value, imported: imports[literal.Pos()]})
		}
		return true
	})
	return literals
}

func scanGoIdentifiers(path string, content []byte, term Term, cfg Config) []Finding {
	allowance := cfg.ExternalProtocolString
	fieldOffsets := nativeFieldLiteralOffsets(path, content, cfg.NativeFieldLiterals)
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
				decoded, err := strconv.Unquote(n.Value)
				if err != nil {
					findings = append(findings, Finding{Path: path, Line: fset.Position(node.Pos()).Line, Class: "invalid-go", Term: "string"})
					return true
				}
				if path == externalProtocolSource || path == nativeManifestProtocolSource || path == b01NativeGoSource || hasNativeControlAllowance(path, cfg) {
					if !imports[n.Pos()] && (fieldOffsets[fset.Position(n.Pos()).Offset] || path == externalProtocolSource && allowance.Path == path && decoded == allowance.Value && protocolLiteralDigest(decoded) == externalEnvironmentDigest || allowsPhysicalProtocolString(path, decoded, allowance) || allowsNativeManifestString(path, decoded, cfg.NativeManifestStrings) || allowsNativeControlLiteral(path, decoded, cfg)) {
						return true
					}
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
