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

const nativeHTTPDomainSource = "spec/s1-b09-native/domain_recording_test.go"
const sourcePlaneProtocolSource = "internal/storage/driver/source_planes_validate.go"
const sourcePlaneFixtureSource = "internal/storage/driver/source_planes_test.go"

func protocolLiteralDigest(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

// Admit the native listener announcement only at its unique parsing call.
// The path, receiver, method, call and entire decoded literal are fixed; a
// copied diagnostic, adjacent file, import or duplicate call gains nothing.
func nativeHTTPListenerOffset(path string, content []byte) (int, bool) {
	if path != nativeHTTPDomainSource {
		return 0, false
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, 0)
	if err != nil {
		return 0, false
	}
	var function *ast.FuncDecl
	for _, declaration := range file.Decls {
		if candidate, ok := declaration.(*ast.FuncDecl); ok && candidate.Name.Name == "styleADomainCreate" {
			if function != nil {
				return 0, false
			}
			function = candidate
		}
	}
	if function == nil || function.Body == nil || function.Recv == nil || len(function.Recv.List) != 1 {
		return 0, false
	}
	pointer, ok := function.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return 0, false
	}
	receiver, ok := pointer.X.(*ast.Ident)
	if !ok || receiver.Name != "recorder" {
		return 0, false
	}
	matches, offset := 0, 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if _, closure := node.(*ast.FuncLit); closure {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok || call.Ellipsis.IsValid() || len(call.Args) != 2 {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "CutPrefix" {
			return true
		}
		name, ok := selector.X.(*ast.Ident)
		if !ok || name.Name != "strings" {
			return true
		}
		literal, ok := call.Args[1].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err == nil && protocolLiteralDigest(value) == "d56ebf9119cfd53f6690d8ec4111275a19c16b5c1c89123d3ff99499b81586cd" {
			matches++
			offset = fset.Position(literal.Pos()).Offset
		}
		return true
	})
	return offset, matches == 1
}

func protocolFieldLiteralOffsets(path string, content []byte, sites []NativeLiteralSite) map[int]bool {
	offsets := nativeFieldLiteralOffsets(path, content, sites)
	if offset, ok := nativeHTTPListenerOffset(path, content); ok {
		if offsets == nil {
			offsets = make(map[int]bool)
		}
		offsets[offset] = true
	}
	return offsets
}

func validSourcePlaneConstants(allowances []GoConstantAllowance) bool {
	want := map[string]bool{
		"fe552dc8dc9e69d88a9f9eb00b1ad65eb317b9d53f956115eaae5efc56376876": true,
		"2a882a552d7e1e6461ee57b1d1b2dfc7b6d05b50f26ef2ce5811e78440f35216": true,
	}
	if len(allowances) != len(want) {
		return false
	}
	for _, allowance := range allowances {
		fields := []string{allowance.Path, allowance.Name, allowance.Value}
		for _, field := range fields {
			if strings.ContainsRune(field, '\x00') {
				return false
			}
		}
		digest := protocolLiteralDigest(strings.Join(fields, "\x00"))
		if !want[digest] {
			return false
		}
		delete(want, digest)
	}
	return true
}

// Only the unique top-level untyped constant's direct string initializer gains
// an offset. Copies, local declarations and other source contexts grant none.
// An invalid declaration of the bound name is refused even if its initializer
// splits the physical spelling across multiple expressions.
func sourcePlaneConstantLiteralOffset(path string, content []byte, allowances []GoConstantAllowance) (offset int, admitted, invalid bool) {
	if path != sourcePlaneProtocolSource && path != sourcePlaneFixtureSource || !validSourcePlaneConstants(allowances) {
		return 0, false, false
	}
	var allowance GoConstantAllowance
	for _, candidate := range allowances {
		if candidate.Path == path {
			allowance = candidate
		}
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, 0)
	if err != nil {
		return 0, false, false
	}
	count := 0
	var selected *ast.BasicLit
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *ast.FuncDecl:
			if node.Name.Name == allowance.Name {
				count++
			}
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					if spec.Name.Name == allowance.Name {
						count++
					}
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						if name.Name != allowance.Name {
							continue
						}
						count++
						if node.Tok != token.CONST || spec.Type != nil || len(spec.Names) != 1 || len(spec.Values) != 1 {
							continue
						}
						literal, ok := spec.Values[0].(*ast.BasicLit)
						if !ok || literal.Kind != token.STRING {
							continue
						}
						if value, err := strconv.Unquote(literal.Value); err == nil && value == allowance.Value {
							selected = literal
						}
					}
				}
			}
		}
	}
	if count != 1 || selected == nil {
		return 0, false, count != 0
	}
	return fset.Position(selected.Pos()).Offset, true, false
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

func validB03NativeGoLiterals(sites []NativeLiteralSite) bool {
	want := siteDigests(canonicalRules.B03NativeGoLiterals)
	if len(sites) != len(want) {
		return false
	}
	for _, site := range sites {
		digest, ok := want[site.ID]
		if !ok {
			return false
		}
		fields := append([]string{site.Path, site.Field}, site.Values...)
		for _, field := range fields {
			if strings.ContainsRune(field, '\x00') {
				return false
			}
		}
		if protocolLiteralDigest(strings.Join(fields, "\x00")) != digest {
			return false
		}
		delete(want, site.ID)
	}
	return true
}

func nativeSelectorIs(expression ast.Expr, receiver, member string) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != member {
		return false
	}
	name, ok := selector.X.(*ast.Ident)
	return ok && name.Name == receiver
}

// Only the actual top-level census initializer and the direct manifest lookup
// in its named verification function can admit these two complete values.
func b03NativeGoLiteralOffsets(path string, content []byte, sites []NativeLiteralSite) map[int]bool {
	if path != b03CatalogGoSource || !validB03NativeGoLiterals(sites) {
		return nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, 0)
	if err != nil {
		return nil
	}
	names := make(map[string]int)
	var table *ast.ValueSpec
	var tableKind token.Token
	var verifier *ast.FuncDecl
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *ast.FuncDecl:
			names[node.Name.Name]++
			if node.Name.Name == "verifyB03PreparedArtifact" {
				verifier = node
			}
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch spec := spec.(type) {
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						names[name.Name]++
						if name.Name == "b03NativeTables" {
							table, tableKind = spec, node.Tok
						}
					}
				case *ast.TypeSpec:
					names[spec.Name.Name]++
				}
			}
		}
	}
	offsets := make(map[int]bool)
	for _, site := range sites {
		var literals []*ast.BasicLit
		switch site.ID {
		case "catalog-go-tables":
			if names["b03NativeTables"] != 1 || tableKind != token.VAR || table == nil || table.Type != nil || len(table.Names) != 1 || len(table.Values) != 1 {
				continue
			}
			call, ok := table.Values[0].(*ast.CallExpr)
			if !ok || call.Ellipsis.IsValid() || !nativeSelectorIs(call.Fun, "strings", "Fields") {
				continue
			}
			literals = nativeStringExpressions(call.Args, site.Values)
		case "catalog-go-artifact":
			if names["verifyB03PreparedArtifact"] != 1 || verifier == nil || verifier.Recv != nil || verifier.Body == nil {
				continue
			}
			matches := 0
			for _, statement := range verifier.Body.List {
				assignment, ok := statement.(*ast.AssignStmt)
				if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
					continue
				}
				name, ok := assignment.Lhs[0].(*ast.Ident)
				if !ok || name.Name != "artifact" {
					continue
				}
				index, ok := assignment.Rhs[0].(*ast.IndexExpr)
				if !ok || !nativeSelectorIs(index.X, "manifest", "Artifacts") {
					continue
				}
				if candidate := nativeStringExpressions([]ast.Expr{index.Index}, site.Values); candidate != nil {
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

// The fixed digests bind every field and the complete ordered decoded vector.
// Delimiters cannot be injected to repartition a tuple; config alone cannot
// introduce another source field or value. An absent/invalid set grants nothing.
func validNativeFieldLiterals(sites []NativeLiteralSite) bool {
	want := siteDigests(canonicalRules.NativeFieldLiterals)
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
		if digest, consumed := cfg.ruleSources[path]; consumed {
			if sha256.Sum256(content) != digest {
				return nil, fmt.Errorf("lexicon rule %s changed after loading", path)
			}
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
	cfg, err := LoadConfig(filepath.Join(root, rulesDirectory))
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
	text := undoReferenceManifestText(path, content)
	var literals []goStringLiteral
	sourcePlaneOffset, sourcePlaneAllowed, sourcePlaneInvalid := sourcePlaneConstantLiteralOffset(path, content, cfg.SourcePlaneConstants)
	if (path == b01NativeJSONSource || path == b03CatalogJSONSource) && !json.Valid(content) {
		return []Finding{{Path: path, Line: 1, Class: "invalid-json", Term: "parse"}}
	}
	fieldOffsets := protocolFieldLiteralOffsets(path, content, cfg.NativeFieldLiterals)
	b03Offsets := b03NativeGoLiteralOffsets(path, content, cfg.B03NativeGoLiterals)
	undoOffsets := undoLiteralOffsets(path, content)
	b02Offsets := b02RecordingLiteralOffsets(path, content)
	b07Offsets := b07RepresentedLiteralOffsets(path, content)
	b07ProtocolOffsets := b07NativeProtocolLiteralOffsets(path, content)
	if path == externalProtocolSource && len(cfg.ExternalProtocolString.PhysicalValues) != 0 || path == nativeManifestProtocolSource && len(cfg.NativeManifestStrings.Values) != 0 || path == b01NativeGoSource || path == b01NativeJSONSource || path == b03CatalogJSONSource || path == b03CatalogGoSource || path == nativeHTTPDomainSource || hasNativeControlAllowance(path, cfg) || path == sourcePlaneProtocolSource || path == sourcePlaneFixtureSource || len(undoLiteralBindings[path]) != 0 || len(b07Offsets) != 0 || len(b07ProtocolOffsets) != 0 || path == b02NativeRecordingSource || len(b02Offsets) != 0 {
		masked := append([]byte(nil), content...)
		sourceLiterals := goStringLiterals(path, content)
		if path == b01NativeJSONSource || path == b03CatalogJSONSource {
			sourceLiterals = jsonStringLiterals(content)
		}
		for _, literal := range sourceLiterals {
			if !literal.imported && (sourcePlaneAllowed && literal.start == sourcePlaneOffset || fieldOffsets[literal.start] || b03Offsets[literal.start] || undoOffsets[literal.start] || b02Offsets[literal.start] || b07Offsets[literal.start] || b07ProtocolOffsets[literal.start] || allowsConfiguredLiteral(path, literal.value, cfg) || allowsNativeManifestString(path, literal.value, cfg.NativeManifestStrings)) {
				start, end := literal.start, literal.end
				if b07ProtocolOffsets[literal.start] {
					start, end = start+1, end-1 // Keep both string delimiters; mask only this node's payload.
				}
				for i := start; i < end; i++ {
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
	if sourcePlaneInvalid {
		findings = append(findings, Finding{Path: path, Line: 1, Class: "invalid-protocol-literal", Term: "source-plane-constant"})
	}
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
	fieldOffsets := protocolFieldLiteralOffsets(path, content, cfg.NativeFieldLiterals)
	b03Offsets := b03NativeGoLiteralOffsets(path, content, cfg.B03NativeGoLiterals)
	undoOffsets := undoLiteralOffsets(path, content)
	b02Offsets := b02RecordingLiteralOffsets(path, content)
	b07Offsets := b07RepresentedLiteralOffsets(path, content)
	b07ProtocolOffsets := b07NativeProtocolLiteralOffsets(path, content)
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
				if !imports[n.Pos()] && (undoOffsets[fset.Position(n.Pos()).Offset] || b02Offsets[fset.Position(n.Pos()).Offset]) {
					return true
				}
				if path == externalProtocolSource || path == nativeManifestProtocolSource || path == b01NativeGoSource || path == b03CatalogGoSource || path == nativeHTTPDomainSource || hasNativeControlAllowance(path, cfg) || len(b02Offsets) != 0 || len(b07Offsets) != 0 || len(b07ProtocolOffsets) != 0 {
					if !imports[n.Pos()] && (fieldOffsets[fset.Position(n.Pos()).Offset] || b03Offsets[fset.Position(n.Pos()).Offset] || b07Offsets[fset.Position(n.Pos()).Offset] || b07ProtocolOffsets[fset.Position(n.Pos()).Offset] || path == externalProtocolSource && allowance.Path == path && decoded == allowance.Value && protocolLiteralDigest(decoded) == externalEnvironmentDigest || allowsPhysicalProtocolString(path, decoded, allowance) || allowsNativeManifestString(path, decoded, cfg.NativeManifestStrings) || allowsNativeControlLiteral(path, decoded, cfg)) {
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
