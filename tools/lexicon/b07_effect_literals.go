package lexicon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

const b07EffectSource = "spec/cardb07effects/native_state_test.go"

type b07EffectSite struct {
	name, valueSHA256 string
	relative, absolute int
}

// This immutable declaration binds only six individual physical protocol
// nodes. The declaration span itself never receives a scanner exemption.
func b07EffectSites() []b07EffectSite {
	return []b07EffectSite{
		{"effectNativeActiveTable", "59c079ce9ec72c92a350981898918652bdf0d1401d544ce378ebd5b776b67619", 35, 283},
		{"effectNativeActiveEdges", "a250fe4bfeccf35cd1f34c8eb7625ef05127294cdeb6884a04adea45796adebc", 70, 318},
		{"effectNativeActiveEvents", "5f3c5fb3612b7ea7e7df0cf4f21c4ad123f1603b84d03ad8605fe4b8f67c5025", 118, 366},
		{"effectNativeActiveTarget", "7853d42458d77d5bd188bd1a1bba51e87257cbe5ec12826168bc639f95bb0a72", 160, 408},
		{"effectNativeJournal", "91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75", 204, 452},
		{"effectNativeSequence", "93602e05a1bff1bd127187c713caaeab338848eb4a6cba051123440a912cdf26", 248, 496},
	}
}

func b07EffectLiteralOffsets(path string, content []byte) map[int]bool {
	return selectB07EffectLiterals(path, content, b07EffectSites())
}

func selectB07EffectLiterals(path string, content []byte, sites []b07EffectSite) map[int]bool {
	if path != b07EffectSource || len(sites) != 6 { return nil }
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, parser.ParseComments)
	if err != nil || file.Name.Name != "cardb20" { return nil }
	declarations := map[string]int{}
	var block *ast.GenDecl
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *ast.FuncDecl:
			declarations[node.Name.Name]++
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					declarations[spec.Name.Name]++
				case *ast.ValueSpec:
					for _, name := range spec.Names { declarations[name.Name]++ }
				}
			}
			if fset.Position(node.Pos()).Offset == 248 { block = node }
		}
	}
	if block == nil || block.Tok != token.CONST || !block.Lparen.IsValid() || len(block.Specs) != 6 || fset.Position(block.End()).Offset != 513 || protocolLiteralDigest(string(content[248:513])) != "f7158e38dfa3ae18b4d2890353af98fb5d1d0af006fec7b6cce5da70fc0dc776" { return nil }
	selected := map[int]bool{}
	for i, site := range sites {
		if declarations[site.name] != 1 || len(site.valueSHA256) != 64 || site.relative < 0 || site.absolute != 248+site.relative { return nil }
		spec, ok := block.Specs[i].(*ast.ValueSpec)
		if !ok || spec.Type != nil || spec.Doc != nil || spec.Comment != nil || len(spec.Names) != 1 || spec.Names[0].Name != site.name || len(spec.Values) != 1 { return nil }
		literal, ok := spec.Values[0].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING || len(literal.Value) < 2 || literal.Value[0] != '"' || fset.Position(literal.Pos()).Offset != site.absolute { return nil }
		value, err := strconv.Unquote(literal.Value)
		if err != nil || protocolLiteralDigest(value) != site.valueSHA256 { return nil }
		occurrences := 0
		ast.Inspect(file, func(node ast.Node) bool {
			candidate, ok := node.(*ast.BasicLit)
			if ok && candidate.Kind == token.STRING {
				decoded, err := strconv.Unquote(candidate.Value)
				if err == nil && protocolLiteralDigest(decoded) == site.valueSHA256 { occurrences++ }
			}
			return true
		})
		if occurrences != 1 || selected[site.absolute] { return nil }
		selected[site.absolute] = true
	}
	if len(selected) != 6 { return nil }
	return selected
}
