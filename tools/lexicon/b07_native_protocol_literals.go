package lexicon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

const b07NativeProtocolSource = "spec/cardb07/native_test.go"

type b07NativeProtocolSite struct {
	method, contextSHA256, valueSHA256 string
	contextStart, contextEnd           int
	relative, absolute                 int
}

// These three nodes belong to two immutable receiver-method contexts. The
// complete spans identify the nodes; they never exempt either method body.
func b07NativeProtocolSites() []b07NativeProtocolSite {
	return []b07NativeProtocolSite{
		{
			method: "metadataAPI", contextStart: 24236, contextEnd: 29179,
			contextSHA256: "371ce34b18a201f2b6430567b126608b7a8478703205f61b1d22937db08866c1",
			valueSHA256:   "32d6f0a5c7dd84ad27ec70dc3497538db281818520ce79b73eebf16c7672ee47",
			relative:      2268, absolute: 26504,
		},
		{
			method: "metadataDelta", contextStart: 29332, contextEnd: 31984,
			contextSHA256: "f308a1608fbd44578f2820a4a42201c3b0f54aa8e250f50799dd67d8fab9dccd",
			valueSHA256:   "91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75",
			relative:      1051, absolute: 30383,
		},
		{
			method: "metadataDelta", contextStart: 29332, contextEnd: 31984,
			contextSHA256: "f308a1608fbd44578f2820a4a42201c3b0f54aa8e250f50799dd67d8fab9dccd",
			valueSHA256:   "93602e05a1bff1bd127187c713caaeab338848eb4a6cba051123440a912cdf26",
			relative:      1998, absolute: 31330,
		},
	}
}

func b07NativeProtocolLiteralOffsets(path string, content []byte) map[int]bool {
	return selectB07NativeProtocolLiterals(path, content, b07NativeProtocolSites())
}

func selectB07NativeProtocolLiterals(path string, content []byte, sites []b07NativeProtocolSite) map[int]bool {
	if path != b07NativeProtocolSource || len(sites) != 3 {
		return nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, 0)
	if err != nil {
		return nil
	}
	declarations := make(map[string]int)
	methods := make(map[string]*ast.FuncDecl)
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *ast.FuncDecl:
			declarations[node.Name.Name]++
			methods[node.Name.Name] = node
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					declarations[spec.Name.Name]++
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						declarations[name.Name]++
					}
				}
			}
		}
	}
	if declarations["b07Native"] != 1 {
		return nil
	}
	selected := make(map[int]bool)
	digests := make(map[string]bool)
	for _, site := range sites {
		method := methods[site.method]
		if declarations[site.method] != 1 || method == nil || method.Body == nil || method.Recv == nil || len(method.Recv.List) != 1 {
			return nil
		}
		pointer, ok := method.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			return nil
		}
		receiver, ok := pointer.X.(*ast.Ident)
		if !ok || receiver.Name != "b07Native" {
			return nil
		}
		start, end := fset.Position(method.Pos()).Offset, fset.Position(method.End()).Offset
		if start != site.contextStart || end != site.contextEnd || protocolLiteralDigest(string(content[start:end])) != site.contextSHA256 {
			return nil
		}
		if site.relative < 0 || site.absolute != start+site.relative || digests[site.valueSHA256] || len(site.valueSHA256) != 64 {
			return nil
		}
		digests[site.valueSHA256] = true
		matches, occurrences := 0, 0
		// Count the value across the complete file, including imports and other
		// declarations. A copied value cannot inherit this node's admission.
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil || protocolLiteralDigest(value) != site.valueSHA256 {
				return true
			}
			occurrences++
			offset := fset.Position(literal.Pos()).Offset
			if offset == site.absolute && offset > start && fset.Position(literal.End()).Offset < end {
				selected[offset] = true
				matches++
			}
			return true
		})
		if matches != 1 || occurrences != 1 {
			return nil
		}
	}
	if len(selected) != 3 {
		return nil
	}
	return selected
}
