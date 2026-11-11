package lexicon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

const b07RepresentedModelSource = "internal/core/model/changes.go"
const b07RepresentedControlSource = "spec/cardb07/opaque_test.go"

type b07RepresentedBinding struct {
	path, function, contextSHA256, valueSHA256 string
	offset                                     int
}

// These two complete function contexts bind one represented-field string
// each. Only the selected nonimport nodes gain admission; every other string,
// comment, identifier, declaration and import continues through the checker.
func b07RepresentedLiteralOffsets(path string, content []byte) map[int]bool {
	binding := b07RepresentedBinding{
		path:        path,
		valueSHA256: "7853d42458d77d5bd188bd1a1bba51e87257cbe5ec12826168bc639f95bb0a72",
	}
	switch path {
	case b07RepresentedModelSource:
		binding.function = "ValidateNativeExtensions"
		binding.contextSHA256 = "dbfa30004d954e236d2440721260d0876f751058c5103b0c62cb49db4fde23d4"
		binding.offset = 396
	case b07RepresentedControlSource:
		binding.function = "b07OpaquePublic"
		binding.contextSHA256 = "8d99db8c4cf3cc7dd812a2940642dabe79a773b0dbedb42225dc582a58486ebe"
		binding.offset = 1879
	default:
		return nil
	}
	return selectB07RepresentedLiteral(path, content, binding)
}

func selectB07RepresentedLiteral(path string, content []byte, binding b07RepresentedBinding) map[int]bool {
	if path != binding.path || binding.offset < 0 || binding.valueSHA256 == "" {
		return nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, 0)
	if err != nil {
		return nil
	}
	var function *ast.FuncDecl
	declarations := 0
	for _, declaration := range file.Decls {
		switch node := declaration.(type) {
		case *ast.FuncDecl:
			if node.Name.Name == binding.function {
				declarations++
				function = node
			}
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch spec := spec.(type) {
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						if name.Name == binding.function {
							declarations++
						}
					}
				case *ast.TypeSpec:
					if spec.Name.Name == binding.function {
						declarations++
					}
				}
			}
		}
	}
	if declarations != 1 || function == nil || function.Recv != nil || function.Body == nil {
		return nil
	}
	start, end := fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset
	if protocolLiteralDigest(string(content[start:end])) != binding.contextSHA256 {
		return nil
	}
	selected := make(map[int]bool)
	ast.Inspect(function.Body, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		offset := fset.Position(literal.Pos()).Offset
		value, err := strconv.Unquote(literal.Value)
		if offset-start == binding.offset && err == nil && protocolLiteralDigest(value) == binding.valueSHA256 {
			selected[offset] = true
		}
		return true
	})
	if len(selected) != 1 {
		return nil
	}
	return selected
}
