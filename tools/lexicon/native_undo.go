package lexicon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
)

// The source inventory's path entries refer to the same exact SQL corpus.
// Mask only complete canonical reference lines. Inventory completeness and
// the referenced file hashes are still enforced independently by the tree
// checker; changed lines, added siblings and every other byte still scan.
func undoReferenceManifestText(path string, content []byte) string {
	if path != sourceManifestPath {
		return string(content)
	}
	references := undoSQLReferences()
	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		if _, ok := references[line]; ok {
			lines[i] = strings.Repeat(" ", len(line))
		}
	}
	return strings.Join(lines, "\n")
}

// A binding selects individual decoded string literals in one unique named
// top-level AST declaration. Its entire declaration context and each literal's
// relative offset and decoded value are independently digest-bound. A context
// hash is evidence for selecting these offsets, never a declaration exemption:
// imports, identifiers, comments and every other literal still scan normally.
type undoLiteralBinding struct {
	name, kind, contextSHA256 string
	literals                  map[int]string
}

func undoLiteralOffsets(path string, content []byte) map[int]bool {
	bindings := undoLiteralBindings[path]
	if len(bindings) == 0 {
		return nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, content, 0)
	if err != nil {
		return nil
	}
	type declaration struct {
		node ast.Node
		kind string
	}
	declarations := make(map[string][]declaration)
	for _, node := range file.Decls {
		switch node := node.(type) {
		case *ast.FuncDecl:
			declarations[node.Name.Name] = append(declarations[node.Name.Name], declaration{node, "func"})
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch spec := spec.(type) {
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						declarations[name.Name] = append(declarations[name.Name], declaration{node, node.Tok.String()})
					}
				case *ast.TypeSpec:
					declarations[spec.Name.Name] = append(declarations[spec.Name.Name], declaration{node, "type"})
				}
			}
		}
	}
	offsets := make(map[int]bool)
	for _, binding := range bindings {
		matches := declarations[binding.name]
		if len(matches) != 1 || matches[0].kind != binding.kind {
			continue
		}
		node := matches[0].node
		start, end := fset.Position(node.Pos()).Offset, fset.Position(node.End()).Offset
		if protocolLiteralDigest(string(content[start:end])) != binding.contextSHA256 {
			continue
		}
		selected := make(map[int]bool)
		ast.Inspect(node, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			offset := fset.Position(literal.Pos()).Offset
			expected, ok := binding.literals[offset-start]
			value, err := strconv.Unquote(literal.Value)
			if ok && err == nil && protocolLiteralDigest(value) == expected {
				selected[offset] = true
			}
			return true
		})
		if len(selected) == len(binding.literals) {
			for offset := range selected {
				offsets[offset] = true
			}
		}
	}
	return offsets
}
