package lexicon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

const b02NativeRecordingSource = "spec/cardb20/b02_native_recording_test.go"

type b02RecordingBinding struct {
	contextSHA256 string
	valueSHA256   string
	offsets       [3]int
}

// Only three direct string nodes in the frozen recorder gain admission. The
// complete unique function context selects those nodes; its other strings,
// comments, identifiers and all surrounding declarations still scan normally.
func b02RecordingLiteralOffsets(path string, content []byte) map[int]bool {
	return selectB02RecordingLiterals(path, content, b02RecordingBinding{
		contextSHA256: "45ca8f4f701e540d468a7d7839d849aed84ec29804b0497ea51ad5562f21d891",
		valueSHA256:   "a250fe4bfeccf35cd1f34c8eb7625ef05127294cdeb6884a04adea45796adebc",
		offsets:       [3]int{4751, 6371, 6996},
	})
}

func selectB02RecordingLiterals(path string, content []byte, binding b02RecordingBinding) map[int]bool {
	if path != b02NativeRecordingSource {
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
			if node.Name.Name == "b02ObserveNative" {
				declarations++
				function = node
			}
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch spec := spec.(type) {
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						if name.Name == "b02ObserveNative" {
							declarations++
						}
					}
				case *ast.TypeSpec:
					if spec.Name.Name == "b02ObserveNative" {
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
	wanted := make(map[int]bool, len(binding.offsets))
	for _, offset := range binding.offsets {
		if offset < 0 || wanted[offset] {
			return nil
		}
		wanted[offset] = true
	}
	selected := make(map[int]bool)
	ast.Inspect(function.Body, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		offset := fset.Position(literal.Pos()).Offset
		value, err := strconv.Unquote(literal.Value)
		if wanted[offset-start] && err == nil && protocolLiteralDigest(value) == binding.valueSHA256 {
			selected[offset] = true
		}
		return true
	})
	if len(selected) != len(binding.offsets) {
		return nil
	}
	return selected
}
