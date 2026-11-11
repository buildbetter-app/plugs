package lexicon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

const b02NativeRecordingSource = "spec/cardb20/b02_native_recording_test.go"
const b02DeltaNativeRecordingSource = "spec/cardb20/b02_delta_native_test.go"

type b02RecordingSite struct {
	offset      int
	valueSHA256 string
}

type b02RecordingBinding struct {
	contextSHA256 string
	sites         []b02RecordingSite
}

// Only five direct string nodes in the frozen recorder gain admission. The
// complete postimage and unique function context bind the supplied source;
// its other strings, comments, identifiers and declarations still scan.
func b02RecordingLiteralOffsets(path string, content []byte) map[int]bool {
	if path == b02DeltaNativeRecordingSource {
		return b02DeltaRecordingLiteralOffsets(path, content)
	}
	if path != b02NativeRecordingSource || protocolLiteralDigest(string(content)) != "32b2b7e810785ab4971000dc782183ed4575910af9d54b6a510b79cbb091208e" {
		return nil
	}
	const tableValueSHA256 = "a250fe4bfeccf35cd1f34c8eb7625ef05127294cdeb6884a04adea45796adebc"
	const columnValueSHA256 = "7853d42458d77d5bd188bd1a1bba51e87257cbe5ec12826168bc639f95bb0a72"
	return selectB02RecordingFunction(path, content, "b02ObserveNative", b02RecordingBinding{
		contextSHA256: "45ca8f4f701e540d468a7d7839d849aed84ec29804b0497ea51ad5562f21d891",
		sites: []b02RecordingSite{
			{4751, tableValueSHA256},
			{6371, tableValueSHA256},
			{6996, tableValueSHA256},
			{6655, columnValueSHA256},
			{7074, columnValueSHA256},
		},
	})
}

// The new public-writer recorder has two independently guarded literal
// contexts. Keeping the formula payload and source-search paths bound to their
// owning functions prevents a copied diagnostic or adjacent test from gaining
// the allowance.
func b02DeltaRecordingLiteralOffsets(path string, content []byte) map[int]bool {
	if path != b02DeltaNativeRecordingSource || protocolLiteralDigest(string(content)) != "2fd5bb652a5980d5e50e977f3af365216fb7e4453f673b09fb348c6fbcb5d68c" {
		return nil
	}
	bindings := []struct {
		function, context string
		sites             []b02RecordingSite
	}{
		{
			function: "TestB02NativeNewPublicWriterWIPAndReasonProvenance",
			context:  "afcd3d14a71f065149257f4c4b23ea0221144292016c4439c44b46f0ff654f9f",
			sites: []b02RecordingSite{
				{4204, "cf99c7433aa76d6d611e334454f1630516bbcc3627557973cfb8b55c37f1d818"},
				{4481, "3661141c121cbe5b082b9852343d1d595ee5b5e0320549aa99dffe51cf0ba36a"},
			},
		},
		{
			function: "b02DeltaWriterSource",
			context:  "c23cec578afb0ebff28140bbf065b4c75008b33b774c67f4ca534c88dceb9105",
			sites: []b02RecordingSite{
				{0, "26b967b81945b1f29e51ee6b66abd6fcaf05f7c245f3fa96efee020b6669dd38"},
				{0, "aa2de5dd0ae80783cec8fd6a7349dad46190fc48074f6f0fd34095e9ad5c0df2"},
				{0, "26b967b81945b1f29e51ee6b66abd6fcaf05f7c245f3fa96efee020b6669dd38"},
			},
		},
	}
	selected := make(map[int]bool)
	for _, binding := range bindings {
		part := selectB02RecordingFunction(path, content, binding.function, b02RecordingBinding{contextSHA256: binding.context, sites: binding.sites})
		if len(part) != len(binding.sites) {
			return nil
		}
		for offset := range part {
			selected[offset] = true
		}
	}
	return selected
}

func selectB02RecordingLiterals(path string, content []byte, binding b02RecordingBinding) map[int]bool {
	return selectB02RecordingFunction(path, content, "b02ObserveNative", binding)
}

func selectB02RecordingFunction(path string, content []byte, functionName string, binding b02RecordingBinding) map[int]bool {
	if path != b02NativeRecordingSource && path != b02DeltaNativeRecordingSource {
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
			if node.Name.Name == functionName {
				declarations++
				function = node
			}
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch spec := spec.(type) {
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						if name.Name == functionName {
							declarations++
						}
					}
				case *ast.TypeSpec:
					if spec.Name.Name == functionName {
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
	wanted := make(map[int]string, len(binding.sites))
	if len(binding.sites) == 0 {
		return nil
	}
	for _, site := range binding.sites {
		if _, duplicate := wanted[site.offset]; site.offset < 0 || duplicate || site.valueSHA256 == "" {
			return nil
		}
		wanted[site.offset] = site.valueSHA256
	}
	selected := make(map[int]bool)
	ast.Inspect(function.Body, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		offset := fset.Position(literal.Pos()).Offset
		value, err := strconv.Unquote(literal.Value)
		if expected, wanted := wanted[offset-start]; wanted && err == nil && protocolLiteralDigest(value) == expected {
			selected[offset] = true
		}
		return true
	})
	if len(selected) != len(binding.sites) {
		return nil
	}
	return selected
}
