package census

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
)

type ArtifactDigest struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type BehaviorDeclaration struct {
	Path   string `json:"path"`
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

type BehaviorCard struct {
	ID                 string                `json:"id"`
	Source             ArtifactDigest        `json:"source"`
	Declarations       []BehaviorDeclaration `json:"declarations"`
	ReferenceArtifacts []ArtifactDigest      `json:"referenceArtifacts"`
	Block              BlockCode             `json:"block"`
}

type BehaviorResult struct {
	Format                 string         `json:"format"`
	SourceCommit           string         `json:"sourceCommit"`
	Cards                  []BehaviorCard `json:"cards"`
	UnassignedDeclarations int            `json:"unassignedDeclarations"`
	SHA256                 string         `json:"sha256"`
}

var cardHeading = regexp.MustCompile(`(?m)^## (B[0-9]{2}) — `)
var cardInTest = regexp.MustCompile(`B[0-9]{2}`)

// MeasureBehaviors enumerates the actual catalog and parsed Go test
// declarations in the explicit spec subtree. Names are coverage hints, never
// runtime evidence: every full card remains blocked until complete twin and
// driver qualification exists. Existing slice artifacts retain their hashes
// and limits rather than being silently promoted to whole-card certificates.
func MeasureBehaviors(root fs.FS) (BehaviorResult, error) {
	result := BehaviorResult{Format: "plug-behavior-census/1", SourceCommit: SourceCommit}
	cards := map[string]*BehaviorCard{}
	for _, name := range []string{"work", "content", "history", "surfaces"} {
		file := "engdocs/design/plug/spec/behaviors/" + name + ".md"
		raw, e := fs.ReadFile(root, file)
		if e != nil {
			return BehaviorResult{}, refused(MeasurementInvalid)
		}
		for _, match := range cardHeading.FindAllSubmatch(raw, -1) {
			id := string(match[1])
			if _, exists := cards[id]; exists {
				return BehaviorResult{}, refused(MeasurementInvalid)
			}
			cards[id] = &BehaviorCard{ID: id, Source: ArtifactDigest{file, bytesDigest(raw)}, Declarations: []BehaviorDeclaration{}, ReferenceArtifacts: []ArtifactDigest{}, Block: FullCardUnqualified}
		}
	}
	if len(cards) != 28 {
		return BehaviorResult{}, refused(MeasurementInvalid)
	}
	for i := 1; i <= 28; i++ {
		if cards[fmt.Sprintf("B%02d", i)] == nil {
			return BehaviorResult{}, refused(MeasurementInvalid)
		}
	}
	e := fs.WalkDir(root, "spec", func(file string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.Type()&fs.ModeSymlink != 0 {
			return refused(MeasurementInvalid)
		}
		if d.IsDir() || !strings.HasSuffix(file, "_test.go") {
			return nil
		}
		raw, e := fs.ReadFile(root, file)
		if e != nil {
			return refused(MeasurementInvalid)
		}
		tree, e := parser.ParseFile(token.NewFileSet(), file, raw, 0)
		if e != nil {
			return refused(MeasurementInvalid)
		}
		for _, declaration := range tree.Decls {
			f, ok := declaration.(*ast.FuncDecl)
			if !ok || f.Recv != nil || !strings.HasPrefix(f.Name.Name, "Test") || f.Type.Params.NumFields() != 1 {
				continue
			}
			pointer, ok := f.Type.Params.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			selector, ok := pointer.X.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "T" {
				continue
			}
			assigned := map[string]bool{}
			for _, id := range cardInTest.FindAllString(f.Name.Name, -1) {
				if cards[id] == nil {
					return refused(MeasurementInvalid)
				}
				if !assigned[id] {
					cards[id].Declarations = append(cards[id].Declarations, BehaviorDeclaration{Path: file, Name: f.Name.Name, SHA256: bytesDigest(raw)})
					assigned[id] = true
				}
			}
			if len(assigned) == 0 {
				result.UnassignedDeclarations++
			}
		}
		return nil
	})
	if e != nil {
		return BehaviorResult{}, refused(MeasurementInvalid)
	}
	// This is an inventory of retained slice inputs. Their owning test targets
	// validate provenance and replay; this census never reinterprets outcomes.
	references := map[string][]string{
		"B01": {"spec/cardb01/reference-run-1.json", "spec/cardb01/reference-run-2.json"},
		"B07": {"spec/recorded/extension/extension.json"},
		"B17": {"spec/recorded/goldens.json"},
		"B19": {"spec/cardb19/reference.json", "spec/cardb19/source-binding.json"},
		"B24": {"spec/recorded/goldens.json", "spec/recorded/b24_untimed_defer_deviation.json"},
		"B25": {"spec/recorded/extension/extension.json"},
		"B28": {"spec/cardb28/reference/provenance.json", "spec/cardb28/reference/run-a.json", "spec/cardb28/reference/run-b.json"},
	}
	for id, files := range references {
		for _, file := range files {
			if !fs.ValidPath(file) || path.Clean(file) != file {
				return BehaviorResult{}, refused(MeasurementInvalid)
			}
			raw, e := fs.ReadFile(root, file)
			if e != nil {
				return BehaviorResult{}, refused(MeasurementInvalid)
			}
			cards[id].ReferenceArtifacts = append(cards[id].ReferenceArtifacts, ArtifactDigest{file, bytesDigest(raw)})
		}
	}
	for i := 1; i <= 28; i++ {
		card := *cards[fmt.Sprintf("B%02d", i)]
		sort.Slice(card.Declarations, func(i, j int) bool {
			a, b := card.Declarations[i], card.Declarations[j]
			if a.Path != b.Path {
				return a.Path < b.Path
			}
			return a.Name < b.Name
		})
		result.Cards = append(result.Cards, card)
	}
	result.SHA256 = jsonDigest(result)
	return result, nil
}
