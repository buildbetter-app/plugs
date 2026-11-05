package driver

import (
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestSelectionCoverageExactFamilyAndInterval(t *testing.T) {
	family := SelectionProjectionFamily{Table: DerivedSortKey, Version: SelectionIndexVersion}
	coverage := SelectionCoverage{Family: family, Incarnation: "inc", Branch: model.Natural64(0), First: model.Natural64(7), Through: model.Natural64(9)}
	for _, at := range []uint64{7, 8, 9} {
		if err := coverage.Require(family, "inc", model.Natural64(0), model.Natural64(at)); err != nil {
			t.Fatal(err)
		}
	}
	for name, change := range map[string]func(*SelectionCoverage){
		"incarnation": func(c *SelectionCoverage) { c.Incarnation = "other" },
		"branch":      func(c *SelectionCoverage) { c.Branch = model.Natural64(1) },
		"version":     func(c *SelectionCoverage) { c.Family.Version++ },
		"family":      func(c *SelectionCoverage) { c.Family.Table = DerivedAttribute; c.Family.Kind = SelectionLabelFact },
		"inverted":    func(c *SelectionCoverage) { c.First = model.Natural64(10) },
		"malformed":   func(c *SelectionCoverage) { c.Through = model.Natural("invalid") },
	} {
		t.Run(name, func(t *testing.T) {
			bad := coverage
			change(&bad)
			if !errors.Is(bad.Require(family, "inc", model.Natural64(0), model.Natural64(8)), ErrSelectionUnavailable) {
				t.Fatal("incompatible coverage admitted")
			}
		})
	}
	for _, at := range []uint64{0, 6, 10} {
		if !errors.Is(coverage.Require(family, "inc", model.Natural64(0), model.Natural64(at)), ErrSelectionUnavailable) {
			t.Fatal("range extended from a final image")
		}
	}
}

func sourceContractRow(ref model.Ref, parent *model.Ref, target *model.Ref, depth uint64) DerivedSelectionRow {
	table, key := DerivedSortKey, "bead/"+ref.ID
	if target != nil {
		table, key = DerivedAncestorClosure, "chain/"+ref.ID+"/"+target.ID
	}
	row := DerivedRow{Key: key, Value: []byte(key)}
	row.Index = &SelectionIndex{Version: SelectionIndexVersion, Row: DerivedRowKey{Table: table, Key: key}, Incarnation: "inc", Branch: model.Natural64(0), ValueDigest: SelectionDigest(row.Value), Source: SelectionSource{Key: LockKey{Kind: LockRecord, Ref: &ref}, Revision: model.Natural64(1), Present: true, Digest: SelectionDigest([]byte("source"))}}
	if target == nil {
		row.Index.Bead = &SelectionBead{Ref: ref, Parent: parent, Type: "issue"}
	} else {
		row.Index.Facts = &SelectionFacts{Kind: SelectionAncestorFact, Rows: []SelectionFact{{Kind: SelectionAncestorFact, Ref: &ref, Target: target, Depth: depth}}}
	}
	return DerivedSelectionRow{Table: table, Row: row}
}

func TestSelectionProjectionBoundsPrecedeCoreDecode(t *testing.T) {
	ref := model.RefByID("a", model.Natural64(0))
	record := Record{Ref: ref, Kind: "bead", Revision: model.Natural64(1), Body: []byte("source")}
	metadata := []GraphSourceMetadata{{Selection: []DerivedSelectionRow{sourceContractRow(ref, nil, nil, 0)}}}
	calls := 0
	projection := GraphSourceProjection{Families: []SelectionProjectionFamily{{Table: DerivedSortKey, Version: SelectionIndexVersion}}, MaxPageRows: 2,
		Inputs: func(Record) ([]DerivedRowRange, error) {
			return []DerivedRowRange{{Table: DerivedAncestorClosure, Prefix: []byte("opaque-\xff/"), Max: 2}}, nil
		},
		Decode: func(Record, []DerivedRow) ([]DerivedSelectionRow, error) { calls++; return nil, nil },
	}
	page, err := NewSelectionProjectionPage([]Record{record}, metadata, projection)
	if err != nil {
		t.Fatal(err)
	}
	record.Body[0] = '!'
	metadata[0].Selection[0].Row.Index.Bead.Type = "mutated"
	rows, err := page.Decode([][]DerivedRow{{{Key: "opaque-\xff/a"}}}, "inc")
	if err != nil || calls != 1 || len(rows) != 1 || rows[0][0].Row.Index.Bead.Type != "issue" {
		t.Fatalf("owned positive page=%v calls=%d error=%v", rows, calls, err)
	}
	for name, input := range map[string][][]DerivedRow{
		"range overflow": {{{Key: "opaque-\xff/a"}, {Key: "opaque-\xff/b"}, {Key: "opaque-\xff/c"}}},
		"prefix":         {{{Key: "foreign/a"}}},
		"duplicate":      {{{Key: "opaque-\xff/a"}, {Key: "opaque-\xff/a"}}},
		"missing slot":   nil,
	} {
		t.Run(name, func(t *testing.T) {
			calls = 0
			if _, err := page.Decode(input, "inc"); !errors.Is(err, ErrSelectionUnavailable) || calls != 0 {
				t.Fatalf("invalid inputs reached core: calls=%d error=%v", calls, err)
			}
		})
	}
	projection.Inputs = func(Record) ([]DerivedRowRange, error) {
		return []DerivedRowRange{{Table: DerivedAncestorClosure, Prefix: []byte("a/"), Max: 2}, {Table: DerivedAncestorClosure, Prefix: []byte("b/"), Max: 2}}, nil
	}
	page, err = NewSelectionProjectionPage([]Record{record}, metadata, projection)
	if err != nil {
		t.Fatal(err)
	}
	calls = 0
	if _, err := page.Decode([][]DerivedRow{{{Key: "a/1"}, {Key: "a/2"}}, {{Key: "b/1"}}}, "inc"); !errors.Is(err, ErrSelectionUnavailable) || calls != 0 {
		t.Fatalf("page overflow reached core: %d %v", calls, err)
	}
}

func TestSelectionParentProofRequiresTheWholeCurrentChain(t *testing.T) {
	a, b, c := model.RefByID("a", model.Natural64(0)), model.RefByID("b", model.Natural64(0)), model.RefByID("c", model.Natural64(0))
	rows := []DerivedSelectionRow{sourceContractRow(a, nil, nil, 0), sourceContractRow(b, &a, nil, 0), sourceContractRow(c, &b, nil, 0), sourceContractRow(a, nil, &a, 0), sourceContractRow(b, nil, &b, 0), sourceContractRow(b, nil, &a, 1), sourceContractRow(c, nil, &c, 0), sourceContractRow(c, nil, &b, 1), sourceContractRow(c, nil, &a, 2)}
	if err := ValidateSelectionParentChains(rows); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func([]DerivedSelectionRow) []DerivedSelectionRow{
		"ancestor moved but descendant source unchanged": func(r []DerivedSelectionRow) []DerivedSelectionRow { r[1].Row.Index.Bead.Parent = nil; return r },
		"wrong intermediate": func(r []DerivedSelectionRow) []DerivedSelectionRow {
			r[7].Row.Index.Facts.Rows[0].Target = &a
			return r
		},
		"dead target": func(r []DerivedSelectionRow) []DerivedSelectionRow {
			dead := model.RefByID("dead", model.Natural64(0))
			r[8].Row.Index.Facts.Rows[0].Target = &dead
			return r
		},
		"missing self":    func(r []DerivedSelectionRow) []DerivedSelectionRow { return append(r[:6], r[7:]...) },
		"missing depth":   func(r []DerivedSelectionRow) []DerivedSelectionRow { r[7].Row.Index.Facts.Rows[0].Depth = 3; return r },
		"duplicate depth": func(r []DerivedSelectionRow) []DerivedSelectionRow { return append(r, r[7].Clone()) },
		"cycle":           func(r []DerivedSelectionRow) []DerivedSelectionRow { r[0].Row.Index.Bead.Parent = &c; return r },
	} {
		t.Run(name, func(t *testing.T) {
			copy := make([]DerivedSelectionRow, len(rows))
			for i := range rows {
				copy[i] = rows[i].Clone()
			}
			if err := ValidateSelectionParentChains(change(copy)); !errors.Is(err, ErrSelectionUnavailable) {
				t.Fatalf("invalid current chain certified: %v", err)
			}
		})
	}
}
