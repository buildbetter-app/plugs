package driver

import (
	"errors"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func selectionTestPlan() SelectionPlan {
	p := SelectionPlan{Incarnation: "inc", Branch: model.Natural64(0), ContentRevision: model.Natural64(1), AuthorityRevision: model.Natural64(1), Membership: SelectionExpr{Op: SelectionTrue}, Authorization: SelectionExpr{Op: SelectionTrue}, Mode: SelectionPageMode, Page: &SelectionPage{Limit: 2}}
	control := model.RefByKey("authority")
	p.Boundary, _ = PrepareSelectionBoundary(SelectionBoundarySpec{Version: 1, PreparedBinding: SelectionDigest([]byte("driver-contract-v1")), Incarnation: p.Incarnation, Branch: p.Branch, AuthorityRevision: p.AuthorityRevision, Sources: []SelectionSource{{Key: LockKey{Kind: LockRecord, Ref: &control}, Revision: model.Natural64(0)}}, Chain: SelectionChainCompiled, Authorization: p.Authorization, Host: SelectionHostAbsent})
	return p
}
func TestSelectionContractRejectsUnknownAndMalformed(t *testing.T) {
	cases := map[string]func(*SelectionPlan){
		"unknown_below_false": func(p *SelectionPlan) {
			p.Authorization = SelectionExpr{Op: SelectionAll, Args: []SelectionExpr{{Op: SelectionFalse}, {Op: "future"}}}
		},
		"unknown_below_true": func(p *SelectionPlan) {
			p.Authorization = SelectionExpr{Op: SelectionAny, Args: []SelectionExpr{{Op: SelectionTrue}, {Op: "future"}}}
		},
		"unknown_below_not": func(p *SelectionPlan) {
			p.Authorization = SelectionExpr{Op: SelectionNot, Args: []SelectionExpr{{Op: "future"}}}
		},
		"unused_arm": func(p *SelectionPlan) { p.Membership.Refs = []model.Ref{model.RefByID("a", model.Natural64(0))} },
		"not_arity":  func(p *SelectionPlan) { p.Membership = SelectionExpr{Op: SelectionNot} },
		"bad_column": func(p *SelectionPlan) { p.Order = []SelectionOrder{{Column: "future"}} },
		"wrong_operand": func(p *SelectionPlan) {
			v := SelectionInt(1)
			p.Membership = SelectionExpr{Op: SelectionCompareOp, Compare: &SelectionCompare{Column: SelectionTitle, Op: "eq", Value: &v}}
		},
		"union_operand": func(p *SelectionPlan) {
			v := SelectionString("x")
			b := true
			v.Bool = &b
			p.Membership = SelectionExpr{Op: SelectionCompareOp, Compare: &SelectionCompare{Column: SelectionTitle, Op: "eq", Value: &v}}
		},
		"wildcard_operator": func(p *SelectionPlan) {
			v := SelectionString("%")
			p.Membership = SelectionExpr{Op: SelectionCompareOp, Compare: &SelectionCompare{Column: SelectionTitle, Op: "like", Value: &v}}
		},
		"incompatible_fact": func(p *SelectionPlan) {
			p.Membership = SelectionExpr{Op: SelectionExistsOp, Exists: &SelectionExists{Row: DerivedRowKey{Table: DerivedPlugins, Key: "facts"}, Kind: SelectionGrantFact}}
		},
		"future_content":       func(p *SelectionPlan) { p.ContentRevision = model.Natural64(2) },
		"malformed_revision":   func(p *SelectionPlan) { p.AuthorityRevision = model.Revision("01") },
		"ordinary_100k":        func(p *SelectionPlan) { p.Cap = SelectionSummaryCap },
		"count_100k":           func(p *SelectionPlan) { p.Mode = SelectionCountMode; p.Page = nil; p.Cap = SelectionSummaryCap },
		"summary_without_type": func(p *SelectionPlan) { p.Mode = SelectionTypeSummary; p.Page = nil },
		"summary_with_page":    func(p *SelectionPlan) { p.Mode = SelectionTypeSummary; p.GroupBy = SelectionGroupType },
		"count_group":          func(p *SelectionPlan) { p.Mode = SelectionCountMode; p.Page = nil; p.GroupBy = SelectionGroupType },
		"offset_keyset":        func(p *SelectionPlan) { p.Page.Offset = 1; p.Page.After = &SelectionKeyset{} },
		"negative_page":        func(p *SelectionPlan) { p.Page.Limit = -1 },
		"cyclic_tree": func(p *SelectionPlan) {
			p.Membership = SelectionExpr{Op: SelectionAll, Args: make([]SelectionExpr, 1)}
			p.Membership.Args[0] = p.Membership
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := selectionTestPlan()
			mutate(&p)
			if err := p.Validate(); err == nil {
				t.Fatal("malformed plan accepted")
			}
		})
	}
	p := selectionTestPlan()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if p.EffectiveCap() != 20000 {
		t.Fatal("ordinary default cap changed")
	}
	p.Mode = SelectionTypeSummary
	p.Page = nil
	p.GroupBy = SelectionGroupType
	if err := p.Validate(); err != nil || p.EffectiveCap() != 100000 {
		t.Fatalf("summary: %v", err)
	}
}

func TestSelectionAnnotationClosedDomainAndOwnedOpaqueBytes(t *testing.T) {
	k := DerivedRowKey{Table: DerivedAttribute, Key: "opaque-\xff"}
	row := DerivedRow{Key: k.Key, Value: []byte("value")}
	row.Index = &SelectionIndex{Version: SelectionIndexVersion, Row: k, Incarnation: "inc", Branch: model.Natural64(0), ValueDigest: SelectionDigest(row.Value), Source: SelectionSource{Key: LockKey{Kind: LockDerived, Derived: &k}, Revision: model.Natural64(0), Present: true, Digest: SelectionDigest(row.Value)}, Facts: &SelectionFacts{Kind: SelectionLabelFact}}
	if err := row.Index.Validate(k.Table, row); err != nil {
		t.Fatal(err)
	}
	copy := row.Clone()
	copy.Index.Facts.Kind = SelectionTeamFact
	copy.Value[0] = '!'
	copy.Index.Source.Key.Derived.Key = "changed"
	if row.Index.Facts.Kind != SelectionLabelFact || string(row.Value) != "value" || row.Index.Source.Key.Derived.Key != k.Key {
		t.Fatal("clone aliases input")
	}
	if row.Clone().Key != k.Key || row.Clone().Index.Row.Key != k.Key {
		t.Fatal("opaque key bytes lost")
	}
	foreign := row.Clone()
	r := model.RefByID("authority", model.Natural64(1))
	foreign.Index.Source.Key = LockKey{Kind: LockRecord, Ref: &r}
	foreign.Index.Source.Present = false
	foreign.Index.Source.Digest = ""
	if err := foreign.Index.Validate(k.Table, foreign); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("foreign branch source accepted: %v", err)
	}
	row.Index.Facts.Rows = []SelectionFact{{Kind: SelectionTeamFact, Name: "team", Active: true}}
	if err := row.Index.Validate(k.Table, row); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("mixed uncovered fact accepted: %v", err)
	}
	row.Index.Facts.Rows = nil
	row.Index.ValueDigest = strings.Repeat("0", 64)
	if err := row.Index.Validate(k.Table, row); err == nil {
		t.Fatal("wrong logical value digest accepted")
	}
}
