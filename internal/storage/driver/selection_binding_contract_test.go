package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestSelectionBoundaryOwnsCompleteDescription(t *testing.T) {
	p := selectionTestPlan()
	spec, _ := p.Boundary.Description()
	ref := model.RefByID("a", model.Natural64(0))
	value := SelectionRef(ref)
	spec.Authorization = SelectionExpr{Op: SelectionAll, Args: []SelectionExpr{{Op: SelectionExistsOp, Exists: &SelectionExists{Row: DerivedRowKey{Table: DerivedAttribute, Key: "labels"}, Kind: SelectionLabelFact, Tests: []SelectionFactTest{{Column: SelectionFactRef, Value: &value}}}}}}
	host := SelectionExpr{Op: SelectionChildrenOp, Sources: &SelectionSources{Refs: []model.Ref{ref}}}
	spec.Host, spec.HostExpression = SelectionHostCompiled, &host
	view := LockKey{Kind: LockRecord, Ref: &ref}
	spec.ViewRevision, spec.ViewSource = model.Natural64(1), &view
	spec.Sources = append(spec.Sources, SelectionSource{Key: view, Revision: model.Natural64(1), Present: true, Digest: SelectionDigest([]byte("view bytes"))})
	b, err := PrepareSelectionBoundary(spec)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := b.GobEncode()
	spec.Authorization.Args[0].Exists.Tests[0].Value.Ref.ID = "mutated"
	spec.HostExpression.Sources.Refs[0].ID = "mutated"
	spec.Sources[1].Key.Ref.ID = "mutated"
	desc, err := b.Description()
	if err != nil {
		t.Fatal(err)
	}
	if desc.Authorization.Args[0].Exists.Tests[0].Value.Ref.ID != "a" || desc.HostExpression.Sources.Refs[0].ID != "a" || desc.ViewSource.Ref.ID != "a" {
		t.Fatal("builder aliases sealed state")
	}
	desc.Authorization.Args[0].Exists.Tests[0].Value.Ref.ID = "mutated output"
	desc.Sources[0].Key.Ref.Key = "mutated output"
	desc.ViewSource.Ref.ID = "mutated output"
	before[0] ^= 0xff
	after, _ := b.GobEncode()
	if bytes.Equal(before, after) {
		t.Fatal("encoded bytes alias seal")
	}
	check, _ := b.Description()
	if check.ViewSource.Ref.ID != "a" || check.Authorization.Args[0].Exists.Tests[0].Value.Ref.ID != "a" {
		t.Fatal("Description aliases seal")
	}
}

func TestSelectionBoundaryPreservesZeroAndOpaqueOperands(t *testing.T) {
	for _, test := range []struct {
		column SelectionColumn
		value  SelectionValue
	}{{SelectionTemplate, SelectionBool(false)}, {SelectionPriority, SelectionInt(0)}, {SelectionTitle, SelectionString("")}, {SelectionTitle, SelectionString("opaque-\xff")}} {
		p := selectionTestPlan()
		spec, _ := p.Boundary.Description()
		spec.Authorization = SelectionExpr{Op: SelectionCompareOp, Compare: &SelectionCompare{Column: test.column, Op: "eq", Value: &test.value}}
		b, err := PrepareSelectionBoundary(spec)
		if err != nil {
			t.Fatal(err)
		}
		copy, err := b.Description()
		if err != nil {
			t.Fatal(err)
		}
		before, _ := selectionExprBytes(spec.Authorization)
		after, _ := selectionExprBytes(copy.Authorization)
		if !bytes.Equal(before, after) {
			t.Fatalf("operand identity lost: %+v", test)
		}
		p.Authorization = copy.Authorization
		p.Boundary = b
		if err := p.Validate(); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Binding(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSelectionBoundaryModeAndErrorClasses(t *testing.T) {
	for _, mode := range []SelectionMode{SelectionPageMode, SelectionCountMode, SelectionTypeSummary} {
		p := selectionTestPlan()
		p.Mode = mode
		if mode != SelectionPageMode {
			p.Page = nil
		}
		if mode == SelectionTypeSummary {
			p.GroupBy = SelectionGroupType
		}
		s, _ := p.Boundary.Description()
		s.Host = SelectionHostCheckOnly
		p.Boundary, _ = PrepareSelectionBoundary(s)
		err := p.Validate()
		var bounded SelectionCheckOnlyError
		var exhausted ErrSelectionCap
		if !errors.As(err, &bounded) || bounded.CandidateCap != 20000 || !errors.Is(err, ErrSelectionCheckOnly) || errors.As(err, &exhausted) || errors.Is(err, ErrSelectionInvalid) || errors.Is(err, ErrSelectionUnavailable) {
			t.Fatalf("mode=%s err=%v", mode, err)
		}
		p.Membership = SelectionExpr{Op: SelectionAll, Args: []SelectionExpr{{Op: SelectionFalse}, {Op: "unknown"}}}
		if err := p.Validate(); !errors.Is(err, ErrSelectionInvalid) || errors.Is(err, ErrSelectionCheckOnly) {
			t.Fatalf("unknown policy took fallback: %v", err)
		}
	}
	cases := map[string]func(*SelectionBoundarySpec){
		"host_missing_arm": func(s *SelectionBoundarySpec) { s.Host = SelectionHostCompiled },
		"host_extra_arm":   func(s *SelectionBoundarySpec) { s.HostExpression = &SelectionExpr{Op: SelectionTrue} },
		"unknown_in_false_host": func(s *SelectionBoundarySpec) {
			s.Host = SelectionHostCompiled
			s.HostExpression = &SelectionExpr{Op: SelectionAll, Args: []SelectionExpr{{Op: SelectionFalse}, {Op: "unknown"}}}
		},
		"unknown_chain": func(s *SelectionBoundarySpec) { s.Chain = "custom" },
		"none_nontrivial": func(s *SelectionBoundarySpec) {
			s.Chain = SelectionChainNone
			s.Authorization = SelectionExpr{Op: SelectionFalse}
		},
		"duplicate_control":      func(s *SelectionBoundarySpec) { s.Sources = append(s.Sources, s.Sources[0]) },
		"no_controls":            func(s *SelectionBoundarySpec) { s.Sources = nil },
		"present_without_digest": func(s *SelectionBoundarySpec) { s.Sources[0].Present = true },
		"miss_with_digest":       func(s *SelectionBoundarySpec) { s.Sources[0].Digest = SelectionDigest(nil) },
		"unobserved_view":        func(s *SelectionBoundarySpec) { s.ViewRevision = model.Natural64(1); s.ViewSource = &s.Sources[0].Key },
		"future_control":         func(s *SelectionBoundarySpec) { s.Sources[0].Revision = model.Natural64(2) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			p := selectionTestPlan()
			s, _ := p.Boundary.Description()
			change(&s)
			if _, err := PrepareSelectionBoundary(s); !errors.Is(err, ErrSelectionInvalid) {
				t.Fatalf("invalid boundary accepted: %v", err)
			}
		})
	}
}

func TestSelectionBindingIncludesEveryAuthorityAndExclusionPlane(t *testing.T) {
	p := selectionTestPlan()
	want, err := p.Binding()
	if err != nil {
		t.Fatal(err)
	}
	changes := map[string]func(*SelectionPlan, *SelectionBoundarySpec){
		"prepared": func(_ *SelectionPlan, s *SelectionBoundarySpec) {
			s.PreparedBinding = SelectionDigest([]byte("different caller"))
		},
		"view": func(_ *SelectionPlan, s *SelectionBoundarySpec) {
			s.InlineViewBinding = SelectionDigest([]byte("fields=[]"))
		},
		"tombstone": func(_ *SelectionPlan, s *SelectionBoundarySpec) { s.Sources[0].Revision = model.Natural64(1) },
		"host": func(_ *SelectionPlan, s *SelectionBoundarySpec) {
			s.Host = SelectionHostCompiled
			s.HostExpression = &SelectionExpr{Op: SelectionFalse}
		},
		"excluded": func(p *SelectionPlan, _ *SelectionBoundarySpec) {
			p.Excluded = []model.Ref{model.RefByID("a", model.Natural64(0))}
		},
		"committed_same_token": func(p *SelectionPlan, _ *SelectionBoundarySpec) { p.ContentCommitted = true },
		"membership":           func(p *SelectionPlan, _ *SelectionBoundarySpec) { p.Membership = SelectionExpr{Op: SelectionFalse} },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			p := selectionTestPlan()
			s, _ := p.Boundary.Description()
			change(&p, &s)
			p.Boundary, err = PrepareSelectionBoundary(s)
			if err != nil {
				t.Fatal(err)
			}
			got, err := p.Binding()
			if err != nil || got == want {
				t.Fatalf("unbound plane: %s %v", got, err)
			}
		})
	}
	p.Authorization = SelectionExpr{Op: SelectionFalse}
	if err := p.Validate(); !errors.Is(err, ErrSelectionInvalid) {
		t.Fatalf("AST substitution accepted: %v", err)
	}
}

func TestSelectionPresenceUsesBooleanTemplateAndNumericPriority(t *testing.T) {
	r := model.RefByID("a", model.Natural64(0))
	k := DerivedRowKey{Table: DerivedSortKey, Key: "a"}
	row := DerivedRow{Key: k.Key, Value: []byte("logical")}
	row.Index = &SelectionIndex{Version: SelectionIndexVersion, Row: k, Incarnation: "inc", Branch: model.Natural64(0), ValueDigest: SelectionDigest(row.Value), Source: SelectionSource{Key: LockKey{Kind: LockRecord, Ref: &r}, Revision: model.Natural64(0), Present: true, Digest: SelectionDigest([]byte("source"))}, Bead: &SelectionBead{Ref: r, Absent: []SelectionColumn{SelectionTemplate, SelectionPriority}}}
	if err := row.Index.Validate(k.Table, row); err != nil {
		t.Fatal(err)
	}
	copy := row.Clone()
	copy.Index.Bead.Absent[0] = SelectionTitle
	if row.Index.Bead.Absent[0] != SelectionTemplate {
		t.Fatal("presence aliases clone")
	}
	row.Index.Bead.Template = true
	if err := row.Index.Validate(k.Table, row); !errors.Is(err, ErrSelectionInvalid) {
		t.Fatalf("contradictory presence accepted: %v", err)
	}
	for _, value := range []SelectionValue{SelectionBool(false), SelectionString("")} {
		p := selectionTestPlan()
		p.Membership = SelectionExpr{Op: SelectionCompareOp, Compare: &SelectionCompare{Column: SelectionTemplate, Op: "eq", Value: &value}}
		err := p.Validate()
		if (value.Bool != nil) != (err == nil) {
			t.Fatalf("template operand %v: %v", value, err)
		}
	}
}

func TestSelectionScopedGrantDomainAndOwnedMetadata(t *testing.T) {
	key := DerivedRowKey{Table: DerivedGrant, Key: "scoped-grants"}
	ref := model.RefByID("registry/project", model.Natural64(0))
	name := "web/api"
	row := DerivedRow{Key: key.Key, Value: []byte("logical source-backed tuples")}
	row.Index = &SelectionIndex{Version: SelectionIndexVersion, Row: key, Incarnation: "inc", Branch: model.Natural64(0), ValueDigest: SelectionDigest(row.Value), Source: SelectionSource{Key: LockKey{Kind: LockDerived, Derived: &key}, Revision: model.Natural64(0), Present: true, Digest: SelectionDigest(row.Value)}, Facts: &SelectionFacts{Kind: SelectionScopedGrantFact, Rows: []SelectionFact{{Kind: SelectionScopedGrantFact, Subject: "opaque subject", Scope: "opaque scope", Role: "involved", ScopeKind: SelectionProjectScope, ScopeRef: &ref, ProjectName: &name}}}}
	if err := row.Index.Validate(key.Table, row); err != nil {
		t.Fatalf("valid registry Name!=Ref.ID rejected: %v", err)
	}
	copy := row.Clone()
	copy.Index.Facts.Rows[0].ScopeRef.ID = "mutated"
	*copy.Index.Facts.Rows[0].ProjectName = "mutated"
	if row.Index.Facts.Rows[0].ScopeRef.ID != "registry/project" || *row.Index.Facts.Rows[0].ProjectName != "web/api" {
		t.Fatal("scoped grant metadata aliases clone")
	}
	cases := map[string]func(*SelectionFact){
		"missing_scope_kind":          func(f *SelectionFact) { f.ScopeKind = "" },
		"unknown_bead_scope":          func(f *SelectionFact) { f.ScopeKind = "bead" },
		"empty_opaque_scope":          func(f *SelectionFact) { f.Scope = "" },
		"missing_scope_ref":           func(f *SelectionFact) { f.ScopeRef = nil },
		"key_scope_ref":               func(f *SelectionFact) { v := model.RefByKey("project"); f.ScopeRef = &v },
		"branch_scope_ref":            func(f *SelectionFact) { f.ScopeRef.Branch = model.Natural64(1) },
		"whitespace_scope_ref":        func(f *SelectionFact) { f.ScopeRef.ID = " project " },
		"nul_scope_ref":               func(f *SelectionFact) { f.ScopeRef.ID = "pro\x00ject" },
		"missing_project_name":        func(f *SelectionFact) { f.ProjectName = nil },
		"empty_project_name":          func(f *SelectionFact) { *f.ProjectName = "" },
		"whitespace_project_name":     func(f *SelectionFact) { *f.ProjectName = " web/api " },
		"authority_with_project_name": func(f *SelectionFact) { f.ScopeKind = SelectionAuthorityScope },
		"generic_with_new_metadata":   func(f *SelectionFact) { f.Kind = SelectionGrantFact },
	}
	for label, mutate := range cases {
		t.Run(label, func(t *testing.T) {
			bad := row.Clone()
			mutate(&bad.Index.Facts.Rows[0])
			if err := bad.Index.Validate(key.Table, bad); !errors.Is(err, ErrSelectionInvalid) {
				t.Fatalf("malformed scoped metadata accepted: %v", err)
			}
		})
	}
	// Authority and team are exact scopes, with no project-name/wildcard arm.
	for _, kind := range []SelectionScopeKind{SelectionAuthorityScope, SelectionTeamScope} {
		other := row.Clone()
		other.Index.Facts.Rows[0].ScopeKind = kind
		other.Index.Facts.Rows[0].ProjectName = nil
		if err := other.Index.Validate(key.Table, other); err != nil {
			t.Fatal(err)
		}
	}
	for _, column := range []SelectionFactColumn{SelectionFactScope, SelectionFactSubject, SelectionFactRole, SelectionFactScopeKind, SelectionFactProjectName} {
		p := selectionTestPlan()
		p.Membership = SelectionExpr{Op: SelectionExistsOp, Exists: &SelectionExists{Row: key, Kind: SelectionScopedGrantFact, Tests: []SelectionFactTest{{Column: column, Candidate: SelectionProject}}}}
		if err := p.Validate(); (err == nil) != (column == SelectionFactProjectName) {
			t.Fatalf("unsupported candidate mapping column=%s err=%v", column, err)
		}
	}
	p := selectionTestPlan()
	value := SelectionString("project")
	p.Membership = SelectionExpr{Op: SelectionExistsOp, Exists: &SelectionExists{Row: key, Kind: SelectionGrantFact, Tests: []SelectionFactTest{{Column: SelectionFactScopeKind, Value: &value}}}}
	if err := p.Validate(); !errors.Is(err, ErrSelectionInvalid) {
		t.Fatalf("generic layout silently acquired scoped column: %v", err)
	}
	invalidScope := SelectionString("bead")
	for _, test := range []SelectionFactTest{
		{Column: SelectionFactProjectName, Candidate: SelectionID},
		{Column: SelectionFactColumn("scope_ref"), Candidate: SelectionID},
		{Column: SelectionFactScopeKind, Value: &invalidScope},
	} {
		p.Membership.Exists.Kind = SelectionScopedGrantFact
		p.Membership.Exists.Tests = []SelectionFactTest{test}
		if err := p.Validate(); !errors.Is(err, ErrSelectionInvalid) {
			t.Fatalf("unsupported scoped operand accepted: %+v/%v", test, err)
		}
	}
}

func TestSelectionGenericFactJSONBytesRemainStable(t *testing.T) {
	// Frozen encoding/json bytes from the pre-scoped SelectionFact shape at
	// 37fa5762e330656474fc03442df884c2bf4ce3a9. W4's existing producer uses
	// these complete sets as logical row Value; do not add zero/null fields.
	for _, tc := range []struct {
		name string
		fact SelectionFact
		want string
	}{
		{"grant", SelectionFact{Kind: SelectionGrantFact, Subject: "opaque subject", Scope: "opaque scope", Role: "involved"}, `{"Kind":"grant","Rows":[{"Kind":"grant","Ref":null,"Target":null,"Link":null,"Subject":"opaque subject","Scope":"opaque scope","Role":"involved","Name":"","Value":"","Active":false,"Depth":0}]}`},
		{"project", SelectionFact{Kind: SelectionProjectFact, Name: "web/api"}, `{"Kind":"project","Rows":[{"Kind":"project","Ref":null,"Target":null,"Link":null,"Subject":"","Scope":"","Role":"","Name":"web/api","Value":"","Active":false,"Depth":0}]}`},
		{"team", SelectionFact{Kind: SelectionTeamFact, Name: "same-text", Active: true}, `{"Kind":"team","Rows":[{"Kind":"team","Ref":null,"Target":null,"Link":null,"Subject":"","Scope":"","Role":"","Name":"same-text","Value":"","Active":true,"Depth":0}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(SelectionFacts{Kind: tc.fact.Kind, Rows: []SelectionFact{tc.fact}})
			if err != nil || string(got) != tc.want {
				t.Fatalf("legacy logical bytes changed: %s/%v", got, err)
			}
		})
	}
	ref := model.RefByID("registry/project", model.Natural64(0))
	name := "web/api"
	fact := SelectionFact{Kind: SelectionScopedGrantFact, ScopeKind: SelectionProjectScope, ScopeRef: &ref, ProjectName: &name}
	encoded, err := json.Marshal(fact)
	if err != nil {
		t.Fatal(err)
	}
	var restored SelectionFact
	if err := json.Unmarshal(encoded, &restored); err != nil || restored.ScopeKind != fact.ScopeKind || restored.ScopeRef == nil || !SelectionSameRef(*restored.ScopeRef, ref) || restored.ProjectName == nil || *restored.ProjectName != name {
		t.Fatalf("scoped metadata lost in JSON: %s/%v", encoded, err)
	}
	name = ""
	encoded, err = json.Marshal(fact)
	if err != nil || !bytes.Contains(encoded, []byte(`"ProjectName":""`)) {
		t.Fatalf("explicit present-empty metadata disappeared: %s/%v", encoded, err)
	}
}
