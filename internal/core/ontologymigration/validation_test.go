package ontologymigration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

func TestUnknownConflictingUnusedAndInvalidDirectives(t *testing.T) {
	target := Target{Project: "P", Type: "issue"}
	set := Directive{ID: "set", Kind: SetField, Target: target, Field: "title", Value: "new"}
	cases := map[string][]Directive{
		"unknown kind":      {{ID: "x", Kind: "invented", Target: target}},
		"extra member":      {{ID: "x", Kind: DropField, Target: target, Field: "title", Value: "ignored"}},
		"missing value":     {{ID: "x", Kind: SetField, Target: target, Field: "title"}},
		"unknown field":     {{ID: "x", Kind: SetField, Target: target, Field: "absent", Value: "new"}},
		"priority":          {{ID: "x", Kind: SetField, Target: target, Field: "priority", Value: 0}},
		"template":          {{ID: "x", Kind: DropField, Target: target, Field: "template"}},
		"nested path":       {{ID: "x", Kind: DropField, Target: target, Field: "fields.title"}},
		"missing project":   {{ID: "x", Kind: SetField, Target: Target{Type: "issue"}, Field: "title", Value: "new"}},
		"unknown project":   {{ID: "x", Kind: SetField, Target: Target{Project: "Z", Type: "issue"}, Field: "title", Value: "new"}},
		"unknown row":       {{ID: "x", Kind: SetField, Target: Target{Project: "P", Type: "issue", ID: "absent"}, Field: "title", Value: "new"}},
		"same status":       {{ID: "x", Kind: MapStatus, Target: target, From: "todo", To: "todo"}},
		"unknown status":    {{ID: "x", Kind: MapStatus, Target: target, From: "todo", To: "absent"}},
		"unknown type":      {{ID: "x", Kind: MapType, Target: target, From: "issue", To: "absent"}},
		"wrong type source": {{ID: "x", Kind: MapType, Target: target, From: "absent", To: "issue"}},
		"duplicate ID":      {set, set},
		"overlapping set":   {set, {ID: "other", Kind: SetField, Target: Target{Project: "P", Type: "issue", ID: "P-1"}, Field: "title", Value: "same"}},
		"set and drop":      {set, {ID: "drop", Kind: DropField, Target: target, Field: "title"}},
		"same value set":    {{ID: "x", Kind: SetField, Target: target, Field: "title", Value: "old"}},
		"missing drop":      {{ID: "x", Kind: DropField, Target: target, Field: "absent"}},
		"unmatched status":  {{ID: "x", Kind: MapStatus, Target: target, From: "done", To: "backlog"}},
	}
	for name, directives := range cases {
		t.Run(name, func(t *testing.T) {
			req := request(t, ontology.DefaultDocument(), ontology.DefaultDocument())
			req.Directives = directives
			p := build(t, req, rowsFor(t, req, bead("P-1", "P", map[string]any{"title": "old"})), noPre())
			if p.Ready() || !hasRefusal(p, "invalid_params") {
				t.Fatalf("invalid directive accepted: %+v", p.Report())
			}
		})
	}
}

func TestMissingAndStaleOntologyFacts(t *testing.T) {
	for _, mode := range []string{"current revision", "candidate revision", "current compilation", "candidate compilation", "not next", "row source", "pre source"} {
		t.Run(mode, func(t *testing.T) {
			req := request(t, ontology.DefaultDocument(), ontology.DefaultDocument())
			var source RowSource = rowsFor(t, req)
			var pre ValidatingPre = noPre()
			switch mode {
			case "current revision":
				req.Current.Revision = ""
			case "candidate revision":
				req.Candidate.Revision = ""
			case "current compilation":
				req.Current.Snapshot = ontology.Snapshot{}
			case "candidate compilation":
				req.Candidate.Snapshot = ontology.Snapshot{}
			case "not next":
				req.Candidate.Revision = "10"
			case "row source":
				source = nil
			case "pre source":
				pre = nil
			}
			p := build(t, req, source, pre)
			if p.Ready() || p.Digest() == "" || !hasRefusal(p, "invalid_params") {
				t.Fatalf("missing/stale ontology facts accepted: %+v", p.Report())
			}
		})
	}
}

func TestIncompleteEnumerationNeverBecomesSuccess(t *testing.T) {
	cases := []string{"omission", "duplicate", "different image", "missing fields", "missing identity", "missing project", "missing type", "missing status", "missing count", "missing snapshot", "missing digest", "stale ontology revision", "stale ontology digest"}
	for _, mode := range cases {
		t.Run(mode, func(t *testing.T) {
			req := request(t, ontology.DefaultDocument(), ontology.DefaultDocument())
			source := rowsFor(t, req, bead("P-1", "P", map[string]any{"title": "complete"}))
			switch mode {
			case "omission":
				source.rows = nil
			case "duplicate":
				source.rows = append(source.rows, source.rows[0])
			case "different image":
				source.rows[0].State.Fields["title"] = "altered"
			case "missing fields":
				source.rows[0].State.Fields = nil
			case "missing identity":
				source.rows[0].State.ID = ""
			case "missing project":
				source.rows[0].State.Project = ""
			case "missing type":
				source.rows[0].State.Type = ""
			case "missing status":
				source.rows[0].State.Status = ""
			case "missing count":
				source.inventory.Count = ""
			case "missing snapshot":
				source.inventory.DataRevision = ""
			case "missing digest":
				source.inventory.RowsDigest = ""
			case "stale ontology revision":
				source.inventory.Ontology.Revision = "6"
			case "stale ontology digest":
				source.inventory.Ontology.Digest = strings.Repeat("0", 64)
			}
			p := build(t, req, source, noPre())
			if p.Ready() || len(p.Report().Refusals) == 0 {
				t.Fatalf("incomplete input accepted: %+v", p.Report())
			}
			if mode == "omission" || mode == "different image" || mode == "duplicate" {
				if !hasRefusal(p, "incomplete_enumeration") || p.Report().ObservedRowsDigest == "" {
					t.Fatalf("missing deterministic refused-enumeration evidence: %+v", p.Report())
				}
			}
		})
	}
}

type unobservedRows struct{ rows *fixtureRows }

func (s unobservedRows) Inventory(ctx context.Context) (Inventory, error) {
	return s.rows.Inventory(ctx)
}
func (s unobservedRows) Scan(ctx context.Context, i Inventory, visit func(Row) error) error {
	return s.rows.Scan(ctx, i, visit)
}

func TestMissingGuardFactsRefuseAndObservedFailureRequiresDisposition(t *testing.T) {
	current, candidate := ontology.DefaultDocument(), ontology.DefaultDocument()
	w := candidate.Workflows["issue"]
	w.Transitions = append(w.Transitions, ontology.Rule{To: "todo", Guards: []ontology.Guard{{Kind: ontology.NoOpenChildren}, {Kind: ontology.EvidenceRequired, Kinds: []string{"link"}}, {Kind: ontology.GatesResolved, Kinds: []string{"human"}}}})
	candidate.Workflows["issue"] = w
	req := request(t, current, candidate)
	source := rowsFor(t, req, bead("P-1", "P", nil))
	if p := build(t, req, unobservedRows{source}, noPre()); p.Ready() || !hasRefusal(p, "missing_facts") {
		t.Fatal("missing guard source was optimistic")
	}
	for _, mode := range []string{"missing observation", "wrong row", "wrong candidate", "missing permission", "missing authz posture", "contradictory authz posture", "missing evidence", "missing gate"} {
		t.Run(mode, func(t *testing.T) {
			source.observe = func(_ context.Context, _ Inventory, c Change) (Observation, error) {
				d, _ := RowDigest(c.After)
				observedTrue := true
				observedFalse := false
				o := Observation{Candidate: c.Candidate, RowDigest: d, AuthzInstalled: &observedFalse, Facts: ontology.GuardFacts{NoOpenChildren: &observedTrue, Evidence: map[string]bool{"link": true}, Gates: map[string]bool{"human": true}}}
				switch mode {
				case "missing observation":
					o.Facts.NoOpenChildren = nil
				case "wrong row":
					o.RowDigest = strings.Repeat("0", 64)
				case "wrong candidate":
					o.Candidate.Revision = "100"
				case "missing permission":
					o.Facts.AuthzInstalled = true
					o.AuthzInstalled = &observedTrue
				case "missing authz posture":
					o.AuthzInstalled = nil
				case "contradictory authz posture":
					o.AuthzInstalled = &observedTrue
				case "missing evidence":
					o.Facts.Evidence = nil
				case "missing gate":
					o.Facts.Gates = nil
				}
				return o, nil
			}
			p := build(t, req, source, noPre())
			if p.Ready() || !hasRefusal(p, "missing_facts") {
				t.Fatalf("missing guard facts accepted: %+v", p.Report())
			}
		})
	}
	source.observe = func(_ context.Context, _ Inventory, c Change) (Observation, error) {
		d, _ := RowDigest(c.After)
		observedFalse := false
		return Observation{Candidate: c.Candidate, RowDigest: d, AuthzInstalled: &observedFalse, Facts: ontology.GuardFacts{NoOpenChildren: &observedFalse, Evidence: map[string]bool{"link": false}, Gates: map[string]bool{"human": false}}}, nil
	}
	p := build(t, req, source, noPre())
	if p.Ready() || len(p.Report().Refusals) != 0 || len(p.Report().Rows[0].Findings) != 3 {
		t.Fatalf("observed guard failures did not remain violations: %+v", p.Report())
	}
	for i, f := range p.Report().Rows[0].Findings {
		req.Directives = append(req.Directives, Directive{ID: fmt.Sprint(i), Kind: Grandfather, Target: Target{Project: "P", Type: "issue", ID: "P-1"}, Violation: f.ID, Reason: "explicit exception"})
	}
	if p := build(t, req, source, noPre()); !p.Ready() || p.Report().Grandfathered != 3 {
		t.Fatalf("observed guard failure dispositions were lost: %+v", p.Report())
	}
}

type manifestPre struct {
	fixturePre
	manifest func(Reference) PreInventory
}

func (p *manifestPre) Inventory(_ context.Context, c Reference) (PreInventory, error) {
	return p.manifest(c), nil
}

func TestIncompletePreManifestAndMalformedCallbackRefuse(t *testing.T) {
	req := request(t, ontology.DefaultDocument(), ontology.DefaultDocument())
	source := rowsFor(t, req, bead("P-1", "P", nil))
	for _, mode := range []string{"nil bindings", "wrong digest", "wrong candidate", "unknown type", "duplicate binding"} {
		t.Run(mode, func(t *testing.T) {
			pre := &manifestPre{manifest: func(c Reference) PreInventory {
				bindings := []Binding{}
				switch mode {
				case "nil bindings":
					bindings = nil
				case "wrong candidate":
					c.Revision = "9"
				case "unknown type":
					bindings = []Binding{{ID: "b", Type: "absent", Digest: strings.Repeat("a", 64)}}
				case "duplicate binding":
					bindings = []Binding{{ID: "b", Type: "issue", Digest: strings.Repeat("a", 64)}, {ID: "b", Type: "issue", Digest: strings.Repeat("a", 64)}}
				}
				d, _ := BindingsDigest(c, bindings)
				if mode == "wrong digest" {
					d = strings.Repeat("0", 64)
				}
				return PreInventory{Candidate: c, Bindings: bindings, Digest: d}
			}}
			p := build(t, req, source, pre)
			if p.Ready() || len(p.Report().Refusals) == 0 {
				t.Fatalf("incomplete host manifest accepted: %+v", p.Report())
			}
		})
	}
	req.Directives = []Directive{{ID: "set", Kind: SetField, Target: Target{Project: "P", Type: "issue"}, Field: "title", Value: "new"}}
	pre := &fixturePre{bindings: []Binding{{ID: "validator", Type: "issue", Digest: strings.Repeat("a", 64)}}, run: func(context.Context, Binding, Change) ([]ontology.Violation, error) {
		return []ontology.Violation{{Code: "unknown_callback_code", Message: "malformed result"}}, nil
	}}
	if p := build(t, req, source, pre); p.Ready() || !hasRefusal(p, "missing_facts") || pre.calls != 1 {
		t.Fatal("malformed actual callback result was accepted")
	}
}

func TestPortErrorsAndMissingRevisionReturnNoPlan(t *testing.T) {
	req := request(t, ontology.DefaultDocument(), ontology.DefaultDocument())
	source := rowsFor(t, req, bead("P-1", "P", nil))
	source.err = errors.New("snapshot unavailable")
	if p, err := Build(context.Background(), req, source, noPre()); err == nil || p.Ready() || p.Digest() != "" {
		t.Fatal("snapshot failure yielded a plan")
	}
	source.err = nil
	source.rows[0].Revision = ""
	if p, err := Build(context.Background(), req, source, noPre()); err == nil || p.Ready() || p.Digest() != "" {
		t.Fatal("unobserved row revision yielded a plan")
	}
	req.Directives = []Directive{{ID: "not-json", Kind: SetField, Target: Target{Project: "P", Type: "issue"}, Field: "title", Value: func() {}}}
	if p, err := Build(context.Background(), req, source, noPre()); err == nil || p.Digest() != "" {
		t.Fatal("non-JSON directive crossed the copy boundary")
	}
}

func TestSuccessorDoesNotNarrowOrExpandLargeRevisions(t *testing.T) {
	for _, pair := range [][2]string{{"7", "8"}, {"99999999999999999999999", "1e23"}, {"18446744073709551616", "18446744073709551617"}, {"1e25", "10000000000000000000000001"}} {
		if !successor(model.Revision(pair[0]), model.Revision(pair[1])) {
			t.Fatalf("valid successor refused: %v", pair)
		}
	}
	if successor("1e1000000000", "2e1000000000") || successor("7", "7") || successor("8", "7") {
		t.Fatal("invalid successor accepted")
	}
}
