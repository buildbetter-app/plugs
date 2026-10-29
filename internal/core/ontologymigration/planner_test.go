package ontologymigration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

type fixtureRows struct {
	inventory Inventory
	rows      []Row
	err       error
	observe   func(context.Context, Inventory, Change) (Observation, error)
}

func (s *fixtureRows) Inventory(context.Context) (Inventory, error) { return s.inventory, s.err }
func (s *fixtureRows) Scan(ctx context.Context, _ Inventory, visit func(Row) error) error {
	for _, row := range s.rows {
		if err := visit(row); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s *fixtureRows) Observe(ctx context.Context, inventory Inventory, change Change) (Observation, error) {
	if s.observe != nil {
		return s.observe(ctx, inventory, change)
	}
	d, err := RowDigest(change.After)
	observedTrue := true
	return Observation{Candidate: change.Candidate, RowDigest: d, Facts: ontology.GuardFacts{NoOpenChildren: &observedTrue, NoOpenBlockers: &observedTrue, Evidence: map[string]bool{"link": true}, Gates: map[string]bool{"human": true}}}, err
}

type fixturePre struct {
	bindings []Binding
	run      func(context.Context, Binding, Change) ([]ontology.Violation, error)
	calls    int
}

func (p *fixturePre) Inventory(_ context.Context, candidate Reference) (PreInventory, error) {
	d, err := BindingsDigest(candidate, p.bindings)
	return PreInventory{Candidate: candidate, Bindings: p.bindings, Digest: d}, err
}
func (p *fixturePre) Validate(ctx context.Context, b Binding, c Change) ([]ontology.Violation, error) {
	p.calls++
	if p.run != nil {
		return p.run(ctx, b, c)
	}
	return nil, nil
}

func compiled(t *testing.T, doc ontology.Document, overlays map[string]ontology.Document, plugins ontology.PluginCatalog) ontology.Snapshot {
	t.Helper()
	snapshot, problems := ontology.Compile(doc, overlays, plugins)
	if len(problems) != 0 {
		t.Fatalf("actual compiler refused fixture: %+v", problems)
	}
	return snapshot
}

func request(t *testing.T, current, candidate ontology.Document) Request {
	t.Helper()
	return Request{Current: Version{Revision: "7", Snapshot: compiled(t, current, nil, nil)}, Candidate: Version{Revision: "8", Snapshot: compiled(t, candidate, nil, nil)}}
}

func bead(id, project string, fields map[string]any) Row {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["priority"] = 2
	fields["template"] = false
	return Row{Revision: "12", State: ontology.BeadState{ID: id, Project: project, Type: "issue", Status: "todo", Fields: fields}}
}

func rowsFor(t *testing.T, req Request, rows ...Row) *fixtureRows {
	t.Helper()
	d, err := RowsDigest(rows)
	if err != nil {
		t.Fatal(err)
	}
	return &fixtureRows{inventory: Inventory{DataRevision: "50", Ontology: Reference{Revision: req.Current.Revision, Digest: req.Current.Snapshot.Digest()}, Count: model.Natural64(uint64(len(rows))), RowsDigest: d}, rows: rows}
}

func build(t *testing.T, req Request, source RowSource, pre ValidatingPre) Plan {
	t.Helper()
	p, err := Build(context.Background(), req, source, pre)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func noPre() *fixturePre { return &fixturePre{bindings: []Binding{}} }

func hasRefusal(p Plan, code string) bool {
	for _, r := range p.Report().Refusals {
		if string(r.Code) == code || r.Reason == code {
			return true
		}
	}
	return false
}

func TestRealCompilerDefaultsAndRequiredRefusal(t *testing.T) {
	current := ontology.DefaultDocument()
	candidate := ontology.DefaultDocument()
	typ := candidate.Types["issue"]
	typ.Fields["severity"] = ontology.Field{Type: "enum", Values: []string{"low", "high"}, Required: true, Default: "low"}
	typ.Fields["estimate"] = ontology.Field{Type: "integer", Required: true}
	req := request(t, current, candidate)
	source := rowsFor(t, req, bead("P-1", "P", nil))
	p := build(t, req, source, noPre())
	if p.Ready() || len(p.Report().Rows[0].Findings) != 1 || p.Report().Rows[0].After.State.Fields["severity"] != "low" {
		t.Fatalf("defaults must not hide missing required estimate: %+v", p.Report())
	}
	req.Directives = []Directive{{ID: "estimate", Kind: SetField, Target: Target{Project: "P", Type: "issue"}, Field: "estimate", Value: 3}}
	p = build(t, req, source, noPre())
	if !p.Ready() || p.Report().Affected != 1 || p.Report().Rows[0].After.State.Fields["estimate"] != json.Number("3") {
		t.Fatalf("explicit correction did not produce a real validated plan: %+v", p.Report())
	}
}

func TestExact5000And5001AffectedBoundary(t *testing.T) {
	current, candidate := ontology.DefaultDocument(), ontology.DefaultDocument()
	candidate.Types["issue"].Fields["new"] = ontology.Field{Type: "string", Default: "v"}
	req := request(t, current, candidate)
	for _, n := range []int{5000, 5001} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			rows := make([]Row, n)
			for i := range rows {
				rows[i] = bead(fmt.Sprintf("P-%05d", i), "P", nil)
			}
			p := build(t, req, rowsFor(t, req, rows...), noPre())
			if p.Report().Affected != n || p.Ready() != (n == 5000) || hasRefusal(p, "ontology_migration_too_large") != (n == 5001) {
				t.Fatalf("wrong affected-row boundary: ready=%v affected=%d refusals=%+v", p.Ready(), p.Report().Affected, p.Report().Refusals)
			}
		})
	}
}

func TestCancellationReturnsNoPlan(t *testing.T) {
	req := request(t, ontology.DefaultDocument(), ontology.DefaultDocument())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p, err := Build(ctx, req, rowsFor(t, req), noPre())
	if !errors.Is(err, context.Canceled) || p.Digest() != "" || p.Ready() {
		t.Fatalf("canceled planning returned evidence: %+v %v", p.Report(), err)
	}
}
