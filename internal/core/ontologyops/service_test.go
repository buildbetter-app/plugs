package ontologyops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

var caller = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type fixture struct {
	store *memory.Store
	lease driver.ActiveLease
	svc   *Service
	base  ontology.Document
	ref   model.Ref
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	s := memory.New()
	t.Cleanup(func() { _ = s.Close() })
	lease, err := s.Lease(context.Background(), "ontologyops-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{store: s, lease: lease, base: ontology.DefaultDocument(), ref: model.Ref{ID: "ontology/test"}}
	f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpCreate, Body: marshalSource(t, service.OntologySource{Base: f.base})})
	f.svc, err = New(s, lease, Config{Source: service.StaticOntologySource(f.ref), ValidatingPre: NoValidatingPre()})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func marshalSource(t *testing.T, source service.OntologySource) []byte {
	t.Helper()
	raw, err := service.MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (f *fixture) seed(t *testing.T, changes ...conformance.FixtureChange) model.Changeset {
	t.Helper()
	entry, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Time: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), Principal: caller}, changes)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func bead(t *testing.T, id, project string, fields map[string]any) conformance.FixtureChange {
	t.Helper()
	if fields == nil {
		fields = map[string]any{"status": "todo", "title": id}
	}
	raw, err := json.Marshal(pipeline.Document{ID: id, Project: project, Type: "issue", Fields: fields})
	if err != nil {
		t.Fatal(err)
	}
	return conformance.FixtureChange{Ref: model.Ref{ID: id}, Kind: "bead", Op: model.OpPatch, Body: raw}
}

func withField(t *testing.T, doc ontology.Document, field ontology.Field) json.RawMessage {
	t.Helper()
	raw, _ := json.Marshal(doc)
	copy, err := ontology.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	typ := copy.Types["issue"]
	typ.Fields["reviewed"] = field
	copy.Types["issue"] = typ
	raw, err = json.Marshal(copy)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func wantCode(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	var got *model.Error
	if !errors.As(err, &got) || got.Data.Code != code {
		t.Fatalf("error = %v, want %s", err, code)
	}
}

func TestProtectedSourceEffectiveReadsSchemasAndIntegrity(t *testing.T) {
	f := newFixture(t)
	overlayType := f.base.Types["issue"]
	overlayType.Name = "Project task"
	overlayType.Fields["reviewed"] = ontology.Field{Type: "boolean", Required: true}
	overlay := ontology.Document{Types: map[string]ontology.Type{"issue": overlayType}}
	f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpPatch, Body: marshalSource(t, service.OntologySource{Base: ontology.DefaultDocument(), Overlays: map[string]ontology.Document{"p": overlay}})}, bead(t, "a", "p", nil))
	ctx := context.Background()
	got, err := f.svc.Get(ctx, caller, "p")
	if err != nil || got.Revision.String() != "2" || got.Document.Types["issue"].Name != "Project task" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	got.Document.Types["issue"] = ontology.Type{Name: "caller mutation"}
	described, err := f.svc.Describe(ctx, caller, "p", "issue")
	if err != nil || described.Document.Types["issue"].Name != "Project task" || len(described.Document.Types) != 1 {
		t.Fatalf("describe = %+v, %v", described, err)
	}
	schema, err := f.svc.Schema(ctx, caller, "p", "issue", "todo")
	if err != nil || schema["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		t.Fatalf("schema = %v, %v", schema, err)
	}
	encoded, _ := json.Marshal(schema)
	if !strings.Contains(string(encoded), `"required":["reviewed"]`) || !strings.Contains(string(encoded), `"const":"issue"`) {
		t.Fatalf("schema does not expose actual fields: %s", encoded)
	}
	check, err := f.svc.Check(ctx, caller, "p")
	if err != nil || check.OK || !check.Drifted || len(check.Problems) != 1 || !strings.Contains(check.Problems[0].Path, "a") {
		t.Fatalf("check = %+v, %v", check, err)
	}
	f.seed(t, bead(t, "a", "p", map[string]any{"status": "todo", "title": "fixed", "reviewed": true}))
	check, err = f.svc.Check(ctx, caller, "p")
	if err != nil || !check.OK || check.Drifted {
		t.Fatalf("fixed check = %+v, %v", check, err)
	}
	_, err = f.svc.Describe(ctx, caller, "p", "missing")
	wantCode(t, err, "not_found")
	_, err = f.svc.Schema(ctx, caller, "", "issue", "")
	wantCode(t, err, "project_required")
}

func TestPlanUsesActualRowsDefaultsAndDetachedRetainedRevision(t *testing.T) {
	f := newFixture(t)
	f.seed(t, bead(t, "a", "p", nil))
	before, _ := f.store.Meta(context.Background())
	result, err := f.svc.Plan(context.Background(), caller, PlanRequest{Project: "p", Document: withField(t, f.base, ontology.Field{Type: "boolean", Default: false}), Expect: []model.Precondition{model.PreconditionByRevision(f.ref, model.Natural64(1))}})
	if err != nil || !result.Changes.Ready || len(result.Violations) != 0 || !result.RequiresHuman {
		t.Fatalf("plan = %+v, %v", result, err)
	}
	if len(result.Changes.Migration.Rows) != 1 || result.Changes.Migration.Affected != 1 || result.Changes.Migration.Rows[0].After.State.Fields["reviewed"] != false {
		t.Fatalf("actual row/default missing: %+v", result.Changes.Migration)
	}
	after, _ := f.store.Meta(context.Background())
	if after.Head.String() != before.Head.String() {
		t.Fatal("planning wrote a revision")
	}
	result.Changes.Migration.Rows[0].After.State.Fields["reviewed"] = "tampered"
	result.Changes.Source.BodyHash = "tampered"
	retained, err := f.svc.Planned(context.Background(), caller, result.PlanDigest)
	if err != nil || retained.Migration.Rows[0].After.State.Fields["reviewed"] != false || retained.Source.BodyHash == "tampered" {
		t.Fatalf("retained plan was mutable: %+v, %v", retained, err)
	}
	_, err = f.svc.Planned(context.Background(), model.PrincipalRef{Kind: "user", ID: "other"}, result.PlanDigest)
	wantCode(t, err, "ontology_plan_stale")
	f.seed(t, bead(t, "a", "p", map[string]any{"status": "todo", "title": "changed"}))
	_, err = f.svc.Planned(context.Background(), caller, result.PlanDigest)
	wantCode(t, err, "ontology_plan_stale")
}

type validatingPre struct {
	calls      int
	mutate     func()
	violations []ontology.Violation
	version    string
}

func (p *validatingPre) Inventory(_ context.Context, candidate ontologymigration.Reference) (ontologymigration.PreInventory, error) {
	version := p.version
	if version == "" {
		version = strings.Repeat("ab", 32)
	}
	bindings := []ontologymigration.Binding{{ID: "actual-validator", Type: "issue", Digest: version}}
	digest, err := ontologymigration.BindingsDigest(candidate, bindings)
	return ontologymigration.PreInventory{Candidate: candidate, Bindings: bindings, Digest: digest}, err
}

func (p *validatingPre) Validate(_ context.Context, _ ontologymigration.Binding, change ontologymigration.Change) ([]ontology.Violation, error) {
	p.calls++
	if p.mutate != nil {
		p.mutate()
	}
	// Mutate callback input to prove the planner owns detached images.
	change.After.State.Fields["reviewed"] = "callback mutation"
	return p.violations, nil
}

func TestPlanInvokesRealValidatorAndRetainsRefusal(t *testing.T) {
	f := newFixture(t)
	f.seed(t, bead(t, "a", "p", nil))
	pre := &validatingPre{violations: []ontology.Violation{{Code: "ontology_guard_failed", Message: "actual validator rejects candidate"}}}
	f.svc.cfg.ValidatingPre = pre
	result, err := f.svc.Plan(context.Background(), caller, PlanRequest{Project: "p", Document: withField(t, f.base, ontology.Field{Type: "boolean", Default: false})})
	if err != nil || pre.calls != 1 || result.Changes.Ready || len(result.Violations) != 1 || !strings.Contains(result.Violations[0].Message, "actual validator") || result.Changes.Migration.Rows[0].After.State.Fields["reviewed"] != false {
		t.Fatalf("validator plan = %+v, calls=%d, %v", result, pre.calls, err)
	}
	_, err = f.svc.Planned(context.Background(), caller, result.PlanDigest)
	wantCode(t, err, "ontology_plan_stale")
}

func TestPlanRefusesSourceMutationDuringCallbacksAndAfterRetention(t *testing.T) {
	for _, during := range []bool{true, false} {
		t.Run(fmt.Sprint("during=", during), func(t *testing.T) {
			f := newFixture(t)
			f.seed(t, bead(t, "a", "p", nil))
			update := func() {
				// Same bytes at a new row revision must still invalidate the plan.
				f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpPatch, Body: marshalSource(t, service.OntologySource{Base: f.base})})
			}
			if during {
				f.svc.cfg.ValidatingPre = &validatingPre{mutate: update}
			}
			result, err := f.svc.Plan(context.Background(), caller, PlanRequest{Project: "p", Document: withField(t, f.base, ontology.Field{Type: "boolean", Default: false})})
			if during {
				wantCode(t, err, "ontology_plan_stale")
				return
			}
			if err != nil || !result.Changes.Ready {
				t.Fatalf("plan = %+v, %v", result, err)
			}
			update()
			_, err = f.svc.Planned(context.Background(), caller, result.PlanDigest)
			wantCode(t, err, "ontology_plan_stale")
		})
	}
}

func TestActualHistoricalVersionsDiffAndIntegerContinuation(t *testing.T) {
	f := newFixture(t)
	f.seed(t, bead(t, "a", "p", nil))
	doc := ontology.DefaultDocument()
	typ := doc.Types["issue"]
	typ.Name = "Renamed type"
	doc.Types["issue"] = typ
	f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpPatch, Body: marshalSource(t, service.OntologySource{Base: doc})}, bead(t, "a", "p", nil))
	ctx := context.Background()
	history, err := f.svc.History(ctx, caller, "p", model.Natural64(0), 1)
	if err != nil || len(history.Revisions) != 1 || history.Revisions[0].Revision.String() != "1" {
		t.Fatalf("history = %+v, %v", history, err)
	}
	more, err := f.svc.History(ctx, caller, "p", history.Revisions[0].Revision, 1)
	if err != nil || len(more.Revisions) != 1 || more.Revisions[0].Revision.String() != "3" || len(more.Revisions[0].Changes) != 1 || more.Revisions[0].Changes[0].Ref.ID != f.ref.ID {
		t.Fatalf("continued source-only history = %+v, %v", more, err)
	}
	diff, err := f.svc.Diff(ctx, caller, "p", model.Natural64(1), model.Natural64(3))
	if err != nil || len(diff.Patch) != 1 || diff.Patch[0].Op != "replace" || diff.Patch[0].Path != "" || diff.Patch[0].Value.Types["issue"].Name != "Renamed type" {
		t.Fatalf("historical diff = %+v, %v", diff, err)
	}
	empty, err := f.svc.Diff(ctx, caller, "p", model.Natural64(1), model.Natural64(2))
	if err != nil || len(empty.Patch) != 0 {
		t.Fatalf("unchanged source diff = %+v, %v", empty, err)
	}
}

func TestSourceAndBoundaryRefusals(t *testing.T) {
	for _, raw := range []string{`{"base":{},"digest":"client assertion"}`, `{"base":{},"base":{}}`, `{} {}`, `{"base":{},"plugins":{"x":"true"}}`} {
		t.Run(raw, func(t *testing.T) {
			f := newFixture(t)
			if !json.Valid([]byte(raw)) {
				// Seed requires valid JSON. Decoder trailing refusal is already
				// exercised by the adopted service dependency's real tests.
				_, err := service.DecodeOntologySource([]byte(raw))
				if err == nil {
					t.Fatal("source accepted trailing JSON")
				}
				return
			}
			f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpPatch, Body: []byte(raw)})
			_, err := f.svc.Get(context.Background(), caller, "p")
			wantCode(t, err, "invalid_params")
		})
	}
	f := newFixture(t)
	f.svc.cfg.Source = service.StaticOntologySource(model.Ref{ID: "ordinary-record"})
	_, err := f.svc.Get(context.Background(), caller, "p")
	wantCode(t, err, "invalid_params")
	f.svc.cfg.Source = service.StaticOntologySource(f.ref)
	f.svc.cfg.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error {
		return failure("forbidden", "actual authorizer denies")
	}
	_, err = f.svc.Get(context.Background(), caller, "p")
	wantCode(t, err, "forbidden")
	f.svc.cfg.AuthorizeRead = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = f.svc.Get(ctx, caller, "p")
	wantCode(t, err, "busy")
	_ = f.lease.Release(context.Background())
	_, err = f.svc.Get(context.Background(), caller, "p")
	wantCode(t, err, "busy")
}
