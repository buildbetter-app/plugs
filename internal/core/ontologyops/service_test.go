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

func TestPlanCASBindingChangesAndBoundedRetention(t *testing.T) {
	f := newFixture(t)
	f.seed(t, bead(t, "a", "p", nil))
	ctx := context.Background()
	document := withField(t, f.base, ontology.Field{Type: "boolean", Default: false})
	_, err := f.svc.Plan(ctx, caller, PlanRequest{Project: "p", Document: document, Expect: []model.Precondition{model.PreconditionByRevision(f.ref, model.Natural64(0))}})
	wantCode(t, err, "ontology_plan_stale")
	hash, _ := pipeline.FieldHash("a")
	pre := &validatingPre{}
	f.svc.cfg.ValidatingPre = pre
	result, err := f.svc.Plan(ctx, caller, PlanRequest{Project: "p", Document: document, Expect: []model.Precondition{model.PreconditionByHash(model.Ref{ID: "a"}, "fields.title", hash)}})
	if err != nil || !result.Changes.Ready {
		t.Fatalf("field CAS plan = %+v, %v", result, err)
	}
	pre.version = strings.Repeat("cd", 32)
	_, err = f.svc.Planned(ctx, caller, result.PlanDigest)
	wantCode(t, err, "ontology_plan_stale")
	f.svc.cfg.ValidatingPre = NoValidatingPre()
	f.svc.cfg.MaxPlans = 1
	f.svc.plans, f.svc.order = map[string]retainedPlan{}, nil
	first, err := f.svc.Plan(ctx, caller, PlanRequest{Project: "p", Document: document})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.svc.Plan(ctx, caller, PlanRequest{Project: "p", Document: withField(t, f.base, ontology.Field{Type: "boolean", Default: true})})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.Planned(ctx, caller, first.PlanDigest)
	wantCode(t, err, "ontology_plan_stale")
	retained, err := f.svc.Planned(ctx, caller, second.PlanDigest)
	if err != nil || retained.Document.Overlays["p"].Types["issue"].Fields["reviewed"].Default != true {
		t.Fatalf("retained candidate = %+v, %v", retained, err)
	}
}

func TestCompleteRowInventoryCrossesStoragePages(t *testing.T) {
	f := newFixture(t)
	changes := make([]conformance.FixtureChange, 0, 1003)
	for i := 0; i < 1003; i++ {
		id := fmt.Sprintf("page-%04d", i)
		changes = append(changes, bead(t, id, "p", map[string]any{
			"status": "todo", "title": id, "priority": ontology.DefaultPriority, "template": false,
		}))
	}
	f.seed(t, changes...)
	doc := ontology.DefaultDocument()
	doc.Types["extra"] = ontology.Type{Name: "New type", Workflow: "issue"}
	raw, _ := json.Marshal(doc)
	plan, err := f.svc.Plan(context.Background(), caller, PlanRequest{Document: raw})
	if err != nil || !plan.Changes.Ready || plan.Changes.Migration.Inventory.Count.String() != "1003" || plan.Changes.Migration.ObservedRowCount != 1003 || len(plan.Changes.Migration.Rows) != 1003 || plan.Changes.Migration.Affected != 0 {
		t.Fatalf("complete inventory: ready=%t count=%s observed=%d rows=%d affected=%d, %v", plan.Changes.Ready, plan.Changes.Migration.Inventory.Count, plan.Changes.Migration.ObservedRowCount, len(plan.Changes.Migration.Rows), plan.Changes.Migration.Affected, err)
	}
}

func TestPlansUseStoredGraphEvidenceAndRefuseUnavailableFacts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ready bool
	}{
		{"blocked", false}, {"satisfied", true}, {"missing-gate", false},
		{"unknown-incoming-relation", false}, {"installed-authorizer", false},
		{"missing-evidence", false}, {"real-evidence", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			doc := pipeline.Document{ID: "target", Project: "p", Type: "issue", Fields: map[string]any{"status": "done", "title": "Closed target"}}
			blockerStatus := "failed"
			if tc.name == "blocked" {
				blockerStatus = "todo"
			}
			doc.Links = []pipeline.Link{{To: model.Ref{ID: "blocker"}, Kind: "blocks"}}
			if tc.name == "real-evidence" {
				doc.Attachments = []model.Attachment{{Kind: "link", URI: "https://example.test/review", MediaType: "text/uri-list", Summary: "Actual stored review"}}
			}
			raw, _ := json.Marshal(doc)
			changes := []conformance.FixtureChange{{Ref: model.Ref{ID: doc.ID}, Kind: "bead", Op: model.OpPatch, Body: raw}, bead(t, "blocker", "p", map[string]any{"status": blockerStatus})}
			if tc.name == "unknown-incoming-relation" {
				child, _ := json.Marshal(pipeline.Document{ID: "child", Project: "p", Type: "issue", Fields: map[string]any{"status": "todo"}, Links: []pipeline.Link{{To: model.Ref{ID: doc.ID}, Kind: "parent-child"}}})
				changes = append(changes, conformance.FixtureChange{Ref: model.Ref{ID: "child"}, Kind: "bead", Op: model.OpPatch, Body: child})
			}
			f.seed(t, changes...)
			candidate := ontology.DefaultDocument()
			workflow := candidate.Workflows["issue"]
			if tc.name == "missing-gate" {
				workflow.Transitions = append(workflow.Transitions, ontology.Rule{To: "done", Guards: []ontology.Guard{{Kind: ontology.GatesResolved, Kinds: []string{"manual"}}}})
			}
			if strings.Contains(tc.name, "evidence") {
				workflow.Transitions = append(workflow.Transitions, ontology.Rule{To: "done", Guards: []ontology.Guard{{Kind: ontology.EvidenceRequired, Kinds: []string{"link"}}}})
			}
			candidate.Workflows["issue"] = workflow
			if tc.name == "installed-authorizer" {
				f.svc.cfg.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error { return nil }
			}
			result, err := f.svc.Plan(context.Background(), caller, PlanRequest{Project: "p", Document: withField(t, candidate, ontology.Field{Type: "boolean", Default: false})})
			if err != nil || result.Changes.Ready != tc.ready || (!tc.ready && len(result.Violations) == 0) {
				t.Fatalf("stored facts plan: ready=%t violations=%+v, %v", result.Changes.Ready, result.Violations, err)
			}
		})
	}
}

func TestSourceCompilationAndReadAuthorizationAreNeverSilentlyFiltered(t *testing.T) {
	f := newFixture(t)
	f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpPatch, Body: []byte(`{"base":{}}`)})
	check, err := f.svc.Check(context.Background(), caller, "p")
	if err != nil || check.OK || !check.Drifted || len(check.Problems) == 0 {
		t.Fatalf("compiler problems = %+v, %v", check, err)
	}
	overlay := ontology.Document{Types: map[string]ontology.Type{"issue": f.base.Types["issue"]}}
	f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpPatch, Body: marshalSource(t, service.OntologySource{Base: f.base, Overlays: map[string]ontology.Document{"private": overlay}})}, bead(t, "a", "p", nil))
	f.svc.cfg.AuthorizeRead = func(_ context.Context, _ model.PrincipalRef, _ model.ResourceRef, project string) error {
		if project == "private" {
			return failure("forbidden", "unreadable project source")
		}
		return nil
	}
	raw, _ := json.Marshal(f.base)
	_, err = f.svc.Plan(context.Background(), caller, PlanRequest{Project: "p", Document: raw})
	wantCode(t, err, "forbidden")
	_, err = f.svc.History(context.Background(), caller, "p", model.Natural64(0), 100)
	wantCode(t, err, "forbidden")
	f.svc.cfg.AuthorizeRead = func(_ context.Context, _ model.PrincipalRef, resource model.ResourceRef, _ string) error {
		if resource.Kind == "bead" {
			return failure("forbidden", "unreadable stored row")
		}
		return nil
	}
	_, err = f.svc.Plan(context.Background(), caller, PlanRequest{Project: "p", Document: raw})
	wantCode(t, err, "forbidden")
	f.svc.cfg.AuthorizeRead = nil
	f.svc.cfg.ValidatingPre = nil
	_, err = f.svc.Plan(context.Background(), caller, PlanRequest{Project: "p", Document: raw})
	wantCode(t, err, "invalid_params")
}

func TestCrossOverlayDiagnosticsAndHistoricalPathsRequireCurrentAccess(t *testing.T) {
	denyPrivate := func(_ context.Context, _ model.PrincipalRef, _ model.ResourceRef, project string) error {
		if project == "private" {
			return failure("forbidden", "unreadable project source")
		}
		return nil
	}
	t.Run("compiler diagnostics", func(t *testing.T) {
		f := newFixture(t)
		overlay := ontology.Document{Types: map[string]ontology.Type{"secret": {Workflow: "private-missing-workflow"}}}
		f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpPatch, Body: marshalSource(t, service.OntologySource{Base: f.base, Overlays: map[string]ontology.Document{"private": overlay}})})
		f.svc.cfg.AuthorizeRead = denyPrivate
		_, err := f.svc.Get(context.Background(), caller, "p")
		wantCode(t, err, "forbidden")
		_, err = f.svc.Check(context.Background(), caller, "p")
		wantCode(t, err, "forbidden")
	})
	t.Run("removed historical overlay", func(t *testing.T) {
		f := newFixture(t)
		overlay := ontology.Document{Types: map[string]ontology.Type{"issue": f.base.Types["issue"]}}
		f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpPatch, Body: marshalSource(t, service.OntologySource{Base: f.base, Overlays: map[string]ontology.Document{"private": overlay}}), ChangedPaths: []string{"overlays.private"}})
		f.seed(t, conformance.FixtureChange{Ref: f.ref, Kind: "ontology", Op: model.OpPatch, Body: marshalSource(t, service.OntologySource{Base: f.base}), ChangedPaths: []string{"overlays.private"}})
		f.svc.cfg.AuthorizeRead = denyPrivate
		for _, after := range []model.Revision{model.Natural64(1), model.Natural64(2)} {
			_, err := f.svc.History(context.Background(), caller, "p", after, 1)
			wantCode(t, err, "forbidden")
		}
		f.svc.cfg.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error { return nil }
		history, err := f.svc.History(context.Background(), caller, "p", model.Natural64(0), 100)
		if err != nil || len(history.Revisions) != 3 || history.Revisions[2].Changes[0].ChangedPaths[0] != "overlays.private" {
			t.Fatalf("authorized actual source history = %+v, %v", history, err)
		}
	})
}
