package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type auditSnapshot struct {
	row   driver.Record
	reads int
}

// mutatingAudit delegates the decision to the real service adapter, then
// advances the protected source before Writer acquires its commit locks. The
// test proves that the adapter's tracked read cannot be used after a source
// revision changes underneath the plan.
type mutatingAudit struct {
	inner  port.OntologyAudit
	store  driver.Store
	lease  driver.ActiveLease
	source model.Ref
	body   []byte
	calls  int
}

func (a *mutatingAudit) DecideConditional(ctx context.Context, change port.Change, reader port.SnapshotReader) (port.ConditionalDecision, error) {
	decision, err := a.inner.DecideConditional(ctx, change, reader)
	if err != nil {
		return port.ConditionalDecision{}, err
	}
	a.calls++
	if _, err := conformance.Seed(ctx, a.store, a.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
		Ref: a.source, Kind: "ontology", Op: model.OpPatch, Body: a.body,
	}}); err != nil {
		return port.ConditionalDecision{}, err
	}
	return decision, nil
}

func (r *auditSnapshot) Snapshot() driver.View { return driver.View{} }
func (r *auditSnapshot) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.reads++
	if len(refs) == 1 && refs[0] == r.row.Ref {
		return []driver.Record{r.row}, nil
	}
	return nil, nil
}
func (r *auditSnapshot) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	return driver.Page[driver.Record]{}, nil
}
func (r *auditSnapshot) Observe(context.Context, []driver.LockKey) ([]driver.LockState, error) {
	return nil, nil
}

func auditSource(t *testing.T, overlays map[string]ontology.Document) (OntologySource, ontology.Snapshot) {
	t.Helper()
	base := ontology.DefaultDocument()
	source := OntologySource{Base: base, Overlays: overlays}
	snapshot, problems := ontology.Compile(base, overlays, nil)
	if len(problems) != 0 {
		t.Fatalf("source compile: %+v", problems)
	}
	return source, snapshot
}

func auditChange(t *testing.T, project, beforeStatus, afterStatus string) port.Change {
	t.Helper()
	ref := model.Ref{ID: "pl-audit-blocker"}
	before, err := json.Marshal(pipeline.Document{ID: ref.ID, Type: "issue", Project: project, Fields: map[string]any{"status": beforeStatus}})
	if err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(pipeline.Document{ID: ref.ID, Type: "issue", Project: project, Fields: map[string]any{"status": afterStatus}})
	if err != nil {
		t.Fatal(err)
	}
	return port.Change{
		Resource: model.ResourceRef{Kind: "bead", Ref: ref},
		Op:       model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{}}},
		Before:   before,
		After:    after,
		SetBy:    map[string]string{"fields.status": "request"},
	}
}

func TestDecodeOntologySourceRoundTrip(t *testing.T) {
	source := OntologySource{
		Base: ontology.DefaultDocument(),
		Overlays: map[string]ontology.Document{"project": {
			Statuses: map[string]ontology.Status{"todo": {Name: "Project todo", Category: ontology.Unstarted}},
		}},
		Plugins: ontology.PluginCatalog{"available": true, "unavailable": false},
	}
	envelope, err := MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := json.Marshal(source.Base)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
		want OntologySource
	}{
		{"envelope", envelope, source},
		{"plain-document", plain, OntologySource{Base: source.Base}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeOntologySource(tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("decoded source=%+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestDecodeOntologySourceRejectsAmbiguousAndUnsupportedBodies(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
	}{
		{"trailing-json", `{} {}`},
		{"non-object", `[]`},
		{"unknown-document-member", `{"digest":"asserted"}`},
		{"unknown-envelope-member", `{"base":{},"digest":"asserted"}`},
		{"duplicate-envelope-member", `{"base":{},"base":{}}`},
		{"duplicate-document-member", `{"base":{"statuses":{},"statuses":{}}}`},
		{"unknown-overlay-member", `{"base":{},"overlays":{"project":{"decision":true}}}`},
		{"invalid-base-shape", `{"base":null}`},
		{"invalid-overlay-shape", `{"base":{},"overlays":{"project":[]}}`},
		{"invalid-plugin-shape", `{"base":{},"plugins":{"plugin":"true"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeOntologySource([]byte(tc.raw)); err == nil {
				t.Fatal("unsupported source body was accepted")
			}
		})
	}
}

func TestTrackedOntologyAuditPinsSourceAndClassifiesOwnProject(t *testing.T) {
	overlay := ontology.Document{
		Statuses: map[string]ontology.Status{"project_failed": {Name: "Project failed", Category: ontology.Completed, Outcome: ontology.Failure}},
		Workflows: map[string]ontology.Workflow{"issue": {
			Statuses: []string{"todo", "in_progress", "done", "failed", "canceled", "backlog", "project_failed"},
			Initial:  "todo",
			Slots:    map[ontology.Slot]string{ontology.Claim: "in_progress", ontology.Close: "done", ontology.Reopen: "todo", ontology.Fail: "project_failed"},
		}},
		Types: map[string]ontology.Type{"issue": {Name: "Issue", Workflow: "issue"}},
	}
	source, compiled := auditSource(t, map[string]ontology.Document{"project": overlay})
	body, err := MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.Ref{ID: "ontology/audit"}
	reader := &auditSnapshot{row: driver.Record{Ref: ref, Kind: "ontology", Revision: model.Natural64(7), Body: body}}
	audit := NewOntologyAudit(StaticOntologySource(ref))

	cases := []struct {
		name, project, after, category, outcome string
		satisfied, terminal                     bool
	}{
		{"success", "project", "done", "completed", "success", false, true},
		{"failure-overlay", "project", "project_failed", "completed", "failure", true, true},
		{"cancel", "project", "canceled", "canceled", "", false, true},
		{"open", "project", "in_progress", "started", "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision, err := audit.DecideConditional(context.Background(), auditChange(t, tc.project, "todo", tc.after), reader)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Ontology.Ref != ref || decision.Ontology.Revision.String() != "7" || decision.Ontology.Digest != compiled.Digest() || decision.Ontology.Project == nil || decision.Ontology.Project.ID != tc.project {
				t.Fatalf("pin=%+v", decision.Ontology)
			}
			if decision.After.Category != tc.category || decision.After.Outcome != tc.outcome || decision.After.Satisfied != tc.satisfied || decision.After.Terminal != tc.terminal {
				t.Fatalf("after=%+v", decision.After)
			}
			if decision.Before.Status != "todo" || decision.Before.Category != "unstarted" || decision.Before.Terminal || decision.Before.Satisfied {
				t.Fatalf("before=%+v", decision.Before)
			}
		})
	}
	if reader.reads != len(cases) {
		t.Fatalf("source reads=%d, want %d", reader.reads, len(cases))
	}
}

func TestTrackedOntologyAuditProjectlessAndArchivedDefinitions(t *testing.T) {
	base := ontology.DefaultDocument()
	base.Statuses["draft"] = ontology.Status{Name: "Draft", Category: ontology.Unstarted}
	workflow := base.Workflows["issue"]
	workflow.Statuses = append(workflow.Statuses, "draft")
	base.Workflows["issue"] = workflow
	baseSnapshot, problems := ontology.Compile(base, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("base source compile: %+v", problems)
	}
	ref := model.Ref{ID: "ontology/audit"}
	baseBody, err := MarshalOntologySource(OntologySource{Base: base})
	if err != nil {
		t.Fatal(err)
	}
	seenProject := "missing"
	locator := func(project string) (model.Ref, error) {
		seenProject = project
		return ref, nil
	}
	reader := &auditSnapshot{row: driver.Record{Ref: ref, Kind: "ontology", Revision: model.Natural64(9), Body: baseBody}}
	decision, err := NewOntologyAudit(locator).DecideConditional(context.Background(), auditChange(t, "", "todo", "done"), reader)
	if err != nil {
		t.Fatal(err)
	}
	if seenProject != "" || decision.Ontology.Project != nil || decision.Ontology.Digest != baseSnapshot.Digest() {
		t.Fatalf("projectless decision=%+v locatorProject=%q", decision, seenProject)
	}

	cases := []struct {
		name    string
		overlay ontology.Document
		status  string
	}{
		{
			name: "archived-status",
			overlay: ontology.Document{Statuses: map[string]ontology.Status{
				"draft": {Archived: true},
			}},
			status: "draft",
		},
		{
			name: "archived-type",
			overlay: ontology.Document{Types: map[string]ontology.Type{
				"issue": {Archived: true},
			}},
			status: "done",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := OntologySource{Base: base, Overlays: map[string]ontology.Document{"project": tc.overlay}}
			body, err := MarshalOntologySource(source)
			if err != nil {
				t.Fatal(err)
			}
			reader := &auditSnapshot{row: driver.Record{Ref: ref, Kind: "ontology", Revision: model.Natural64(10), Body: body}}
			_, err = NewOntologyAudit(StaticOntologySource(ref)).DecideConditional(context.Background(), auditChange(t, "project", "todo", tc.status), reader)
			if err == nil {
				t.Fatal("archived ontology definition was accepted")
			}
		})
	}
}

func TestTrackedOntologyAuditRefusesUnpinnedAndInvalidSources(t *testing.T) {
	source, _ := auditSource(t, nil)
	body, err := MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	change := auditChange(t, "project", "todo", "done")
	cases := []struct {
		name   string
		ref    model.Ref
		kind   string
		body   []byte
		locate OntologySourceLocator
	}{
		{"nil-locator", model.Ref{}, "", nil, nil},
		{"unprotected-ref", model.Ref{ID: "bead/source"}, "ontology", body, StaticOntologySource(model.Ref{ID: "bead/source"})},
		{"wrong-kind", model.Ref{ID: "ontology/audit"}, "bead", body, StaticOntologySource(model.Ref{ID: "ontology/audit"})},
		{"unsupported-digest-assertion", model.Ref{ID: "ontology/audit"}, "ontology", []byte(`{"base":{},"digest":"` + strings.Repeat("a", 64) + `"}`), StaticOntologySource(model.Ref{ID: "ontology/audit"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &auditSnapshot{row: driver.Record{Ref: tc.ref, Kind: tc.kind, Revision: "1", Body: tc.body}}
			_, err := NewOntologyAudit(tc.locate).DecideConditional(context.Background(), change, reader)
			if err == nil {
				t.Fatal("invalid source was accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &auditSnapshot{row: driver.Record{Ref: model.Ref{ID: "ontology/audit"}, Kind: "ontology", Revision: "1", Body: body}}
	_, err = NewOntologyAudit(StaticOntologySource(reader.row.Ref)).DecideConditional(ctx, change, reader)
	if !errors.Is(err, context.Canceled) || reader.reads != 0 {
		t.Fatalf("cancelled decision=%v reads=%d", err, reader.reads)
	}
}

func TestTrackedOntologyAuditRunsThroughWriterAndPinsTrackedMemorySource(t *testing.T) {
	f := setup(t)
	source, compiled := auditSource(t, nil)
	body, err := MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	sourceRef := model.Ref{ID: "ontology/audit"}
	blocker := model.Ref{ID: "pl-audit-blocker"}
	dependent := model.Ref{ID: "pl-audit-dependent"}
	blockerBody, _ := json.Marshal(pipeline.Document{ID: blocker.ID, Type: "issue", Project: "blocker-project", Fields: map[string]any{"title": "blocker", "status": "todo"}})
	dependentBody, _ := json.Marshal(pipeline.Document{ID: dependent.ID, Type: "issue", Project: "dependent-project", Fields: map[string]any{"title": "dependent", "status": "todo"}, Links: []pipeline.Link{{To: blocker, Kind: "conditional-blocks"}}})
	_, err = conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: body},
		{Ref: blocker, Kind: "bead", Op: model.OpCreate, Body: blockerBody},
		{Ref: dependent, Kind: "bead", Op: model.OpCreate, Body: dependentBody},
	})
	if err != nil {
		t.Fatal(err)
	}
	normalizer := NewOntology(compiled.Effective)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(normalizer), pipeline.WithOntologyAudit(NewOntologyAudit(StaticOntologySource(sourceRef))))
	meta := meta(t)
	meta.Project = "blocker-project"
	meta.Method = "bead.close"
	commit, err := writer.Apply(context.Background(), anonymous, model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: blocker, Fields: map[string]any{}}},
		{Kind: model.OpComment, Comment: &model.CommentOp{Target: blocker, Body: "close"}},
	}}, meta)
	if err != nil {
		t.Fatal(err)
	}
	storeMeta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(context.Background(), driver.View{Incarnation: storeMeta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	entries, err := reader.Changesets(context.Background(), nil, 20)
	if err != nil {
		t.Fatal(err)
	}
	var entry *model.Changeset
	for i := range entries {
		if entries[i].Revision.String() == commit.Revision.String() {
			entry = &entries[i]
		}
	}
	if entry == nil || len(entry.Events) != 2 || entry.Events[1].Details == nil {
		t.Fatalf("changeset=%+v", entry)
	}
	details := entry.Events[1].Details
	if !sameResolvedRef(details.Ontology.Ref, sourceRef) || details.Ontology.Revision.String() != "1" || details.Ontology.Digest != compiled.Digest() || details.Ontology.Project == nil || details.Ontology.Project.ID != "blocker-project" {
		t.Fatalf("ontology pin=%+v", details.Ontology)
	}
	if details.After.Status != "done" || details.After.Category != "completed" || details.After.Outcome != "success" || details.After.Satisfied || !details.After.Terminal {
		t.Fatalf("decision=%+v", details.After)
	}
	versions, err := reader.Versions(context.Background(), dependent, driver.PageReq{})
	if err != nil || len(versions.Items) != 1 {
		t.Fatalf("dependent versions=%+v err=%v", versions, err)
	}
}

func TestTrackedOntologyAuditSourceChangeInvalidatesWriterPlan(t *testing.T) {
	f := setup(t)
	source, compiled := auditSource(t, nil)
	body, err := MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	sourceRef := model.Ref{ID: "ontology/audit"}
	blocker := model.Ref{ID: "pl-audit-race-blocker"}
	dependent := model.Ref{ID: "pl-audit-race-dependent"}
	blockerBody, _ := json.Marshal(pipeline.Document{ID: blocker.ID, Type: "issue", Project: "blocker-project", Fields: map[string]any{"title": "blocker", "status": "todo"}})
	dependentBody, _ := json.Marshal(pipeline.Document{ID: dependent.ID, Type: "issue", Project: "dependent-project", Fields: map[string]any{"title": "dependent", "status": "todo"}, Links: []pipeline.Link{{To: blocker, Kind: "conditional-blocks"}}})
	_, err = conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: body},
		{Ref: blocker, Kind: "bead", Op: model.OpCreate, Body: blockerBody},
		{Ref: dependent, Kind: "bead", Op: model.OpCreate, Body: dependentBody},
	})
	if err != nil {
		t.Fatal(err)
	}
	inner := NewOntologyAudit(StaticOntologySource(sourceRef))
	tracing := &mutatingAudit{inner: inner, store: f.store, lease: f.lease, source: sourceRef, body: body}
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(NewOntology(compiled.Effective)), pipeline.WithOntologyAudit(tracing))
	meta := meta(t)
	meta.Project = "blocker-project"
	meta.Method = "bead.close"
	_, err = writer.Apply(context.Background(), anonymous, model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: blocker, Fields: map[string]any{}}},
	}}, meta)
	wantCode(t, err, "write_conflict")
	if tracing.calls != 4 {
		t.Fatalf("adapter calls=%d, want four guarded attempts", tracing.calls)
	}
	storeMeta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(context.Background(), driver.View{Incarnation: storeMeta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(context.Background(), []model.Ref{blocker, sourceRef})
	if err != nil || len(rows) != 2 {
		t.Fatalf("post-race rows=%+v err=%v", rows, err)
	}
	for _, row := range rows {
		if row.Ref == blocker {
			doc, decodeErr := pipeline.DecodeDocument(row.Body)
			if decodeErr != nil || doc.Fields["status"] != "todo" {
				t.Fatalf("blocker changed after source race: %+v err=%v", doc, decodeErr)
			}
		}
	}
}
