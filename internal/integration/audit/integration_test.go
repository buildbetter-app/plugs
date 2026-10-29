package audit_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

var (
	auditWriterPrincipal = model.PrincipalRef{Kind: "user", ID: "audit-writer"}
	auditReaderPrincipal = model.PrincipalRef{Kind: "user", ID: "audit-reader"}
)

type fixture struct {
	t         *testing.T
	ctx       context.Context
	store     *memory.Store
	lease     driver.ActiveLease
	inc       string
	sourceRef model.Ref
	source    service.OntologySource
	compiled  ontology.Snapshot
	normalize *service.Ontology
	writer    *pipeline.Writer
	svc       *service.Service
	ids       []string
	idIndex   int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	store, err := memory.NewWithOptions(memory.Options{
		StoreID:     "audit-integration",
		Incarnation: "audit-integration",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "audit-integration", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })

	sourceRef := model.Ref{ID: "ontology/audit"}
	source := service.OntologySource{Base: ontology.DefaultDocument()}
	// The same status means success in the dependents' project and failure
	// in the blocker's project. Consulting a dependent's ontology would make
	// the real fail transition emit notices or refuse instead of satisfying.
	dependentWorkflow := ontology.DefaultDocument().Workflows["issue"]
	delete(dependentWorkflow.Slots, ontology.Fail)
	source.Overlays = map[string]ontology.Document{
		"dependent-project": {
			Statuses:  map[string]ontology.Status{"failed": {Name: "Successful end", Category: ontology.Completed, Outcome: ontology.Success}},
			Workflows: map[string]ontology.Workflow{"issue": dependentWorkflow},
		},
	}
	body, err := service.MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{
		Principal: auditWriterPrincipal,
	}, []conformance.FixtureChange{{
		Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: body,
	}}); err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		t.Fatalf("fixture ontology compile: %+v", problems)
	}
	normalize := service.NewOntology(compiled.Effective)
	f := &fixture{
		t:         t,
		ctx:       ctx,
		store:     store,
		lease:     lease,
		inc:       meta.Incarnation,
		sourceRef: sourceRef,
		source:    source,
		compiled:  compiled,
		normalize: normalize,
		ids:       []string{"pl-audit-blocker", "pl-audit-dependent-b", "pl-audit-dependent-a"},
	}
	f.writer = pipeline.New(store, lease,
		pipeline.WithOntology(normalize),
		pipeline.WithOntologyAudit(service.NewOntologyAudit(service.StaticOntologySource(sourceRef))),
		pipeline.WithIDGenerator(f.nextID),
	)
	f.svc = service.New(store, lease, f.writer, service.WithOntology(normalize))
	return f
}

func (f *fixture) nextID() (string, error) {
	if f.idIndex >= len(f.ids) {
		return fmt.Sprintf("pl-audit-extra-%d", f.idIndex), nil
	}
	id := f.ids[f.idIndex]
	f.idIndex++
	return id, nil
}

func (f *fixture) meta(n int) model.WriteMeta {
	return model.WriteMeta{
		IdempotencyKey: fmt.Sprintf("%032x", n),
		Project:        "blocker-project",
	}
}

func (f *fixture) create(t *testing.T, project, title string, key int) model.Ref {
	t.Helper()
	commit, err := f.svc.Create(f.ctx, auditWriterPrincipal, service.CreateRequest{
		Type:   "issue",
		Fields: map[string]any{"title": title},
	}, model.WriteMeta{Project: project, IdempotencyKey: fmt.Sprintf("%032x", key)})
	if err != nil || len(commit.Results) != 1 {
		t.Fatalf("service create=%+v err=%v", commit, err)
	}
	return commit.Results[0].Ref
}

func (f *fixture) graph(t *testing.T) (blocker, dependentA, dependentB model.Ref, link model.Commit) {
	t.Helper()
	blocker = f.create(t, "blocker-project", "blocker", 2)
	dependentB = f.create(t, "dependent-project", "dependent b", 3)
	dependentA = f.create(t, "dependent-project", "dependent a", 4)
	link, err := f.writer.Apply(f.ctx, auditWriterPrincipal, model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpLink, Link: &model.LinkOp{From: dependentB, To: blocker, LinkKind: "conditional-blocks"}},
		{Kind: model.OpLink, Link: &model.LinkOp{From: dependentA, To: blocker, LinkKind: "conditional-blocks"}},
	}}, model.WriteMeta{Method: "bead.update", Project: "dependent-project", IdempotencyKey: fmt.Sprintf("%032x", 5)})
	if err != nil || len(link.Results) != 2 {
		t.Fatalf("writer conditional edges=%+v err=%v", link, err)
	}
	return blocker, dependentA, dependentB, link
}

func (f *fixture) close(t *testing.T, blocker model.Ref, key int, dryRun bool) port.WriteOutcome {
	t.Helper()
	meta := f.meta(key)
	meta.DryRun = dryRun
	outcome, err := f.svc.CloseWithOutcome(f.ctx, auditWriterPrincipal, service.CloseRequest{
		ID: blocker, Reason: "terminal reason",
	}, meta)
	if err != nil {
		t.Fatalf("service close err=%v", err)
	}
	return outcome
}

func (f *fixture) sourcePosition(t *testing.T, revision model.Revision) coreevents.Position {
	t.Helper()
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.inc})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	sets, err := reader.Changesets(f.ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, set := range sets {
		if set.Revision.String() == revision.String() {
			count := len(set.Changes)
			if set.Events != nil {
				count = len(set.Events)
			}
			if count == 0 {
				return coreevents.Position{Incarnation: f.inc, Revision: revision}
			}
			return coreevents.Position{Incarnation: f.inc, Revision: revision, Index: model.Natural64(uint64(count - 1))}
		}
	}
	t.Fatalf("missing changeset %s", revision)
	return coreevents.Position{}
}

func (f *fixture) reader(auth coreevents.Authorization) *coreevents.Reader {
	return &coreevents.Reader{
		Log:           serverevents.Log{Store: f.store},
		Materializer:  serverevents.HistoryMaterializer{Store: f.store},
		Authorization: auth,
	}
}

func (f *fixture) closeEvents(t *testing.T, after coreevents.Position, auth coreevents.Authorization) []model.Event {
	t.Helper()
	page, err := f.reader(auth).Read(f.ctx, coreevents.ReadRequest{
		After: after, Principal: auditReaderPrincipal, Limit: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return page.Events()
}

func eventSet(t *testing.T, f *fixture, revision model.Revision) model.Changeset {
	t.Helper()
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.inc})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	sets, err := reader.Changesets(f.ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, set := range sets {
		if set.Revision.String() == revision.String() {
			if err := set.ValidateEvents(); err != nil {
				t.Fatalf("persisted event plan invalid: %v", err)
			}
			return set
		}
	}
	t.Fatalf("missing event set %s", revision)
	return model.Changeset{}
}

func TestConnectedServiceWriterAuditPlanAndSuppression(t *testing.T) {
	f := newFixture(t)
	blocker, dependentA, dependentB, link := f.graph(t)
	start := f.sourcePosition(t, link.Revision)

	dry := f.close(t, blocker, 6, true)
	if !dry.Commit.WouldCommit || dry.Replayed {
		t.Fatalf("dry-run outcome=%+v", dry)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil || meta.Head.String() != link.Revision.String() {
		t.Fatalf("dry-run published graph revision: %+v err=%v", meta, err)
	}

	first := f.close(t, blocker, 7, false)
	if first.Replayed || first.Commit.Revision.String() != "6" {
		t.Fatalf("first close outcome=%+v", first)
	}
	entry := eventSet(t, f, first.Commit.Revision)
	if len(entry.Changes) != 1 || len(entry.Events) != 3 || entry.Events[0].Type != model.EventBeadClosed {
		t.Fatalf("close plan=%+v", entry)
	}
	for i, descriptor := range entry.Events {
		if descriptor.Index.String() != fmt.Sprint(i) || descriptor.ChangeIndex.String() != "0" || descriptor.Provenance.Method != "bead.close" || descriptor.Provenance.Slot != "close" || descriptor.Provenance.Cause.Root != fmt.Sprintf("%032x", 7) || !descriptor.Provenance.Cause.Depth.IsZero() || len(descriptor.Provenance.Cause.Chain) != 0 || len(descriptor.Provenance.Chain) != 0 || descriptor.Provenance.SetBy["fields.status"] != "request" {
			t.Fatalf("provenance[%d]=%+v", i, descriptor.Provenance)
		}
	}
	for _, descriptor := range entry.Events[1:] {
		if descriptor.Resource.Ref.ID != dependentA.ID && descriptor.Resource.Ref.ID != dependentB.ID {
			t.Fatalf("notice resource=%+v", descriptor)
		}
		if descriptor.Details == nil || !sameRef(descriptor.Details.Ontology.Ref, f.sourceRef) || descriptor.Details.Ontology.Revision.String() != "1" || descriptor.Details.Ontology.Digest != f.compiled.Digest() || descriptor.Details.Ontology.Project == nil || !sameRef(*descriptor.Details.Ontology.Project, model.Ref{ID: "blocker-project"}) {
			project := "<nil>"
			if descriptor.Details != nil && descriptor.Details.Ontology.Project != nil {
				project = descriptor.Details.Ontology.Project.ID
			}
			t.Fatalf("notice source pin=%+v project=%q source=%+v", descriptor.Details, project, f.sourceRef)
		}
		details := descriptor.Details
		if !sameRef(details.BlockerBefore.Ref, blocker) || !sameRef(details.BlockerAfter.Ref, blocker) || details.BlockerBefore.Revision.String() != "2" || details.BlockerAfter.Revision.String() != "6" || !sameRef(details.Dependent.Ref, descriptor.Resource.Ref) || details.Dependent.Revision.String() != link.Revision.String() || !sameRef(details.Edge.Dependent, details.Dependent.Ref) || !sameRef(details.Edge.Blocker, blocker) || details.Edge.Kind != "conditional-blocks" || details.Edge.Revision.String() != link.Revision.String() {
			t.Fatalf("notice revisions/decision=%+v", descriptor.Details)
		}
		if details.Before != (model.ConditionDecision{Status: "todo", Category: "unstarted"}) || details.After != (model.ConditionDecision{Status: "done", Category: "completed", Outcome: "success", Terminal: true}) {
			t.Fatalf("notice resolved decisions=%+v -> %+v", details.Before, details.After)
		}
	}
	if entry.Events[1].Resource.Ref.ID != dependentA.ID || entry.Events[2].Resource.Ref.ID != dependentB.ID {
		t.Fatalf("notice order=%+v", entry.Events)
	}

	events := f.closeEvents(t, start, coreevents.AllowAll{})
	if len(events) != 3 || events[0].Durable.Type != model.EventBeadClosed {
		t.Fatalf("materialized close events=%+v", events)
	}
	ordinary := events[0].Durable
	if ordinary.Before["fields"].(map[string]any)["status"] != "todo" || ordinary.After["fields"].(map[string]any)["status"] != "done" || !containsString(ordinary.Changed, "/fields/status") || ordinary.Derived || ordinary.Cause.Root != fmt.Sprintf("%032x", 7) {
		t.Fatalf("ordinary envelope=%+v", ordinary)
	}
	for i, event := range events[1:] {
		assertNoticeEnvelope(t, event, entry, i+1)
	}
	t.Logf("service close revision=%s: ordinary index=0, sorted notices=%s,%s; ontology=%s@1 digest=%s", first.Commit.Revision, dependentA.ID, dependentB.ID, f.sourceRef.ID, f.compiled.Digest())

	replayed := f.close(t, blocker, 7, false)
	if !replayed.Replayed || !reflect.DeepEqual(replayed.Commit, first.Commit) {
		t.Fatalf("replay outcome=%+v want=%+v", replayed, first)
	}
	noOp, err := f.svc.CloseWithOutcome(f.ctx, auditWriterPrincipal, service.CloseRequest{ID: blocker}, model.WriteMeta{
		Project: "blocker-project", IdempotencyKey: fmt.Sprintf("%032x", 8),
	})
	if err != nil {
		t.Fatal(err)
	}
	if noOp.Replayed || noOp.Commit.Revision.String() != first.Commit.Revision.String() {
		t.Fatalf("close no-op=%+v", noOp)
	}
	meta, err = f.store.Meta(f.ctx)
	if err != nil || meta.Head.String() != first.Commit.Revision.String() {
		t.Fatalf("replay/no-op changed head=%+v err=%v", meta, err)
	}

	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.inc})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, ref := range []model.Ref{dependentA, dependentB} {
		versions, err := reader.Versions(f.ctx, ref, driver.PageReq{})
		if err != nil || len(versions.Items) != 2 {
			t.Fatalf("dependent %s versions=%+v err=%v", ref.ID, versions, err)
		}
		rows, err := reader.Get(f.ctx, []model.Ref{ref})
		if err != nil || len(rows) != 1 || rows[0].Revision.String() != link.Revision.String() {
			t.Fatalf("notice changed dependent head %s: %+v err=%v", ref.ID, rows, err)
		}
	}

	// Advance only the protected fixture source. Its current interpretation of
	// "done" now satisfies the condition; historical delivery must retain the
	// original success decision and digest without a live ontology lookup.
	nextSource := service.OntologySource{Base: ontology.DefaultDocument()}
	nextSource.Base.Statuses["done"] = nextSource.Base.Statuses["failed"]
	nextSource.Base.Statuses["new_success"] = f.source.Base.Statuses["done"]
	workflow := nextSource.Base.Workflows["issue"]
	workflow.Statuses = append(workflow.Statuses, "new_success")
	workflow.Slots[ontology.Close] = "new_success"
	nextSource.Base.Workflows["issue"] = workflow
	nextCompiled, problems := ontology.Compile(nextSource.Base, nextSource.Overlays, nextSource.Plugins)
	if len(problems) != 0 || nextCompiled.Digest() == f.compiled.Digest() || !nextCompiled.Effective("blocker-project").SatisfiesConditionalBlocks("done") {
		t.Fatalf("updated fixture source does not change the decision: %+v", problems)
	}
	body, err := service.MarshalOntologySource(nextSource)
	if err != nil {
		t.Fatal(err)
	}
	sourceChange, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Principal: auditWriterPrincipal}, []conformance.FixtureChange{{
		Ref: f.sourceRef, Kind: "ontology", Op: model.OpPatch, Body: body,
	}})
	if err != nil {
		t.Fatal(err)
	}
	// A consumer also owns the copies returned from the changeset log.
	entry.Events[1].Details.Ontology.Project.ID = "consumer-mutated"
	entry.Events[1].Details.After.Status = "consumer-mutated"
	entry.Events[1].Provenance.SetBy["fields.status"] = "consumer-mutated"
	request := coreevents.ReadRequest{After: start, Principal: auditReaderPrincipal, Limit: 3}
	historical, err := f.reader(coreevents.AllowAll{}).Read(f.ctx, request)
	if err != nil || !reflect.DeepEqual(historical.Events(), events) {
		t.Fatalf("historical close changed after source revision %s: %+v err=%v", sourceChange.Revision, historical.Events(), err)
	}
	historical.Events()[1].Durable.Details.Ontology.Project.ID = "consumer-mutated"
	historical.Events()[1].Durable.Details.After.Status = "consumer-mutated"
	again, err := f.reader(coreevents.AllowAll{}).Read(f.ctx, request)
	if err != nil || !reflect.DeepEqual(again.Events(), events) {
		t.Fatalf("materialized details alias persisted history: %+v err=%v", again.Events(), err)
	}
	t.Logf("historical revision=%s remains exact after ontology source revision=%s and caller-copy mutations", first.Commit.Revision, sourceChange.Revision)
}

func sameRef(left, right model.Ref) bool {
	return left.IsID() && right.IsID() && left.ID == right.ID && left.Branch.String() == right.Branch.String()
}

func assertNoticeEnvelope(t *testing.T, event model.Event, set model.Changeset, index int) {
	t.Helper()
	notice := event.Durable
	if notice == nil || notice.Type != model.EventDependencyConditionUnmet || notice.Changed == nil || len(notice.Changed) != 0 || notice.Before != nil || notice.After != nil || notice.Derived || notice.Index.String() != fmt.Sprint(index) || notice.Revision.String() != set.Revision.String() || notice.Incarnation != set.Incarnation {
		t.Fatalf("notice envelope=%+v", notice)
	}
	descriptor := set.Events[index]
	if notice.Resource != "bead/"+descriptor.Resource.Ref.ID || !reflect.DeepEqual(notice.Details, descriptor.Details) || !reflect.DeepEqual(notice.Principal, set.Principal) || !reflect.DeepEqual(notice.SetBy, descriptor.Provenance.SetBy) || !reflect.DeepEqual(notice.Chain, descriptor.Provenance.Chain) || !reflect.DeepEqual(notice.Cause, descriptor.Provenance.Cause) {
		t.Fatalf("delivered notice differs from persisted descriptor: %+v want %+v", notice, descriptor)
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func TestConnectedAuditOutcomesSuppressUnmetNotices(t *testing.T) {
	for _, tc := range []struct {
		name     string
		method   string
		ordinary model.DurableEventType
		notices  int
		decision model.ConditionDecision
	}{
		{name: "cancel-direct-writer", method: "bead.cancel", ordinary: model.EventBeadCanceled, notices: 2, decision: model.ConditionDecision{Status: "canceled", Category: "canceled", Terminal: true}},
		{name: "failure-direct-writer", method: "bead.fail", ordinary: model.EventBeadFailed, notices: 0, decision: model.ConditionDecision{Status: "failed", Category: "completed", Outcome: "failure", Terminal: true, Satisfied: true}},
		{name: "open-direct-writer", method: "bead.defer", ordinary: model.EventBeadDeferred, notices: 0, decision: model.ConditionDecision{Status: "backlog", Category: "backlog"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			blocker, _, _, link := f.graph(t)
			var outcome port.WriteOutcome
			var err error
			outcome, err = f.writer.ApplyWithOutcome(f.ctx, auditWriterPrincipal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: blocker, Fields: map[string]any{}}}}}, model.WriteMeta{
				Method: tc.method, Project: "blocker-project", IdempotencyKey: fmt.Sprintf("%032x", 6),
			})
			if err != nil || outcome.Replayed {
				t.Fatalf("direct %s outcome=%+v err=%v", tc.method, outcome, err)
			}
			entry := eventSet(t, f, outcome.Commit.Revision)
			if len(entry.Events) != 1+tc.notices || entry.Events[0].Type != tc.ordinary {
				t.Fatalf("%s event plan=%+v", tc.method, entry)
			}
			page := f.closeEvents(t, f.sourcePosition(t, link.Revision), coreevents.AllowAll{})
			if len(page) != 1+tc.notices || page[0].Durable.Type != tc.ordinary {
				t.Fatalf("%s materialized=%+v", tc.method, page)
			}
			if page[0].Durable.After["fields"].(map[string]any)["status"] != tc.decision.Status {
				t.Fatalf("%s did not reach its real ontology slot: %+v", tc.method, page[0].Durable.After)
			}
			for i, event := range page[1:] {
				assertNoticeEnvelope(t, event, entry, i+1)
				if event.Durable.Details.After != tc.decision || event.Durable.Details.Ontology.Digest != f.compiled.Digest() || event.Durable.Details.Ontology.Project == nil || event.Durable.Details.Ontology.Project.ID != "blocker-project" {
					t.Fatalf("%s notice decision/source=%+v", tc.method, event.Durable.Details)
				}
			}
			t.Logf("direct Writer %s: status=%s revision=%s ordinary+notices=1+%d", tc.method, tc.decision.Status, outcome.Commit.Revision, tc.notices)
		})
	}
}

func TestConnectedAuditSourceInvalidationRefusesCommit(t *testing.T) {
	f := newFixture(t)
	blocker, _, _, link := f.graph(t)
	variant := 0
	audit := service.NewOntologyAudit(func(string) (model.Ref, error) {
		variant++
		source := service.OntologySource{Base: ontology.DefaultDocument()}
		source.Base.Statuses["todo"] = ontology.Status{Name: fmt.Sprintf("Todo %d", variant), Category: ontology.Unstarted}
		body, err := service.MarshalOntologySource(source)
		if err != nil {
			return model.Ref{}, err
		}
		_, err = conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Principal: auditWriterPrincipal}, []conformance.FixtureChange{{
			Ref: f.sourceRef, Kind: "ontology", Op: model.OpPatch, Body: body,
		}})
		return f.sourceRef, err
	})
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(f.normalize), pipeline.WithOntologyAudit(audit), pipeline.WithIDGenerator(f.nextID))
	_, err := writer.Apply(f.ctx, auditWriterPrincipal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: blocker, Fields: map[string]any{}}}}}, model.WriteMeta{
		Method: "bead.close", Project: "blocker-project", IdempotencyKey: fmt.Sprintf("%032x", 6),
	})
	var modelErr *model.Error
	if err == nil || !errors.As(err, &modelErr) || modelErr.Data.Code != "write_conflict" {
		t.Fatalf("source invalidation accepted/unmapped error=%v", err)
	}
	if variant != 4 {
		t.Fatalf("guard retry count=%d want 4", variant)
	}
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.inc})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(f.ctx, []driver.Ref{blocker})
	if err != nil || len(rows) != 1 {
		t.Fatalf("blocker read=%+v err=%v", rows, err)
	}
	var doc pipeline.Document
	if err := jsonDecode(rows[0].Body, &doc); err != nil || doc.Fields["status"] != "todo" {
		t.Fatalf("stale source race changed blocker=%+v err=%v", doc, err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil || meta.Head.String() != fmt.Sprint(5+variant) || link.Revision.String() != "5" {
		t.Fatalf("source race head=%+v variant=%d err=%v", meta, variant, err)
	}
	t.Logf("source invalidation: %d guarded attempts refused, head=%s contains only protected-source advances; blocker stays todo", variant, meta.Head)
}

func TestConnectedAuditResourceAuthorizationAndLostAckRecovery(t *testing.T) {
	f := newFixture(t)
	blocker, _, _, link := f.graph(t)
	close := f.close(t, blocker, 6, false)
	if close.Commit.Revision.String() != "6" {
		t.Fatalf("close=%+v", close)
	}
	start := f.sourcePosition(t, link.Revision)

	if _, err := f.reader(ordinaryOnlyAuthorization{}).Read(f.ctx, coreevents.ReadRequest{After: start, Principal: auditReaderPrincipal, Limit: 100}); !errors.Is(err, coreevents.ErrResourceAuthorization) {
		t.Fatalf("missing ResourceAuthorization error=%v", err)
	}
	masked, err := f.reader(denyOntologyAuthorization{}).Read(f.ctx, coreevents.ReadRequest{After: start, Principal: auditReaderPrincipal, Limit: 100})
	if err != nil || len(masked.Events()) != 1 || masked.Events()[0].Durable.Type != model.EventBeadClosed {
		t.Fatalf("masked notice page=%+v err=%v", masked, err)
	}

	reader := f.reader(coreevents.AllowAll{})
	checkpoints, err := serverevents.NewCheckpoints(f.store, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	binding := coreevents.Binding{ID: "connected-audit-listener", Class: coreevents.Listener, Principal: auditReaderPrincipal, Handler: deliveryHandler(func(_ context.Context, item coreevents.Delivery) error {
		return nil
	})}
	if _, err := checkpoints.Save(f.ctx, binding.ID, 0, coreevents.State{Position: start, Class: binding.Class}); err != nil {
		t.Fatal(err)
	}
	var deliveries []coreevents.Delivery
	binding.Handler = deliveryHandler(func(_ context.Context, item coreevents.Delivery) error {
		deliveries = append(deliveries, item)
		return nil
	})
	fault := &lostAckCheckpoints{Checkpoints: checkpoints, revision: close.Commit.Revision, index: model.Natural64(1)}
	dispatcher := &coreevents.Dispatcher{Reader: reader, Checkpoints: fault, Active: f.lease}
	if _, err := dispatcher.Step(f.ctx, binding); err == nil {
		t.Fatal("lost acknowledgement became a successful step")
	}
	state, _, err := checkpoints.Load(f.ctx, binding.ID)
	if err != nil || state.Position.Revision.String() != close.Commit.Revision.String() || !state.Position.Index.IsZero() {
		t.Fatalf("checkpoint after lost ack=%+v err=%v", state, err)
	}

	restarted := &coreevents.Dispatcher{Reader: f.reader(coreevents.AllowAll{}), Checkpoints: checkpoints, Active: f.lease}
	for i := 0; i < 2; i++ {
		progress, err := restarted.Step(f.ctx, binding)
		if err != nil {
			t.Fatal(err)
		}
		if !progress.Advanced {
			break
		}
	}
	if len(deliveries) != 4 || deliveries[1].Key.String() != deliveries[2].Key.String() || !reflect.DeepEqual(deliveries[1].Event, deliveries[2].Event) || deliveries[0].Event.Durable.Type != model.EventBeadClosed || deliveries[3].Event.Durable.Type != model.EventDependencyConditionUnmet {
		t.Fatalf("recovery deliveries=%+v", deliveries)
	}
	if deliveries[1].Key.Index.String() != "1" || deliveries[1].Key.Revision.String() != close.Commit.Revision.String() || deliveries[1].Key.Incarnation != f.inc || deliveries[1].Key.Binding != binding.ID {
		t.Fatalf("lost-ack retry is not the exact first notice key: %+v", deliveries[1].Key)
	}
	state, _, err = checkpoints.Load(f.ctx, binding.ID)
	if err != nil || state.Position.Revision.String() != close.Commit.Revision.String() || state.Position.Index.String() != "2" {
		t.Fatalf("final checkpoint=%+v err=%v", state, err)
	}
	t.Logf("authorization refusal/masking passed; lost-ack notice key=%s replayed byte-equivalent payload; final logical index=%s", deliveries[1].Key.String(), state.Position.Index)
}

// jsonDecode keeps this test's source-image assertion on the existing strict
// pipeline decoder without importing an unexported helper.
func jsonDecode(body []byte, out *pipeline.Document) error {
	decoded, err := pipeline.DecodeDocument(body)
	if err != nil {
		return err
	}
	*out = decoded
	return nil
}

type ordinaryOnlyAuthorization struct{}

func (ordinaryOnlyAuthorization) Read(_ context.Context, _ model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	return coreevents.Visibility{Readable: true, Before: event.Before, After: event.After}, nil
}

type denyOntologyAuthorization struct{}

func (denyOntologyAuthorization) Read(_ context.Context, _ model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	return coreevents.Visibility{Readable: true, Before: event.Before, After: event.After}, nil
}
func (denyOntologyAuthorization) ReadResource(_ context.Context, _ model.PrincipalRef, resource model.AuditResource) (coreevents.ResourceVisibility, error) {
	if resource.Kind == "ontology" {
		return coreevents.ResourceVisibility{Readable: false}, nil
	}
	return coreevents.ResourceVisibility{Readable: true}, nil
}

type deliveryHandler func(context.Context, coreevents.Delivery) error

func (h deliveryHandler) Deliver(ctx context.Context, delivery coreevents.Delivery) error {
	return h(ctx, delivery)
}

type lostAckCheckpoints struct {
	coreevents.Checkpoints
	revision model.Revision
	index    model.Natural
	failed   bool
}

func (c *lostAckCheckpoints) Save(ctx context.Context, binding string, generation uint64, state coreevents.State) (uint64, error) {
	if !c.failed && state.Position.Revision.String() == c.revision.String() && state.Position.Index.String() == c.index.String() {
		c.failed = true
		return generation, errors.New("connected audit lost acknowledgement")
	}
	return c.Checkpoints.Save(ctx, binding, generation, state)
}
