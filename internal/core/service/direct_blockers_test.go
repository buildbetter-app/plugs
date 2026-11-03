package service

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type directBlockerFixture struct {
	store  driver.Store
	lease  driver.ActiveLease
	writer *pipeline.Writer
	svc    *Service
	now    time.Time
}

type failingDirectBlockerReader struct {
	driver.Reader
	err error
}

func (r failingDirectBlockerReader) Get(context.Context, []model.Ref) ([]driver.Record, error) {
	return nil, r.err
}

func directBlockerFixtures(t *testing.T, run func(*testing.T, *directBlockerFixture)) {
	t.Helper()
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			f := &directBlockerFixture{now: time.Date(2026, 10, 30, 9, 0, 0, 0, time.UTC)}
			if engine == "memory" {
				f.store = memory.New()
			} else {
				var err error
				f.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "direct-blockers.db")})
				if err != nil {
					t.Fatal(err)
				}
			}
			var err error
			f.lease, err = f.store.Lease(ctx, "direct-blockers-test", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = f.lease.Release(ctx)
				_ = f.store.Close()
			})
			o := NewOntology(nil)
			f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return f.now }))
			f.svc = New(f.store, f.lease, f.writer, WithOntology(o), WithClock(func() time.Time { return f.now }))
			run(t, f)
		})
	}
}

func directBlockerMeta(t *testing.T) model.WriteMeta {
	t.Helper()
	value := meta(t)
	value.Project = "dependent-project"
	value.Agent = "direct-blockers-test"
	value.AgentLabel = "direct-blockers-test"
	return value
}

func seedDirectBlockerOntology(t *testing.T, f *directBlockerFixture, source OntologySource) model.Ref {
	t.Helper()
	body, err := MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID("ontology/direct-blockers", model.Natural64(0))
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "direct-blockers-test"}}, []conformance.FixtureChange{{Ref: ref, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	return ref
}

func seedDirectBlockerGraph(t *testing.T, f *directBlockerFixture, kind string) (model.Ref, model.Ref) {
	t.Helper()
	ctx := context.Background()
	blockerMeta := directBlockerMeta(t)
	blockerMeta.Project = "blocker-project"
	blockerCommit, err := f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": "blocker"}}, blockerMeta)
	if err != nil {
		t.Fatal(err)
	}
	dependentCommit, err := f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": "dependent"}}, directBlockerMeta(t))
	if err != nil {
		t.Fatal(err)
	}
	blocker := blockerCommit.Results[0].Ref
	dependent := dependentCommit.Results[0].Ref
	meta := directBlockerMeta(t)
	meta.Project = "dependent-project"
	if _, err := f.writer.Apply(ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: dependent, To: blocker, LinkKind: kind}}}}, meta); err != nil {
		t.Fatal(err)
	}
	return dependent, blocker
}

func directBlockerReader(t *testing.T, f *directBlockerFixture, at *model.Revision) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func TestReadDirectBlockersUsesPinnedOwnProjectOntologyAndDoesNotWrite(t *testing.T) {
	directBlockerFixtures(t, func(t *testing.T, f *directBlockerFixture) {
		ctx := context.Background()
		base := ontology.DefaultDocument()
		source := OntologySource{Base: base}
		ontologyRef := seedDirectBlockerOntology(t, f, source)
		dependent, blocker := seedDirectBlockerGraph(t, f, "blocks")
		before, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r := directBlockerReader(t, f, &before.Head)
		result, err := ReadDirectBlockers(ctx, r, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef), Limit: 8})
		if err != nil {
			t.Fatal(err)
		}
		if result.Dependent != dependent || len(result.Edges) != 1 || result.Edges[0].Blocker != blocker || result.Edges[0].State != DirectBlockerUnsatisfied {
			t.Fatalf("direct blocker result: %+v", result)
		}
		after, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if after.Head.String() != before.Head.String() {
			t.Fatalf("read changed head: before=%s after=%s", before.Head, after.Head)
		}
	})
}

func TestReadDirectBlockersPreservesUnknownAndOntologySnapshotChanges(t *testing.T) {
	directBlockerFixtures(t, func(t *testing.T, f *directBlockerFixture) {
		ctx := context.Background()
		initial := ontology.Document{Statuses: map[string]ontology.Status{"project_waiting": {Name: "Project waiting", Category: ontology.Backlog}}}
		ontologyRef := seedDirectBlockerOntology(t, f, OntologySource{Base: ontology.DefaultDocument(), Overlays: map[string]ontology.Document{"blocker-project": initial}})
		dependent, blocker := seedDirectBlockerGraph(t, f, "blocks")
		blockerBody, err := json.Marshal(pipeline.Document{ID: blocker.ID, Type: "issue", Project: "blocker-project", Fields: map[string]any{"title": "blocker", "status": "project_waiting"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "direct-blockers-test"}}, []conformance.FixtureChange{{Ref: blocker, Kind: "bead", Op: model.OpPatch, Body: blockerBody, Graph: &conformance.FixtureGraphSource{}}}); err != nil {
			t.Fatal(err)
		}
		firstMeta, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		first := directBlockerReader(t, f, &firstMeta.Head)
		changed := ontology.Document{Statuses: map[string]ontology.Status{"project_waiting": {Name: "Project waiting", Category: ontology.Completed, Outcome: ontology.Success}}}
		body, err := MarshalOntologySource(OntologySource{Base: ontology.DefaultDocument(), Overlays: map[string]ontology.Document{"blocker-project": changed}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "direct-blockers-test"}}, []conformance.FixtureChange{{Ref: ontologyRef, Kind: "ontology", Op: model.OpPatch, Body: body}}); err != nil {
			t.Fatal(err)
		}
		oldResult, err := ReadDirectBlockers(ctx, first, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef), Limit: 8})
		if err != nil || len(oldResult.Edges) != 1 || oldResult.Edges[0].State != DirectBlockerUnsatisfied {
			t.Fatalf("pinned old ontology result=%+v err=%v", oldResult, err)
		}
		latestMeta, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		latest := directBlockerReader(t, f, &latestMeta.Head)
		newResult, err := ReadDirectBlockers(ctx, latest, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef), Limit: 8})
		if err != nil || len(newResult.Edges) != 1 || newResult.Edges[0].State != DirectBlockerSatisfied {
			t.Fatalf("changed ontology result=%+v err=%#v", newResult, err)
		}
		_ = blocker
	})
}

func TestReadDirectBlockersMatchesConditionalReadinessSemantics(t *testing.T) {
	directBlockerFixtures(t, func(t *testing.T, f *directBlockerFixture) {
		ctx := context.Background()
		ontologyRef := seedDirectBlockerOntology(t, f, OntologySource{Base: ontology.DefaultDocument()})
		dependent, blocker := seedDirectBlockerGraph(t, f, "conditional-blocks")
		beforeMeta, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		before := directBlockerReader(t, f, &beforeMeta.Head)
		blocked, err := ReadDirectBlockers(ctx, before, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef), Limit: 8})
		if err != nil || len(blocked.Edges) != 1 || blocked.Edges[0].State != DirectBlockerUnsatisfied {
			t.Fatalf("open conditional blocker result=%+v err=%#v", blocked, err)
		}
		failMeta := directBlockerMeta(t)
		failMeta.Project = "blocker-project"
		if _, err := f.svc.Fail(ctx, anonymous, FailRequest{ID: blocker, Reason: "failed as required"}, failMeta); err != nil {
			t.Fatal(err)
		}
		afterMeta, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		after := directBlockerReader(t, f, &afterMeta.Head)
		satisfied, err := ReadDirectBlockers(ctx, after, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef), Limit: 8})
		if err != nil || len(satisfied.Edges) != 1 || satisfied.Edges[0].State != DirectBlockerSatisfied {
			t.Fatalf("failed conditional blocker result=%+v err=%#v", satisfied, err)
		}
		pinnedAgain, err := ReadDirectBlockers(ctx, before, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef), Limit: 8})
		if err != nil || pinnedAgain.Edges[0].State != DirectBlockerUnsatisfied {
			t.Fatalf("old reader drifted after blocker failure: %+v err=%#v", pinnedAgain, err)
		}
	})
}

func TestReadDirectBlockersDistinguishesMissingAndRefusesUnboundedInput(t *testing.T) {
	directBlockerFixtures(t, func(t *testing.T, f *directBlockerFixture) {
		ctx := context.Background()
		ontologyRef := seedDirectBlockerOntology(t, f, OntologySource{Base: ontology.DefaultDocument()})
		dependent, blocker := seedDirectBlockerGraph(t, f, "blocks")
		meta, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pinned := directBlockerReader(t, f, &meta.Head)
		locatorErr := errors.New("ontology locator unavailable")
		_, err = ReadDirectBlockers(ctx, pinned, DirectBlockerRequest{Dependent: dependent, Ontology: func(string) (model.Ref, error) { return model.Ref{}, locatorErr }, Limit: 8})
		if !errors.Is(err, locatorErr) {
			t.Fatalf("locator error was not preserved: %#v", err)
		}
		missingOntology, err := ReadDirectBlockers(ctx, pinned, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(model.RefByID("ontology/missing", model.Natural64(0))), Limit: 8})
		if err != nil || len(missingOntology.Edges) != 1 || missingOntology.Edges[0].State != DirectBlockerUnknown {
			t.Fatalf("missing ontology result=%+v err=%#v", missingOntology, err)
		}
		_, err = ReadDirectBlockers(ctx, pinned, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(model.RefByID("not-ontology", model.Natural64(0))), Limit: 8})
		wantCode(t, err, "invalid_params")
		secondMeta := directBlockerMeta(t)
		secondMeta.Project = "blocker-project"
		secondCommit, err := f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": "second blocker"}}, secondMeta)
		if err != nil {
			t.Fatal(err)
		}
		linkMeta := directBlockerMeta(t)
		if _, err := f.writer.Apply(ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: dependent, To: secondCommit.Results[0].Ref, LinkKind: "blocks"}}}}, linkMeta); err != nil {
			t.Fatal(err)
		}
		boundedMeta, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		bounded := directBlockerReader(t, f, &boundedMeta.Head)
		_, err = ReadDirectBlockers(ctx, bounded, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef), Limit: 1})
		wantCode(t, err, "closure_too_large")
		missing := model.RefByID("missing-blocker", model.Natural64(0))
		body := pipeline.Document{ID: dependent.ID, Type: "issue", Project: "dependent-project", Fields: map[string]any{"status": "todo"}, Links: []pipeline.Link{{To: missing, Kind: "blocks"}}}
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "direct-blockers-test"}}, []conformance.FixtureChange{{Ref: dependent, Kind: "bead", Op: model.OpPatch, Body: encoded}}); err != nil {
			t.Fatal(err)
		}
		meta, err = f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r := directBlockerReader(t, f, &meta.Head)
		result, err := ReadDirectBlockers(ctx, r, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef), Limit: 8})
		if err != nil || len(result.Edges) != 1 || result.Edges[0].State != DirectBlockerUnknown {
			t.Fatalf("missing blocker result=%+v err=%v", result, err)
		}
		if _, err := ReadDirectBlockers(ctx, r, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef), Limit: 1001}); err == nil {
			t.Fatal("unbounded limit unexpectedly accepted")
		}
		_ = blocker
	})
}

func TestReadDirectBlockersPropagatesReaderErrors(t *testing.T) {
	var typed *model.Error
	_, err := ReadDirectBlockers(context.Background(), nil, DirectBlockerRequest{})
	if !errors.As(err, &typed) {
		t.Fatalf("nil reader error=%T %v", err, err)
	}
	readErr := errors.New("pinned reader unavailable")
	_, err = ReadDirectBlockers(context.Background(), failingDirectBlockerReader{err: readErr}, DirectBlockerRequest{
		Dependent: model.RefByID("dependent", model.Natural64(0)),
		Ontology:  StaticOntologySource(model.RefByID("ontology/direct-blockers", model.Natural64(0))),
		Limit:     1,
	})
	if !errors.Is(err, readErr) {
		t.Fatalf("reader error was not preserved: %#v", err)
	}
}
