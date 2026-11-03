package service

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type fanoutReader struct {
	driver.Reader
	queries int
	failAt  int
	failure error
	empty   bool
}

func (r *fanoutReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	r.queries++
	if r.failAt == r.queries {
		if r.empty {
			return driver.Page[driver.Record]{NextCursor: &driver.Cursor{}}, nil
		}
		return driver.Page[driver.Record]{}, r.failure
	}
	query.Limit = 1 // Exercise a genuine driver's continuation, not a seeded page.
	return r.Reader.Query(ctx, query)
}

func TestReadDirectWaitsForPinsMetadataAndCompleteFanout(t *testing.T) {
	directBlockerFixtures(t, func(t *testing.T, f *directBlockerFixture) {
		ctx := context.Background()
		ontologyRef := seedDirectBlockerOntology(t, f, OntologySource{Base: ontology.DefaultDocument()})
		create := func(title string, parent *model.Ref, project string) model.Ref {
			t.Helper()
			meta := directBlockerMeta(t)
			meta.Project = project
			commit, err := f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Parent: parent, Fields: map[string]any{"title": title}}, meta)
			if err != nil {
				t.Fatal(err)
			}
			return commit.Results[0].Ref
		}
		spawner := create("spawner", nil, "blocker-project")
		waiter := create("waiter", nil, "dependent-project")
		if _, err := f.svc.Link(ctx, anonymous, LinkRequest{From: waiter, To: spawner, Kind: "waits-for", Metadata: &model.LinkMetadata{Gate: "any-children"}}, directBlockerMeta(t)); err != nil {
			t.Fatal(err)
		}
		emptyMeta, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		empty := directBlockerReader(t, f, &emptyMeta.Head)
		child := create("child", &spawner, "child-project")
		openMeta, err := f.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		open := directBlockerReader(t, f, &openMeta.Head)
		meta := directBlockerMeta(t)
		meta.Project = "child-project"
		if _, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: child}, meta); err != nil {
			t.Fatal(err)
		}
		current := directBlockerReader(t, f, nil)
		request := DirectBlockerRequest{Dependent: waiter, Ontology: StaticOntologySource(ontologyRef)}
		for _, test := range []struct {
			name   string
			reader driver.Reader
			want   DirectBlockerState
		}{{"old empty", empty, DirectBlockerSatisfied}, {"old open child", open, DirectBlockerUnsatisfied}, {"current terminal child", current, DirectBlockerSatisfied}} {
			t.Run(test.name, func(t *testing.T) {
				paged := &fanoutReader{Reader: test.reader}
				verdict, err := ReadDirectBlockers(ctx, paged, request)
				if err != nil || len(verdict.Edges) != 1 || verdict.Edges[0].State != test.want || paged.queries < 2 {
					t.Fatalf("pinned fanout=%+v err=%v queries=%d want=%s", verdict, err, paged.queries, test.want)
				}
			})
		}
		failure := errors.New("second fanout page unavailable")
		_, err = ReadDirectBlockers(ctx, &fanoutReader{Reader: current, failAt: 2, failure: failure}, request)
		if !errors.Is(err, failure) {
			t.Fatalf("fanout page failure became a verdict: %v", err)
		}
		_, err = ReadDirectBlockers(ctx, &fanoutReader{Reader: current, failAt: 2, empty: true}, request)
		wantCode(t, err, "internal_error")
		missingChildSource := request
		missingChildSource.Ontology = func(project string) (model.Ref, error) {
			if project == "child-project" {
				return model.RefByID("ontology/missing-child", model.Natural64(0)), nil
			}
			return ontologyRef, nil
		}
		verdict, err := ReadDirectBlockers(ctx, current, missingChildSource)
		if err != nil || len(verdict.Edges) != 1 || verdict.Edges[0].State != DirectBlockerUnknown {
			t.Fatalf("missing child ontology manufactured a terminal witness: %+v %v", verdict, err)
		}
		if _, err := f.svc.Link(ctx, anonymous, LinkRequest{From: waiter, To: spawner, Kind: "waits-for", Metadata: &model.LinkMetadata{Gate: "all-children", AlsoBlocks: true}}, directBlockerMeta(t)); err != nil {
			t.Fatal(err)
		}
		changed := directBlockerReader(t, f, nil)
		verdict, err = ReadDirectBlockers(ctx, changed, request)
		if err != nil || len(verdict.Edges) != 1 || verdict.Edges[0].State != DirectBlockerUnsatisfied {
			t.Fatalf("current metadata was ignored: %+v %v", verdict, err)
		}
		verdict, err = ReadDirectBlockers(ctx, current, request)
		if err != nil || len(verdict.Edges) != 1 || verdict.Edges[0].State != DirectBlockerSatisfied {
			t.Fatalf("pinned metadata drifted: %+v %v", verdict, err)
		}
	})
}
