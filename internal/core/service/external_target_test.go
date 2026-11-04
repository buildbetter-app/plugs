package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type noExternalLookup struct {
	driver.Reader
	external string
}

func (r noExternalLookup) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	for _, ref := range refs {
		if ref.ID == r.external {
			return nil, fmt.Errorf("external identity was used as a local lookup")
		}
	}
	return r.Reader.Get(ctx, refs)
}

func TestExternalDirectBlockerNeverLoadsCollisionOrSuppressesLocalEdge(t *testing.T) {
	directBlockerFixtures(t, func(t *testing.T, f *directBlockerFixture) {
		ctx := context.Background()
		ontologyRef := seedDirectBlockerOntology(t, f, OntologySource{Base: ontology.DefaultDocument()})
		create := func(title string) model.Ref {
			t.Helper()
			commit, err := f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": title}}, directBlockerMeta(t))
			if err != nil {
				t.Fatal(err)
			}
			return commit.Results[0].Ref
		}
		blocker := create("local collision")
		if _, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: blocker}, directBlockerMeta(t)); err != nil {
			t.Fatal(err)
		}
		for _, externalFirst := range []bool{true, false} {
			dependent := create("dependent")
			kinds := []model.LinkTargetKind{"", model.LinkTargetExternal}
			if externalFirst {
				kinds[0], kinds[1] = kinds[1], kinds[0]
			}
			for _, targetKind := range kinds {
				if _, err := f.svc.Link(ctx, anonymous, LinkRequest{From: dependent, To: blocker, Kind: "blocks", TargetKind: targetKind}, directBlockerMeta(t)); err != nil {
					t.Fatal(err)
				}
			}
			verdict, err := ReadDirectBlockers(ctx, directBlockerReader(t, f, nil), DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef)})
			if err != nil || len(verdict.Edges) != 2 {
				t.Fatalf("verdict=%+v err=%v", verdict, err)
			}
			for _, edge := range verdict.Edges {
				want := DirectBlockerSatisfied
				if edge.TargetKind == model.LinkTargetExternal {
					want = DirectBlockerUnknown
				}
				if edge.State != want {
					t.Fatalf("externalFirst=%v edge=%+v want=%v", externalFirst, edge, want)
				}
			}
		}
		dependent := create("external only")
		if _, err := f.svc.Link(ctx, anonymous, LinkRequest{From: dependent, To: blocker, Kind: "waits-for", TargetKind: model.LinkTargetExternal}, directBlockerMeta(t)); err != nil {
			t.Fatal(err)
		}
		verdict, err := ReadDirectBlockers(ctx, noExternalLookup{Reader: directBlockerReader(t, f, nil), external: blocker.ID}, DirectBlockerRequest{Dependent: dependent, Ontology: StaticOntologySource(ontologyRef)})
		if err != nil || len(verdict.Edges) != 1 || verdict.Edges[0].State != DirectBlockerUnknown || verdict.Edges[0].TargetKind != model.LinkTargetExternal {
			t.Fatalf("external lookup isolation=%+v err=%v", verdict, err)
		}
	})
}
