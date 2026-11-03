// Package sourceactivation exercises real source publication and lifecycle
// validation after the ontology-only publication used by configured servers.
package sourceactivation

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// OntologyFirst never initializes or backfills an index in the fixture. The
// ordinary driver publications must establish coverage themselves.
func OntologyFirst(t *testing.T, store driver.Store, lease driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil && renewCtx.Err() == nil {
					done <- err
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref("ontology/global"), Kind: "ontology", Op: model.OpCreate, Body: []byte(`{"version":1}`)}}); err != nil {
		t.Fatal(err)
	}
	parent := ref("pl-parent")
	changes := make([]conformance.FixtureChange, 20000)
	for i := range changes {
		id := fmt.Sprintf("pl-source-%05d", i)
		doc := pipeline.Document{ID: id, Type: "issue", Fields: map[string]any{"status": "todo", "title": "source activation regression"}}
		if i == 0 {
			doc.ID = parent.ID
		} else if i <= 1000 {
			doc.Parent = &parent
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		changes[i] = conformance.FixtureChange{Ref: ref(doc.ID), Kind: "bead", Op: model.OpCreate, Body: body, Graph: &conformance.FixtureGraphSource{Parent: doc.Parent}}
	}
	// Use bounded publications, as the server does; one giant SQL publication
	// would hold the final lease row fence across all 20,000 images.
	for first := 0; first < len(changes); first += 100 {
		if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, changes[first:first+100]); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	for name, lookup := range map[string]func() ([]driver.Record, bool, error){
		"parent": func() ([]driver.Record, bool, error) {
			return r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, []model.Ref{parent}, 1)
		},
		"links": func() ([]driver.Record, bool, error) {
			return r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, []model.Ref{parent}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 1)
		},
	} {
		rows, more, err := lookup()
		if err != nil || name == "parent" && (len(rows) != 1 || !more) || name == "links" && (len(rows) != 0 || more) {
			t.Errorf("%s coverage after ontology-first bootstrap: rows=%d more=%t err=%v", name, len(rows), more, err)
		}
	}
	_ = r.Close()

	// Count the real fallback instead of making a timing-only assertion. Every
	// underlying read/write and both source capabilities pass through unchanged.
	observed := &observedStore{Store: store}
	w := pipeline.New(observed, lease, pipeline.WithOntology(service.NewOntology(nil)))
	started := time.Now()
	commit, err := w.Apply(ctx, model.PrincipalRef{Kind: "system", ID: "source-activation"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpClose, Close: &model.CloseOp{Target: changes[1].Ref}}}}, model.WriteMeta{Method: "bead.close", IdempotencyKey: "a5d1e7f024ab41c0b2ae124aa0098765"})
	if err != nil || commit.Revision.String() != "202" {
		t.Errorf("lifecycle close under 1000-child parent: commit=%+v err=%v", commit, err)
	}
	if observed.queries != 0 || observed.parentReads == 0 || observed.inverseReads == 0 {
		t.Errorf("lifecycle used whole-graph fallback: queries=%d parent=%d inverse=%d", observed.queries, observed.parentReads, observed.inverseReads)
	}
	t.Logf("20000 beads, 1000-child parent: close=%s whole-graph queries=%d parent reads=%d inverse reads=%d; production two-second pre-phase deadline unchanged", time.Since(started), observed.queries, observed.parentReads, observed.inverseReads)
}

type observedStore struct {
	driver.Store
	queries, parentReads, inverseReads int
}

func (s *observedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &observedTx{WriteTx: tx, counts: s}, nil
}

type observedTx struct {
	driver.WriteTx
	counts *observedStore
}

func (t *observedTx) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	if q.Filter["kind"] == "bead" {
		t.counts.queries++
	}
	return t.WriteTx.Query(ctx, q)
}
func (t *observedTx) HierarchyChildren(ctx context.Context, refs []model.Ref, limit int) ([]driver.Record, bool, error) {
	return t.HierarchyChildrenExcluding(ctx, refs, nil, limit)
}
func (t *observedTx) HierarchyChildrenExcluding(ctx context.Context, refs, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	t.counts.parentReads++
	return t.WriteTx.(driver.HierarchyChildrenExcludingReader).HierarchyChildrenExcluding(ctx, refs, excluded, limit)
}
func (t *observedTx) InverseSourceLinks(ctx context.Context, refs []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	t.counts.inverseReads++
	return t.WriteTx.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, refs, kinds, excluded, limit)
}
func (t *observedTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	return t.WriteTx.(driver.GraphSourceWriter).PutGraphRecord(ctx, record, parent, links)
}
