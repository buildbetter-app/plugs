package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type sourceIndexResultStore struct {
	driver.Store
	result error
}

func (s sourceIndexResultStore) BuildGraphSourceIndexes(context.Context, driver.ActiveLease, driver.GraphSourceDecoder, ...driver.GraphSourceProjection) error {
	return s.result
}

func TestSourceIndexStartupRequiresDirectGraphCompletion(t *testing.T) {
	store := memory.New()
	defer store.Close()
	safe := driver.NewSelectionBackfillRefused(fmt.Errorf("selection: %w", driver.ErrSelectionUnavailable))
	for _, test := range []struct {
		name     string
		err      error
		optional bool
	}{
		{"success", nil, true},
		{"completed_unavailable", safe, true},
		{"completed_unsupported", driver.NewSelectionBackfillRefused(driver.ErrUnsupported), true},
		{"raw_graph_unavailable", driver.ErrSelectionUnavailable, false},
		{"raw_graph_unsupported", driver.ErrUnsupported, false},
		{"mixed_cancellation", errors.Join(driver.ErrSelectionUnavailable, context.Canceled), false},
		{"mixed_fence", errors.Join(driver.ErrSelectionUnavailable, driver.ErrFenced{}), false},
		{"wrapped_completion", fmt.Errorf("unconfirmed outer operation: %w", safe), false},
		{"joined_completion", errors.Join(safe, errors.New("SQL cleanup failed")), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := BuildSourceIndexes(context.Background(), sourceIndexResultStore{Store: store, result: test.err}, nil)
			if test.optional {
				if err != nil {
					t.Fatalf("completed graph lost optional fallback: %v", err)
				}
			} else if err != test.err {
				t.Fatalf("public startup suppressed or changed a fatal result: want=%v got=%v", test.err, err)
			}
		})
	}
}

func TestSourceIndexStartupRejectsMismatchedCoreDocument(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "invalid-legacy-source", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	ref := model.RefByID("pl-stored", model.Natural64(0))
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: []byte(`{"id":"pl-not-the-record","type":"issue","fields":{}}`)}}); err != nil {
		t.Fatal(err)
	}
	before, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := BuildSourceIndexes(ctx, store, lease); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("malformed legacy document was certified: %v", err)
	}
	after, err := store.Meta(ctx)
	if err != nil || before != after {
		t.Fatalf("failed activation changed metadata: before=%+v after=%+v err=%v", before, after, err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: after.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if rows, more, err := r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("failed core decode published parent coverage: %v", err)
	}
	if rows, more, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, nil, nil, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("failed core decode published link coverage: %v", err)
	}
}

// Startup membership activation is intentionally not a semantic certificate.
// Invalid components can be fully indexed; the next hierarchy write must still
// use complete graph validation rather than treating empty inverse results as
// proof that an unrelated restored component is valid.
func TestSourceIndexStartupDoesNotCertifyInvalidGraph(t *testing.T) {
	for _, malformed := range []string{"blocking-cycle", "dangling-custom-link"} {
		t.Run(malformed, func(t *testing.T) {
			ctx := context.Background()
			store := memory.New()
			defer store.Close()
			lease, err := store.Lease(ctx, "invalid-legacy-graph", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			u := Document{ID: "pl-u", Type: "issue", Fields: map[string]any{}, Links: []Link{{Kind: "blocks", To: model.Ref{ID: "pl-v"}}}}
			v := Document{ID: "pl-v", Type: "issue", Fields: map[string]any{}, Links: []Link{{Kind: "blocks", To: model.Ref{ID: "pl-u"}}}}
			if malformed == "dangling-custom-link" {
				u.Links = []Link{{Kind: "custom-link", To: model.Ref{ID: "pl-missing"}}}
				v.Links = nil
			}
			changes := make([]conformance.FixtureChange, 0, 2)
			for _, doc := range []Document{u, v} {
				body, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				changes = append(changes, conformance.FixtureChange{Ref: model.Ref{ID: doc.ID}, Kind: "bead", Op: model.OpCreate, Body: body})
			}
			if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, changes); err != nil {
				t.Fatal(err)
			}
			if err := BuildSourceIndexes(ctx, store, lease); err != nil {
				t.Fatal(err)
			}
			meta, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if _, _, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, nil, nil, nil, 0); err != nil {
				t.Fatalf("test did not activate typed membership coverage: %v", err)
			}
			states, err := r.Observe(ctx, []driver.LockKey{GraphValidityLockKey()})
			if err != nil || len(states) != 1 || states[0].Present {
				t.Fatalf("membership-only startup certified a malformed graph: states=%+v err=%v", states, err)
			}
		})
	}
}

func TestGraphValidityCertificateMaintainedByValidatedWrites(t *testing.T) {
	h := setup(t)
	w := h.writer()
	created := apply(t, w, model.ChangeSet{Ops: []model.Op{create("parent"), create("child")}}, meta(200))
	parent, child := created.Results[0].Ref, created.Results[1].Ref
	assertCertificate := func(want bool) {
		t.Helper()
		r := h.read(t)
		states, err := r.Observe(context.Background(), []driver.LockKey{GraphValidityLockKey(), driver.GraphBlockingSentinelLock})
		if err != nil || len(states) != 2 {
			t.Fatalf("certificate observation=%+v err=%v", states, err)
		}
		current := states[0].Present && states[1].Present && states[0].Revision.String() == states[1].Revision.String()
		if current != want {
			t.Fatalf("current certificate=%t want=%t states=%+v", current, want, states)
		}
	}
	assertCertificate(false)
	apply(t, w, model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: child, Parent: &parent}}}}, meta(201))
	assertCertificate(true)
	apply(t, w, model.ChangeSet{Ops: []model.Op{patch(parent, map[string]any{"title": "updated"})}}, meta(202))
	assertCertificate(true)
	apply(t, w, model.ChangeSet{Ops: []model.Op{create("new-root")}}, meta(203))
	assertCertificate(true)
	apply(t, w, model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: child}}}}, meta(204))
	assertCertificate(true)
	// A trusted typed publication may carry complete membership for an invalid
	// image. It bumps graph/blocking but not the semantic certificate.
	invalid := model.Ref{ID: "pl-unrelated-invalid"}
	body, err := json.Marshal(Document{ID: invalid.ID, Type: "issue", Project: "test", Fields: map[string]any{}, Links: []Link{{Kind: "custom-link", To: model.Ref{ID: "pl-missing"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(context.Background(), h.store, h.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
		Ref: invalid, Kind: "bead", Op: model.OpCreate, Body: body,
		Graph: &conformance.FixtureGraphSource{Links: []driver.SourceLink{{Kind: "custom-link", To: model.Ref{ID: "pl-missing"}}}},
	}}); err != nil {
		t.Fatal(err)
	}
	assertCertificate(false)
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: child, Parent: &parent}}}}, meta(205))
	code(t, err, "ontology_guard_failed")
	assertCertificate(false)
	// A field-only write may preserve that unvalidated graph, but cannot turn
	// it into a certified one merely because its own sources have no links.
	apply(t, w, model.ChangeSet{Ops: []model.Op{patch(child, map[string]any{"title": "unvalidated"})}}, meta(206))
	assertCertificate(false)

}

// Field-only writes preserve any valid graph, not just the pre-read graph.
// An unrelated committed root create between preparation and Lock must not
// consume a stale-snapshot retry or unnecessarily invalidate the certificate.
func TestGraphCertificatePreservationUsesSerializedGraph(t *testing.T) {
	h := setup(t)
	other := h.writer()
	created := apply(t, other, model.ChangeSet{Ops: []model.Op{create("parent"), create("child")}}, meta(300))
	parent, child := created.Results[0].Ref, created.Results[1].Ref
	apply(t, other, model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: child, Parent: &parent}}}}, meta(301))
	store := &certificateInterferenceStore{Store: h.store}
	store.interfere = func() {
		apply(t, other, model.ChangeSet{Ops: []model.Op{create("unrelated")}}, meta(310+store.writes))
	}
	writer := New(store, h.lease, WithOntology(testOntology{}), WithClock(func() time.Time { return h.now }))
	apply(t, writer, model.ChangeSet{Ops: []model.Op{patch(parent, map[string]any{"title": "serialized"})}}, meta(302))
	if store.writes != 1 {
		t.Fatalf("source-preserving write retried on an unrelated graph change: attempts=%d", store.writes)
	}
	states, err := h.read(t).Observe(context.Background(), []driver.LockKey{GraphValidityLockKey(), driver.GraphBlockingSentinelLock})
	if err != nil || len(states) != 2 || !currentGraphCertificate(states) {
		t.Fatalf("serialized write lost the current semantic certificate: %+v err=%v", states, err)
	}
}

type certificateInterferenceStore struct {
	driver.Store
	interfere func()
	writes    int
}

func (s *certificateInterferenceStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	s.interfere()
	return s.Store.Write(ctx, lease)
}
