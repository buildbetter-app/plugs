package service

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This adapter is only for direct prepared-image semantic comparisons, not
// Writer read-set or commit proof. All source membership is from the concrete
// driver's certified index. The real Writer and phantom controls live in the
// companion integration test and retain pipeline's fresh source observations.
type w8PreparedSnapshot struct {
	driver.Reader
	driver.HierarchyChildrenExcludingReader
	driver.InverseSourceLinksReader
}

func (r *w8PreparedSnapshot) HierarchyClosureRows(ctx context.Context, refs []model.Ref) ([]driver.DerivedRow, error) {
	rows := []driver.DerivedRow{}
	for _, ref := range refs {
		prefix, err := pipeline.AncestorPrefix(ref.ID)
		if err != nil {
			return nil, err
		}
		group, err := r.Reader.DerivedRows(ctx, driver.DerivedAncestorClosure, prefix)
		if err != nil {
			return nil, err
		}
		rows = append(rows, group...)
	}
	return rows, nil
}

func TestIndexedOntologyPreparedFinalOverlayMatchesCompleteGraph(t *testing.T) {
	// Explicit expected outcomes keep agreement between two incomplete paths
	// from counting as proof. Missing endpoints are deliberately prepared
	// images: the real Writer's hierarchy/link planner rejects such dangling
	// writes earlier, so these cases make no Writer admission claim.
	cases := []struct {
		name string
		code model.ErrorCode
	}{
		{"close-blocker-and-dependent", ""},
		{"close-conditional-success-still-blocks", "ontology_guard_failed"},
		{"remove-last-blocker", ""},
		{"introduce-open-blocker", "ontology_guard_failed"},
		{"move-last-child-away", ""},
		{"move-open-child-under-closing-parent", "ontology_guard_failed"},
		{"close-last-child-and-parent", ""},
		{"missing-blocker", "ontology_guard_failed"},
		{"missing-ancestor", "ontology_guard_failed"},
		{"waits-for-final-any-children", ""},
		{"waits-for-final-all-children", "ontology_guard_failed"},
		{"waits-for-final-also-blocks", "ontology_guard_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
				ctx := context.Background()
				parent := w8Create(t, f, "parent", nil, "")
				child := w8Create(t, f, "child", &parent, "")
				other := w8Create(t, f, "other", nil, "")
				blocker := w8Create(t, f, "blocker", nil, "")
				target := w8Create(t, f, "dependent", nil, "")
				if tc.name == "close-blocker-and-dependent" || tc.name == "remove-last-blocker" {
					w8Link(t, f, target, blocker, "blocks", nil)
				}
				if tc.name == "close-conditional-success-still-blocks" {
					w8Link(t, f, target, blocker, "conditional-blocks", nil)
				}
				if tc.name == "waits-for-final-any-children" || tc.name == "waits-for-final-all-children" || tc.name == "waits-for-final-also-blocks" {
					terminal := w8Create(t, f, "terminal sibling", &parent, "")
					w8Slot(t, f, terminal, "bead.close", "")
					w8Link(t, f, target, parent, "waits-for", &model.LinkMetadata{Gate: "all-children"})
				}
				key := meta(t)
				key.Method = "tx.apply"
				before := w8Capture(t, f, key)
				raw := f.reader(t)
				defer raw.Close()
				hierarchy, ok := raw.(driver.HierarchyChildrenExcludingReader)
				if !ok {
					t.Fatal("real driver lacks canonical prepared-source exclusions")
				}
				inverse, ok := raw.(driver.InverseSourceLinksReader)
				if !ok {
					t.Fatal("real driver lacks inverse source links")
				}
				reader := &w8PreparedSnapshot{Reader: raw, HierarchyChildrenExcludingReader: hierarchy, InverseSourceLinksReader: inverse}
				p := port.ProposedChange{Principal: anonymous, BaseRevision: *raw.Snapshot().At, Meta: key}
				change := func(ref model.Ref, kind model.OpKind, edit func(*pipeline.Document)) port.Change {
					t.Helper()
					rows, err := raw.Get(ctx, []model.Ref{ref})
					if err != nil || len(rows) != 1 {
						t.Fatalf("prepared source %s: %v", ref.ID, err)
					}
					doc, err := pipeline.DecodeDocument(rows[0].Body)
					if err != nil {
						t.Fatal(err)
					}
					op := model.Op{Kind: kind}
					if kind == model.OpClose {
						doc.Fields["status"], _ = f.svc.ontology.effective(doc.Project).SlotStatus(doc.Type, ontology.Close)
						op.Close = &model.CloseOp{Target: ref}
					}
					if edit != nil {
						edit(&doc)
					}
					return port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: ref}, Op: op, Before: rows[0].Body, After: mustJSON(t, doc)}
				}
				switch tc.name {
				case "close-blocker-and-dependent", "close-conditional-success-still-blocks":
					// Deliberately put the dependent first: the guard must use
					// every final image, rather than the request prefix.
					p.Changes = []port.Change{change(target, model.OpClose, nil), change(blocker, model.OpClose, nil)}
				case "remove-last-blocker":
					p.Changes = []port.Change{change(target, model.OpClose, func(doc *pipeline.Document) { doc.Links = nil })}
				case "introduce-open-blocker":
					p.Changes = []port.Change{change(target, model.OpClose, func(doc *pipeline.Document) { doc.Links = []pipeline.Link{{Kind: "blocks", To: blocker}} })}
				case "move-last-child-away":
					p.Changes = []port.Change{change(parent, model.OpClose, nil), change(child, model.OpReparent, func(doc *pipeline.Document) { doc.Parent = &other })}
				case "move-open-child-under-closing-parent":
					p.Changes = []port.Change{change(other, model.OpClose, nil), change(child, model.OpReparent, func(doc *pipeline.Document) { doc.Parent = &other })}
				case "close-last-child-and-parent":
					p.Changes = []port.Change{change(parent, model.OpClose, nil), change(child, model.OpClose, nil)}
				case "missing-blocker":
					p.Changes = []port.Change{change(target, model.OpClose, func(doc *pipeline.Document) {
						doc.Links = []pipeline.Link{{Kind: "blocks", To: model.RefByID("00000000-0000-4000-8000-999999999999", model.Natural64(0))}}
					})}
				case "missing-ancestor":
					p.Meta.Method = "bead.claim"
					p.Changes = []port.Change{change(target, model.OpPatch, func(doc *pipeline.Document) {
						doc.Fields["status"], _ = f.svc.ontology.effective(doc.Project).SlotStatus(doc.Type, ontology.Claim)
						ref := model.RefByID("00000000-0000-4000-8000-999999999999", model.Natural64(0))
						doc.Parent = &ref
					})}
				case "waits-for-final-any-children", "waits-for-final-all-children", "waits-for-final-also-blocks":
					p.Changes = []port.Change{change(target, model.OpClose, func(doc *pipeline.Document) {
						gate := "all-children"
						if tc.name != "waits-for-final-all-children" {
							gate = "any-children"
						}
						doc.Links[0].Metadata = &model.LinkMetadata{Gate: gate, AlsoBlocks: tc.name == "waits-for-final-also-blocks"}
					})}
				}
				o := &w8DifferentialOntology{Ontology: f.svc.ontology, t: t, store: f.store}
				err := o.Validate(ctx, p, reader)
				if tc.code == "" {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					wantCode(t, err, tc.code)
				}
				if o.calls != 1 || o.queries != 0 {
					t.Fatal("prepared final-overlay comparison did not use the certified indexed path")
				}
				w8SameState(t, before, w8Capture(t, f, key))
			})
		})
	}
}
