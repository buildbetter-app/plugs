package memory

import (
	"context"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSelectionReference(t *testing.T) {
	conformance.RunSelection(t, func(*testing.T) driver.Store { return New() })
}

func TestSelectionRefsCanonicalAndRequestLocal(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	// Colons are opaque ID bytes in the bead/closure domain; slashes are not.
	refs := []model.Ref{sourceRef("opaque-a:b"), sourceRef("opaque:a-b"), sourceRef("opaque:a:b")}
	tx, header := beginSourceTest(t, s, lease, refs...)
	for _, ref := range refs {
		if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{}`)}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, header)
	admissionBuild(t, s, lease, startupSortProjection().Families...)
	r := sourceReader(t, s, nil)
	all := driver.SelectionExpr{Op: driver.SelectionTrue}
	refExpr := func(refs []model.Ref) driver.SelectionExpr {
		return driver.SelectionExpr{Op: driver.SelectionRefs, Refs: refs}
	}
	run := func(name string, member, auth driver.SelectionExpr, host *driver.SelectionExpr, want ...string) {
		t.Run(name, func(t *testing.T) {
			p := admissionPlan(t, r)
			p.Mode, p.Page = driver.SelectionPageMode, &driver.SelectionPage{Limit: 10}
			p.Order = []driver.SelectionOrder{{Column: driver.SelectionID}}
			p.Membership, p.Authorization = member, auth
			p = admissionSeal(t, p, host)
			got, err := r.Select(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0, len(got.Keys))
			for _, key := range got.Keys {
				ids = append(ids, key.Ref.ID)
			}
			if got.Total != uint64(len(want)) || !reflect.DeepEqual(ids, append([]string{}, want...)) {
				t.Fatalf("selection ids=%v total=%d, want %v", ids, got.Total, want)
			}
		})
	}
	alias := model.RefByID(refs[0].ID, model.Natural("0.0"))
	run("canonical_duplicates", refExpr([]model.Ref{alias, refs[0], alias, refs[1]}), all, nil, refs[0].ID, refs[1].ID)
	run("nil_refs", refExpr(nil), all, nil)
	run("empty_refs", refExpr([]model.Ref{}), all, nil)
	run("unrestricted", all, all, nil, refs[0].ID, refs[1].ID, refs[2].ID)
	run("split_vectors", driver.SelectionExpr{Op: driver.SelectionAny, Args: []driver.SelectionExpr{refExpr(refs[:2]), refExpr(refs[1:])}}, all, nil, refs[0].ID, refs[1].ID, refs[2].ID)
	// Independent nodes may use overlapping slices with different lengths.
	withoutFirst := driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{refExpr(refs[:1])}}
	run("overlapping_vectors", driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{refExpr(refs), withoutFirst}}, all, nil, refs[1].ID, refs[2].ID)
	host := driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{refExpr(refs[2:])}}
	run("three_expression_planes", refExpr(refs), refExpr(refs[1:]), &host, refs[1].ID)
	mutable := []model.Ref{refs[0]}
	run("first_request", refExpr(mutable), all, nil, refs[0].ID)
	mutable[0] = refs[2]
	run("next_request_rebuilds_refs", refExpr(mutable), all, nil, refs[2].ID)
}
