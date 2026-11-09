package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func selectionOptionalPresence(t *testing.T, factory Factory) {
	ctx, s, lease, _ := setup(t, factory)
	a, b, c := selectionImageOf("a", "issue", "P", "", 0), selectionImageOf("b", "issue", "P", "", 0), selectionImageOf("c", "issue", "P", "", -2)
	a.bead.Absent = []driver.SelectionColumn{driver.SelectionTemplate, driver.SelectionPriority, driver.SelectionTitle}
	c.bead.Template = true
	for _, image := range []*selectionImage{&a, &b, &c} {
		fields := map[string]any{}
		if image.bead.Ref.ID != "a" {
			fields["template"] = image.bead.Template
			fields["priority"] = image.bead.Priority
			fields["title"] = image.bead.Title
		}
		image.body, _ = json.Marshal(map[string]any{"id": image.bead.Ref.ID, "type": "issue", "project": "P", "fields": fields})
	}
	selectionSeed(t, ctx, s, lease, []selectionImage{a, b, c}, nil)
	r, p := selectionOpen(t, ctx, s)
	p.Membership = selectionCompare(driver.SelectionTemplate, "eq", driver.SelectionBool(false))
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "b")
	p.Membership = driver.SelectionExpr{Op: driver.SelectionCompareOp, Compare: &driver.SelectionCompare{Column: driver.SelectionTemplate, Op: "absent"}}
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "a")
	p.Membership = driver.SelectionExpr{Op: driver.SelectionTrue}
	p.Order = []driver.SelectionOrder{{Column: driver.SelectionPriority}}
	p.Page = &driver.SelectionPage{Limit: 1}
	first := selectionSelect(t, ctx, r, p)
	selectionWant(t, first, 3, "c")
	if first.Next == nil {
		t.Fatal("missing numeric keyset")
	}
	p.Page.After = first.Next
	p.Page.Limit = 2
	selectionWant(t, selectionSelect(t, ctx, r, p), 3, "a", "b")
	// Unsupported/null/wrong-type producer data has no annotation, rather
	// than being coerced into the valid present-false representation.
	bad := selectionImageOf("unsupported", "issue", "P", "", 0)
	bad.body = []byte(`{"id":"unsupported","type":"issue","project":"P","fields":{"template":null}}`)
	bad.omit = true
	selectionSeed(t, ctx, s, lease, []selectionImage{bad}, nil)
	current, cp := selectionOpen(t, ctx, s)
	cp.Membership = driver.SelectionExpr{Op: driver.SelectionFalse}
	cp.Page.Limit = 0
	got, err := current.(driver.SelectionReader).Select(ctx, cp)
	if !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("unrelated incomplete coverage hid below false: %+v/%v", got, err)
	}
}

func selectionBoundaryControls(t *testing.T, factory Factory) {
	ctx, s, lease, _ := setup(t, factory)
	a := selectionImageOf("a", "issue", "P", "title", 0)
	selectionSeed(t, ctx, s, lease, []selectionImage{a}, nil)
	view := model.RefByKey("actual-view")
	change := FixtureChange{Ref: view, Kind: "view", Op: model.OpCreate, Body: []byte(`{"fields":[]}`)}
	if _, err := Seed(ctx, s, lease, driver.ChangesetHeader{}, []FixtureChange{change}); err != nil {
		t.Fatal(err)
	}
	r, p := selectionOpen(t, ctx, s)
	key := driver.LockKey{Kind: driver.LockRecord, Ref: &view}
	states, err := r.Observe(ctx, []driver.LockKey{key})
	if err != nil || len(states) != 1 {
		t.Fatalf("view observation %v", err)
	}
	source := driver.SelectionSource{Key: key, Present: states[0].Present, Revision: states[0].Revision, Digest: driver.SelectionDigest(states[0].Record.Body)}
	spec, _ := p.Boundary.Description()
	spec.Sources = append(spec.Sources, source)
	spec.ViewSource = &key
	spec.ViewRevision = source.Revision
	p.Boundary, err = driver.PrepareSelectionBoundary(spec)
	if err != nil {
		t.Fatal(err)
	}
	selectDirect := func(reader driver.Reader, plan driver.SelectionPlan) (driver.SelectionResult, error) {
		return reader.(driver.SelectionReader).Select(ctx, plan)
	}
	if got, err := selectDirect(r, p); err != nil || got.Total != 1 {
		t.Fatalf("bound view %v %+v", err, got)
	}
	for _, bad := range []string{"digest", "presence", "revision"} {
		t.Run(bad, func(t *testing.T) {
			copy, _ := p.Boundary.Description()
			w := &copy.Sources[len(copy.Sources)-1]
			switch bad {
			case "digest":
				w.Digest = driver.SelectionDigest([]byte("other"))
			case "presence":
				w.Present = false
				w.Digest = ""
				copy.ViewSource = nil
				copy.ViewRevision = model.Natural64(0)
			case "revision":
				w.Revision = model.Natural64(0)
				copy.ViewRevision = model.Natural64(0)
				copy.ViewSource = nil
			}
			badPlan := p
			badPlan.Boundary, err = driver.PrepareSelectionBoundary(copy)
			if err != nil {
				t.Fatal(err)
			}
			got, err := selectDirect(r, badPlan)
			if !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
				t.Fatalf("stale source accepted: %+v %v", got, err)
			}
		})
	}
	// A source replacement at the committed token invalidates the old seal by
	// its bytes, independently of source/head revision equality.
	tx, _ := selectionStage(t, ctx, s, lease, nil, []selectionFactWrite{{table: driver.DerivedGrant, key: "source-lock", kind: driver.SelectionGrantFact, source: &source}})
	if err = tx.Put(ctx, driver.Record{Ref: view, Kind: "view", Body: []byte(`{"fields":["id"]}`)}); err != nil {
		t.Fatal(err)
	}
	got, err := selectDirect(tx, p)
	if !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("revision-equal source substitution: %+v %v", got, err)
	}
	_ = tx.Rollback(ctx)
	// Actual miss and tombstone are not interchangeable, including an empty
	// page whose predicate would otherwise return immediately.
	missing := model.RefByKey("missing-control")
	missKey := driver.LockKey{Kind: driver.LockRecord, Ref: &missing}
	missPlan := p
	ms, _ := p.Boundary.Description()
	ms.Sources = append(ms.Sources, driver.SelectionSource{Key: missKey, Revision: model.Natural64(0)})
	missPlan.Boundary, _ = driver.PrepareSelectionBoundary(ms)
	missPlan.Membership = driver.SelectionExpr{Op: driver.SelectionFalse}
	missPlan.Page = &driver.SelectionPage{Limit: 0}
	if _, err = selectDirect(r, missPlan); err != nil {
		t.Fatal(err)
	}
	if _, err = Seed(ctx, s, lease, driver.ChangesetHeader{}, []FixtureChange{{Ref: missing, Kind: "control", Op: model.OpCreate, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err = Seed(ctx, s, lease, driver.ChangesetHeader{}, []FixtureChange{{Ref: missing, Kind: "control", Op: model.OpDelete}}); err != nil {
		t.Fatal(err)
	}
	current, currentPlan := selectionOpen(t, ctx, s)
	ms.AuthorityRevision = currentPlan.AuthorityRevision
	missPlan.AuthorityRevision = currentPlan.AuthorityRevision
	missPlan.ContentRevision = currentPlan.ContentRevision
	missPlan.Boundary, _ = driver.PrepareSelectionBoundary(ms)
	got, err = selectDirect(current, missPlan)
	if !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("miss reused tombstone: %+v %v", got, err)
	}
	obs, err := current.Observe(ctx, []driver.LockKey{missKey})
	if err != nil {
		t.Fatal(err)
	}
	ms.Sources[len(ms.Sources)-1].Revision = obs[0].Revision
	missPlan.Boundary, _ = driver.PrepareSelectionBoundary(ms)
	if got, err = selectDirect(current, missPlan); err != nil || got.Total != 0 || got.More {
		t.Fatalf("honest tombstone refusal %v %+v", err, got)
	}
}

func selectionAtomicAndFresh(t *testing.T, factory Factory) {
	ctx, s, lease, _ := setup(t, factory)
	a, b := selectionImageOf("a", "issue", "P", "old", 0), selectionImageOf("b", "issue", "P", "other", 1)
	selectionSeed(t, ctx, s, lease, []selectionImage{a, b}, nil)
	r, plan := selectionOpen(t, ctx, s)
	plan.Mode = driver.SelectionCountMode
	plan.Page = nil
	beforeHead, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	fresh, ok := beforeHead.(driver.FreshSelectionExistsReader)
	if !ok {
		t.Fatal("missing fresh capability")
	}
	if yes, err := fresh.FreshSelectionExists(ctx, plan); yes || !errors.Is(err, driver.ErrSelectionInvalid) {
		t.Fatalf("fresh before Head=%v/%v", yes, err)
	}
	_ = beforeHead.Close()
	a.deleted, b.deleted = true, true
	created := selectionImageOf("staged-only", "issue", "P", "staged", 2)
	tx, _ := selectionStage(t, ctx, s, lease, []selectionImage{a, b, created}, nil)
	defer tx.Rollback(ctx)
	staged := selectionSelect(t, ctx, tx, selectionPlan(tx.Snapshot()))
	selectionWant(t, staged, 1, "staged-only")
	if yes, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, plan); err != nil || !yes {
		t.Fatalf("staged deletes hid committed eligibility: %v/%v", yes, err)
	}
	stagedOnly := plan
	stagedOnly.Membership = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []model.Ref{created.bead.Ref}}
	if yes, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, stagedOnly); err != nil || yes {
		t.Fatalf("staged create leaked into committed EXISTS: %v/%v", yes, err)
	}
	for _, mutate := range []func(*driver.SelectionPlan){func(p *driver.SelectionPlan) { p.Cap = 1 }, func(p *driver.SelectionPlan) { p.Page = &driver.SelectionPage{Limit: 0} }, func(p *driver.SelectionPlan) {
		p.Mode = driver.SelectionTypeSummary
		p.GroupBy = driver.SelectionGroupType
	}, func(p *driver.SelectionPlan) { p.AuthorityRevision = model.Natural64(0) }} {
		bad := plan
		mutate(&bad)
		if yes, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, bad); yes || err == nil {
			t.Fatalf("invalid fresh plan=%v/%v", yes, err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if yes, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(canceled, plan); yes || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel minted absence=%v/%v", yes, err)
	}
	_ = tx.Rollback(ctx)
	// Whole-call validation precedes deletion or any good addition. An old
	// layout never reinterprets the boolean/presence annotation as version 2.
	a.deleted, b.deleted = false, false
	write, _ := selectionStage(t, ctx, s, lease, []selectionImage{a, b}, nil)
	defer write.Rollback(ctx)
	keyA, keyB := selectionProjectionKey(a.bead.Ref), selectionProjectionKey(b.bead.Ref)
	rows, err := write.DerivedRows(ctx, keyB.Table, keyB.Key)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v %v", rows, err)
	}
	bad := rows[0].Clone()
	bad.Index.Version = 1
	if err = write.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, keyA.Table, []driver.DerivedRow{{Key: keyA.Key}}, []driver.DerivedRow{rows[0], bad}); err == nil {
		t.Fatal("old/malformed annotation accepted")
	}
	selectionWant(t, selectionSelect(t, ctx, write, selectionPlan(write.Snapshot())), 2, "a", "b")
	selectionWant(t, selectionSelect(t, ctx, r, selectionPlan(r.Snapshot())), 2, "a", "b")
}
