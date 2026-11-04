package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	modernsqlite "modernc.org/sqlite"
)

func selectionStore(t *testing.T) driver.Store {
	s, err := (Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "selection.db")})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSelectionConformance(t *testing.T) {
	conformance.RunSelection(t, selectionStore)
}

func TestSelectionUntypedWriteRefusal(t *testing.T) {
	conformance.RunSelectionRefusal(t, func(t *testing.T) driver.Store {
		s, err := (Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "selection.db")})
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestSelectionOfflineRestoreNeedsExplicitCoverageBuild(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	ref := testRef("selection-restored")
	key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "selection-restored"}
	lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
	tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{lock}, false, ref)
	meta, err := tx.PrepareOfflineRestore(ctx, "selection-import-source")
	if err != nil {
		t.Fatal(err)
	}
	bead, err := driver.NewBeadSentinel(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking, bead}); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"selection-restored","type":"task"}`)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
		t.Fatal(err)
	}
	value := []byte(`{"typed":true}`)
	row := driver.DerivedRow{Key: key.Key, Value: value, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: key, Incarnation: meta.Incarnation,
		Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
		Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref},
			Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)},
		Bead: &driver.SelectionBead{Ref: ref, Type: "task"},
	}}
	if err := tx.PutSelectionDerived(ctx, key.Table, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	header.Incarnation = meta.Incarnation
	header.Kind = model.ChangesetRestore
	header.Method = "offline.restore"
	header.Principal = model.PrincipalRef{Kind: "system", ID: "plug-converter"}
	header.IdempotencyKey = "selection-offline-restore"
	header.RequestHash = strings.Repeat("a", 64)
	commitSourceTest(t, tx, header)
	family := sqliteSelectionFamilies()[0]
	ready, err := selectionFamilyReady(ctx, s.db, meta.Incarnation, family, revKey(1))
	if err != nil || ready {
		t.Fatalf("restore certified virgin family coverage: ready=%t err=%v", ready, err)
	}
	control := testRef("selection-restore-control")
	plan := driver.SelectionPlan{Incarnation: meta.Incarnation, Branch: model.Natural64(0),
		ContentRevision: model.Natural64(1), AuthorityRevision: model.Natural64(1),
		Membership:    driver.SelectionExpr{Op: driver.SelectionTrue},
		Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionCountMode}
	plan.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{
		Version: 1, PreparedBinding: driver.SelectionDigest([]byte("restore-coverage-control")),
		Incarnation: meta.Incarnation, Branch: model.Natural64(0), AuthorityRevision: model.Natural64(1),
		Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control},
			Revision: model.Natural64(0)}},
		Chain: driver.SelectionChainNone, Authorization: plan.Authorization, Host: driver.SelectionHostAbsent,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := sourceReader(t, s, nil)
	_, err = before.Select(ctx, plan)
	_ = before.Close()
	if !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("restored projection admitted before explicit coverage build: %v", err)
	}
	decode, project := selectionBackfillRaceProjection(meta.Incarnation, nil)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	ready, err = selectionFamilyReady(ctx, s.db, meta.Incarnation, family, revKey(1))
	if err != nil || !ready {
		t.Fatalf("explicit restore coverage build: ready=%t err=%v", ready, err)
	}
	after := sourceReader(t, s, nil)
	defer after.Close()
	got, err := after.Select(ctx, plan)
	if err != nil || got.Total != 1 {
		t.Fatalf("explicit restore coverage read: %+v %v", got, err)
	}
}

// A real late SQL abort happens after the commit-local source revision has
// been rebound. The still-open writer must retain its observed precommit
// annotation and be able to select and retry the same complete publication.
func TestSelectionLatePublicationAbortKeepsStagedSourceToken(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	// Explicit complete empty startup certifies token zero. Open itself leaves
	// every family unbuilt; the staged post-abort read needs this real floor.
	decode, project := selectionBackfillRaceProjection(getMeta(t, ctx, s).Incarnation, nil)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	ref := testRef("selection-late-abort")
	projection := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "selection-late-abort"}
	lock := driver.LockKey{Kind: driver.LockDerived, Derived: &projection}
	tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{lock}, true, ref)
	body := []byte(`{"id":"selection-late-abort","type":"task","fields":{"title":"before"}}`)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
		t.Fatal(err)
	}
	bead := driver.SelectionBead{Ref: ref, Type: "task", Title: "before"}
	value := []byte(`{"title":"before"}`)
	row := driver.DerivedRow{Key: projection.Key, Value: value, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: projection, Incarnation: header.Incarnation,
		Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
		Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref},
			Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)},
		Bead: &bead,
	}}
	if err := tx.PutSelectionDerived(ctx, projection.Table, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	header.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)}}
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_typed_projection BEFORE INSERT ON selection_bead_history BEGIN SELECT RAISE(ABORT,'injected selection publication failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, header); err == nil || !strings.Contains(err.Error(), "injected selection publication failure") {
		t.Fatalf("expected late SQL abort, got %v", err)
	}
	if got := tx.derived[keyID(lock)].Derived.Index.Source.Revision; !got.IsZero() {
		t.Fatalf("aborted publication retained future source revision %s", got)
	}
	control := testRef("selection-absent-control")
	plan := driver.SelectionPlan{Incarnation: header.Incarnation, Branch: model.Natural64(0),
		ContentRevision: model.Natural64(0), AuthorityRevision: model.Natural64(0),
		Membership: driver.SelectionExpr{Op: driver.SelectionTrue}, Authorization: driver.SelectionExpr{Op: driver.SelectionTrue},
		Mode: driver.SelectionCountMode}
	var err error
	plan.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{
		Version: 1, PreparedBinding: driver.SelectionDigest([]byte("late-abort-control")),
		Incarnation: header.Incarnation, Branch: model.Natural64(0), AuthorityRevision: model.Natural64(0),
		Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}},
		Chain:   driver.SelectionChainNone, Authorization: plan.Authorization, Host: driver.SelectionHostAbsent,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := tx.Select(ctx, plan)
	if err != nil || got.Total != 1 {
		t.Fatalf("staged read after abort: %+v %v", got, err)
	}
	if _, err := s.db.ExecContext(ctx, `DROP TRIGGER reject_typed_projection`); err != nil {
		t.Fatal(err)
	}
	entry, err := tx.Commit(ctx, header)
	if err != nil || entry.Revision.String() != "1" {
		t.Fatalf("retry publication: %+v %v", entry, err)
	}
}

// The production typed-writer path may bind facts directly to a bead record.
// Count and empty-fields summary must validate that source without reading its
// large body (or decoding the complete derived-row history) before selection.
func TestSelectionRecordSourcedFactCountAndSummaryStayThin(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	ref := testRef("selection-fact-source")
	sortKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "selection-fact-source"}
	factKey := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "selection-fact-source-labels"}
	closure, err := driver.NewSentinel("closure/selection-thin-fact")
	if err != nil {
		t.Fatal(err)
	}
	extra := []driver.LockKey{{Kind: driver.LockDerived, Derived: &sortKey}, {Kind: driver.LockDerived, Derived: &factKey}, {Kind: driver.LockSentinel, Sentinel: closure}}
	tx, header := beginSourceTestWithLocks(t, s, lease, extra, true, ref)
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{closure}); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"selection-fact-source","type":"issue","fields":{"title":"` + strings.Repeat("x", 1<<18) + `"}}`)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
		t.Fatal(err)
	}
	source := driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)}
	bead := driver.SelectionBead{Ref: ref, Type: "issue"}
	sortValue := []byte(`{"type":"issue"}`)
	sortRow := driver.DerivedRow{Key: sortKey.Key, Value: sortValue, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: sortKey, Incarnation: header.Incarnation, Branch: model.Natural64(0),
		ValueDigest: driver.SelectionDigest(sortValue), Source: source, Bead: &bead,
	}}
	if err := tx.PutSelectionDerived(ctx, sortKey.Table, nil, []driver.DerivedRow{sortRow}); err != nil {
		t.Fatal(err)
	}
	set := driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionLabelFact, Ref: &ref, Name: "color", Value: "blue"}}}
	factValue := []byte(`{"color":"blue"}`)
	factRow := driver.DerivedRow{Key: factKey.Key, Value: factValue, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: factKey, Incarnation: header.Incarnation, Branch: model.Natural64(0),
		ValueDigest: driver.SelectionDigest(factValue), Source: source, Facts: &set,
	}}
	if err := tx.PutSelectionDerived(ctx, factKey.Table, nil, []driver.DerivedRow{factRow}); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, header)
	committed := getMeta(t, ctx, s)
	for _, family := range sqliteSelectionFamilies() {
		ready, err := selectionFamilyReady(ctx, s.db, committed.Incarnation, family, revKey(mustUint(committed.Head)))
		if err != nil {
			t.Fatal(err)
		}
		want := family.Kind == "" || family.Kind == driver.SelectionLabelFact
		if ready != want {
			t.Fatalf("independent family %s/%s ready=%t want=%t", family.Table, family.Kind, ready, want)
		}
	}
	r := sourceReader(t, s, nil)
	control := testRef("selection-thin-control")
	label := driver.SelectionString("color")
	p := driver.SelectionPlan{Incarnation: header.Incarnation, Branch: model.Natural64(0),
		ContentRevision: r.meta.Head, AuthorityRevision: r.meta.Head,
		Membership: driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{
			Row: factKey, Kind: driver.SelectionLabelFact,
			Tests: []driver.SelectionFactTest{{Column: driver.SelectionFactName, Value: &label}},
		}},
		Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionCountMode}
	p.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{
		Version: 1, PreparedBinding: driver.SelectionDigest([]byte("selection-thin-fact-control")),
		Incarnation: header.Incarnation, Branch: model.Natural64(0), AuthorityRevision: r.meta.Head,
		Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}},
		Chain:   driver.SelectionChainNone, Authorization: p.Authorization, Host: driver.SelectionHostAbsent,
	})
	if err != nil {
		t.Fatal(err)
	}
	work := &totalOnlySourceWorkObserver{total: map[driver.ReadWorkKind]int64{}}
	measured := driver.WithReadWorkObserver(ctx, work)
	count, err := r.Select(measured, p)
	if err != nil || count.Total != 1 || len(count.Keys) != 0 {
		t.Fatalf("record-sourced count: %+v %v", count, err)
	}
	p.Mode, p.GroupBy = driver.SelectionTypeSummary, driver.SelectionGroupType
	summary, err := r.Select(measured, p)
	if err != nil || summary.Total != 1 || len(summary.Keys) != 0 || len(summary.Groups) != 1 {
		t.Fatalf("record-sourced summary: %+v %v", summary, err)
	}
	for _, kind := range []driver.ReadWorkKind{driver.ReadPayloadRecords, driver.ReadPayloadBytes, driver.ReadPrefixStates, driver.ReadPrefixBytes, driver.ReadSelectorKeys} {
		if work.total[kind] != 0 {
			t.Fatalf("Count/Summary reconstructed a body or emitted keys: %s=%d", kind, work.total[kind])
		}
	}
	// At the same revision token a changed source invalidates current facts,
	// while committed content must retain its old fact/source observation.
	staged, _ := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &sortKey}}, true, ref)
	newBody := []byte(`{"id":"selection-fact-source","type":"issue","fields":{"title":"new"}}`)
	if err := staged.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: newBody}, nil, nil); err != nil {
		t.Fatal(err)
	}
	newValue := []byte(`{"type":"issue","title":"new"}`)
	newBead := driver.SelectionBead{Ref: ref, Type: "issue", Title: "new"}
	newRow := driver.DerivedRow{Key: sortKey.Key, Value: newValue, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: sortKey, Incarnation: header.Incarnation, Branch: model.Natural64(0),
		ValueDigest: driver.SelectionDigest(newValue),
		Source:      driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: r.meta.Head, Present: true, Digest: driver.SelectionDigest(newBody)},
		Bead:        &newBead,
	}}
	if err := staged.PutSelectionDerived(ctx, sortKey.Table, nil, []driver.DerivedRow{newRow}); err != nil {
		t.Fatal(err)
	}
	p.Mode, p.GroupBy, p.ContentCommitted = driver.SelectionCountMode, "", true
	if got, err := staged.Select(ctx, p); err != nil || got.Total != 1 {
		t.Fatalf("same-token committed content lost old fact source: %+v %v", got, err)
	}
	p.ContentCommitted = false
	if got, err := staged.Select(ctx, p); err == nil || got.Total != 0 {
		t.Fatalf("same-token staged content reused stale fact source: %+v %v", got, err)
	}
}

// Real typed publication supplies both the sparse membership keys and the
// authorization fact set. The SQL plan and pager work are observed separately
// from Go payload/key counters; neither is called executor row visits.
// This direct typed-writer fixture proves the SQLite physical path. Actual
// W4 producer-to-fixed-policy equivalence is a separate integration gate.
func TestSelectionTypedWriterSparseRefsAndScopedGrantWork(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	// The staged read below uses Head 0. Establish its coverage with an
	// explicit complete empty startup; a new store has no implicit range.
	meta := getMeta(t, ctx, s)
	sortFamily := sqliteSelectionFamilies()[0]
	ready, err := selectionFamilyReady(ctx, s.db, meta.Incarnation, sortFamily, revKey(0))
	if err != nil || ready {
		t.Fatalf("new store certified sort family before startup: ready=%t err=%v", ready, err)
	}
	initial := driver.SelectionPlan{Incarnation: meta.Incarnation, Branch: model.Natural64(0),
		ContentRevision: meta.Head, AuthorityRevision: meta.Head,
		Membership:    driver.SelectionExpr{Op: driver.SelectionTrue},
		Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionCountMode}
	controlRef := testRef("sparse-initial-coverage-control")
	initial.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{
		Version: 1, PreparedBinding: driver.SelectionDigest([]byte("sparse-initial-coverage")),
		Incarnation: meta.Incarnation, Branch: model.Natural64(0), AuthorityRevision: meta.Head,
		Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &controlRef},
			Revision: model.Natural64(0)}},
		Chain: driver.SelectionChainNone, Authorization: initial.Authorization, Host: driver.SelectionHostAbsent,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := sourceReader(t, s, nil)
	_, err = before.Select(ctx, initial)
	_ = before.Close()
	if !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("unbuilt Head 0 admitted an ordinary selection: %v", err)
	}
	decode, project := selectionBackfillRaceProjection(meta.Incarnation, nil)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	ready, err = selectionFamilyReady(ctx, s.db, meta.Incarnation, sortFamily, revKey(0))
	if err != nil || !ready {
		t.Fatalf("explicit empty startup did not certify sort family: ready=%t err=%v", ready, err)
	}
	after := sourceReader(t, s, nil)
	empty, err := after.Select(ctx, initial)
	_ = after.Close()
	if err != nil || empty.Total != 0 {
		t.Fatalf("explicit empty startup did not admit an empty ordinary selection: %+v %v", empty, err)
	}
	const population, selected = 2048, 100
	refs := make([]model.Ref, population)
	projections := make([]driver.DerivedRowKey, population)
	extra := make([]driver.LockKey, 0, population+2)
	for i := range refs {
		refs[i] = testRef(fmt.Sprintf("sparse-source-%04d", i))
		projections[i] = driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "sparse-projection:" + refs[i].ID}
		extra = append(extra, driver.LockKey{Kind: driver.LockDerived, Derived: &projections[i]})
	}
	grantKey := driver.DerivedRowKey{Table: driver.DerivedGrant, Key: "sparse-authority-facts"}
	grantSentinel, err := driver.NewSentinel("grant/selection-sparse")
	if err != nil {
		t.Fatal(err)
	}
	extra = append(extra, driver.LockKey{Kind: driver.LockDerived, Derived: &grantKey}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: grantSentinel})
	tx, header := beginSourceTestWithLocks(t, s, lease, extra, true, refs...)
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{grantSentinel}); err != nil {
		t.Fatal(err)
	}
	rows := make([]driver.DerivedRow, 0, population)
	grantRows := make([]driver.SelectionFact, 0, selected)
	for i, ref := range refs {
		project := fmt.Sprintf("project-%04d", i)
		body := []byte(fmt.Sprintf(`{"id":%q,"type":"issue","project":%q}`, ref.ID, project))
		if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
			t.Fatal(err)
		}
		bead := driver.SelectionBead{Ref: ref, Type: "issue", Project: &project}
		value := []byte(fmt.Sprintf(`{"project":%q}`, project))
		rows = append(rows, driver.DerivedRow{Key: projections[i].Key, Value: value, Index: &driver.SelectionIndex{
			Version: driver.SelectionIndexVersion, Row: projections[i], Incarnation: header.Incarnation, Branch: model.Natural64(0),
			ValueDigest: driver.SelectionDigest(value),
			Source:      driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &refs[i]}, Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)},
			Bead:        &bead,
		}})
		if i < selected {
			// The fixed registry ID differs from the bead's project NAME.
			// Only the explicit scoped-grant name column may join a candidate.
			scopeRef := model.RefByID("registry-"+project, model.Natural64(0))
			grantRows = append(grantRows, driver.SelectionFact{Kind: driver.SelectionScopedGrantFact,
				Subject: "opaque-user", Scope: "opaque-scope-" + project, Role: "read",
				ScopeKind: driver.SelectionProjectScope, ScopeRef: &scopeRef, ProjectName: &project})
		}
	}
	if err := tx.PutSelectionDerived(ctx, driver.DerivedSortKey, nil, rows); err != nil {
		t.Fatal(err)
	}
	grantValue := []byte(`{"complete":true}`)
	grant := driver.DerivedRow{Key: grantKey.Key, Value: grantValue, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: grantKey, Incarnation: header.Incarnation, Branch: model.Natural64(0),
		ValueDigest: driver.SelectionDigest(grantValue),
		Source:      driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &grantKey}, Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(grantValue)},
		Facts:       &driver.SelectionFacts{Kind: driver.SelectionScopedGrantFact, Rows: grantRows},
	}}
	if err := tx.PutSelectionDerived(ctx, driver.DerivedGrant, nil, []driver.DerivedRow{grant}); err != nil {
		t.Fatal(err)
	}
	control := testRef("sparse-authority-control")
	memberRefs := append([]model.Ref(nil), refs[:selected]...)
	subject := driver.SelectionString("opaque-user")
	role := driver.SelectionString("read")
	scopeKind := driver.SelectionString(string(driver.SelectionProjectScope))
	authorization := driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{
		Row: grantKey, Kind: driver.SelectionScopedGrantFact,
		Tests: []driver.SelectionFactTest{
			{Column: driver.SelectionFactSubject, Value: &subject},
			{Column: driver.SelectionFactRole, Value: &role},
			{Column: driver.SelectionFactScopeKind, Value: &scopeKind},
			{Column: driver.SelectionFactProjectName, Candidate: driver.SelectionProject},
		},
	}}
	makePlan := func(at model.Revision) driver.SelectionPlan {
		t.Helper()
		p := driver.SelectionPlan{Incarnation: header.Incarnation, Branch: model.Natural64(0),
			ContentRevision: at, AuthorityRevision: at,
			Membership:    driver.SelectionExpr{Op: driver.SelectionRefs, Refs: memberRefs},
			Authorization: authorization, Mode: driver.SelectionCountMode}
		var err error
		p.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{
			Version: 1, PreparedBinding: driver.SelectionDigest([]byte("producer-sparse-fact-work")),
			Incarnation: header.Incarnation, Branch: model.Natural64(0), AuthorityRevision: at,
			Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}},
			Chain:   driver.SelectionChainCompiled, Authorization: authorization, Host: driver.SelectionHostAbsent,
		})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	staged, err := tx.Select(ctx, makePlan(model.Natural64(0)))
	if err != nil || staged.Total != selected || len(staged.Keys) != 0 {
		t.Fatalf("staged sparse scoped grant count=%+v err=%v", staged, err)
	}
	stagedOpen := makePlan(model.Natural64(0))
	stagedOpen.Membership = driver.SelectionExpr{Op: driver.SelectionTrue}
	staged, err = tx.Select(ctx, stagedOpen)
	if err != nil || staged.Total != selected || len(staged.Keys) != 0 {
		t.Fatalf("staged full-vector scoped grant count=%+v err=%v", staged, err)
	}
	commitSourceTest(t, tx, header)
	r := sourceReader(t, s, nil)
	p := makePlan(r.meta.Head)
	work := &totalOnlySourceWorkObserver{total: map[driver.ReadWorkKind]int64{}}
	got, err := r.Select(driver.WithReadWorkObserver(ctx, work), p)
	if err != nil || got.Total != selected || len(got.Keys) != 0 || work.total[driver.ReadPayloadRecords] != 0 || work.total[driver.ReadSelectorKeys] != 0 {
		t.Fatalf("sparse typed-writer count=%+v work=%+v err=%v", got, work.total, err)
	}
	facts, err := r.preflightSelection(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	b := sqliteSelectionSQL{facts: facts, meta: r.meta}
	query, err := b.query(p)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := r.tx.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, b.args...)
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for plan.Next() {
		var node, parent, unused int
		var detail string
		if err := plan.Scan(&node, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := plan.Err(); err != nil {
		t.Fatal(err)
	}
	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH selection_bead_history USING INDEX selection_bead_history_record") || !strings.Contains(joined, "SEARCH f EXISTS USING INDEX selection_facts_scoped_project") {
		t.Fatalf("sparse content/fact lookups lost indexed plan: %s", joined)
	}
	for _, detail := range details {
		if strings.HasPrefix(detail, "SCAN ") && detail != "SCAN CONSTANT ROW" && detail != "SCAN capped" {
			t.Fatalf("sparse plan gained an unbounded scan: %s", joined)
		}
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cacheHits := func(reset bool) (int, error) {
		var hits int
		err := conn.Raw(func(raw any) error {
			status, ok := raw.(modernsqlite.DBStatus)
			if !ok {
				return fmt.Errorf("SQLite connection does not expose cache status")
			}
			var e error
			hits, _, e = status.Status(modernsqlite.DBStatusCacheHit, reset)
			return e
		})
		return hits, err
	}
	if _, err := cacheHits(true); err != nil {
		t.Fatal(err)
	}
	var tag, count int64
	var ignored [15]any
	dest := []any{&tag, &count}
	for i := range ignored {
		dest = append(dest, &ignored[i])
	}
	if err := conn.QueryRowContext(ctx, query, b.args...).Scan(dest...); err != nil {
		t.Fatal(err)
	}
	hits, err := cacheHits(false)
	if err != nil {
		t.Fatal(err)
	}
	if tag != 0 || count != selected || hits <= 0 || hits > selected*8 {
		t.Fatalf("sparse typed-writer SQL result tag=%d count=%d cache_hits=%d", tag, count, hits)
	}
	t.Logf("typed-writer sparse refs=%d corpus=%d cache_hits=%d EXPLAIN=%q executor_row_visits=UNMEASURED", selected, population, hits, details)
}

// This physical microfixture verifies the actual SQLite search plan and
// page-cache work separately from the producer-backed conformance suite.
// Cache hits are real connection work, not a count of executor row visits.
func TestSelectionSparseRefPlanAndCacheWork(t *testing.T) {
	ctx := context.Background()
	opened := selectionStore(t)
	s := opened.(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	const population = 2048
	for i := 0; i < population; i++ {
		id := fmt.Sprintf("selection-plan-%04d", i)
		ref := model.RefByID(id, model.Natural64(0))
		key := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		_, err = tx.ExecContext(ctx, `INSERT INTO selection_bead_history(
		 incarnation,branch,derived_key,record_key,start_revision,bead_id,bead_type,status,title,template,
		 priority,membership_leaf,revision_text,source_revision,source_digest)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, meta.Incarnation, revKey(0), "3:sort_key:plan:"+id,
			key, revKey(0), id, "task", "open", "title", 0, 0, "", "0", revKey(0), driver.SelectionDigest(nil))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	needle := model.RefByID("selection-plan-1024", model.Natural64(0))
	p := driver.SelectionPlan{Incarnation: meta.Incarnation, Branch: model.Natural64(0),
		ContentRevision: meta.Head, AuthorityRevision: meta.Head,
		Membership:    driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []model.Ref{needle}},
		Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionCountMode}
	control := testRef("selection-plan-control")
	p.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{
		Version: 1, PreparedBinding: driver.SelectionDigest([]byte("selection-plan-microfixture")),
		Incarnation: meta.Incarnation, Branch: model.Natural64(0), AuthorityRevision: meta.Head,
		Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}},
		Chain:   driver.SelectionChainNone, Authorization: p.Authorization, Host: driver.SelectionHostAbsent,
	})
	if err != nil {
		t.Fatal(err)
	}
	b := sqliteSelectionSQL{meta: meta}
	query, err := b.query(p)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	plan, err := conn.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, b.args...)
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for plan.Next() {
		var node, parent, unused int
		var detail string
		if err = plan.Scan(&node, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err = plan.Err(); err != nil {
		t.Fatal(err)
	}
	if err = plan.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(details, "\n"), "SEARCH selection_bead_history USING INDEX selection_bead_history_record") {
		t.Fatalf("sparse refs did not drive the temporal record index: %s", strings.Join(details, " | "))
	}
	status := func(reset bool) (int, error) {
		var hits int
		err := conn.Raw(func(raw any) error {
			s, ok := raw.(modernsqlite.DBStatus)
			if !ok {
				return fmt.Errorf("SQLite connection does not expose cache status")
			}
			var e error
			hits, _, e = s.Status(modernsqlite.DBStatusCacheHit, reset)
			return e
		})
		return hits, err
	}
	if _, err = status(true); err != nil {
		t.Fatal(err)
	}
	var tag, count int64
	var ignored [15]any
	dest := []any{&tag, &count}
	for i := range ignored {
		dest = append(dest, &ignored[i])
	}
	if err = conn.QueryRowContext(ctx, query, b.args...).Scan(dest...); err != nil {
		t.Fatal(err)
	}
	hits, err := status(false)
	if err != nil {
		t.Fatal(err)
	}
	if tag != 0 || count != 1 || hits <= 0 {
		t.Fatalf("sparse selection count=%d tag=%d cache_hits=%d", count, tag, hits)
	}
	t.Logf("SQLite sparse selection EXPLAIN=%q cache_hits=%d executor_row_visits=UNMEASURED", details, hits)
}

// The affected ancestor proof must drive the temporal family lookup from the
// changed source set. The old family interval index placed start_revision
// before source_key, which made SQLite scan the whole family before applying
// the affected-source join as the store grew.
func TestSelectionAffectedFamilyPlanUsesSourceTemporalIndex(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	at := revKey(mustUint(meta.Head))
	query := `EXPLAIN QUERY PLAN WITH affected(record_key) AS (VALUES (?)),
		sets AS (
			SELECT f.row_key,f.start_revision,f.source_key
			FROM selection_fact_sets f
			JOIN affected a ON a.record_key=f.source_key
			WHERE f.incarnation=? AND f.branch=? AND f.row_table=? AND f.kind=?
			AND f.start_revision<=? AND (f.end_revision IS NULL OR f.end_revision>?)
		)
		SELECT * FROM sets`
	plan, err := s.db.QueryContext(ctx, query,
		"0:selection-plan-seed", meta.Incarnation, revKey(0),
		string(driver.DerivedAncestorClosure), string(driver.SelectionAncestorFact), at, at)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Close()
	var details []string
	for plan.Next() {
		var node, parent, unused int
		var detail string
		if err := plan.Scan(&node, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := plan.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH f USING INDEX selection_fact_sets_family_source") ||
		!strings.Contains(joined, "source_key=?") {
		t.Fatalf("affected family proof lost source-bounded temporal plan: %s", joined)
	}
	if strings.Contains(joined, "SEARCH f USING INDEX selection_fact_sets_family ") {
		t.Fatalf("affected family proof fell back to the broad interval index: %s", joined)
	}
}

// The per-row old-envelope check has an exact row_key predicate. The source
// range is only a bead-owner discriminator and must not make SQLite walk every
// live source before applying that exact row identity.
func TestSelectionOldEnvelopePlanUsesCurrentRowIndex(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	query := `EXPLAIN QUERY PLAN SELECT f.source_key,f.kind
		FROM selection_fact_sets f
		JOIN objects o ON o.key=f.source_key AND o.kind='bead'
		WHERE f.incarnation=? AND f.branch=? AND f.row_key=? AND f.row_table=?
		AND f.end_revision IS NULL AND f.source_key>='0:' AND f.source_key<'0;'`
	plan, err := s.db.QueryContext(ctx, query,
		meta.Incarnation, revKey(0), "3:selection-plan-row", string(driver.DerivedAncestorClosure))
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Close()
	var details []string
	for plan.Next() {
		var node, parent, unused int
		var detail string
		if err := plan.Scan(&node, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := plan.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH f USING INDEX selection_fact_sets_current_row") ||
		!strings.Contains(joined, "row_key=?") {
		t.Fatalf("old envelope lookup lost row-bounded current plan: %s", joined)
	}
	if strings.Contains(joined, "selection_fact_sets_source") ||
		strings.Contains(joined, "selection_fact_sets_family ") {
		t.Fatalf("old envelope lookup fell back to a source/family census: %s", joined)
	}
}

// Touched-source accounting must visit only that source's active physical
// envelopes, including every depth row of a canonical ancestor closure.
func TestSelectionTouchedSourceRowsUseSourceIndex(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+selectionSourceFactRowsSQL,
		meta.Incarnation, revKey(0), string(driver.DerivedAncestorClosure),
		string(driver.SelectionAncestorFact), "0:selection-touched-source")
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Close()
	var details []string
	for plan.Next() {
		var node, parent, unused int
		var detail string
		if err := plan.Scan(&node, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := plan.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH selection_fact_sets USING INDEX selection_fact_sets_source") ||
		!strings.Contains(joined, "source_key=?") {
		t.Fatalf("touched-source fact rows lost source-bounded plan: %s", joined)
	}
}

// A changed source closes every current physical envelope, even when the
// invalidation crosses a SQL batch boundary. An aborted publication must
// restore all intervals; a source outside the changed set stays current.
func TestSelectionSourceInvalidationBatchesAndRollsBack(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	branch, next := revKey(0), revKey(2)
	changed := make(map[string]bool, selectionFactInvalidationBatch+1)
	sources := make([]string, 0, selectionFactInvalidationBatch+1)
	insert := func(row, source string, end any) {
		t.Helper()
		_, err := s.db.ExecContext(ctx, `INSERT INTO selection_fact_sets(
			incarnation,branch,row_key,row_table,kind,start_revision,end_revision,
			source_key,source_revision,source_present,source_digest)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`, meta.Incarnation, branch, row,
			string(driver.DerivedAncestorClosure), string(driver.SelectionAncestorFact),
			revKey(0), end, source, revKey(0), 1, "test-digest")
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i <= selectionFactInvalidationBatch; i++ {
		source := fmt.Sprintf("0:invalidation-source-%03d", i)
		sources = append(sources, source)
		changed[source] = true
		insert(fmt.Sprintf("3:invalidation-row-%03d", i), source, nil)
	}
	insert("3:invalidation-extra", sources[0], nil)
	insert("3:invalidation-untouched", "0:untouched-source", nil)
	insert("3:invalidation-old", sources[0], revKey(1))

	// Check the exact UPDATE the publication helper executes, including its
	// partial-index predicate and the largest bound parameter list.
	args := []any{next, meta.Incarnation, branch}
	for _, source := range sources[:selectionFactInvalidationBatch] {
		args = append(args, source)
	}
	plan, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+selectionFactInvalidationSQL(selectionFactInvalidationBatch), args...)
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for plan.Next() {
		var node, parent, unused int
		var detail string
		if err := plan.Scan(&node, &parent, &unused, &detail); err != nil {
			_ = plan.Close()
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := plan.Err(); err != nil {
		_ = plan.Close()
		t.Fatal(err)
	}
	_ = plan.Close()
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH selection_fact_sets USING INDEX selection_fact_sets_source") ||
		!strings.Contains(joined, "source_key=?") || strings.Contains(joined, "SCAN selection_fact_sets") {
		t.Fatalf("source invalidation lost source-key lookup: %s", joined)
	}

	check := func(wantActive, wantClosed int) {
		t.Helper()
		var active, closed, old int
		err := s.db.QueryRowContext(ctx, `SELECT SUM(end_revision IS NULL),
			SUM(end_revision=?),SUM(end_revision=?) FROM selection_fact_sets
			WHERE incarnation=? AND branch=?`, next, revKey(1), meta.Incarnation, branch).Scan(&active, &closed, &old)
		if err != nil || active != wantActive || closed != wantClosed || old != 1 {
			t.Fatalf("fact intervals active=%d closed=%d old=%d err=%v, want %d/%d/1", active, closed, old, err, wantActive, wantClosed)
		}
	}
	check(selectionFactInvalidationBatch+3, 0)
	abort := errors.New("abort after both invalidation batches")
	err = s.immediate(ctx, nil, func(c *sql.Conn) error {
		if err := invalidateSelectionFactSets(ctx, c, meta.Incarnation, next, changed); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatalf("invalidation abort: %v", err)
	}
	check(selectionFactInvalidationBatch+3, 0)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return invalidateSelectionFactSets(ctx, c, meta.Incarnation, next, changed)
	}); err != nil {
		t.Fatal(err)
	}
	check(1, selectionFactInvalidationBatch+2)
}

// The thin source observation is a temporal envelope, including tombstones
// and derived values. Both sides of a 128-row batch boundary must publish
// together, and an error after the second batch must restore every interval.
func TestSelectionSourceHistoryBatchesAndRollsBack(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	branch, previous, next := revKey(0), revKey(100), revKey(101)
	tx := &transaction{records: make(map[string]*driver.Record), derived: make(map[string]driver.LockState)}
	want := make(map[string]selectionSourceObservation)
	for i := 0; i <= selectionSourceHistoryBatch; i++ {
		id := fmt.Sprintf("0:history-source-%03d", i)
		observation := selectionSourceObservation{key: id}
		if i%7 != 0 {
			body := []byte(fmt.Sprintf(`{"source":%d}`, i))
			tx.records[id] = &driver.Record{Body: body}
			observation.present = 1
			observation.digest = driver.SelectionDigest(body)
		} else {
			tx.records[id] = nil
		}
		want[id] = observation
	}
	derivedKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "history-batch-derived"}
	derivedLock := driver.LockKey{Kind: driver.LockDerived, Derived: &derivedKey}
	derivedID := keyID(derivedLock)
	derivedValue := []byte(`{"derived":true}`)
	tx.derived[derivedID] = driver.LockState{Key: derivedLock, Present: true,
		Derived: &driver.DerivedRow{Value: derivedValue}}
	want[derivedID] = selectionSourceObservation{key: derivedID, present: 1,
		digest: driver.SelectionDigest(derivedValue)}
	deletedDerivedKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "history-batch-deleted"}
	deletedDerivedLock := driver.LockKey{Kind: driver.LockDerived, Derived: &deletedDerivedKey}
	deletedDerivedID := keyID(deletedDerivedLock)
	tx.derived[deletedDerivedID] = driver.LockState{Key: deletedDerivedLock}
	want[deletedDerivedID] = selectionSourceObservation{key: deletedDerivedID}
	untouched := "0:history-untouched"
	insertOld := func(id string) {
		t.Helper()
		_, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(
			incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest)
			VALUES(?,?,?,?,?,?,?)`, meta.Incarnation, branch, id, previous, previous, 1, "old-digest")
		if err != nil {
			t.Fatal(err)
		}
	}
	for id := range want {
		insertOld(id)
	}
	insertOld(untouched)
	// A deep closed history for one touched source must stay historical. The
	// current-row index lets the close statement ignore these older versions.
	historicalID := "0:history-source-000"
	for revision := uint64(0); revision < 100; revision++ {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(
			incarnation,branch,source_key,start_revision,end_revision,source_revision,source_present,source_digest)
			VALUES(?,?,?,?,?,?,?,?)`, meta.Incarnation, branch, historicalID,
			revKey(revision), revKey(revision+1), revKey(revision), 1, "historical-digest"); err != nil {
			t.Fatal(err)
		}
	}
	// This is the production UPDATE, with its largest batch of source keys.
	args := []any{next, meta.Incarnation, branch}
	for id := range want {
		if len(args) == 3+selectionSourceHistoryBatch {
			break
		}
		args = append(args, id)
	}
	plan, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+selectionSourceHistoryCloseSQL(selectionSourceHistoryBatch), args...)
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for plan.Next() {
		var node, parent, unused int
		var detail string
		if err := plan.Scan(&node, &parent, &unused, &detail); err != nil {
			_ = plan.Close()
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := plan.Err(); err != nil {
		_ = plan.Close()
		t.Fatal(err)
	}
	_ = plan.Close()
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH selection_source_history USING INDEX selection_source_history_current") ||
		!strings.Contains(joined, "source_key=?") || strings.Contains(joined, "SCAN selection_source_history") {
		t.Fatalf("source history close lost source-key lookup: %s", joined)
	}
	check := func(published bool) {
		t.Helper()
		for id, observation := range want {
			var end sql.NullString
			if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_source_history
				WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=?`,
				meta.Incarnation, branch, id, previous).Scan(&end); err != nil {
				t.Fatal(err)
			}
			if published != end.Valid || published && end.String != next {
				t.Fatalf("source %q old interval end=%+v published=%t", id, end, published)
			}
			var present int
			var digest, revision string
			err := s.db.QueryRowContext(ctx, `SELECT source_revision,source_present,source_digest
				FROM selection_source_history WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=?`,
				meta.Incarnation, branch, id, next).Scan(&revision, &present, &digest)
			if !published {
				if !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("source %q survived rollback: %v", id, err)
				}
				continue
			}
			if err != nil || revision != next || present != observation.present || digest != observation.digest {
				t.Fatalf("source %q new observation revision=%q present=%d digest=%q err=%v, want %+v",
					id, revision, present, digest, err, observation)
			}
		}
		var end sql.NullString
		if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_source_history
			WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=?`,
			meta.Incarnation, branch, untouched, previous).Scan(&end); err != nil || end.Valid {
			t.Fatalf("untouched source interval end=%+v err=%v", end, err)
		}
		var historicalCount int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_source_history
			WHERE incarnation=? AND branch=? AND source_key=? AND start_revision<?
			AND end_revision IS NOT NULL AND source_digest=?`,
			meta.Incarnation, branch, historicalID, previous, "historical-digest").Scan(&historicalCount); err != nil ||
			historicalCount != 100 {
			t.Fatalf("historical source interval count=%d err=%v", historicalCount, err)
		}
		for revision := uint64(0); revision < 100; revision++ {
			var historicalEnd string
			if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_source_history
				WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=?`,
				meta.Incarnation, branch, historicalID, revKey(revision)).Scan(&historicalEnd); err != nil ||
				historicalEnd != revKey(revision+1) {
				t.Fatalf("historical source revision %d end=%q err=%v", revision, historicalEnd, err)
			}
		}
	}
	check(false)
	abort := errors.New("abort after source history batches")
	err = s.immediate(ctx, nil, func(c *sql.Conn) error {
		if err := tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, next); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatalf("source history abort: %v", err)
	}
	check(false)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, next)
	}); err != nil {
		t.Fatal(err)
	}
	check(true)
}

func TestSelectionPredicateFamilyIndexWorkAndCommit(t *testing.T) {
	ready, err := driver.NewReadySentinel("project", "task")
	if err != nil {
		t.Fatal(err)
	}
	closure, err := driver.NewClosureSentinel("root")
	if err != nil {
		t.Fatal(err)
	}
	grant, err := driver.NewGrantSentinel("scope")
	if err != nil {
		t.Fatal(err)
	}
	makeDerived := func(table driver.DerivedTable, name string) driver.LockState {
		key := driver.DerivedRowKey{Table: table, Key: name}
		return driver.LockState{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &key}}
	}
	staged := &transaction{derived: make(map[string]driver.LockState), bumps: make(map[string]driver.LockKey)}
	for i := 0; i < 257; i++ {
		state := makeDerived(driver.DerivedReadyState, fmt.Sprintf("ready-%03d", i))
		staged.derived[keyID(state.Key)] = state
	}
	for _, table := range []driver.DerivedTable{
		driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute,
		driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedSortKey,
	} {
		state := makeDerived(table, "other")
		staged.derived[keyID(state.Key)] = state
	}
	for i := 0; i < 129; i++ {
		sentinel, err := driver.NewReadySentinel("project", fmt.Sprintf("task-%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
		staged.bumps[keyID(key)] = key
	}
	for _, sentinel := range []driver.Sentinel{closure, grant, driver.SentinelOntology, driver.SentinelPlugins} {
		key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
		staged.bumps[keyID(key)] = key
	}
	readFact := driver.LockKey{Kind: driver.LockReadFact, Fact: driver.ReadFact(ready)}
	staged.bumps[keyID(readFact)] = readFact
	work, err := staged.validateDerivedPredicates()
	if err != nil {
		t.Fatal(err)
	}
	if work.BumpsVisited != len(staged.bumps) || work.DerivedVisited != len(staged.derived) ||
		work.FamilyChecks != len(staged.derived)-1 {
		t.Fatalf("predicate work %+v for %d bumps and %d staged rows", work, len(staged.bumps), len(staged.derived))
	}
	grantKey := driver.LockKey{Kind: driver.LockSentinel, Sentinel: grant}
	delete(staged.bumps, keyID(grantKey))
	if _, err := staged.validateDerivedPredicates(); err == nil ||
		!strings.Contains(err.Error(), "derived table grant requires grant/ predicate bump") {
		t.Fatalf("missing grant family accepted: %v", err)
	}
	wrongKind := &transaction{
		derived: map[string]driver.LockState{"ready": makeDerived(driver.DerivedReadyState, "ready")},
		bumps:   map[string]driver.LockKey{keyID(readFact): readFact},
	}
	if _, err := wrongKind.validateDerivedPredicates(); err == nil ||
		!strings.Contains(err.Error(), "requires ready/ predicate bump") {
		t.Fatalf("read-fact text counted as a ready sentinel: %v", err)
	}
	wrongKind.bumps = map[string]driver.LockKey{"invalid-ontology": {
		Kind: driver.LockSentinel, Sentinel: "ontology/extra"}}
	wrongKind.derived = map[string]driver.LockState{"ontology": makeDerived(driver.DerivedOntology, "ontology")}
	if _, err := wrongKind.validateDerivedPredicates(); err == nil ||
		!strings.Contains(err.Error(), "requires ontology predicate bump") {
		t.Fatalf("ontology prefix counted as exact sentinel: %v", err)
	}

	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	var extra []driver.LockKey
	var rows []driver.DerivedRow
	var sentinels []driver.Sentinel
	for i := 0; i < 257; i++ {
		row := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: fmt.Sprintf("committed-%03d", i)}
		extra = append(extra, driver.LockKey{Kind: driver.LockDerived, Derived: &row})
		rows = append(rows, driver.DerivedRow{Key: row.Key, Value: []byte(`{"ready":true}`)})
	}
	for i := 0; i < 129; i++ {
		sentinel, err := driver.NewReadySentinel("committed", fmt.Sprintf("task-%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		extra = append(extra, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		sentinels = append(sentinels, sentinel)
	}
	positive, header := beginSourceTestWithLocks(t, s, lease, extra, true)
	if err := positive.PutDerived(ctx, driver.DerivedReadyState, nil, rows); err != nil {
		t.Fatal(err)
	}
	if err := positive.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	committedWork, err := positive.validateDerivedPredicates()
	if err != nil || committedWork.BumpsVisited != len(positive.bumps) ||
		committedWork.DerivedVisited != len(rows) || committedWork.FamilyChecks != len(rows) {
		t.Fatalf("real staged predicate work %+v err=%v", committedWork, err)
	}
	entry, err := positive.Commit(ctx, header)
	if err != nil || entry.Revision.String() != "1" {
		t.Fatalf("ready-family Commit: %+v %v", entry, err)
	}
	commitOne := func(name string, family driver.Sentinel) (model.Changeset, error) {
		t.Helper()
		row := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: name}
		rowKey := driver.LockKey{Kind: driver.LockDerived, Derived: &row}
		familyKey := driver.LockKey{Kind: driver.LockSentinel, Sentinel: family}
		tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{rowKey, familyKey}, true)
		if err := tx.PutDerived(ctx, row.Table, nil, []driver.DerivedRow{{Key: row.Key, Value: []byte(`{"ready":true}`)}}); err != nil {
			t.Fatal(err)
		}
		if err := tx.BumpSentinels(ctx, []driver.Sentinel{family}); err != nil {
			t.Fatal(err)
		}
		return tx.Commit(ctx, header)
	}
	_, err = commitOne("refused", grant)
	if err == nil || !strings.Contains(err.Error(), "derived table ready_state requires ready/ predicate bump") {
		t.Fatalf("wrong-family Commit: %v", err)
	}
	meta, err := s.Meta(ctx)
	if err != nil || meta.Head.String() != "1" {
		t.Fatalf("refused Commit changed Head: %+v %v", meta, err)
	}
}
