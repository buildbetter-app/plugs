package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

// The production startup caller must observe a completed graph activation as
// distinct from a selection refusal.  This uses the real SQLite source image
// and pipeline decoder; the missing closure is a genuine optional capability
// refusal, while the graph coverage remains publishable.
func TestSelectionBackfillPublicCallerKeepsGraphOnOptionalRefusal(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	ref := testRef("selection-public-refusal")
	tx, header := beginSourceTest(t, s, lease, ref)
	body := []byte(`{"id":"selection-public-refusal","type":"issue","fields":{}}`)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, header)

	if err := pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
		t.Fatalf("optional selection refusal escaped public startup: %v", err)
	}
	meta := getMeta(t, ctx, s)
	coverage, err := sourceindex.SQLite.ReadCoverage(ctx, s.db, meta.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if err := coverage.Parent.Require(mustUint(meta.Head)); err != nil {
		t.Fatalf("graph Parent coverage was not committed: %v", err)
	}
	if err := coverage.Links.Require(mustUint(meta.Head)); err != nil {
		t.Fatalf("graph Links coverage was not committed: %v", err)
	}
	familyReady, err := selectionFamilyReady(ctx, s.db, meta.Incarnation, sqliteSelectionFamilies()[3], revKey(mustUint(meta.Head)))
	if err != nil {
		t.Fatal(err)
	}
	if familyReady {
		t.Fatal("selection Ancestor family was certified despite missing closure")
	}
}

func TestSelectionBackfillPreparationRefusalKeepsGraph(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	ref := testRef("selection-preparation-refusal")
	tx, header := beginSourceTest(t, s, lease, ref)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{"id":"selection-preparation-refusal","type":"issue"}`)}, nil, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, header)
	decode, project := selectionBackfillRaceProjection(header.Incarnation, nil)
	project.Inputs = nil // A declaration-time optional refusal; no hook exists.
	err := s.BuildGraphSourceIndexes(ctx, lease, decode, project)
	if !sourceindex.IsSelectionBackfillRefused(err) || !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("preparation refusal was not tagged after graph publication: %v", err)
	}
	meta := getMeta(t, ctx, s)
	coverage, err := sourceindex.SQLite.ReadCoverage(ctx, s.db, meta.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if err := coverage.Parent.Require(mustUint(meta.Head)); err != nil {
		t.Fatalf("graph Parent coverage was not committed: %v", err)
	}
	if err := coverage.Links.Require(mustUint(meta.Head)); err != nil {
		t.Fatalf("graph Links coverage was not committed: %v", err)
	}
}

func TestSelectionBackfillPreservesIndependentFactSources(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	bead, ontology := testRef("selection-built-bead"), testRef("selection-known-ontology")
	closure, err := driver.NewClosureSentinel(bead.ID)
	if err != nil {
		t.Fatal(err)
	}
	derivedKey := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "selection-independent-derived"}
	recordKey := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "selection-independent-record"}
	derivedLock := driver.LockKey{Kind: driver.LockDerived, Derived: &derivedKey}
	recordLock := driver.LockKey{Kind: driver.LockDerived, Derived: &recordKey}
	tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{derivedLock, recordLock,
		{Kind: driver.LockSentinel, Sentinel: closure}}, true, bead, ontology)
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{closure}); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: bead, Kind: "bead", Body: []byte(`{}`)}, nil, nil); err != nil {
		t.Fatal(err)
	}
	ontologyBody := []byte(`{"kind":"ontology"}`)
	if err := tx.Put(ctx, driver.Record{Ref: ontology, Kind: "ontology", Body: ontologyBody}); err != nil {
		t.Fatal(err)
	}
	makeFact := func(key driver.DerivedRowKey, source driver.SelectionSource, name string) driver.DerivedRow {
		value := []byte("independent/" + name)
		return driver.DerivedRow{Key: key.Key, Value: value, Index: &driver.SelectionIndex{
			Version: driver.SelectionIndexVersion, Row: key, Incarnation: header.Incarnation,
			Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value), Source: source,
			Facts: &driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{
				{Kind: driver.SelectionLabelFact, Ref: &bead, Name: name, Value: "yes"},
			}},
		}}
	}
	derivedValue := []byte("independent/derived")
	rows := []driver.DerivedRow{
		makeFact(derivedKey, driver.SelectionSource{Key: derivedLock, Revision: model.Natural64(0),
			Present: true, Digest: driver.SelectionDigest(derivedValue)}, "derived"),
		makeFact(recordKey, driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ontology},
			Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(ontologyBody)}, "record"),
	}
	if err := tx.PutSelectionDerived(ctx, driver.DerivedAttribute, nil, rows); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, header)
	before := make(map[string][]byte)
	for _, key := range []driver.LockKey{derivedLock, recordLock} {
		id := keyID(key)
		var data []byte
		if err := s.db.QueryRowContext(ctx, `SELECT data FROM key_history WHERE key=? ORDER BY rev DESC LIMIT 1`, id).Scan(&data); err != nil {
			t.Fatal(err)
		}
		before[id] = data
	}
	decode, project := selectionBackfillRaceProjection(header.Incarnation, nil)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	for _, key := range []driver.LockKey{derivedLock, recordLock} {
		id := keyID(key)
		var after []byte
		if err := s.db.QueryRowContext(ctx, `SELECT data FROM key_history WHERE key=? ORDER BY rev DESC LIMIT 1`, id).Scan(&after); err != nil || !bytes.Equal(after, before[id]) {
			t.Fatalf("independent source logical bytes changed: %s %v", id, err)
		}
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_fact_sets WHERE incarnation=? AND row_key=? AND end_revision IS NULL`, header.Incarnation, id).Scan(&count); err != nil || count != 1 {
			t.Fatalf("independent fact envelope removed: %s count=%d err=%v", id, count, err)
		}
	}
	r := sourceReader(t, s, nil)
	defer r.Close()
	control := testRef("selection-independent-control")
	for _, fact := range []struct {
		key  driver.DerivedRowKey
		name string
	}{{derivedKey, "derived"}, {recordKey, "record"}} {
		name := driver.SelectionString(fact.name)
		plan := driver.SelectionPlan{Incarnation: header.Incarnation, Branch: model.Natural64(0),
			ContentRevision: r.meta.Head, AuthorityRevision: r.meta.Head,
			Membership: driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{
				Row: fact.key, Kind: driver.SelectionLabelFact,
				Tests: []driver.SelectionFactTest{{Column: driver.SelectionFactRef, Candidate: driver.SelectionID},
					{Column: driver.SelectionFactName, Value: &name}},
			}},
			Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionCountMode}
		var err error
		plan.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{
			Version: 1, PreparedBinding: driver.SelectionDigest([]byte("independent-source-control")),
			Incarnation: header.Incarnation, Branch: model.Natural64(0), AuthorityRevision: r.meta.Head,
			Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control},
				Revision: model.Natural64(0)}},
			Chain: driver.SelectionChainNone, Authorization: plan.Authorization, Host: driver.SelectionHostAbsent,
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.Select(ctx, plan)
		if err != nil || got.Total != 1 {
			t.Fatalf("independent %s source refused after startup: %+v %v", fact.name, got, err)
		}
	}
}

func TestSelectionBackfillPreservesCutClosureAncestor(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	bead := testRef("selection-cut-bead")
	closure, err := driver.NewClosureSentinel(bead.ID)
	if err != nil {
		t.Fatal(err)
	}
	sortKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "selection-cut-sort"}
	cutKey := driver.DerivedRowKey{Table: driver.DerivedCutClosure, Key: "selection-cut-independent"}
	extra := []driver.LockKey{{Kind: driver.LockDerived, Derived: &sortKey}, {Kind: driver.LockDerived, Derived: &cutKey},
		{Kind: driver.LockSentinel, Sentinel: closure}}
	tx, header := beginSourceTestWithLocks(t, s, lease, extra, true, bead)
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{closure}); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{}`)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: bead, Kind: "bead", Body: body}, nil, nil); err != nil {
		t.Fatal(err)
	}
	source := driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &bead},
		Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)}
	sortValue := []byte("selection-cut-sort-value")
	sortRow := driver.DerivedRow{Key: sortKey.Key, Value: sortValue, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: sortKey, Incarnation: header.Incarnation,
		Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(sortValue), Source: source,
		Bead: &driver.SelectionBead{Ref: bead, Type: "task"},
	}}
	if err := tx.PutSelectionDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{sortRow}); err != nil {
		t.Fatal(err)
	}
	cutValue := []byte("selection-cut-logical-value")
	cutRow := driver.DerivedRow{Key: cutKey.Key, Value: cutValue, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: cutKey, Incarnation: header.Incarnation,
		Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(cutValue), Source: source,
		Facts: &driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{
			{Kind: driver.SelectionAncestorFact, Ref: &bead, Target: &bead, Depth: 0},
		}},
	}}
	if err := tx.PutSelectionDerived(ctx, driver.DerivedCutClosure, nil, []driver.DerivedRow{cutRow}); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, header)
	meta := getMeta(t, ctx, s)
	sortReady, err := selectionFamilyReady(ctx, s.db, meta.Incarnation, sqliteSelectionFamilies()[0], revKey(mustUint(meta.Head)))
	if err != nil || !sortReady {
		t.Fatalf("typed sort family must admit the independent cut: ready=%t err=%v", sortReady, err)
	}
	ancestorReady, err := selectionFamilyReady(ctx, s.db, meta.Incarnation, sqliteSelectionFamilies()[3], revKey(mustUint(meta.Head)))
	if err != nil || ancestorReady {
		t.Fatalf("cut envelope certified core ancestor family: ready=%t err=%v", ancestorReady, err)
	}
	cutID := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &cutKey})
	var before []byte
	if err := s.db.QueryRowContext(ctx, `SELECT data FROM key_history WHERE key=? ORDER BY rev DESC LIMIT 1`, cutID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	selectCut := func(phase string) {
		r := sourceReader(t, s, nil)
		defer r.Close()
		plan := driver.SelectionPlan{Incarnation: meta.Incarnation, Branch: model.Natural64(0),
			ContentRevision: r.meta.Head, AuthorityRevision: r.meta.Head,
			Membership: driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{
				Row: cutKey, Kind: driver.SelectionAncestorFact,
				Tests: []driver.SelectionFactTest{{Column: driver.SelectionFactRef, Candidate: driver.SelectionID},
					{Column: driver.SelectionFactTarget, Candidate: driver.SelectionID}},
			}},
			Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionCountMode}
		control := testRef("selection-cut-control")
		var err error
		plan.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{
			Version: 1, PreparedBinding: driver.SelectionDigest([]byte("cut-independent-control")),
			Incarnation: meta.Incarnation, Branch: model.Natural64(0), AuthorityRevision: r.meta.Head,
			Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control},
				Revision: model.Natural64(0)}},
			Chain: driver.SelectionChainNone, Authorization: plan.Authorization, Host: driver.SelectionHostAbsent,
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.Select(ctx, plan)
		if err != nil || got.Total != 1 {
			t.Fatalf("%s cut envelope read: %+v %v", phase, got, err)
		}
	}
	selectCut("before startup")
	decode, project := selectionBackfillRaceProjection(meta.Incarnation, nil)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	selectCut("after startup")
	var after []byte
	if err := s.db.QueryRowContext(ctx, `SELECT data FROM key_history WHERE key=? ORDER BY rev DESC LIMIT 1`, cutID).Scan(&after); err != nil || !bytes.Equal(after, before) {
		t.Fatalf("cut logical Key/Value/Index changed: %v", err)
	}
	var sets, facts int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_fact_sets WHERE incarnation=? AND row_key=?`, meta.Incarnation, cutID).Scan(&sets); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_facts WHERE incarnation=? AND row_key=?`, meta.Incarnation, cutID).Scan(&facts); err != nil {
		t.Fatal(err)
	}
	if sets != 1 || facts != 1 {
		t.Fatalf("cut physical history changed: sets=%d facts=%d", sets, facts)
	}
}

func TestSelectionBackfillRejectsUnknownRecordSourceOrphan(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	ref := testRef("selection-real-source")
	tx, header := beginSourceTest(t, s, lease, ref)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{}`)}, nil, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, header)
	missing := testRef("selection-missing-source")
	orphan := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "selection-orphan-labels"}
	_, err := s.db.ExecContext(ctx, `INSERT INTO selection_fact_sets(
		incarnation,branch,row_key,row_table,kind,start_revision,source_key,source_revision,source_present,source_digest)
		VALUES(?,?,?,?,?,?,?,?,?,?)`, header.Incarnation, revKey(0), keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &orphan}),
		string(driver.DerivedAttribute), string(driver.SelectionLabelFact), revKey(1), keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &missing}),
		revKey(1), 1, driver.SelectionDigest([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	var runsBefore int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_backfill_runs WHERE incarnation=?`, header.Incarnation).Scan(&runsBefore); err != nil {
		t.Fatal(err)
	}
	decode, project := selectionBackfillRaceProjection(header.Incarnation, nil)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("unknown record-source orphan certified: %v", err)
	}
	var runsAfter, orphanRows int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_backfill_runs WHERE incarnation=?`, header.Incarnation).Scan(&runsAfter); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_fact_sets WHERE incarnation=? AND row_key=? AND end_revision IS NULL`,
		header.Incarnation, keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &orphan})).Scan(&orphanRows); err != nil {
		t.Fatal(err)
	}
	if runsAfter != runsBefore || orphanRows != 1 {
		t.Fatalf("orphan refusal changed staging before census: runs=%d->%d orphan=%d", runsBefore, runsAfter, orphanRows)
	}
}

func TestSelectionSQLiteFinalParentChainAndExactSortCensus(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	root, child := testRef("selection-chain-root"), testRef("selection-chain-child")
	tx, header := beginSourceTest(t, s, lease, root, child)
	for _, item := range []struct {
		ref    model.Ref
		parent *model.Ref
	}{{root, nil}, {child, &root}} {
		if err := tx.PutGraphRecord(ctx, driver.Record{Ref: item.ref, Kind: "bead", Body: []byte(`{}`)}, item.parent, nil); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, header)
	decode, project := selectionBackfillRaceProjection(header.Incarnation, nil)
	plainDecode, plainProject := decode, project.Decode
	decode = func(record driver.Record) (driver.GraphSourceMetadata, error) {
		metadata, err := plainDecode(record)
		if err == nil && record.Ref.ID == child.ID {
			metadata.Selection[0].Row.Index.Bead.Parent = &root
		}
		return metadata, err
	}
	project.Decode = func(record driver.Record, inputs []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
		rows, err := plainProject(record, inputs)
		if err == nil && record.Ref.ID == child.ID {
			// Production emits one physical closure row per depth. Keep the
			// depth-one fact in its own envelope so this test exercises the
			// canonical multi-row-by-source representation.
			depthOne := rows[0].Clone()
			depthOne.Row.Key += ":depth-1"
			depthOne.Row.Index.Row.Key = depthOne.Row.Key
			depthOne.Row.Index.Facts.Rows = []driver.SelectionFact{{
				Kind: driver.SelectionAncestorFact, Ref: &child, Target: &root, Depth: 1,
			}}
			depthOne.Row.Index.ValueDigest = driver.SelectionDigest(depthOne.Row.Value)
			rows = append(rows, depthOne)
		}
		return rows, err
	}
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, s)
	at := revKey(mustUint(meta.Head))
	valid, err := verifySQLiteSelectionParentChains(ctx, s.db, meta.Incarnation, at)
	if err != nil || !valid {
		t.Fatalf("valid two-level chain refused: valid=%t err=%v", valid, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE selection_facts SET ref_key=NULL WHERE incarnation=? AND kind='ancestor' AND depth=1`, meta.Incarnation); err != nil {
		t.Fatal(err)
	}
	valid, err = verifySQLiteSelectionParentChains(ctx, s.db, meta.Incarnation, at)
	if err != nil || valid {
		t.Fatalf("null intermediate identity certified: valid=%t err=%v", valid, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE selection_facts SET ref_key=? WHERE incarnation=? AND kind='ancestor' AND depth=1`,
		keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &child}), meta.Incarnation); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE selection_bead_history SET parent_key=NULL WHERE incarnation=? AND record_key=? AND end_revision IS NULL`,
		meta.Incarnation, keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &child})); err != nil {
		t.Fatal(err)
	}
	valid, err = verifySQLiteSelectionParentChains(ctx, s.db, meta.Incarnation, at)
	if err != nil || valid {
		t.Fatalf("stale current Parent chain certified: valid=%t err=%v", valid, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE selection_bead_history SET parent_key=? WHERE incarnation=? AND record_key=? AND end_revision IS NULL`,
		keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &root}), meta.Incarnation,
		keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &child})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_bead_history(
		incarnation,branch,derived_key,record_key,start_revision,end_revision,
		bead_id,bead_type,project,status,title,template,parent_key,priority,
		membership_leaf,revision_text,source_revision,source_digest)
		SELECT incarnation,branch,derived_key||'/duplicate',record_key,start_revision,?,
		bead_id,bead_type,project,status,title,template,parent_key,priority,
		membership_leaf,revision_text,source_revision,source_digest
		FROM selection_bead_history WHERE incarnation=? AND record_key=? AND end_revision IS NULL`,
		revKey(mustUint(meta.Head)+1), meta.Incarnation,
		keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &child})); err != nil {
		t.Fatal(err)
	}
	if err := verifySQLiteSelectionFamilyCensus(ctx, s.db, meta.Incarnation, at, sqliteSelectionFamilies()[0]); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("duplicate live sort projection certified: %v", err)
	}
}

func TestSelectionAffectedParentProofIncludesUnchangedDescendants(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	root, child, sibling := testRef("selection-affected-root"), testRef("selection-affected-child"), testRef("selection-affected-sibling")
	tx, header := beginSourceTest(t, s, lease, root, child, sibling)
	for _, item := range []struct {
		ref    model.Ref
		parent *model.Ref
	}{{root, nil}, {child, &root}, {sibling, &root}} {
		if err := tx.PutGraphRecord(ctx, driver.Record{Ref: item.ref, Kind: "bead", Body: []byte(`{}`)}, item.parent, nil); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, header)
	decode, project := selectionBackfillRaceProjection(header.Incarnation, nil)
	plainDecode, plainProject := decode, project.Decode
	decode = func(record driver.Record) (driver.GraphSourceMetadata, error) {
		metadata, err := plainDecode(record)
		if err == nil && (record.Ref.ID == child.ID || record.Ref.ID == sibling.ID) {
			metadata.Selection[0].Row.Index.Bead.Parent = &root
		}
		return metadata, err
	}
	project.Decode = func(record driver.Record, inputs []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
		rows, err := plainProject(record, inputs)
		if err == nil && (record.Ref.ID == child.ID || record.Ref.ID == sibling.ID) {
			depthOne := rows[0].Clone()
			depthOne.Row.Key += ":depth-1"
			depthOne.Row.Index.Row.Key = depthOne.Row.Key
			depthOne.Row.Index.Facts.Rows = []driver.SelectionFact{{
				Kind: driver.SelectionAncestorFact, Ref: &record.Ref, Target: &root, Depth: 1,
			}}
			depthOne.Row.Index.ValueDigest = driver.SelectionDigest(depthOne.Row.Value)
			rows = append(rows, depthOne)
		}
		return rows, err
	}
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, s)
	at := revKey(mustUint(meta.Head))
	rootKey := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &root})
	valid, err := verifySQLiteSelectionParentChainsAffected(ctx, s.db, meta.Incarnation, at, []string{rootKey})
	if err != nil || !valid {
		t.Fatalf("affected proof rejected valid root component: valid=%t err=%v", valid, err)
	}
	childKey := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &child})
	valid, err = verifySQLiteSelectionParentChainsAffected(ctx, s.db, meta.Incarnation, at, []string{childKey})
	if err != nil || !valid {
		t.Fatalf("affected proof rejected valid child-seeded component: valid=%t err=%v", valid, err)
	}
	// A leaf seed must not walk back to root and then down into an unrelated
	// sibling. The sibling is still checked when root itself is the seed.
	siblingKey := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &sibling})
	if _, err := s.db.ExecContext(ctx, `UPDATE selection_bead_history SET parent_key=NULL
		WHERE incarnation=? AND record_key=? AND end_revision IS NULL`, meta.Incarnation, siblingKey); err != nil {
		t.Fatal(err)
	}
	valid, err = verifySQLiteSelectionParentChainsAffected(ctx, s.db, meta.Incarnation, at, []string{childKey})
	if err != nil || !valid {
		t.Fatalf("leaf-seeded proof expanded into unrelated sibling: valid=%t err=%v", valid, err)
	}
	valid, err = verifySQLiteSelectionParentChainsAffected(ctx, s.db, meta.Incarnation, at, []string{rootKey})
	if err != nil || valid {
		t.Fatalf("root-seeded proof missed stale sibling: valid=%t err=%v", valid, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE selection_bead_history SET parent_key=?
		WHERE incarnation=? AND record_key=? AND end_revision IS NULL`, rootKey, meta.Incarnation, siblingKey); err != nil {
		t.Fatal(err)
	}
	// The child is not a seed, but it is a current descendant of root. A stale
	// typed Parent image must therefore invalidate the affected component.
	if _, err := s.db.ExecContext(ctx, `UPDATE selection_bead_history SET parent_key=NULL
		WHERE incarnation=? AND record_key=? AND end_revision IS NULL`, meta.Incarnation, childKey); err != nil {
		t.Fatal(err)
	}
	valid, err = verifySQLiteSelectionParentChainsAffected(ctx, s.db, meta.Incarnation, at, []string{rootKey})
	if err != nil || valid {
		t.Fatalf("affected proof certified stale unchanged descendant: valid=%t err=%v", valid, err)
	}
}

func TestSelectionFamilyCompleteKeepsCanonicalMultiRowSource(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	root, child, other := testRef("selection-multirow-root"), testRef("selection-multirow-child"), testRef("selection-multirow-other")
	tx, header := beginSourceTest(t, s, lease, root, child, other)
	for _, item := range []struct {
		ref    model.Ref
		parent *model.Ref
	}{{root, nil}, {child, &root}, {other, nil}} {
		if err := tx.PutGraphRecord(ctx, driver.Record{Ref: item.ref, Kind: "bead", Body: []byte(`{}`)}, item.parent, nil); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, header)
	decode, project := selectionBackfillRaceProjection(header.Incarnation, nil)
	plainDecode, plainProject := decode, project.Decode
	decode = func(record driver.Record) (driver.GraphSourceMetadata, error) {
		metadata, err := plainDecode(record)
		if err == nil && record.Ref.ID == child.ID {
			metadata.Selection[0].Row.Index.Bead.Parent = &root
		}
		return metadata, err
	}
	project.Decode = func(record driver.Record, inputs []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
		rows, err := plainProject(record, inputs)
		if err == nil && record.Ref.ID == child.ID {
			depthOne := rows[0].Clone()
			depthOne.Row.Key += ":depth-1"
			depthOne.Row.Index.Row.Key = depthOne.Row.Key
			depthOne.Row.Index.Facts.Rows = []driver.SelectionFact{{
				Kind: driver.SelectionAncestorFact, Ref: &child, Target: &root, Depth: 1,
			}}
			depthOne.Row.Index.ValueDigest = driver.SelectionDigest(depthOne.Row.Value)
			rows = append(rows, depthOne)
		}
		return rows, err
	}
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, s)
	family := sqliteSelectionFamilies()[3]
	childSource := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &child})
	var oldRows []string
	rows, err := s.db.QueryContext(ctx, `SELECT row_key FROM selection_fact_sets
		WHERE incarnation=? AND branch=? AND row_table=? AND kind=? AND source_key=? AND end_revision IS NULL ORDER BY row_key`,
		meta.Incarnation, revKey(0), string(family.Table), string(family.Kind), childSource)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		oldRows = append(oldRows, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(oldRows) != 2 {
		t.Fatalf("canonical child source rows=%v, want self+depth", oldRows)
	}

	newRow := func(row driver.DerivedSelectionRow, key string) driver.DerivedSelectionRow {
		copy := row.Clone()
		copy.Row.Key = key
		copy.Row.Index.Row.Key = key
		return copy
	}
	state := func(row driver.DerivedSelectionRow, present bool) driver.LockState {
		key := driver.DerivedRowKey{Table: row.Table, Key: row.Row.Key}
		lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
		state := driver.LockState{Key: lock, Present: present}
		if present {
			copy := row.Row.Clone()
			state.Derived = &copy
		}
		return state
	}
	childRecord := driver.Record{Ref: child, Kind: "bead", Body: []byte(`{}`)}
	childRows, err := project.Decode(childRecord, nil)
	if err != nil || len(childRows) != 2 {
		t.Fatalf("child fixture rows=%d err=%v", len(childRows), err)
	}
	otherRecord := driver.Record{Ref: other, Kind: "bead", Body: []byte(`{}`)}
	otherRows, err := project.Decode(otherRecord, nil)
	if err != nil || len(otherRows) != 1 {
		t.Fatalf("other fixture rows=%d err=%v", len(otherRows), err)
	}
	owner := &transaction{records: map[string]*driver.Record{}}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// An unrelated one-row replacement must not re-census or reject the
	// existing child's valid self+depth closure.
	unrelated := map[string]driver.LockState{}
	otherRow := otherRows[0]
	unrelated[keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: otherRow.Table, Key: otherRow.Row.Key}})] = state(otherRow, true)
	complete, err := owner.selectionFamilyCompleteForCommit(ctx, conn, meta.Incarnation, family, unrelated)
	if err != nil || !complete {
		t.Fatalf("unrelated replacement rejected existing multi-row source: complete=%t err=%v", complete, err)
	}

	// A canonical replacement may remove both old physical rows and add a
	// complete self+depth set under new opaque keys.
	fullReplacement := map[string]driver.LockState{}
	for _, row := range childRows {
		replacement := newRow(row, "replacement/"+row.Row.Key)
		fullReplacement[keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: replacement.Table, Key: replacement.Row.Key}})] = state(replacement, true)
	}
	for _, row := range childRows {
		rowKey := driver.DerivedRowKey{Table: family.Table, Key: row.Row.Key}
		id := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &rowKey})
		fullReplacement[id] = driver.LockState{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &rowKey}}
	}
	complete, err = owner.selectionFamilyCompleteForCommit(ctx, conn, meta.Incarnation, family, fullReplacement)
	if err != nil || !complete {
		t.Fatalf("complete multi-row replacement rejected: complete=%t err=%v", complete, err)
	}

	// A partial replacement that drops the self row must remain unavailable.
	partial := map[string]driver.LockState{}
	depth := newRow(childRows[1], "replacement/partial-depth")
	partial[keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: depth.Table, Key: depth.Row.Key}})] = state(depth, true)
	depthKey := driver.DerivedRowKey{Table: family.Table, Key: childRows[1].Row.Key}
	partial[keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &depthKey})] = driver.LockState{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &depthKey}}
	complete, err = owner.selectionFamilyCompleteForCommit(ctx, conn, meta.Incarnation, family, partial)
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("partial multi-row replacement certified without the unchanged self row")
	}
}

// A raw bead delete closes its source envelopes through changedSources even
// when no typed projection tombstone is staged.  Ready ancestor coverage must
// fail closed in that case; the canonical pipeline path is covered separately
// by the complete remove-plus-replacement checks above.
func TestSelectionFamilyCoverageRejectsRawDeleteWithoutTypedTombstones(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	ref := testRef("selection-raw-delete")
	tx, header := beginSourceTest(t, s, lease, ref)
	body := []byte(`{"id":"selection-raw-delete","type":"issue"}`)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, header)
	decode, project := selectionBackfillRaceProjection(header.Incarnation, nil)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, s)
	ancestor := sqliteSelectionFamilies()[3]
	ready, err := selectionFamilyReady(ctx, s.db, meta.Incarnation, ancestor, revKey(mustUint(meta.Head)))
	if err != nil || !ready {
		t.Fatalf("typed ancestor control was not ready before delete: ready=%t err=%v", ready, err)
	}

	remove, removeHeader := beginSourceTest(t, s, lease, ref)
	if err := remove.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, remove, removeHeader)
	meta = getMeta(t, ctx, s)
	ready, err = selectionFamilyReady(ctx, s.db, meta.Incarnation, ancestor, revKey(mustUint(meta.Head)))
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("raw bead delete carried ready ancestor coverage without typed tombstones")
	}
}

type selectionCountingQueryer struct {
	conn       *sql.Conn
	rowQueries int
	queries    int
}

func (q *selectionCountingQueryer) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.rowQueries++
	return q.conn.QueryRowContext(ctx, query, args...)
}

func (q *selectionCountingQueryer) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.queries++
	return q.conn.QueryContext(ctx, query, args...)
}

func TestSelectionVirginMarkerUsesConstantAdmissionWork(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.db.ExecContext(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1200)
		INSERT INTO objects(key,kind) SELECT printf('marker-bead-%04d', i), 'bead' FROM n`); err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, s)
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE kind='bead'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1200 {
		t.Fatalf("seeded marker work store=%d", count)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	owner := &transaction{records: map[string]*driver.Record{
		"new": &driver.Record{Kind: "bead"},
	}}
	counter := &selectionCountingQueryer{conn: conn}
	virgin, err := owner.selectionNeverSourcedBefore(ctx, counter, meta)
	if err != nil {
		t.Fatal(err)
	}
	if !virgin {
		t.Fatal("virgin marker did not admit the first affected bead")
	}
	if counter.rowQueries != 1 || counter.queries != 0 {
		t.Fatalf("virgin admission work grew beyond marker lookup: row_queries=%d queries=%d", counter.rowQueries, counter.queries)
	}
}

// The callbacks here exercise the storage backfill fence. Production supplies
// the core's body and closure decoder; these typed fixture rows make a real
// first page durable before a concurrent ordinary source publication.
func selectionBackfillRaceProjection(incarnation string, pause func()) (driver.GraphSourceDecoder, driver.GraphSourceProjection) {
	makeRow := func(record driver.Record, table driver.DerivedTable, kind driver.SelectionFactKind, facts []driver.SelectionFact) driver.DerivedSelectionRow {
		key := driver.DerivedRowKey{Table: table, Key: "backfill:" + string(table) + ":" + record.Ref.ID}
		value := []byte("typed-backfill-fixture")
		index := &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key,
			Incarnation: incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
			Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref},
				Revision: record.Revision, Present: true, Digest: driver.SelectionDigest(record.Body)}}
		if kind == "" {
			index.Bead = &driver.SelectionBead{Ref: record.Ref, Type: "task"}
		} else {
			index.Facts = &driver.SelectionFacts{Kind: kind, Rows: facts}
		}
		return driver.DerivedSelectionRow{Table: table, Row: driver.DerivedRow{Key: key.Key, Value: value, Index: index}}
	}
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		if pause != nil {
			pause()
		}
		return driver.GraphSourceMetadata{Selection: []driver.DerivedSelectionRow{
			makeRow(record, driver.DerivedSortKey, "", nil),
			makeRow(record, driver.DerivedAttribute, driver.SelectionLabelFact, nil),
			makeRow(record, driver.DerivedReferenceIndex, driver.SelectionLinkFact, nil),
		}}, nil
	}
	project := driver.GraphSourceProjection{
		Families: []driver.SelectionProjectionFamily{
			{Table: driver.DerivedSortKey, Version: driver.SelectionIndexVersion},
			{Table: driver.DerivedAttribute, Kind: driver.SelectionLabelFact, Version: driver.SelectionIndexVersion},
			{Table: driver.DerivedReferenceIndex, Kind: driver.SelectionLinkFact, Version: driver.SelectionIndexVersion},
			{Table: driver.DerivedAncestorClosure, Kind: driver.SelectionAncestorFact, Version: driver.SelectionIndexVersion},
		},
		MaxPageRows: 5000,
		Inputs:      func(driver.Record) ([]driver.DerivedRowRange, error) { return nil, nil },
		Decode: func(record driver.Record, _ []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
			ref := record.Ref
			return []driver.DerivedSelectionRow{makeRow(record, driver.DerivedAncestorClosure, driver.SelectionAncestorFact,
				[]driver.SelectionFact{{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &ref, Depth: 0}})}, nil
		},
	}
	return decode, project
}

func TestSelectionBackfillOrdinarySaveBetweenShortPagesInvalidates(t *testing.T) {
	for _, mutation := range []string{"create", "update", "delete"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			stopRenew := make(chan struct{})
			defer close(stopRenew)
			go func() {
				tick := time.NewTicker(time.Second)
				defer tick.Stop()
				for {
					select {
					case <-stopRenew:
						return
					case <-tick.C:
						_ = lease.Renew(ctx)
					}
				}
			}()
			refs := make([]model.Ref, driver.SelectionBackfillPageSize+1)
			for i := range refs {
				refs[i] = testRef(fmt.Sprintf("backfill-race-%03d", i))
			}
			tx, header := beginSourceTest(t, s, lease, refs...)
			for _, ref := range refs {
				if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{}`)}, nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			commitSourceTest(t, tx, header)
			sortFamily := sqliteSelectionFamilies()[0]
			initial := getMeta(t, ctx, s)
			ready, err := selectionFamilyReady(ctx, s.db, header.Incarnation, sortFamily, revKey(mustUint(initial.Head)))
			if err != nil || ready {
				t.Fatalf("untyped source write certified sort family=%t/%v", ready, err)
			}
			atSecondPage, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseBuild := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseBuild()
			seen := 0
			decode, project := selectionBackfillRaceProjection(header.Incarnation, func() {
				seen++
				if seen == len(refs) {
					close(atSecondPage)
					<-release
				}
			})
			result := make(chan error, 1)
			go func() { result <- s.BuildGraphSourceIndexes(ctx, lease, decode, project) }()
			select {
			case <-atSecondPage:
			case <-time.After(20 * time.Second):
				releaseBuild()
				t.Fatal("selection builder did not reach second page")
			}
			var staged int
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_backfill_rows`).Scan(&staged); err != nil || staged == 0 {
				releaseBuild()
				t.Fatalf("first page was not durably staged: %d/%v", staged, err)
			}
			target := refs[0]
			if mutation == "create" {
				target = testRef("backfill-race-new")
			}
			ordinary, ordinaryHeader := beginSourceTest(t, s, lease, target)
			if mutation == "delete" {
				if err := ordinary.Delete(ctx, target); err != nil {
					releaseBuild()
					t.Fatal(err)
				}
			} else {
				if err := ordinary.PutGraphRecord(ctx, driver.Record{Ref: target, Kind: "bead", Body: []byte(`{"changed":true}`)}, nil, nil); err != nil {
					releaseBuild()
					t.Fatal(err)
				}
			}
			entry := commitSourceTest(t, ordinary, ordinaryHeader)
			releaseBuild()
			select {
			case err := <-result:
				if !errors.Is(err, driver.ErrSelectionUnavailable) {
					t.Fatalf("changed Head certified a stale selection build: %v", err)
				}
			case <-time.After(20 * time.Second):
				t.Fatal("selection builder did not release its failed attempt")
			}
			meta := getMeta(t, ctx, s)
			if meta.Head != entry.Revision {
				t.Fatalf("ordinary mutation lost: head=%s commit=%s", meta.Head, entry.Revision)
			}
			ready, err = selectionFamilyReady(ctx, s.db, meta.Incarnation, sortFamily, revKey(mustUint(meta.Head)))
			if err != nil || ready {
				t.Fatalf("stale build certified sort family=%t/%v", ready, err)
			}
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_backfill_rows`).Scan(&staged); err != nil || staged != 0 {
				t.Fatalf("failed build retained page rows: %d/%v", staged, err)
			}
		})
	}
}
