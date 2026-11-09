package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

func pgStartupSortProjection() driver.GraphSourceProjection {
	return driver.GraphSourceProjection{Families: []driver.SelectionProjectionFamily{selectionBeadFamily()}, MaxPageRows: 1,
		Inputs: func(driver.Record) ([]driver.DerivedRowRange, error) { return nil, nil },
		Decode: func(driver.Record, []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) { return nil, nil },
	}
}
func pgStartupSortRow(record driver.Record, incarnation string) driver.DerivedSelectionRow {
	key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "startup/" + record.Ref.ID}
	row := driver.DerivedRow{Key: key.Key, Value: []byte("unchanged logical value")}
	ref := record.Ref
	row.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key, Incarnation: incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(row.Value), Source: driver.SelectionSource{Key: testLock(ref), Revision: record.Revision, Present: true, Digest: driver.SelectionDigest(record.Body)}, Bead: &driver.SelectionBead{Ref: ref, Type: "issue"}}
	return driver.DerivedSelectionRow{Table: key.Table, Row: row}
}
func pgStartupRead(t *testing.T, s *Store) *reader { t.Helper(); return testRead(t, s, nil).(*reader) }
func pgStartupCountPlan(r driver.Reader) driver.SelectionPlan {
	p := selectionSQLPlan(r)
	p.Mode = driver.SelectionCountMode
	p.Page = nil
	return p
}
func requirePGFamily(t *testing.T, r *reader, f driver.SelectionProjectionFamily, at uint64, ready bool) {
	t.Helper()
	err := r.requireSelectionCoverage(context.Background(), f, at)
	if ready && err != nil || !ready && !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("family %s/%s revision%d ready=%t: %v", f.Table, f.Kind, at, ready, err)
	}
}

func TestSelectionStartupExplicitEmptyFamilyAndSourceFreeHistory(t *testing.T) {
	for _, head := range []uint64{0, 1} {
		t.Run(fmt.Sprint(head), func(t *testing.T) {
			ctx := context.Background()
			s := openTest(t, freshDSN(t))
			lease := testLease(t, s)
			if head == 1 {
				tx, h := beginTest(t, s, lease)
				if _, err := tx.Commit(ctx, h); err != nil {
					t.Fatal(err)
				}
			}
			before := pgStartupRead(t, s)
			requirePGFamily(t, before, selectionBeadFamily(), head, false)
			p := pgStartupCountPlan(before)
			p.Membership = driver.SelectionExpr{Op: driver.SelectionFalse}
			if _, err := before.Select(ctx, p); !errors.Is(err, driver.ErrSelectionUnavailable) {
				t.Fatalf("false plan on unbuilt store: %v", err)
			}
			decode := func(driver.Record) (driver.GraphSourceMetadata, error) {
				t.Fatal("empty startup decoded a record")
				return driver.GraphSourceMetadata{}, nil
			}
			if err := s.BuildGraphSourceIndexes(ctx, lease, decode, pgStartupSortProjection()); err != nil {
				t.Fatal(err)
			}
			after := pgStartupRead(t, s)
			requirePGFamily(t, after, selectionBeadFamily(), head, true)
			requirePGFamily(t, after, selectionFamilies()[1], head, false)
			requirePGFamily(t, before, selectionBeadFamily(), head, false) // actual retained SQL snapshot
			if after.meta.Head.String() != before.meta.Head.String() {
				t.Fatal("startup allocated a revision")
			}
			if head > 0 {
				requirePGFamily(t, after, selectionBeadFamily(), 0, false)
			}
			if got, err := after.Select(ctx, pgStartupCountPlan(after)); err != nil || got.Total != 0 {
				t.Fatalf("explicit empty startup=%+v/%v", got, err)
			}
		})
	}
}

func TestSelectionStartupPreparationRefusalPreservesGraphActivation(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	parent, child := testRef("pl-optional-parent"), testRef("pl-optional-child")
	tx, header := beginSourceTest(t, s, lease, parent, child)
	for _, doc := range []pipeline.Document{
		{ID: parent.ID, Type: "issue", Fields: map[string]any{}},
		{ID: child.ID, Type: "issue", Parent: &parent, Links: []pipeline.Link{{Kind: "blocks", To: parent}}, Fields: map[string]any{}},
	} {
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		// An actual legacy raw publication supplies neither graph declarations
		// nor selection annotations. Startup must decode the real source images.
		if err = tx.Put(ctx, sourceRecord(doc.ID, string(body))); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, header)
	before := pgStartupRead(t, s)
	if _, _, err := before.HierarchyChildren(ctx, []model.Ref{parent}, 10); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("legacy Parent unexpectedly ready: %v", err)
	}
	logical := pgStartupLogicalImage(t, s)
	expected, err := before.Get(ctx, []model.Ref{child})
	if err != nil || len(expected) != 1 {
		t.Fatalf("legacy child=%+v/%v", expected, err)
	}
	decodeCalls := 0
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		decodeCalls++
		doc, err := pipeline.DecodeDocument(record.Body)
		if err != nil {
			return driver.GraphSourceMetadata{}, err
		}
		metadata := driver.GraphSourceMetadata{Parent: doc.Parent}
		for _, link := range doc.Links {
			metadata.Links = append(metadata.Links, driver.SourceLink{Kind: link.Kind, To: link.To})
		}
		return metadata, nil
	}
	invalid := pgStartupSortProjection()
	invalid.Inputs = nil // Refuses in adapter preparation, before any hook exists.
	if err = s.BuildGraphSourceIndexes(ctx, lease, decode, invalid); !errors.Is(err, driver.ErrSelectionUnavailable) || !sourceindex.IsSelectionBackfillRefused(err) || decodeCalls != 2 {
		t.Fatalf("optional preparation refusal=%v; completed=%t graph decoded=%d", err, sourceindex.IsSelectionBackfillRefused(err), decodeCalls)
	}
	after := pgStartupRead(t, s)
	for name, lookup := range map[string]func() ([]driver.Record, bool, error){
		"Parent": func() ([]driver.Record, bool, error) { return after.HierarchyChildren(ctx, []model.Ref{parent}, 10) },
		"Links": func() ([]driver.Record, bool, error) {
			return after.InverseSourceLinks(ctx, []model.Ref{parent}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 10)
		},
	} {
		rows, more, err := lookup()
		if err != nil || more || !reflect.DeepEqual(rows, expected) {
			t.Fatalf("optional preparation refusal lost %s graph: rows=%+v more=%t err=%v", name, rows, more, err)
		}
	}
	for _, family := range selectionFamilies() {
		requirePGFamily(t, after, family, after.at, false)
	}
	if after.meta != before.meta || !reflect.DeepEqual(logical, pgStartupLogicalImage(t, s)) {
		t.Fatal("optional refusal changed Head or logical history bytes")
	}
	if _, _, err := before.HierarchyChildren(ctx, []model.Ref{parent}, 10); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("retained snapshot borrowed later graph activation: %v", err)
	}
}

func TestSelectionStartupPreparationRefusalDoesNotMaskGraphFailure(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	ref := testRef("pl-optional-graph-failure")
	tx, header := beginSourceTest(t, s, lease, ref)
	if err := tx.Put(ctx, sourceRecord(ref.ID, `{}`)); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, header)
	before, logical := pgStartupRead(t, s), pgStartupLogicalImage(t, s)
	invalid := pgStartupSortProjection()
	invalid.Inputs = nil
	graphFailure := fmt.Errorf("incompatible graph decoder: %w", driver.ErrUnsupported)
	err := s.BuildGraphSourceIndexes(ctx, lease, func(driver.Record) (driver.GraphSourceMetadata, error) {
		return driver.GraphSourceMetadata{}, graphFailure
	}, invalid)
	if !errors.Is(err, graphFailure) || sourceindex.IsSelectionBackfillRefused(err) {
		t.Fatalf("selection preparation refusal masked graph failure: %v", err)
	}
	after := pgStartupRead(t, s)
	if _, _, err := after.HierarchyChildren(ctx, []model.Ref{ref}, 10); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("failed graph build certified Parent: %v", err)
	}
	if _, _, err := after.InverseSourceLinks(ctx, []model.Ref{ref}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 10); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("failed graph build certified Links: %v", err)
	}
	for _, family := range selectionFamilies() {
		requirePGFamily(t, after, family, after.at, false)
	}
	if after.meta != before.meta || !reflect.DeepEqual(logical, pgStartupLogicalImage(t, s)) {
		t.Fatal("failed graph build changed Head or logical history bytes")
	}
}

// A source-only save is accepted while its optional selection range becomes
// unavailable. A later typed image does not heal the missing temporal interval.
func TestSelectionStartupFamilyFloorsSurviveRepairAndContinuation(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	ref := testRef("pl-startup-floor")
	key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "startup/" + ref.ID}
	put := func(typed bool) {
		tx, h := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}}, true, ref)
		prior, exists, err := tx.recordAt(ctx, selectionRefKey(ref))
		if err != nil {
			t.Fatal(err)
		}
		record := sourceRecord(ref.ID, `{"opaque":"body"}`)
		record.Revision = model.Natural64(0)
		if exists {
			record.Revision = prior.record.Revision
		}
		if err = tx.PutGraphRecord(ctx, record, nil, nil); err != nil {
			t.Fatal(err)
		}
		if typed {
			row := pgStartupSortRow(record, h.Incarnation)
			if err = tx.PutSelectionDerived(ctx, row.Table, nil, []driver.DerivedRow{row.Row}); err != nil {
				t.Fatal(err)
			}
		}
		commitSourceTest(t, tx, h)
	}
	tx, h := beginTest(t, s, lease)
	if _, err := tx.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	put(true)
	first := pgStartupRead(t, s)
	requirePGFamily(t, first, selectionBeadFamily(), 2, true)
	requirePGFamily(t, first, selectionBeadFamily(), 1, false)
	put(false)
	put(true)
	before := pgStartupRead(t, s)
	requirePGFamily(t, before, selectionBeadFamily(), 3, false)
	requirePGFamily(t, before, selectionBeadFamily(), 4, false)
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		return driver.GraphSourceMetadata{Selection: []driver.DerivedSelectionRow{pgStartupSortRow(record, before.meta.Incarnation)}}, nil
	}
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, pgStartupSortProjection()); err != nil {
		t.Fatal(err)
	}
	built := pgStartupRead(t, s)
	requirePGFamily(t, built, selectionBeadFamily(), 2, true)
	requirePGFamily(t, built, selectionBeadFamily(), 3, false)
	requirePGFamily(t, built, selectionBeadFamily(), 4, true)
	put(true)
	after := pgStartupRead(t, s)
	requirePGFamily(t, after, selectionBeadFamily(), 5, true)
	requirePGFamily(t, after, selectionBeadFamily(), 3, false)
	var floor string
	if err := s.db.QueryRowContext(ctx, `SELECT built_from::text FROM plug_postgres.selection_coverage WHERE family_table='sort_key' ORDER BY start_revision DESC LIMIT 1`).Scan(&floor); err != nil || floor != "4" {
		t.Fatalf("carried family floor=%s/%v", floor, err)
	}
	requirePGFamily(t, first, selectionBeadFamily(), 2, true)
}

// Replay an actual create/delete history into a fresh database. A complete
// typed image after import must not regain the new-store lifetime privilege.
func TestSelectionStartupHistoryImportCannotReseedVirginCoverage(t *testing.T) {
	ctx := context.Background()
	source := openTest(t, freshDSN(t))
	sourceLease := testLease(t, source)
	ref := testRef("pl-startup-import-deleted")
	create, header := beginSourceTest(t, source, sourceLease, ref)
	if err := create.PutGraphRecord(ctx, sourceRecord(ref.ID, `{"title":"retained imported bytes"}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	first := commitSourceTest(t, create, header)
	image, err := pgStartupRead(t, source).Get(ctx, []model.Ref{ref})
	if err != nil || len(image) != 1 {
		t.Fatalf("actual import source=%v/%v", image, err)
	}
	remove, header := beginSourceTest(t, source, sourceLease, ref)
	if err = remove.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	last := commitSourceTest(t, remove, header)
	history := driver.OfflineHistory{Source: "postgres-selection-startup", Verified: true, Complete: true, First: 1, Head: 2, Batches: []driver.SourceHistoryBatch{
		{Sequence: 1, CommitIDs: []string{first.Revision.String()}, Changes: []driver.SourceHistoryChange{{After: &image[0]}}},
		{Sequence: 2, CommitIDs: []string{last.Revision.String()}, Changes: []driver.SourceHistoryChange{{Before: &image[0]}}},
	}}
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	sourceMeta, err := source.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: sourceMeta.StoreID, Method: "offline.restore.import", IdempotencyKey: "postgres-selection-startup-history", ArchiveDigest: strings.Repeat("b", 64), DerivedDigest: strings.Repeat("c", 64), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.Format(time.RFC3339Nano)}
	profile.HistoryDigest, err = driver.OfflineHistoryDigest(history)
	if err != nil {
		t.Fatal(err)
	}
	marker := model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))
	// Preparation must precede all staging, including sentinel bumps.
	importer, h := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockRecord, Ref: &marker}}, false, ref)
	meta, err := importer.PrepareOfflineHistory(ctx, profile, history, when)
	if err != nil {
		t.Fatal(err)
	}
	sentinels := []driver.Sentinel{}
	for _, key := range h.LockKeys {
		if key.Kind == driver.LockSentinel {
			sentinels = append(sentinels, key.Sentinel)
		}
	}
	if err = importer.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	h.Incarnation, h.Kind, h.Time, h.Method, h.IdempotencyKey, h.Principal = meta.Incarnation, model.ChangesetRestore, when, profile.Method, profile.IdempotencyKey, profile.Admission.Actor
	h.RequestHash, err = profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = importer.CommitOfflineHistory(ctx, h); err != nil {
		t.Fatal(err)
	}
	imported := pgStartupRead(t, s)
	versions, err := imported.Versions(ctx, ref, driver.PageReq{Limit: 10})
	if err != nil || len(versions.Items) != 2 {
		t.Fatalf("imported create/delete history=%+v/%v", versions, err)
	}
	// A new current bead has a complete typed sort image, but the imported
	// lifetime is not a virgin predecessor, even with no surviving old beads.
	current := sourceRecord("pl-startup-after-import", `{"title":"new typed image"}`)
	current.Revision = model.Natural64(0)
	row := pgStartupSortRow(current, meta.Incarnation)
	key := row.Row.Index.Row
	tx, h := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}}, true, current.Ref)
	if err = tx.PutGraphRecord(ctx, current, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err = tx.PutSelectionDerived(ctx, row.Table, nil, []driver.DerivedRow{row.Row}); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	before := pgStartupRead(t, s)
	for at := uint64(0); at <= 4; at++ {
		requirePGFamily(t, before, selectionBeadFamily(), at, false)
	}
	// The ordinary typed publication advanced Head from the imported snapshot.
	// Compare complete version pages across startup at the SAME Head, while
	// separately proving that publication preserved the imported version items.
	beforeVersions, err := before.Versions(ctx, ref, driver.PageReq{Limit: 10})
	if err != nil || !reflect.DeepEqual(versions.Items, beforeVersions.Items) || beforeVersions.AsOf.String() != "4" || beforeVersions.NextCursor != nil {
		t.Fatalf("typed publication changed imported versions: before=%+v after=%+v err=%v", versions, beforeVersions, err)
	}
	logical := pgStartupLogicalImage(t, s)
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		return driver.GraphSourceMetadata{Selection: []driver.DerivedSelectionRow{pgStartupSortRow(record, meta.Incarnation)}}, nil
	}
	if err = s.BuildGraphSourceIndexes(ctx, lease, decode, pgStartupSortProjection()); err != nil {
		t.Fatal(err)
	}
	after := pgStartupRead(t, s)
	requirePGFamily(t, after, selectionBeadFamily(), 4, true)
	requirePGFamily(t, after, selectionBeadFamily(), 3, false)
	got, err := after.Versions(ctx, ref, driver.PageReq{Limit: 10})
	if err != nil || !reflect.DeepEqual(beforeVersions, got) {
		t.Fatalf("startup changed same-Head imported version page: before=%+v after=%+v err=%v", beforeVersions, got, err)
	}
	if !reflect.DeepEqual(logical, pgStartupLogicalImage(t, s)) {
		t.Fatal("startup changed imported logical bytes")
	}
	retained, err := imported.Versions(ctx, ref, driver.PageReq{Limit: 10})
	if err != nil || !reflect.DeepEqual(versions, retained) {
		t.Fatalf("startup changed retained imported version page: before=%+v after=%+v err=%v", versions, retained, err)
	}
}

// Use real core document/closure codecs to make a populated pre-annotation
// store. The driver is responsible only for envelopes and scalar relations.
func pgStartupLegacyChain(t *testing.T, s *Store, lease driver.ActiveLease, n int) []model.Ref {
	t.Helper()
	ctx := context.Background()
	refs := make([]model.Ref, n)
	extra := []driver.LockKey{}
	closures := []driver.DerivedRow{}
	closureSentinel, err := driver.NewClosureSentinel("pg-startup")
	if err != nil {
		t.Fatal(err)
	}
	extra = append(extra, driver.LockKey{Kind: driver.LockSentinel, Sentinel: closureSentinel})
	for i := range refs {
		refs[i] = testRef(fmt.Sprintf("pl-startup-%04d", i))
		chain := []int{i}
		if i == 1 {
			chain = append(chain, 0)
		}
		if i == 2 {
			chain = append(chain, 1, 0)
		}
		for depth, target := range chain {
			row, e := pipeline.AncestorRow(refs[i].ID, fmt.Sprintf("pl-startup-%04d", target), uint64(depth))
			if e != nil {
				t.Fatal(e)
			}
			closures = append(closures, row)
			key := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}
			extra = append(extra, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
		}
	}
	tx, h := beginSourceTestWithLocks(t, s, lease, extra, true, refs...)
	for i, ref := range refs {
		var parent *model.Ref
		if i == 1 {
			parent = &refs[0]
		}
		if i == 2 {
			parent = &refs[1]
		}
		body, e := json.Marshal(pipeline.Document{ID: ref.ID, Type: "issue", Fields: map[string]any{}, Parent: parent})
		if e != nil {
			t.Fatal(e)
		}
		if e = tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, parent, nil); e != nil {
			t.Fatal(e)
		}
	}
	if err = tx.PutDerived(ctx, driver.DerivedAncestorClosure, nil, closures); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	return refs
}
func pgStartupLogicalImage(t *testing.T, s *Store) [][]byte {
	t.Helper()
	rows, err := s.db.QueryContext(context.Background(), `SELECT value FROM plug_postgres.states ORDER BY key,revision`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := [][]byte{}
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			t.Fatal(err)
		}
		result = append(result, bytes.Clone(b))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestSelectionStartupCoreLegacyParentChainPreservesBytesAndHistory(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	refs := pgStartupLegacyChain(t, s, lease, 131)
	before := pgStartupRead(t, s)
	image := pgStartupLogicalImage(t, s)
	records, err := before.Get(ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	log, err := before.Changesets(ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	closureBefore, err := before.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range closureBefore {
		if row.Index != nil {
			t.Fatal("legacy fixture already annotated")
		}
	}
	if err = pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
		t.Fatal(err)
	}
	after := pgStartupRead(t, s)
	if after.meta != before.meta || !reflect.DeepEqual(image, pgStartupLogicalImage(t, s)) {
		t.Fatal("startup changed logical bytes or metadata")
	}
	gotRecords, err := after.Get(ctx, refs)
	if err != nil || !reflect.DeepEqual(records, gotRecords) {
		t.Fatal("startup changed source bytes")
	}
	gotLog, err := after.Changesets(ctx, nil, 1000)
	if err != nil || !reflect.DeepEqual(log, gotLog) {
		t.Fatal("startup changed changesets")
	}
	for _, family := range selectionFamilies() {
		requirePGFamily(t, after, family, 1, true)
		requirePGFamily(t, after, family, 0, false)
	}
	closureAfter, err := after.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil || len(closureAfter) != len(closureBefore) {
		t.Fatal("closure membership changed")
	}
	keys := []driver.LockKey{}
	for i, row := range closureAfter {
		if row.Key != closureBefore[i].Key || !bytes.Equal(row.Value, closureBefore[i].Value) || row.Index == nil || row.Index.Source.Revision.String() != "1" || row.Index.Validate(driver.DerivedAncestorClosure, row) != nil {
			t.Fatal("startup annotation did not bind exact logical/source bytes")
		}
		key := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
	}
	observed, err := after.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range observed {
		if state.Revision.String() != "1" || state.Derived == nil || state.Derived.Index == nil {
			t.Fatal("Observe lost logical token or startup annotation")
		}
	}
	// A retained snapshot at the very same Head cannot borrow the later build.
	retained, err := before.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range retained {
		if row.Index != nil {
			t.Fatal("retained snapshot borrowed annotation")
		}
	}
	if result, err := after.Select(ctx, pgStartupCountPlan(after)); err != nil || result.Total != uint64(len(refs)) {
		t.Fatalf("real core startup count=%+v/%v", result, err)
	}
}

type pgStartupTapStore struct {
	driver.Store
	tap func(int) error
}

func (s pgStartupTapStore) BuildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder, projects ...driver.GraphSourceProjection) error {
	n := 0
	return s.Store.(driver.GraphSourceIndexBuilder).BuildGraphSourceIndexes(ctx, lease, func(record driver.Record) (driver.GraphSourceMetadata, error) {
		n++
		if s.tap != nil {
			if err := s.tap(n); err != nil {
				return driver.GraphSourceMetadata{}, err
			}
		}
		return decode(record)
	}, projects...)
}

func TestSelectionStartupPagesArePrivateAndFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	pgStartupLegacyChain(t, s, lease, 130)
	before := pgStartupRead(t, s)
	image := pgStartupLogicalImage(t, s)
	injected := errors.New("injected second source-page failure")
	reached := false
	tap := pgStartupTapStore{Store: s, tap: func(n int) error {
		if n != driver.SelectionBackfillPageSize+1 {
			return nil
		}
		reached = true
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.selection_rows WHERE layout=2`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("uncommitted page visible rows=%d/%v", count, err)
		}
		requirePGFamily(t, pgStartupRead(t, s), selectionBeadFamily(), 1, false)
		return injected
	}}
	if err := pipeline.BuildSourceIndexes(ctx, tap, lease); !errors.Is(err, injected) || !reached {
		t.Fatalf("injected lifecycle=%v reached=%t", err, reached)
	}
	if !reflect.DeepEqual(image, pgStartupLogicalImage(t, s)) {
		t.Fatal("failed build changed logical state")
	}
	after := pgStartupRead(t, s)
	if after.meta != before.meta {
		t.Fatal("failed build changed Head")
	}
	requirePGFamily(t, after, selectionBeadFamily(), 1, false)
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.selection_rows WHERE layout=2`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed build retained pages=%d/%v", count, err)
	}
	if err := pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
		t.Fatal(err)
	}
	requirePGFamily(t, pgStartupRead(t, s), selectionBeadFamily(), 1, true)
}

// Publish only one real core-produced, nonempty Label projection in a populated
// legacy store. The family is incomplete, but its current-Head physical parent
// already has fact children when startup clears it.
func pgStartupPartialFactFamily(t *testing.T, s *Store, lease driver.ActiveLease, count int) driver.DerivedRowKey {
	t.Helper()
	ctx := context.Background()
	refs := pgStartupLegacyChain(t, s, lease, count)
	r := pgStartupRead(t, s)
	old, err := r.Get(ctx, []model.Ref{refs[0]})
	if err != nil || len(old) != 1 {
		t.Fatalf("partial source=%v/%v", old, err)
	}
	var doc pipeline.Document
	if err = json.Unmarshal(old[0].Body, &doc); err != nil {
		t.Fatal(err)
	}
	doc.Labels = []string{"partially-published"}
	record := old[0]
	record.Body, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := pipeline.PrepareSourceRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	projections, err := plan.SelectionRows(r.meta.Incarnation, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range projections {
		if row.Index == nil || row.Index.Facts == nil || row.Index.Facts.Kind != driver.SelectionLabelFact {
			continue
		}
		if len(row.Index.Facts.Rows) == 0 {
			t.Fatal("core did not produce the nonempty partial Label fact")
		}
		key := row.Index.Row
		closure, err := driver.NewClosureSentinel("pg-startup-partial-fact")
		if err != nil {
			t.Fatal(err)
		}
		tx, h := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}, {Kind: driver.LockSentinel, Sentinel: closure}}, true, record.Ref)
		if err = tx.PutGraphRecord(ctx, record, doc.Parent, nil); err != nil {
			t.Fatal(err)
		}
		if err = tx.PutSelectionDerived(ctx, key.Table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
		commitSourceTest(t, tx, h)
		before := pgStartupRead(t, s)
		requirePGFamily(t, before, selectionFamilies()[1], 2, false)
		var facts int
		if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.selection_facts f JOIN plug_postgres.selection_rows r USING(incarnation,row_key,start_revision) WHERE r.start_revision=2 AND r.family_table='derived_attribute' AND r.fact_kind='label' AND r.source_kind='record'`).Scan(&facts); err != nil || facts == 0 {
			t.Fatalf("partial current-Head fact children=%d/%v", facts, err)
		}
		return key
	}
	t.Fatal("core Label projection missing")
	return driver.DerivedRowKey{}
}

func pgStartupPhysicalImage(t *testing.T, s *Store) []string {
	t.Helper()
	image := []string{}
	for _, table := range []string{"selection_rows", "selection_facts"} {
		var raw string
		if err := s.db.QueryRowContext(context.Background(), `SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY incarnation,row_key,start_revision),'[]')::text FROM plug_postgres.`+table+` r`).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		image = append(image, raw)
	}
	return image
}

func TestSelectionStartupRebuildsPartialNonemptyFactFamilyAtHead(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	key := pgStartupPartialFactFamily(t, s, lease, 3)
	before := pgStartupRead(t, s)
	lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
	old, err := before.Observe(ctx, []driver.LockKey{lock})
	if err != nil {
		t.Fatal(err)
	}
	logical := pgStartupLogicalImage(t, s)
	var buildErr error
	if err = pipeline.BuildSourceIndexes(ctx, pgStartupCaptureStore{Store: s, err: &buildErr}, lease); err != nil || buildErr != nil {
		t.Fatalf("same-Head populated fact rebuild=%v/%v", err, buildErr)
	}
	after := pgStartupRead(t, s)
	requirePGFamily(t, after, selectionFamilies()[1], 2, true)
	requirePGFamily(t, before, selectionFamilies()[1], 2, false)
	got, err := after.Observe(ctx, []driver.LockKey{lock})
	if err != nil || !reflect.DeepEqual(old, got) || !reflect.DeepEqual(logical, pgStartupLogicalImage(t, s)) {
		t.Fatal("partial-family rebuild changed logical bytes, annotation or history")
	}
}

func TestSelectionStartupPopulatedFactClearRollsBack(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	pgStartupPartialFactFamily(t, s, lease, driver.SelectionBackfillPageSize+1)
	physical, logical := pgStartupPhysicalImage(t, s), pgStartupLogicalImage(t, s)
	injected := errors.New("failure after populated fact clear and first page")
	reached := false
	tap := pgStartupTapStore{Store: s, tap: func(n int) error {
		if n == driver.SelectionBackfillPageSize+1 {
			reached = true
			return injected
		}
		return nil
	}}
	if err := pipeline.BuildSourceIndexes(ctx, tap, lease); !errors.Is(err, injected) || !reached {
		t.Fatalf("populated clear rollback=%v reached=%t", err, reached)
	}
	if !reflect.DeepEqual(physical, pgStartupPhysicalImage(t, s)) || !reflect.DeepEqual(logical, pgStartupLogicalImage(t, s)) {
		t.Fatal("failed populated clear changed physical children/parents or logical history")
	}
	requirePGFamily(t, pgStartupRead(t, s), selectionFamilies()[1], 2, false)
}

func TestSelectionStartupRejectsStaleFullParentChain(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	refs := pgStartupLegacyChain(t, s, lease, 3)
	// The middle becomes a root, while the descendant's unchanged, contiguous
	// depth0/1/2 closure is now stale. Own-source/depth1 tests alone miss this.
	tx, h := beginSourceTest(t, s, lease, refs[1])
	old, _, err := tx.recordAt(ctx, selectionRefKey(refs[1]))
	if err != nil {
		t.Fatal(err)
	}
	doc := pipeline.Document{ID: refs[1].ID, Type: "issue", Fields: map[string]any{}}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	record := old.record
	record.Body = body
	if err = tx.PutGraphRecord(ctx, record, nil, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	// Fix only middle's own closure. The leaf still carries an extra ancestor.
	staleRow, err := pipeline.AncestorRow(refs[1].ID, refs[0].ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	closureKey := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: staleRow.Key}
	sentinel, _ := driver.NewClosureSentinel(refs[1].ID)
	drop, header := beginTest(t, s, lease, driver.LockKey{Kind: driver.LockDerived, Derived: &closureKey}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	if err = drop.PutDerived(ctx, closureKey.Table, []driver.DerivedRow{staleRow}, nil); err != nil {
		t.Fatal(err)
	}
	if err = drop.BumpSentinels(ctx, []driver.Sentinel{sentinel}); err != nil {
		t.Fatal(err)
	}
	if _, err = drop.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
	image := pgStartupLogicalImage(t, s)
	// BuildSourceIndexes intentionally preserves ordinary fail-closed fallback;
	// tap the actual builder error rather than the optional pipeline wrapper.
	var buildErr error
	tap := pgStartupCaptureStore{Store: s, err: &buildErr}
	if err = pipeline.BuildSourceIndexes(ctx, tap, lease); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(buildErr, driver.ErrSelectionUnavailable) {
		t.Fatalf("stale final chain admitted: %v", buildErr)
	}
	after := pgStartupRead(t, s)
	requirePGFamily(t, after, selectionBeadFamily(), 3, false)
	if !reflect.DeepEqual(image, pgStartupLogicalImage(t, s)) {
		t.Fatal("failed chain proof changed logical bytes")
	}
}

type pgStartupCaptureStore struct {
	driver.Store
	err *error
}

func (s pgStartupCaptureStore) BuildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder, projects ...driver.GraphSourceProjection) error {
	err := s.Store.(driver.GraphSourceIndexBuilder).BuildGraphSourceIndexes(ctx, lease, decode, projects...)
	*s.err = err
	return err
}

func TestSelectionStartupPreservesIndependentDerivedEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table driver.DerivedTable
		kind  driver.SelectionFactKind
	}{
		{"label", driver.DerivedAttribute, driver.SelectionLabelFact},
		{"cut_closure", driver.DerivedCutClosure, driver.SelectionAncestorFact},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pgStartupIndependentEnvelope(t, tc.table, tc.kind)
		})
	}
}

func pgStartupIndependentEnvelope(t *testing.T, table driver.DerivedTable, kind driver.SelectionFactKind) {
	t.Helper()
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	pgStartupLegacyChain(t, s, lease, 3)
	keys := seedSelectionSQLFacts(t, s, lease, map[driver.DerivedTable]driver.SelectionFacts{table: {Kind: kind}})
	key := keys[table]
	lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
	before := pgStartupRead(t, s)
	old, err := before.Observe(ctx, []driver.LockKey{lock})
	if err != nil || old[0].Derived == nil || old[0].Derived.Index == nil {
		t.Fatal("independent envelope missing")
	}
	image := pgStartupLogicalImage(t, s)
	independent := driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: key, Kind: kind}}
	if err = before.selectionFactCoverage(ctx, independent, before.at, false); err != nil {
		t.Fatalf("independent pre-startup envelope admission: %v", err)
	}
	var buildErr error
	if err = pipeline.BuildSourceIndexes(ctx, pgStartupCaptureStore{Store: s, err: &buildErr}, lease); err != nil || buildErr != nil {
		t.Fatalf("coexisting derived envelope refused=%v/%v", err, buildErr)
	}
	after := pgStartupRead(t, s)
	got, err := after.Observe(ctx, []driver.LockKey{lock})
	if err != nil || !reflect.DeepEqual(old, got) || !reflect.DeepEqual(image, pgStartupLogicalImage(t, s)) {
		t.Fatal("startup rewrote independent source/value/index/history")
	}
	plan := pgStartupCountPlan(after)
	plan.Membership = driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{independent}}
	if result, err := after.Select(ctx, plan); err != nil || result.Total != 3 {
		t.Fatalf("own-source complete envelope admission=%+v/%v", result, err)
	}
}

func TestSelectionStartupRefusesOrphanTypedCoreRowFromUnbuiltDomain(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	ref := testRef("pl-startup-orphan")
	canonical := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "startup/" + ref.ID}
	extra := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "extra/orphan"}
	tx, h := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &canonical}, {Kind: driver.LockDerived, Derived: &extra}}, true, ref)
	record := sourceRecord(ref.ID, `{"opaque":"body"}`)
	record.Revision = model.Natural64(0)
	if err := tx.PutGraphRecord(ctx, record, nil, nil); err != nil {
		t.Fatal(err)
	}
	first := pgStartupSortRow(record, h.Incarnation)
	second := first.Clone()
	second.Row.Key = extra.Key
	second.Row.Index.Row = extra
	if err := tx.PutSelectionDerived(ctx, canonical.Table, nil, []driver.DerivedRow{first.Row, second.Row}); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	before := pgStartupRead(t, s)
	requirePGFamily(t, before, selectionBeadFamily(), 1, false)
	image := pgStartupLogicalImage(t, s)
	decode := func(r driver.Record) (driver.GraphSourceMetadata, error) {
		return driver.GraphSourceMetadata{Selection: []driver.DerivedSelectionRow{pgStartupSortRow(r, h.Incarnation)}}, nil
	}
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, pgStartupSortProjection()); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("orphan census=%v", err)
	}
	requirePGFamily(t, pgStartupRead(t, s), selectionBeadFamily(), 1, false)
	if !reflect.DeepEqual(image, pgStartupLogicalImage(t, s)) {
		t.Fatal("orphan refusal changed logical state")
	}
}

func TestSelectionFreshFalseStillConsumesFamilyCoverage(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	decode := func(driver.Record) (driver.GraphSourceMetadata, error) { return driver.GraphSourceMetadata{}, nil }
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, pgStartupSortProjection()); err != nil {
		t.Fatal(err)
	}
	tx, _ := beginTest(t, s, lease)
	p := pgStartupCountPlan(tx)
	p.Membership = driver.SelectionExpr{Op: driver.SelectionFalse}
	if exists, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, p); err != nil || exists {
		t.Fatalf("healthy empty false=%t/%v", exists, err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM plug_postgres.selection_coverage WHERE family_table='sort_key'`); err != nil {
		t.Fatal(err)
	}
	if exists, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, p); !errors.Is(err, driver.ErrSelectionUnavailable) || exists {
		t.Fatalf("missing coverage became false absence=%t/%v", exists, err)
	}
}

func TestSelectionStartupAnnotationRequiresExactValueAndRespectsStaging(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	refs := pgStartupLegacyChain(t, s, lease, 3)
	if err := pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
		t.Fatal(err)
	}
	reader := pgStartupRead(t, s)
	prefix, err := pipeline.AncestorPrefix(refs[2].ID)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reader.DerivedRows(ctx, driver.DerivedAncestorClosure, prefix)
	if err != nil || len(rows) != 3 {
		t.Fatalf("closure=%v/%v", rows, err)
	}
	original := rows[0].Clone()
	different := []driver.DerivedRow{{Key: original.Key, Value: []byte("a different logical value")}}
	if err = reader.selectionAnnotations(ctx, different, driver.DerivedAncestorClosure); err != nil || different[0].Index != nil {
		t.Fatalf("mismatching value inherited physical annotation: %v", err)
	}
	key := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: original.Key}
	sentinel, _ := driver.NewClosureSentinel(refs[2].ID)
	tx, _ := beginTest(t, s, lease, driver.LockKey{Kind: driver.LockDerived, Derived: &key}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	staged := driver.DerivedRow{Key: original.Key, Value: bytes.Clone(original.Value)}
	if err = tx.PutDerived(ctx, key.Table, nil, []driver.DerivedRow{staged}); err != nil {
		t.Fatal(err)
	}
	observed, err := tx.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}})
	if err != nil || len(observed) != 1 || observed[0].Derived == nil || observed[0].Derived.Index != nil {
		t.Fatalf("committed annotation overrode staged untyped row: %+v/%v", observed, err)
	}
}

func TestSelectionFreshHiddenRecordFactRequiresItsOwnFamily(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	refs := pgStartupLegacyChain(t, s, lease, 3)
	if err := pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
		t.Fatal(err)
	}
	selectionKeys, err := pipeline.SourceSelectionRows(refs[0])
	if err != nil {
		t.Fatal(err)
	}
	label := selectionKeys[1]
	tx, _ := beginTest(t, s, lease)
	plan := pgStartupCountPlan(tx)
	plan.Membership = driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, {Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: label, Kind: driver.SelectionLabelFact}}}}
	exists, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, plan)
	if err != nil || exists {
		t.Fatalf("healthy hidden branch=%t/%v", exists, err)
	}
	if _, err = s.db.ExecContext(ctx, `DELETE FROM plug_postgres.selection_coverage WHERE family_table='derived_attribute' AND fact_kind='label'`); err != nil {
		t.Fatal(err)
	}
	exists, err = tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, plan)
	if !errors.Is(err, driver.ErrSelectionUnavailable) || exists {
		t.Fatalf("missing hidden family became false absence=%t/%v", exists, err)
	}
}

func TestSelectionStartupRangeOverflowRefusesBeforeCoreProjectionDecode(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	pgStartupLegacyChain(t, s, lease, 3)
	called := false
	project := driver.GraphSourceProjection{Families: []driver.SelectionProjectionFamily{selectionFamilies()[3]}, MaxPageRows: 1,
		Inputs: func(record driver.Record) ([]driver.DerivedRowRange, error) {
			prefix, err := pipeline.AncestorPrefix(record.Ref.ID)
			return []driver.DerivedRowRange{{Table: driver.DerivedAncestorClosure, Prefix: []byte(prefix), Max: 1}}, err
		},
		Decode: func(driver.Record, []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
			called = true
			return nil, nil
		},
	}
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		doc, err := pipeline.DecodeDocument(record.Body)
		return driver.GraphSourceMetadata{Parent: doc.Parent}, err
	}
	image := pgStartupLogicalImage(t, s)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("range overflow=%v", err)
	}
	if called || !reflect.DeepEqual(image, pgStartupLogicalImage(t, s)) {
		t.Fatal("overflow invoked core projection decode or changed state")
	}
	requirePGFamily(t, pgStartupRead(t, s), selectionFamilies()[3], 1, false)
}
