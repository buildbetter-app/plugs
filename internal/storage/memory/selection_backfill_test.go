package memory

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func startupBeadRow(record driver.Record, incarnation string) driver.DerivedSelectionRow {
	key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "startup/" + record.Ref.ID}
	row := driver.DerivedRow{Key: key.Key, Value: []byte("unchanged logical value")}
	ref := record.Ref
	row.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key, Incarnation: incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(row.Value), Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: record.Revision, Present: true, Digest: driver.SelectionDigest(record.Body)}, Bead: &driver.SelectionBead{Ref: ref, Type: "issue"}}
	return driver.DerivedSelectionRow{Table: key.Table, Row: row}
}

func startupSortProjection() driver.GraphSourceProjection {
	return driver.GraphSourceProjection{Families: []driver.SelectionProjectionFamily{{Table: driver.DerivedSortKey, Version: driver.SelectionIndexVersion}}, MaxPageRows: 1, Inputs: func(driver.Record) ([]driver.DerivedRowRange, error) { return nil, nil }, Decode: func(driver.Record, []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) { return nil, nil }}
}

func TestSelectionStartupEmptyUnbuiltAndExplicitEmptyBuild(t *testing.T) {
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	family := startupSortProjection().Families[0]
	if !errors.Is((selectionView{state: s.state}).requireSelectionCoverage(family, 0), driver.ErrSelectionUnavailable) {
		t.Fatal("new empty store inferred ready coverage")
	}
	decode := func(driver.Record) (driver.GraphSourceMetadata, error) {
		t.Fatal("empty build decoded a source")
		return driver.GraphSourceMetadata{}, nil
	}
	if err := s.BuildGraphSourceIndexes(context.Background(), lease, decode, startupSortProjection()); err != nil {
		t.Fatal(err)
	}
	if err := (selectionView{state: s.state}).requireSelectionCoverage(family, 0); err != nil {
		t.Fatal(err)
	}
	label := driver.SelectionProjectionFamily{Table: driver.DerivedAttribute, Kind: driver.SelectionLabelFact, Version: driver.SelectionIndexVersion}
	if !errors.Is((selectionView{state: s.state}).requireSelectionCoverage(label, 0), driver.ErrSelectionUnavailable) {
		t.Fatal("sort-only build certified another family")
	}
}

func TestSelectionStartupIndependentRangesDoNotBridgeARepairedImage(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	tx, header := beginSourceTest(t, s, lease)
	commitSourceTest(t, tx, header) // an actual source-free commit is not a build.
	family := startupSortProjection().Families[0]
	if !errors.Is((selectionView{state: s.state}).requireSelectionCoverage(family, 1), driver.ErrSelectionUnavailable) {
		t.Fatal("source-free history became ready")
	}
	ref := sourceRef("startup")
	key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "startup/" + ref.ID}
	put := func(annotated bool) {
		r := sourceReader(t, s, nil)
		observed, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}})
		if err != nil {
			t.Fatal(err)
		}
		tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}}, true, ref)
		record := driver.Record{Ref: ref, Kind: "bead", Revision: observed[0].Revision, Body: []byte(`{"opaque":"source"}`)}
		if err := tx.PutGraphRecord(ctx, record, nil, nil); err != nil {
			t.Fatal(err)
		}
		if annotated {
			row := startupBeadRow(record, tx.Snapshot().Incarnation)
			if err := tx.PutSelectionDerived(ctx, row.Table, nil, []driver.DerivedRow{row.Row}); err != nil {
				t.Fatal(err)
			}
		}
		commitSourceTest(t, tx, header)
	}
	put(true)
	first := s.state
	if err := (selectionView{state: first}).requireSelectionCoverage(family, 2); err != nil {
		t.Fatal(err)
	}
	for _, at := range []uint64{0, 1} {
		if !errors.Is((selectionView{state: first}).requireSelectionCoverage(family, at), driver.ErrSelectionUnavailable) {
			t.Fatal("first typed publication certified predecessor history")
		}
	}
	put(false) // accepted source save breaks the annotation's source revision.
	put(true)  // a repaired final image must not bridge that missing revision.
	for _, at := range []uint64{3, 4} {
		if !errors.Is((selectionView{state: s.state}).requireSelectionCoverage(family, at), driver.ErrSelectionUnavailable) {
			t.Fatal("repaired image recertified a range")
		}
	}
	before := s.state
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		return driver.GraphSourceMetadata{Selection: []driver.DerivedSelectionRow{startupBeadRow(record, before.meta.Incarnation)}}, nil
	}
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, startupSortProjection()); err != nil {
		t.Fatal(err)
	}
	if before.meta != s.state.meta || !reflect.DeepEqual(before.records, s.state.records) || !reflect.DeepEqual(before.log, s.state.log) {
		t.Fatal("selection-only activation changed source metadata/history")
	}
	for _, at := range []uint64{2, 4} {
		if err := (selectionView{state: s.state}).requireSelectionCoverage(family, at); err != nil {
			t.Fatal(err)
		}
	}
	if !errors.Is((selectionView{state: s.state}).requireSelectionCoverage(family, 3), driver.ErrSelectionUnavailable) {
		t.Fatal("startup certified a prebuild gap")
	}
	if err := (selectionView{state: first}).requireSelectionCoverage(family, 2); err != nil {
		t.Fatal("later range mutated a retained state")
	}
	put(true)
	if err := (selectionView{state: s.state}).requireSelectionCoverage(family, 5); err != nil {
		t.Fatal("complete ordinary publication lost the build floor")
	}
	if s.state.selection.ranges[family][1].First.String() != "4" {
		t.Fatal("ordinary publication reset the startup floor")
	}
}

func TestSelectionStartupRefusesOrphanPublishedRows(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	ref := sourceRef("orphan-owner")
	key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "extra/orphan"}
	canonical := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "startup/" + ref.ID}
	tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}, {Kind: driver.LockDerived, Derived: &canonical}}, true, ref)
	record := driver.Record{Ref: ref, Kind: "bead", Revision: model.Natural64(0), Body: []byte(`{}`)}
	if err := tx.PutGraphRecord(ctx, record, nil, nil); err != nil {
		t.Fatal(err)
	}
	row := startupBeadRow(record, tx.Snapshot().Incarnation)
	row.Row.Key = key.Key
	row.Row.Index.Row = key
	// Two actual typed bead rows for this source leave the family unavailable;
	// the canonical one must not make the extra census entry disappear.
	if err := tx.PutSelectionDerived(ctx, key.Table, nil, []driver.DerivedRow{startupBeadRow(record, tx.Snapshot().Incarnation).Row, row.Row}); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, header)
	before := s.state
	family := startupSortProjection().Families[0]
	if !errors.Is((selectionView{state: before}).requireSelectionCoverage(family, mustSelectionHead(before)), driver.ErrSelectionUnavailable) {
		t.Fatal("orphan-negative must reach the unavailable-family rebuild")
	}
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		return driver.GraphSourceMetadata{Selection: []driver.DerivedSelectionRow{startupBeadRow(record, before.meta.Incarnation)}}, nil
	}
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, startupSortProjection()); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("orphan typed row survived certification: %v", err)
	}
	if before != s.state {
		t.Fatal("failed orphan census published partial selection state")
	}
}
