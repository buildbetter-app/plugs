package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func demandSourceKey(name string) driver.DerivedRowKey {
	return driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: name}
}

func demandSourceLock(key driver.DerivedRowKey) driver.LockKey {
	return driver.LockKey{Kind: driver.LockDerived, Derived: &key}
}

func demandDependentState(incarnation string, source, dependent driver.DerivedRowKey, sourceRevision uint64, present bool, digest string) (string, driver.LockState) {
	sourceLock := demandSourceLock(source)
	dependentLock := demandSourceLock(dependent)
	value := []byte(`{"dependent":true}`)
	row := driver.DerivedRow{Key: dependent.Key, Value: value, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: dependent, Incarnation: incarnation,
		Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
		Source: driver.SelectionSource{Key: sourceLock, Revision: model.Natural64(sourceRevision), Present: present, Digest: digest},
	}}
	return keyID(dependentLock), driver.LockState{Key: dependentLock, Present: true, Derived: &row}
}

func seedDemandSourceKeyHistory(t *testing.T, s *Store, incarnation string, key driver.DerivedRowKey, revision uint64, value []byte) {
	t.Helper()
	lock := demandSourceLock(key)
	state := driver.LockState{Key: lock, Revision: model.Natural64(revision), Present: true,
		Derived: &driver.DerivedRow{Key: key.Key, Value: value}}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(context.Background(), `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(lock), revKey(revision), data); err != nil {
		t.Fatal(err)
	}
}

func demandSourceHistoryRows(t *testing.T, s *Store, key string) (count int, start, end, sourceRevision string, present int, digest string) {
	t.Helper()
	err := s.db.QueryRowContext(context.Background(), `SELECT COUNT(*),COALESCE(MAX(start_revision),''),COALESCE(MAX(end_revision),''),COALESCE(MAX(source_revision),''),COALESCE(MAX(source_present),0),COALESCE(MAX(source_digest),'') FROM selection_source_history WHERE source_key=?`, key).Scan(&count, &start, &end, &sourceRevision, &present, &digest)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestDemandSelectionSourceHistoryColdBindAndOpenReuse(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	source := demandSourceKey("cold-source")
	dependent := demandSourceKey("cold-dependent")
	value := []byte(`{"source":"r1"}`)
	seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, value)
	dependentID, dependentState := demandDependentState(meta.Incarnation, source, dependent, 1, true, driver.SelectionDigest(value))
	tx := &transaction{reader: &reader{at: 1}, records: make(map[string]*driver.Record), derived: map[string]driver.LockState{dependentID: dependentState}}
	keyPublicationTriggerMarker(t, s)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(2))
	}); err != nil {
		t.Fatal(err)
	}
	sourceID := keyID(demandSourceLock(source))
	if count, start, end, sourceRevision, present, digest := demandSourceHistoryRows(t, s, sourceID); count != 1 || start != revKey(2) || end != "" || sourceRevision != revKey(1) || present != 1 || digest != driver.SelectionDigest(value) {
		t.Fatalf("cold demand history count=%d start=%q end=%q source_revision=%q present=%d digest=%q", count, start, end, sourceRevision, present, digest)
	}
	if count, _, _, _, _, _ := demandSourceHistoryRows(t, s, dependentID); count != 0 {
		t.Fatalf("unreferenced dependent received source history rows=%d", count)
	}

	// A second bind sees the same exact open interval and must not create a
	// duplicate or close/reopen it.
	keyPublicationTriggerMarker(t, s)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(3))
	}); err != nil {
		t.Fatal(err)
	}
	if count, start, end, sourceRevision, present, digest := demandSourceHistoryRows(t, s, sourceID); count != 1 || start != revKey(2) || end != "" || sourceRevision != revKey(1) || present != 1 || digest != driver.SelectionDigest(value) {
		t.Fatalf("open reuse changed history count=%d start=%q end=%q source_revision=%q present=%d digest=%q", count, start, end, sourceRevision, present, digest)
	}
}

func TestDemandSelectionSourceHistoryUsesPinnedRevisionBeforeAdvancedHead(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	source := demandSourceKey("pinned-source")
	dependent := demandSourceKey("pinned-dependent")
	oldValue := []byte(`{"source":"old-pinned"}`)
	newValue := []byte(`{"source":"new-head"}`)
	seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, oldValue)
	seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 2, newValue)
	if _, err := s.db.ExecContext(ctx, `UPDATE meta SET head=? WHERE singleton=1`, revKey(2)); err != nil {
		t.Fatal(err)
	}

	// The transaction was admitted against t.at=1 while the database head
	// advanced to 2. A loader using next-1 would see newValue and refuse or,
	// worse, bind the wrong image; the demand proof must read oldValue at t.at.
	dependentID, dependentState := demandDependentState(meta.Incarnation, source, dependent, 1, true, driver.SelectionDigest(oldValue))
	tx := &transaction{reader: &reader{at: 1}, records: make(map[string]*driver.Record), derived: map[string]driver.LockState{dependentID: dependentState}}
	keyPublicationTriggerMarker(t, s)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(3))
	}); err != nil {
		t.Fatal(err)
	}
	sourceID := keyID(demandSourceLock(source))
	if count, start, end, sourceRevision, present, digest := demandSourceHistoryRows(t, s, sourceID); count != 1 || start != revKey(3) || end != "" || sourceRevision != revKey(1) || present != 1 || digest != driver.SelectionDigest(oldValue) {
		t.Fatalf("pinned demand history count=%d start=%q end=%q source_revision=%q present=%d digest=%q", count, start, end, sourceRevision, present, digest)
	}
}

func TestDemandSelectionSourceHistoryRejectsOverlappingIntervals(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	source := demandSourceKey("overlap-source")
	dependent := demandSourceKey("overlap-dependent")
	value := []byte(`{"source":"overlap"}`)
	seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, value)
	sourceID := keyID(demandSourceLock(source))
	for _, interval := range [][2]uint64{{1, 5}, {3, 7}} {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(
			incarnation,branch,source_key,start_revision,end_revision,source_revision,source_present,source_digest)
			VALUES(?,?,?,?,?,?,?,?)`, meta.Incarnation, revKey(0), sourceID, revKey(interval[0]), revKey(interval[1]), revKey(1), 1, driver.SelectionDigest(value)); err != nil {
			t.Fatal(err)
		}
	}
	dependentID, dependentState := demandDependentState(meta.Incarnation, source, dependent, 1, true, driver.SelectionDigest(value))
	tx := &transaction{reader: &reader{at: 1}, records: make(map[string]*driver.Record), derived: map[string]driver.LockState{dependentID: dependentState}}
	err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(8))
	})
	if !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("overlapping source history error=%v, want unsupported", err)
	}
	if count, _, _, _, _, _ := demandSourceHistoryRows(t, s, sourceID); count != 2 {
		t.Fatalf("overlap refusal changed source rows=%d", count)
	}
}

func TestDemandSelectionSourceHistoryRebindsReplacementAndInvalidatesFacts(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	source := demandSourceKey("replacement-source")
	dependent := demandSourceKey("replacement-dependent")
	oldValue := []byte(`{"source":"old"}`)
	newValue := []byte(`{"source":"new"}`)
	seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, oldValue)
	sourceID := keyID(demandSourceLock(source))
	if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(
		incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest)
		VALUES(?,?,?,?,?,?,?)`, meta.Incarnation, revKey(0), sourceID, revKey(1), revKey(1), 1, driver.SelectionDigest(oldValue)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_fact_sets(
		incarnation,branch,row_key,row_table,kind,start_revision,source_key,source_revision,source_present,source_digest)
		VALUES(?,?,?,?,?,?,?,?,?,?)`, meta.Incarnation, revKey(0), "replacement-fact", string(driver.DerivedAncestorClosure), string(driver.SelectionAncestorFact), revKey(1), sourceID, revKey(1), 1, driver.SelectionDigest(oldValue)); err != nil {
		t.Fatal(err)
	}
	newState := driver.LockState{Key: demandSourceLock(source), Present: true, Derived: &driver.DerivedRow{Key: source.Key, Value: newValue}}
	dependentID, dependentState := demandDependentState(meta.Incarnation, source, dependent, 1, true, driver.SelectionDigest(newValue))
	tx := &transaction{reader: &reader{at: 1}, records: make(map[string]*driver.Record), derived: map[string]driver.LockState{sourceID: newState, dependentID: dependentState}}
	err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		if err := invalidateSelectionFactSets(ctx, c, meta.Incarnation, revKey(2), map[string]bool{sourceID: true}); err != nil {
			return err
		}
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(2))
	})
	if err != nil {
		t.Fatal(err)
	}
	var oldEnd string
	if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_source_history WHERE source_key=? AND start_revision=?`, sourceID, revKey(1)).Scan(&oldEnd); err != nil {
		t.Fatal(err)
	}
	if oldEnd != revKey(2) {
		t.Fatalf("replacement old source end=%q", oldEnd)
	}
	var sourceRevision, digest string
	var present int
	if err := s.db.QueryRowContext(ctx, `SELECT source_revision,source_present,source_digest FROM selection_source_history WHERE source_key=? AND start_revision=?`, sourceID, revKey(2)).Scan(&sourceRevision, &present, &digest); err != nil {
		t.Fatal(err)
	}
	if sourceRevision != revKey(2) || present != 1 || digest != driver.SelectionDigest(newValue) {
		t.Fatalf("replacement source revision=%q present=%d digest=%q", sourceRevision, present, digest)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_fact_sets WHERE row_key=?`, "replacement-fact").Scan(&oldEnd); err != nil {
		t.Fatal(err)
	}
	if oldEnd != revKey(2) {
		t.Fatalf("replacement fact end=%q", oldEnd)
	}
}

func TestDemandSelectionSourceHistoryChangedSourceClosesAndSeedsTombstone(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	source := demandSourceKey("changed-source")
	dependent := demandSourceKey("changed-dependent")
	oldValue := []byte(`{"source":"old"}`)
	seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, oldValue)
	sourceID := keyID(demandSourceLock(source))
	if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest) VALUES(?,?,?,?,?,?,?)`, meta.Incarnation, revKey(0), sourceID, revKey(1), revKey(1), 1, driver.SelectionDigest(oldValue)); err != nil {
		t.Fatal(err)
	}
	dependentID, dependentState := demandDependentState(meta.Incarnation, source, dependent, 1, false, "")
	changedState := driver.LockState{Key: demandSourceLock(source), Present: false}
	tx := &transaction{reader: &reader{at: 1}, records: make(map[string]*driver.Record), derived: map[string]driver.LockState{sourceID: changedState, dependentID: dependentState}}
	keyPublicationTriggerMarker(t, s)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(2))
	}); err != nil {
		t.Fatal(err)
	}
	var oldEnd string
	var currentPresent int
	var currentRevision string
	if err := s.db.QueryRowContext(ctx, `SELECT (SELECT end_revision FROM selection_source_history WHERE source_key=? AND start_revision=?),(SELECT source_present FROM selection_source_history WHERE source_key=? AND start_revision=?)`, sourceID, revKey(1), sourceID, revKey(2)).Scan(&oldEnd, &currentPresent); err != nil {
		t.Fatal(err)
	}
	if oldEnd != revKey(2) || currentPresent != 0 {
		t.Fatalf("changed tombstone old_end=%q current_present=%d", oldEnd, currentPresent)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT source_revision FROM selection_source_history WHERE source_key=? AND start_revision=?`, sourceID, revKey(2)).Scan(&currentRevision); err != nil {
		t.Fatal(err)
	}
	if currentRevision != revKey(2) {
		t.Fatalf("changed tombstone source_revision=%q", currentRevision)
	}
}

func TestDemandSelectionSourceHistoryMissingAndMalformedRefuseAtomically(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	source := demandSourceKey("malformed-source")
	dependent := demandSourceKey("malformed-dependent")
	sourceID := keyID(demandSourceLock(source))
	dependentID, dependentState := demandDependentState(meta.Incarnation, source, dependent, 1, true, driver.SelectionDigest([]byte(`{"source":"expected"}`)))
	if _, err := s.db.ExecContext(ctx, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, sourceID, revKey(1), []byte(`{"bad":`)); err != nil {
		t.Fatal(err)
	}
	tx := &transaction{reader: &reader{at: 1}, records: make(map[string]*driver.Record), derived: map[string]driver.LockState{dependentID: dependentState}}
	err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(2))
	})
	if err == nil {
		t.Fatal("malformed cold source unexpectedly seeded")
	}
	if count, _, _, _, _, _ := demandSourceHistoryRows(t, s, sourceID); count != 0 {
		t.Fatalf("malformed source publication left rows=%d", count)
	}

	// A missing witness cannot satisfy an admitted present source. It must
	// refuse before the source-history INSERT rather than becoming a current
	// value or an implicit no-source result.
	missingPresent := demandSourceKey("missing-present")
	presentDependent := demandSourceKey("missing-present-dependent")
	presentID, presentState := demandDependentState(meta.Incarnation, missingPresent, presentDependent, 1, true, driver.SelectionDigest([]byte(`{"source":"expected"}`)))
	tx = &transaction{reader: &reader{at: 1}, records: make(map[string]*driver.Record), derived: map[string]driver.LockState{presentID: presentState}}
	err = s.immediate(ctx, nil, func(c *sql.Conn) error {
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(2))
	})
	if !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("missing present source error=%v, want selection unavailable", err)
	}
	if count, _, _, _, _, _ := demandSourceHistoryRows(t, s, keyID(demandSourceLock(missingPresent))); count != 0 {
		t.Fatalf("missing present refusal left rows=%d", count)
	}

	// A never-present demanded source is an explicit absent interval, not a
	// silent current-value substitution.
	missing := demandSourceKey("never-present")
	missingDependent := demandSourceKey("missing-dependent")
	missingID, missingState := demandDependentState(meta.Incarnation, missing, missingDependent, 0, false, "")
	tx = &transaction{reader: &reader{at: 1}, records: make(map[string]*driver.Record), derived: map[string]driver.LockState{missingID: missingState}}
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(2))
	}); err != nil {
		t.Fatal(err)
	}
	if count, _, _, sourceRevision, present, digest := demandSourceHistoryRows(t, s, keyID(demandSourceLock(missing))); count != 1 || sourceRevision != revKey(0) || present != 0 || digest != "" {
		t.Fatalf("missing source interval count=%d source_revision=%q present=%d digest=%q", count, sourceRevision, present, digest)
	}
}

func TestDemandSelectionSourceHistoryBatchRollback(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	derived := make(map[string]driver.LockState, selectionSourceHistoryBatch+1)
	for i := 0; i <= selectionSourceHistoryBatch; i++ {
		source := demandSourceKey(fmt.Sprintf("batch-source-%03d", i))
		dependent := demandSourceKey(fmt.Sprintf("batch-dependent-%03d", i))
		value := []byte(fmt.Sprintf(`{"source":%d}`, i))
		seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, value)
		id, state := demandDependentState(meta.Incarnation, source, dependent, 1, true, driver.SelectionDigest(value))
		derived[id] = state
	}
	tx := &transaction{reader: &reader{at: 1}, records: make(map[string]*driver.Record), derived: derived}
	abort := errors.New("abort demand history after both batches")
	err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		if err := tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(2)); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatalf("batch rollback err=%v", err)
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_source_history`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("demand history rollback left %d rows", count)
	}
}
