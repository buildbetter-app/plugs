package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func demandSourceKey(name string) driver.DerivedRowKey {
	return driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: name}
}

func demandFactKey(name string) driver.DerivedRowKey {
	return driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: name}
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
		Facts:  &driver.SelectionFacts{Kind: driver.SelectionLabelFact},
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
	dependent := demandFactKey("cold-dependent")
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
	dependent := demandFactKey("pinned-dependent")
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
	dependent := demandFactKey("overlap-dependent")
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
	dependent := demandFactKey("replacement-dependent")
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
	dependent := demandFactKey("changed-dependent")
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
	dependent := demandFactKey("malformed-dependent")
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
	presentDependent := demandFactKey("missing-present-dependent")
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
	missingDependent := demandFactKey("missing-dependent")
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
		dependent := demandFactKey(fmt.Sprintf("batch-dependent-%03d", i))
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

// These controls exercise the public writer/selector boundaries. Unlike the
// isolated publisher controls above, they establish coverage through the real
// empty-store build and stage a complete, valid Facts annotation. They expose
// the emitter-only prototype's unresolved precommit and pure-reader gaps; do
// not skip or weaken them to qualify the sparse-history implementation.
func demandAPIStore(t *testing.T) (*Store, driver.ActiveLease) {
	t.Helper()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	meta := getMeta(t, ctx, s)
	decode, projection := selectionBackfillRaceProjection(meta.Incarnation, nil)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, projection); err != nil {
		t.Fatal(err)
	}
	return s, lease
}

func demandAPIWriteSource(t *testing.T, s *Store, lease driver.ActiveLease, source driver.DerivedRowKey, value []byte, present bool) model.Revision {
	t.Helper()
	tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{demandSourceLock(source)}, false)
	row := driver.DerivedRow{Key: source.Key, Value: value}
	var del, add []driver.DerivedRow
	if present {
		add = []driver.DerivedRow{row}
	} else {
		del = []driver.DerivedRow{{Key: source.Key}}
	}
	if err := tx.PutDerived(context.Background(), source.Table, del, add); err != nil {
		t.Fatal(err)
	}
	return commitSourceTest(t, tx, header).Revision
}

func demandAPIPlan(t *testing.T, r *reader, source driver.SelectionSource) driver.SelectionPlan {
	t.Helper()
	p := driver.SelectionPlan{Incarnation: r.meta.Incarnation, Branch: model.Natural64(0),
		ContentRevision: model.Natural64(r.at), AuthorityRevision: model.Natural64(r.at),
		Membership:    driver.SelectionExpr{Op: driver.SelectionFalse},
		Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionCountMode}
	var err error
	p.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{
		Version: 1, PreparedBinding: driver.SelectionDigest([]byte("demand-source-control")),
		Incarnation: p.Incarnation, Branch: p.Branch, AuthorityRevision: p.AuthorityRevision,
		Sources: []driver.SelectionSource{source}, Chain: driver.SelectionChainNone,
		Authorization: p.Authorization, Host: driver.SelectionHostAbsent,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func demandAPIFactRow(t *testing.T, incarnation string, source driver.SelectionSource, key driver.DerivedRowKey) driver.DerivedRow {
	t.Helper()
	value := []byte(`{"complete":"empty-label-set"}`)
	row := driver.DerivedRow{Key: key.Key, Value: value, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: key, Incarnation: incarnation,
		Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value), Source: source,
		Facts: &driver.SelectionFacts{Kind: driver.SelectionLabelFact},
	}}
	if err := row.Index.Validate(key.Table, row); err != nil {
		t.Fatal(err)
	}
	return row
}

func TestDemandSelectionColdSourceSelectBeforeCommit(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement_%t", replacement), func(t *testing.T) {
			ctx := context.Background()
			s, lease := demandAPIStore(t)
			source, fact := demandSourceKey("precommit-source"), demandFactKey("precommit-facts")
			value := []byte(`{"ready":"old"}`)
			revision := demandAPIWriteSource(t, s, lease, source, value, true)
			tx, _ := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{demandSourceLock(source), demandSourceLock(fact)}, false)
			if replacement {
				value = []byte(`{"ready":"replacement"}`)
				if err := tx.PutDerived(ctx, source.Table, nil, []driver.DerivedRow{{Key: source.Key, Value: value}}); err != nil {
					t.Fatal(err)
				}
			}
			want := driver.SelectionSource{Key: demandSourceLock(source), Revision: revision, Present: true, Digest: driver.SelectionDigest(value)}
			row := demandAPIFactRow(t, tx.meta.Incarnation, want, fact)
			if err := tx.PutSelectionDerived(ctx, fact.Table, nil, []driver.DerivedRow{row}); err != nil {
				t.Fatalf("ordinary pinned admission refused: %v", err)
			}
			control := testRef("never-present-demand-control")
			plan := demandAPIPlan(t, tx.reader, driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)})
			got, err := tx.Select(ctx, plan)
			if err != nil || got.Total != 0 || got.More {
				t.Fatalf("admitted first binding must support same-token Select before Commit: %+v %v", got, err)
			}
		})
	}
}

func TestDemandSelectionPureReaderBoundarySource(t *testing.T) {
	for _, name := range []string{"present", "opaque_present", "never_present", "tombstone"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, lease := demandAPIStore(t)
			source := demandSourceKey("boundary-source")
			if name == "opaque_present" {
				source.Key += string([]byte{0xff, 0xfe})
			}
			value := []byte(`{"ready":"boundary"}`)
			want := driver.SelectionSource{Key: demandSourceLock(source), Revision: model.Natural64(0)}
			if name != "never_present" {
				want.Revision = demandAPIWriteSource(t, s, lease, source, value, true)
				want.Present, want.Digest = true, driver.SelectionDigest(value)
			}
			if name == "tombstone" {
				want.Revision = demandAPIWriteSource(t, s, lease, source, nil, false)
				want.Present, want.Digest = false, ""
			}
			r := sourceReader(t, s, nil)
			got, err := r.Select(ctx, demandAPIPlan(t, r, want))
			if err != nil || got.Total != 0 || got.More {
				t.Errorf("honest pure-reader source must validate before a false predicate: %+v %v", got, err)
			}
			if name == "tombstone" {
				forged := want
				forged.Revision = model.Natural64(0)
				got, err = r.Select(ctx, demandAPIPlan(t, r, forged))
				if !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
					t.Errorf("created-and-deleted derived source accepted as never-present: %+v %v", got, err)
				}
			}
		})
	}
}

func TestDemandSelectionStaleFirstBindingRefusesWithoutPublication(t *testing.T) {
	for _, name := range []string{"update", "delete", "ready_churn"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, lease := demandAPIStore(t)
			source, fact := demandFactKey("stale-source"), demandFactKey("stale-facts")
			if name == "ready_churn" {
				source.Table = driver.DerivedReadyState
			}
			oldValue, newValue := []byte(`{"source":"old"}`), []byte(`{"source":"new"}`)
			oldRevision := demandAPIWriteSource(t, s, lease, source, oldValue, true)
			demandAPIWriteSource(t, s, lease, source, newValue, name != "delete")
			beforeHistory := selectionSourceHistoryImages(t, s)
			beforeKeys := keyPublicationImages(t, s)
			beforeMeta := getMeta(t, ctx, s)
			tx, _ := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{demandSourceLock(source), demandSourceLock(fact)}, false)
			want := driver.SelectionSource{Key: demandSourceLock(source), Revision: oldRevision, Present: true, Digest: driver.SelectionDigest(oldValue)}
			row := demandAPIFactRow(t, tx.meta.Incarnation, want, fact)
			err := tx.PutSelectionDerived(ctx, fact.Table, nil, []driver.DerivedRow{row})
			if !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("old mutation token admitted after %s: %v", name, err)
			}
			if len(tx.derived) != 0 || tx.selectionAnnotated {
				t.Fatal("stale admission mutated the overlay")
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(selectionSourceHistoryImages(t, s), beforeHistory) ||
				!reflect.DeepEqual(keyPublicationImages(t, s), beforeKeys) || !reflect.DeepEqual(getMeta(t, ctx, s), beforeMeta) {
				t.Fatal("stale admission changed committed source state")
			}
			var factRows, values, beads int
			if err := s.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM selection_fact_sets),
				(SELECT COUNT(*) FROM selection_facts),(SELECT COUNT(*) FROM selection_bead_history)`).Scan(&factRows, &values, &beads); err != nil {
				t.Fatal(err)
			}
			if factRows != 0 || values != 0 || beads != 0 {
				t.Fatalf("stale refusal published selection rows: %d/%d/%d", factRows, values, beads)
			}
		})
	}
}

func TestDemandSelectionOpaqueColdSourceCommit(t *testing.T) {
	ctx := context.Background()
	s, lease := demandAPIStore(t)
	source, fact := demandSourceKey("opaque-source-"+string([]byte{0xff, 0xfe})), demandFactKey("opaque-facts")
	value := []byte{0, 0xff, 0xfe, 'v'}
	revision := demandAPIWriteSource(t, s, lease, source, value, true)
	tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{demandSourceLock(source), demandSourceLock(fact)}, false)
	want := driver.SelectionSource{Key: demandSourceLock(source), Revision: revision, Present: true, Digest: driver.SelectionDigest(value)}
	row := demandAPIFactRow(t, tx.meta.Incarnation, want, fact)
	if err := tx.PutSelectionDerived(ctx, fact.Table, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	bound := commitSourceTest(t, tx, header)
	r := sourceReader(t, s, nil)
	state, digest, err := r.selectionSourceState(ctx, keyID(want.Key), mustUint(bound.Revision), false)
	if err != nil || !state.Present || state.Revision.String() != revision.String() || digest != want.Digest {
		t.Fatalf("raw opaque cold binding changed source observation: %#v %q %v", state, digest, err)
	}
	if err := r.validateSelectionFactSource(ctx, keyID(want.Key), revKey(mustUint(revision)), want.Digest, 1, mustUint(bound.Revision), true); err != nil {
		t.Fatalf("committed opaque fact envelope did not bind raw source: %v", err)
	}
}

func TestDemandSelectionBoundHistoricalFactsAndRebinding(t *testing.T) {
	ctx := context.Background()
	s, lease := demandAPIStore(t)
	source, fact := demandSourceKey("historical-source"), demandFactKey("historical-facts")
	firstValue, secondValue := []byte(`{"source":1}`), []byte(`{"source":2}`)
	firstSourceRevision := demandAPIWriteSource(t, s, lease, source, firstValue, true)
	bind := func(sourceRevision model.Revision, value []byte) model.Revision {
		t.Helper()
		tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{demandSourceLock(source), demandSourceLock(fact)}, false)
		want := driver.SelectionSource{Key: demandSourceLock(source), Revision: sourceRevision, Present: true, Digest: driver.SelectionDigest(value)}
		row := demandAPIFactRow(t, tx.meta.Incarnation, want, fact)
		if err := tx.PutSelectionDerived(ctx, fact.Table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
		return commitSourceTest(t, tx, header).Revision
	}
	firstBinding := bind(firstSourceRevision, firstValue)
	secondSourceRevision := demandAPIWriteSource(t, s, lease, source, secondValue, true)
	var factEnd, sourceEnd string
	if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_fact_sets WHERE row_key=? AND start_revision=?`, keyID(demandSourceLock(fact)), revKey(mustUint(firstBinding))).Scan(&factEnd); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_source_history WHERE source_key=? AND end_revision=?`, keyID(demandSourceLock(source)), revKey(mustUint(secondSourceRevision))).Scan(&sourceEnd); err != nil {
		t.Fatal(err)
	}
	if factEnd != revKey(mustUint(secondSourceRevision)) || sourceEnd != factEnd {
		t.Fatalf("source replacement did not atomically close fact/source intervals: %q/%q", factEnd, sourceEnd)
	}
	secondBinding := bind(secondSourceRevision, secondValue)
	beforeReuse := selectionSourceHistoryImages(t, s)
	thirdBinding := bind(secondSourceRevision, secondValue)
	if !reflect.DeepEqual(selectionSourceHistoryImages(t, s), beforeReuse) {
		t.Fatal("rebinding unchanged source closed or duplicated its open interval")
	}
	r := sourceReader(t, s, nil)
	control := testRef("historical-never-present-control")
	for _, content := range []model.Revision{firstBinding, secondBinding, thirdBinding} {
		plan := demandAPIPlan(t, r, driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)})
		plan.ContentRevision = content
		plan.ContentCommitted = true
		plan.Membership = driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: fact, Kind: driver.SelectionLabelFact}}
		got, err := r.Select(ctx, plan)
		if err != nil || got.Total != 0 || got.More {
			t.Fatalf("retained fact at content=%s/current=%s did not bind exact historical source: %+v %v", content.String(), r.meta.Head.String(), got, err)
		}
	}
}

func TestDemandSelectionPresentNullAndEmptyValueRemainPresent(t *testing.T) {
	for _, value := range [][]byte{nil, {}, []byte("null")} {
		t.Run(fmt.Sprintf("value_%v", value), func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			meta := getMeta(t, ctx, s)
			source, fact := demandSourceKey("empty-source"), demandFactKey("empty-facts")
			seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, value)
			id, state := demandDependentState(meta.Incarnation, source, fact, 1, true, driver.SelectionDigest(value))
			tx := &transaction{reader: &reader{at: 1}, derived: map[string]driver.LockState{id: state}}
			if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(2))
			}); err != nil {
				t.Fatal(err)
			}
			if count, _, _, revision, present, digest := demandSourceHistoryRows(t, s, keyID(demandSourceLock(source))); count != 1 || revision != revKey(1) || present != 1 || digest != driver.SelectionDigest(value) || digest == "" {
				t.Fatalf("present null/empty value became a tombstone: %d/%q/%d/%q", count, revision, present, digest)
			}
		})
	}
}

type demandSourceHistoryWork struct {
	keyPublicationWork
	queries int
}

func (w *demandSourceHistoryWork) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	w.queries++
	return w.conn.QueryRowContext(ctx, query, args...)
}

func TestDemandSelectionColdWitnessErrorsPrecedePublication(t *testing.T) {
	for _, name := range []string{"malformed_json", "native_table_error", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			meta := getMeta(t, ctx, s)
			source, fact := demandSourceKey("error-source"), demandFactKey("error-facts")
			value := []byte(`{"source":"expected"}`)
			seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, value)
			id, state := demandDependentState(meta.Incarnation, source, fact, 1, true, driver.SelectionDigest(value))
			tx := &transaction{reader: &reader{at: 1}, records: map[string]*driver.Record{
				"0:error-order-record": {Body: []byte(`{}`)},
			}, derived: map[string]driver.LockState{id: state}}
			beforeKeys, beforeHistory := keyPublicationImages(t, s), selectionSourceHistoryImages(t, s)
			cleanupErr := errors.New("cleanup must not mask the source error")
			prepareErr := errors.New("source error must precede prepare")
			var work demandSourceHistoryWork
			err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				work = demandSourceHistoryWork{keyPublicationWork: keyPublicationWork{
					selectionCloseWork: selectionCloseWork{conn: c}, failPrepareAt: 1,
					prepareErr: prepareErr, closeErr: cleanupErr,
				}}
				call := ctx
				switch name {
				case "malformed_json":
					if _, err := c.ExecContext(ctx, `UPDATE key_history SET data=? WHERE key=?`, []byte(`{"bad":`), keyID(demandSourceLock(source))); err != nil {
						return err
					}
				case "native_table_error":
					if _, err := c.ExecContext(ctx, `DROP TABLE key_history`); err != nil {
						return err
					}
				case "cancelled":
					var cancel context.CancelFunc
					call, cancel = context.WithCancel(ctx)
					cancel()
				}
				return tx.publishSelectionSourceHistory(call, &work, meta.Incarnation, revKey(2))
			})
			if err == nil || errors.Is(err, prepareErr) || errors.Is(err, cleanupErr) {
				t.Fatalf("source error lost to publication/cleanup: %v", err)
			}
			if name == "malformed_json" {
				var syntax *json.SyntaxError
				if !errors.As(err, &syntax) {
					t.Fatalf("malformed JSON error was rewritten: %v", err)
				}
			}
			if name == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation was rewritten: %v", err)
			}
			if work.prepared != 0 || work.prepareAttempts != 0 || work.statements != 0 || work.closeCalls != 0 {
				t.Fatalf("publication reached before source refusal: %+v", work)
			}
			if !reflect.DeepEqual(keyPublicationImages(t, s), beforeKeys) || !reflect.DeepEqual(selectionSourceHistoryImages(t, s), beforeHistory) {
				t.Fatal("source refusal changed committed images")
			}
		})
	}
}

// This is an existing-contract fence control, not a simulated successful
// migration. The running binary remains on its real compiled schema version;
// an independent SQL transaction changes only the advertised version. Public
// calls must refuse new snapshots/publication while an earlier pinned reader
// continues to see its exact old source state. Actual 1/2-to-3 migration and
// independent old/new executables remain separate integration obligations.
func TestDemandSelectionSchemaFenceNewViewsAndHeldPublication(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		t.Run(fmt.Sprintf("pinned_tombstone_%t", tombstone), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "source-version-fence.db")
			s := openStore(t, ctx, path)
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			meta := getMeta(t, ctx, s)
			decode, projection := selectionBackfillRaceProjection(meta.Incarnation, nil)
			if err := s.BuildGraphSourceIndexes(ctx, lease, decode, projection); err != nil {
				t.Fatal(err)
			}
			source, fact := demandSourceKey("version-source-"+string([]byte{0xff})), demandFactKey("version-facts")
			value := []byte{0, 0xff, 'v'}
			want := driver.SelectionSource{Key: demandSourceLock(source), Present: true, Digest: driver.SelectionDigest(value)}
			want.Revision = demandAPIWriteSource(t, s, lease, source, value, true)
			if tombstone {
				want.Revision = demandAPIWriteSource(t, s, lease, source, nil, false)
				want.Present, want.Digest = false, ""
			}
			// Bind normally so this fence control is independent of the sparse
			// prototype's separately captured first-boundary publication gap.
			bound, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{want.Key, demandSourceLock(fact)}, false)
			row := demandAPIFactRow(t, meta.Incarnation, want, fact)
			if err := bound.PutSelectionDerived(ctx, fact.Table, nil, []driver.DerivedRow{row}); err != nil {
				t.Fatal(err)
			}
			commitSourceTest(t, bound, header)
			pinned := sourceReader(t, s, nil)
			plan := demandAPIPlan(t, pinned, want)
			if got, err := pinned.Select(ctx, plan); err != nil || got.Total != 0 || got.More {
				t.Fatalf("pre-fence source binding: %+v %v", got, err)
			}
			pendingKey := demandSourceKey("must-not-publish-across-version")
			pending, pendingHeader := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{demandSourceLock(pendingKey)}, false)
			if err := pending.PutDerived(ctx, pendingKey.Table, nil, []driver.DerivedRow{{Key: pendingKey.Key, Value: []byte("pending")}}); err != nil {
				t.Fatal(err)
			}
			if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				_, err := c.ExecContext(ctx, `UPDATE meta SET schema_version=? WHERE singleton=1`, schemaVersion+1)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			beforeKeys, beforeHistory := keyPublicationImages(t, s), selectionSourceHistoryImages(t, s)
			beforePublication := demandPublicationImages(t, s)
			if got, err := pinned.Select(ctx, plan); err != nil || got.Total != 0 || got.More {
				t.Fatalf("version change altered already-pinned source selection: %+v %v", got, err)
			}
			opened, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
			if opened != nil {
				_ = opened.Close()
			}
			if !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("old binary Open accepted an unknown version: %v", err)
			}
			newReader, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if newReader != nil {
				_ = newReader.Close()
			}
			if !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("existing handle acquired a new snapshot across version fence: %v", err)
			}
			newWriter, err := s.Write(ctx, lease)
			if newWriter != nil {
				_ = newWriter.Close()
			}
			if !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("existing handle acquired a new writer across version fence: %v", err)
			}
			if _, err := s.Meta(ctx); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("Meta accepted an unknown version: %v", err)
			}
			if _, err := s.Migrate(ctx, schemaVersion); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("Migrate silently downgraded unknown representation: %v", err)
			}
			if _, err := pending.Commit(ctx, pendingHeader); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("held writer published across version fence: %v", err)
			}
			if !reflect.DeepEqual(keyPublicationImages(t, s), beforeKeys) ||
				!reflect.DeepEqual(selectionSourceHistoryImages(t, s), beforeHistory) ||
				!reflect.DeepEqual(demandPublicationImages(t, s), beforePublication) {
				t.Fatal("version refusal changed source, selection, head, history or receipts")
			}
		})
	}
}

func demandPublicationImages(t *testing.T, s *Store) [][][]any {
	t.Helper()
	var image [][][]any
	for _, query := range []string{
		`SELECT * FROM meta ORDER BY singleton`,
		`SELECT * FROM selection_fact_sets ORDER BY incarnation,branch,row_key,start_revision`,
		`SELECT * FROM selection_facts ORDER BY incarnation,branch,row_key,start_revision,ordinal`,
		`SELECT * FROM selection_bead_history ORDER BY incarnation,branch,derived_key,start_revision`,
		`SELECT * FROM changesets ORDER BY rev`,
		`SELECT * FROM replays ORDER BY scope`,
	} {
		rows, err := s.db.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		var table [][]any
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = append([]byte(nil), raw...)
				}
			}
			table = append(table, values)
		}
		err = rows.Err()
		closeErr := rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		image = append(image, table)
	}
	return image
}
