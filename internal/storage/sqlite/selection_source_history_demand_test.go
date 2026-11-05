package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

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
	state := driver.LockState{Key: demandSourceLock(key), Revision: model.Natural64(revision), Present: true,
		Derived: &driver.DerivedRow{Key: key.Key, Value: value}}
	if err := s.immediate(context.Background(), nil, func(c *sql.Conn) error {
		return publishKey(context.Background(), c, state)
	}); err != nil {
		t.Fatal(err)
	}
}

func demandSourceHistoryRows(t *testing.T, s *Store, id string) (count int, start, end, revision string, present int, digest string) {
	t.Helper()
	err := s.db.QueryRowContext(context.Background(), `SELECT COUNT(*),COALESCE(MAX(start_revision),''),
		COALESCE(MAX(end_revision),''),COALESCE(MAX(source_revision),''),COALESCE(MAX(source_present),0),
		COALESCE(MAX(source_digest),'') FROM selection_source_history WHERE source_key=?`, id).
		Scan(&count, &start, &end, &revision, &present, &digest)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func checkProjectedSource(t *testing.T, s *Store, key driver.DerivedRowKey, at, revision uint64, present bool, digest string) {
	t.Helper()
	meta := getMeta(t, context.Background(), s)
	state, got, err := projectedDerivedSource(context.Background(), s.db, meta.Incarnation, keyID(demandSourceLock(key)), at)
	if err != nil || mustUint(state.Revision) != revision || state.Present != present || got != digest {
		t.Fatalf("projected source at=%d got=%+v/%q err=%v want=%d/%t/%q", at, state, got, err, revision, present, digest)
	}
}

func seedLegacyDemandSource(t *testing.T, s *Store, source driver.DerivedRowKey, revision uint64, value []byte) {
	t.Helper()
	ctx := context.Background()
	meta := getMeta(t, ctx, s)
	state := driver.LockState{Key: demandSourceLock(source), Revision: model.Natural64(revision), Present: true,
		Derived: &driver.DerivedRow{Key: source.Key, Value: value}}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(state.Key), revKey(revision), data); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest) VALUES(?,?,?,?,?,?,?)`,
		meta.Incarnation, revKey(0), keyID(state.Key), revKey(revision), revKey(revision), 1, driver.SelectionDigest(value)); err != nil {
		t.Fatal(err)
	}
}

func TestDemandSelectionSourceHistoryColdBindAndOpenReuse(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	source, dependent := demandSourceKey("cold-source"), demandFactKey("cold-dependent")
	value := []byte(`{"source":"r1"}`)
	seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, value)
	id, state := demandDependentState(meta.Incarnation, source, dependent, 1, true, driver.SelectionDigest(value))
	tx := &transaction{derived: map[string]driver.LockState{id: state}}
	before := demandPublicationImages(t, s)
	for _, next := range []uint64{2, 3} {
		if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
			return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(next))
		}); err != nil {
			t.Fatal(err)
		}
		checkProjectedSource(t, s, source, next, 1, true, driver.SelectionDigest(value))
		if count, _, _, _, _, _ := demandSourceHistoryRows(t, s, keyID(demandSourceLock(source))); count != 0 {
			t.Fatal("binding duplicated eager projection")
		}
	}
	if !reflect.DeepEqual(before, demandPublicationImages(t, s)) {
		t.Fatal("rebinding altered the retained source")
	}
}

func TestDemandSelectionSourceHistoryUsesPinnedRevisionBeforeAdvancedHead(t *testing.T) {
	ctx := context.Background()
	s, lease := demandAPIStore(t)
	source := demandSourceKey("held-pin-source")
	oldValue, newValue := []byte("old"), []byte("new")
	r1 := demandAPIWriteSource(t, s, lease, source, oldValue, true)
	held := sourceReader(t, s, nil)
	want := driver.SelectionSource{Key: demandSourceLock(source), Revision: r1, Present: true, Digest: driver.SelectionDigest(oldValue)}
	heldPlan := demandAPIPlan(t, held, want)
	r2 := demandAPIWriteSource(t, s, lease, source, newValue, true)
	r3 := demandAPIWriteSource(t, s, lease, source, nil, false)
	if _, err := held.Select(ctx, heldPlan); err != nil {
		t.Fatalf("held snapshot changed after replacement/deletion: %v", err)
	}
	checkProjectedSource(t, s, source, mustUint(r1), mustUint(r1), true, driver.SelectionDigest(oldValue))
	checkProjectedSource(t, s, source, mustUint(r2), mustUint(r2), true, driver.SelectionDigest(newValue))
	checkProjectedSource(t, s, source, mustUint(r3), mustUint(r3), false, "")
	current := sourceReader(t, s, nil)
	if _, err := current.Select(ctx, demandAPIPlan(t, current, want)); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("current snapshot accepted old token: %v", err)
	}
}

func TestDemandSelectionSourceHistoryRejectsOverlappingIntervals(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	source := demandSourceKey("legacy-overlap")
	value := []byte("old")
	seedLegacyDemandSource(t, s, source, 1, value)
	id := keyID(demandSourceLock(source))
	if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(incarnation,branch,source_key,start_revision,end_revision,source_revision,source_present,source_digest) VALUES(?,?,?,?,?,?,?,?)`, meta.Incarnation, revKey(0), id, revKey(2), revKey(5), revKey(1), 1, driver.SelectionDigest(value)); err != nil {
		t.Fatal(err)
	}
	before := demandPublicationImages(t, s)
	_, _, err := projectedDerivedSource(ctx, s.db, meta.Incarnation, id, 3)
	if !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("overlap error=%v", err)
	}
	if !reflect.DeepEqual(before, demandPublicationImages(t, s)) {
		t.Fatal("read refusal changed rows")
	}
}

func TestDemandSelectionSourceHistoryRebindsReplacementAndInvalidatesFacts(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprint(present), func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			meta := getMeta(t, ctx, s)
			source := demandSourceKey("legacy-replacement")
			oldValue, newValue := []byte("old"), []byte("new")
			seedLegacyDemandSource(t, s, source, 1, oldValue)
			id := keyID(demandSourceLock(source))
			if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_fact_sets(incarnation,branch,row_key,row_table,kind,start_revision,source_key,source_revision,source_present,source_digest) VALUES(?,?,?,?,?,?,?,?,?,?)`, meta.Incarnation, revKey(0), "legacy-fact", string(driver.DerivedAttribute), string(driver.SelectionLabelFact), revKey(1), id, revKey(1), 1, driver.SelectionDigest(oldValue)); err != nil {
				t.Fatal(err)
			}
			state := driver.LockState{Key: demandSourceLock(source), Present: present}
			digest := ""
			if present {
				state.Derived = &driver.DerivedRow{Key: source.Key, Value: newValue}
				digest = driver.SelectionDigest(newValue)
			}
			tx := &transaction{derived: map[string]driver.LockState{id: state}}
			if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				if err := publishDerivedKeys(ctx, c, model.Natural64(2), tx.derived); err != nil {
					return err
				}
				if err := invalidateSelectionFactSets(ctx, c, meta.Incarnation, revKey(2), map[string]bool{id: true}); err != nil {
					return err
				}
				return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(2))
			}); err != nil {
				t.Fatal(err)
			}
			checkProjectedSource(t, s, source, 1, 1, true, driver.SelectionDigest(oldValue))
			checkProjectedSource(t, s, source, 2, 2, present, digest)
			var factEnd string
			if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_fact_sets WHERE row_key='legacy-fact'`).Scan(&factEnd); err != nil {
				t.Fatal(err)
			}
			if count, _, end, _, _, _ := demandSourceHistoryRows(t, s, id); count != 1 || end != revKey(2) || factEnd != end {
				t.Fatalf("legacy close/fact invalidation count=%d end=%q fact=%q", count, end, factEnd)
			}
		})
	}
}

func TestDemandSelectionSourceHistoryMissingAndMalformedRefuseAtomically(t *testing.T) {
	ctx := context.Background()
	s, lease := demandAPIStore(t)
	source := demandSourceKey("malformed-source")
	revision := demandAPIWriteSource(t, s, lease, source, []byte("value"), true)
	id := keyID(demandSourceLock(source))
	for _, test := range []struct {
		name            string
		present, digest any
		want            error
	}{
		{"missing_legacy", nil, nil, driver.ErrSelectionUnavailable},
		{"partial_presence", 1, nil, driver.ErrUnsupported},
		{"partial_digest", nil, driver.SelectionDigest([]byte("value")), driver.ErrUnsupported},
		{"invalid_presence", 2, "", driver.ErrUnsupported},
		{"absent_digest", 0, "nonempty", driver.ErrUnsupported},
		{"invalid_digest", 1, "not-a-digest", driver.ErrUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := s.db.ExecContext(ctx, `UPDATE key_history SET derived_source_present=?,derived_source_digest=? WHERE key=?`, test.present, test.digest, id); err != nil {
				t.Fatal(err)
			}
			before := demandPublicationImages(t, s)
			r := sourceReader(t, s, nil)
			want := driver.SelectionSource{Key: demandSourceLock(source), Revision: revision, Present: true, Digest: driver.SelectionDigest([]byte("value"))}
			if _, err := r.Select(ctx, demandAPIPlan(t, r, want)); !errors.Is(err, test.want) {
				t.Fatalf("malformed source error=%v want=%v", err, test.want)
			}
			if !reflect.DeepEqual(before, demandPublicationImages(t, s)) {
				t.Fatal("refusal wrote rows")
			}
		})
	}
	missing := demandSourceKey("never-present")
	checkProjectedSource(t, s, missing, mustUint(revision), 0, false, "")
	r := sourceReader(t, s, nil)
	want := driver.SelectionSource{Key: demandSourceLock(missing), Revision: revision, Present: true, Digest: driver.SelectionDigest([]byte("expected"))}
	if _, err := r.Select(ctx, demandAPIPlan(t, r, want)); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("missing present observation=%v", err)
	}
}

func TestDemandSelectionSourceHistoryBatchRollback(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	derived, _ := keyPublicationInputs(t, 2*keyPublicationBatchSize+1)
	before := demandPublicationImages(t, s)
	abort := errors.New("abort after projected batches and legacy closes")
	var work keyPublicationWork
	err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
		if err := publishDerivedKeys(ctx, &work, model.Natural64(1), derived); err != nil {
			return err
		}
		tx := &transaction{derived: derived}
		if err := tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, revKey(1)); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) || work.statements != 3 || work.rows != 257 || work.maxArgs != 5*128 {
		t.Fatalf("rollback control work=%+v err=%v", work, err)
	}
	if !reflect.DeepEqual(before, demandPublicationImages(t, s)) {
		t.Fatal("rollback changed projected columns or intervals")
	}
}

// These controls exercise the public writer/selector boundaries. Unlike the
// isolated publisher controls above, they establish coverage through the real
// empty-store build and stage a complete, valid Facts annotation. They expose
// the emitter-only prototype's precommit and pure-reader regressions. They
// remain required controls for the corrected eager physical representation.
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

// Keep the real lock census and every structural predicate family in these
// API fixtures. Offline preparation requests bump=false and stages the same
// complete sentinel set only after PrepareOfflineHistory succeeds.
func demandBeginWrite(t *testing.T, s *Store, lease driver.ActiveLease, extra []driver.LockKey, bump bool, refs ...model.Ref) (*transaction, driver.ChangesetHeader) {
	t.Helper()
	keys := append([]driver.LockKey(nil), extra...)
	var ready, closure bool
	for _, key := range extra {
		if key.Kind != driver.LockDerived || key.Derived == nil {
			continue
		}
		switch key.Derived.Table {
		case driver.DerivedReadyState:
			ready = true
		case driver.DerivedAttribute, driver.DerivedAncestorClosure, driver.DerivedCutClosure:
			closure = true
		}
	}
	if ready {
		sentinel, err := driver.NewReadySentinel("projection-control", "task")
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	if closure {
		sentinel, err := driver.NewClosureSentinel("projection-control")
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	tx, header := beginSourceTestWithLocks(t, s, lease, keys, false, refs...)
	if bump {
		var sentinels []driver.Sentinel
		for _, key := range header.LockKeys {
			if key.Kind == driver.LockSentinel {
				sentinels = append(sentinels, key.Sentinel)
			}
		}
		if err := tx.BumpSentinels(context.Background(), sentinels); err != nil {
			t.Fatal(err)
		}
	}
	return tx, header
}

func demandAPIWriteSource(t *testing.T, s *Store, lease driver.ActiveLease, source driver.DerivedRowKey, value []byte, present bool) model.Revision {
	t.Helper()
	tx, header := demandBeginWrite(t, s, lease, []driver.LockKey{demandSourceLock(source)}, true)
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
			tx, _ := demandBeginWrite(t, s, lease, []driver.LockKey{demandSourceLock(source), demandSourceLock(fact)}, true)
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
			tx, _ := demandBeginWrite(t, s, lease, []driver.LockKey{demandSourceLock(source), demandSourceLock(fact)}, true)
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
	tx, header := demandBeginWrite(t, s, lease, []driver.LockKey{demandSourceLock(source), demandSourceLock(fact)}, true)
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
		tx, header := demandBeginWrite(t, s, lease, []driver.LockKey{demandSourceLock(source), demandSourceLock(fact)}, true)
		want := driver.SelectionSource{Key: demandSourceLock(source), Revision: sourceRevision, Present: true, Digest: driver.SelectionDigest(value)}
		row := demandAPIFactRow(t, tx.meta.Incarnation, want, fact)
		if err := tx.PutSelectionDerived(ctx, fact.Table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
		return commitSourceTest(t, tx, header).Revision
	}
	firstBinding := bind(firstSourceRevision, firstValue)
	secondSourceRevision := demandAPIWriteSource(t, s, lease, source, secondValue, true)
	var factEnd string
	if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_fact_sets WHERE row_key=? AND start_revision=?`, keyID(demandSourceLock(fact)), revKey(mustUint(firstBinding))).Scan(&factEnd); err != nil {
		t.Fatal(err)
	}
	if factEnd != revKey(mustUint(secondSourceRevision)) {
		t.Fatalf("source replacement did not atomically close fact interval: %q", factEnd)
	}
	checkProjectedSource(t, s, source, mustUint(firstBinding), mustUint(firstSourceRevision), true, driver.SelectionDigest(firstValue))
	checkProjectedSource(t, s, source, mustUint(secondSourceRevision), mustUint(secondSourceRevision), true, driver.SelectionDigest(secondValue))
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
			checkProjectedSource(t, s, source, 2, 1, true, driver.SelectionDigest(value))

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
			bound, header := demandBeginWrite(t, s, lease, []driver.LockKey{want.Key, demandSourceLock(fact)}, true)
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
			pending, pendingHeader := demandBeginWrite(t, s, lease, []driver.LockKey{demandSourceLock(pendingKey)}, true)
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
		`SELECT * FROM key_history ORDER BY key,rev`,
		`SELECT * FROM selection_source_history ORDER BY incarnation,branch,source_key,start_revision`,
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

// A selector never reads the opaque data envelope, while ordinary admission
// still observes it and retains its existing native/JSON error precedence.
func TestDemandSelectionColdObservationErrorsPrecedePublication(t *testing.T) {
	for _, name := range []string{"malformed_json", "native_table_error", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, lease := demandAPIStore(t)
			source := demandSourceKey("error-source")
			value := []byte("opaque")
			revision := demandAPIWriteSource(t, s, lease, source, value, true)
			id := keyID(demandSourceLock(source))
			if name == "malformed_json" {
				if _, err := s.db.ExecContext(ctx, `UPDATE key_history SET data=? WHERE key=?`, []byte(`{"bad":`), id); err != nil {
					t.Fatal(err)
				}
				r := sourceReader(t, s, nil)
				want := driver.SelectionSource{Key: demandSourceLock(source), Revision: revision, Present: true, Digest: driver.SelectionDigest(value)}
				if _, err := r.Select(ctx, demandAPIPlan(t, r, want)); err != nil {
					t.Fatalf("metadata selector decoded source body: %v", err)
				}
				before := demandPublicationImages(t, s)
				tx, err := s.Write(ctx, lease)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				// Lock must surface the existing malformed observation before
				// staging or publication is possible.
				_, err = tx.Lock(ctx, []driver.LockRequest{{Key: demandSourceLock(source), Mode: driver.LockUpdate}})
				var syntax *json.SyntaxError
				if !errors.As(err, &syntax) {
					t.Fatalf("opaque malformed source error=%v", err)
				}
				if !reflect.DeepEqual(before, demandPublicationImages(t, s)) {
					t.Fatal("admission error changed publication")
				}
				return
			}
			before := demandPublicationImages(t, s)
			err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				call := ctx
				if name == "native_table_error" {
					if _, err := c.ExecContext(ctx, `DROP TABLE key_history`); err != nil {
						return err
					}
				} else {
					var cancel context.CancelFunc
					call, cancel = context.WithCancel(ctx)
					cancel()
				}
				_, _, err := projectedDerivedSource(call, c, getMeta(t, ctx, s).Incarnation, id, mustUint(revision))
				return err
			})
			if name == "cancelled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel error=%v", err)
				}
			} else {
				var native interface{ Code() int }
				if !errors.As(err, &native) || native.Code() != 1 {
					t.Fatalf("native SQLite error replaced: %v", err)
				}
			}
			if !reflect.DeepEqual(before, demandPublicationImages(t, s)) {
				t.Fatal("failed read lost transaction rollback")
			}
		})
	}
}

func TestDemandSelectionLegacyObservationMustMatchMutation(t *testing.T) {
	for _, name := range []string{"missing_interval", "wrong_token", "no_key_row", "other_incarnation", "other_branch"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			meta := getMeta(t, ctx, s)
			source := demandSourceKey("legacy-" + name + string([]byte{0xff}))
			seedLegacyDemandSource(t, s, source, 1, []byte("legacy"))
			id := keyID(demandSourceLock(source))
			var query string
			switch name {
			case "missing_interval":
				query = `DELETE FROM selection_source_history WHERE source_key=?`
			case "wrong_token":
				query = `UPDATE selection_source_history SET source_revision='00000000000000000000' WHERE source_key=?`
			case "no_key_row":
				query = `DELETE FROM key_history WHERE key=?`
			case "other_incarnation":
				query = `UPDATE selection_source_history SET incarnation='other' WHERE source_key=?`
			case "other_branch":
				query = `UPDATE selection_source_history SET branch='00000000000000000001' WHERE source_key=?`
			}
			if _, err := s.db.ExecContext(ctx, query, id); err != nil {
				t.Fatal(err)
			}
			before := demandPublicationImages(t, s)
			_, _, err := projectedDerivedSource(ctx, s.db, meta.Incarnation, id, 1)
			if !errors.Is(err, driver.ErrSelectionUnavailable) {
				t.Fatalf("inconsistent retained observation became miss/current: %v", err)
			}
			if !reflect.DeepEqual(before, demandPublicationImages(t, s)) {
				t.Fatal("legacy refusal changed rows")
			}
		})
	}
}

// Construct the actual old physical layout, not a current store relabelled as
// old. A held SQL read transaction represents the old binary's WAL snapshot;
// independent executable compatibility remains a separate gate.
func demandLegacyDatabase(t *testing.T, version int) (string, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), fmt.Sprintf("legacy%d.db", version))
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	legacy := strings.Replace(schema, " derived_source_present INTEGER, derived_source_digest TEXT, PRIMARY KEY(key, rev)", " PRIMARY KEY(key, rev)", 1)
	if legacy == schema {
		t.Fatal("legacy DDL did not remove projection columns")
	}
	if version == 1 {
		legacy = `CREATE TABLE meta(singleton INTEGER PRIMARY KEY,store_id TEXT,incarnation TEXT,schema_version INTEGER,head TEXT,history_floor TEXT);
		CREATE TABLE active_lease(singleton INTEGER PRIMARY KEY,epoch TEXT,token TEXT,holder TEXT,expires INTEGER);
		CREATE TABLE replays(scope TEXT PRIMARY KEY,created INTEGER,rev TEXT,request_hash TEXT,response BLOB,changeset BLOB,finalized INTEGER,post BLOB);`
	}
	if _, err := db.ExecContext(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO meta VALUES(1,'projection-store','projection-incarnation',?,?,?)`, version, revKey(0), revKey(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO active_lease VALUES(1,?,'','',0)`, revKey(17)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO replays(scope,created,rev,request_hash,response,finalized,post) VALUES('final',1,?,'final-hash',?,1,?)`, revKey(0), []byte(`{"revision":0,"results":[],"warnings":[]}`), []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	if version == 2 {
		if _, err := db.ExecContext(ctx, `INSERT INTO replays(scope,created,rev,request_hash,response,finalized,post,pending) VALUES('pending',2,?,'pending-hash',?,0,NULL,1)`, revKey(0), []byte(`{"revision":0,"results":[],"warnings":[]}`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	return path, db
}

func TestDemandSelectionMetadataMigrationAtomicAndIdempotent(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ctx := context.Background()
			path, db := demandLegacyDatabase(t, version)
			// Fail precisely at the last publication step, after both ALTERs
			// (or the v1 CREATE) and pending-receipt migration have executed.
			if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_projection_version BEFORE UPDATE OF schema_version ON meta BEGIN SELECT RAISE(ABORT,'reject projection version'); END`); err != nil {
				t.Fatal(err)
			}
			if s, err := (Driver{}).Open(ctx, driver.Config{DSN: path}); err == nil {
				_ = s.Close()
				t.Fatal("failed migration opened")
			}
			var gotVersion, columns, pendingColumn int
			var post []byte
			if err := db.QueryRowContext(ctx, `SELECT schema_version FROM meta`).Scan(&gotVersion); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('key_history') WHERE name LIKE 'derived_source_%'`).Scan(&columns); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('replays') WHERE name='pending'`).Scan(&pendingColumn); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT post FROM replays WHERE scope='final'`).Scan(&post); err != nil {
				t.Fatal(err)
			}
			if gotVersion != version || columns != 0 || pendingColumn != version-1 || string(post) != "null" {
				t.Fatalf("partial migration escaped: version=%d columns=%d pending=%d post=%s", gotVersion, columns, pendingColumn, post)
			}
			if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_projection_version`); err != nil {
				t.Fatal(err)
			}
			// Concurrent Open calls serialize on the real publication lock.
			start := make(chan struct{})
			results := make(chan error, 2)
			for range 2 {
				go func() {
					<-start
					s, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
					if err == nil {
						err = s.Close()
					}
					results <- err
				}()
			}
			close(start)
			for range 2 {
				if err := <-results; err != nil {
					t.Fatal(err)
				}
			}
			s := openStore(t, ctx, path)
			meta := getMeta(t, ctx, s)
			if meta.SchemaVersion != 3 || meta.StoreID != "projection-store" || meta.Incarnation != "projection-incarnation" || !meta.Head.IsZero() || !meta.HistoryFloor.IsZero() {
				t.Fatalf("migration changed identity/floor/head: %+v", meta)
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('key_history') WHERE name LIKE 'derived_source_%'`).Scan(&columns); err != nil {
				t.Fatal(err)
			}
			if columns != 2 {
				t.Fatalf("projection columns=%d", columns)
			}
			var epoch string
			if err := db.QueryRowContext(ctx, `SELECT epoch FROM active_lease`).Scan(&epoch); err != nil {
				t.Fatal(err)
			}
			if epoch != revKey(17) {
				t.Fatalf("migration changed epoch=%s", epoch)
			}
			if err := db.QueryRowContext(ctx, `SELECT post FROM replays WHERE scope='final'`).Scan(&post); err != nil {
				t.Fatal(err)
			}
			wantPost := "null"
			if version == 1 {
				wantPost = "[]"
			}
			if string(post) != wantPost {
				t.Fatalf("final receipt post=%s want=%s", post, wantPost)
			}
			if version == 2 {
				var pending int
				if err := db.QueryRowContext(ctx, `SELECT pending FROM replays WHERE scope='pending'`).Scan(&pending); err != nil || pending != 1 {
					t.Fatalf("pending receipt changed: %d/%v", pending, err)
				}
			}
		})
	}
}

func TestDemandSelectionMigrationMixedHistoryAndHeldPin(t *testing.T) {
	ctx := context.Background()
	path, db := demandLegacyDatabase(t, 2)
	source := demandSourceKey("legacy-opaque-" + string([]byte{0xff, 0xfe}))
	id := keyID(demandSourceLock(source))
	value := []byte("legacy-value")
	state := driver.LockState{Key: demandSourceLock(source), Revision: model.Natural64(1), Present: true, Derived: &driver.DerivedRow{Key: source.Key, Value: value}}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, id, revKey(1), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO selection_source_history(incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest) VALUES('projection-incarnation',?,?,?,?,?,?)`, revKey(0), id, revKey(1), revKey(1), 1, driver.SelectionDigest(value)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE meta SET head=?`, revKey(1)); err != nil {
		t.Fatal(err)
	}
	held, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback()
	var version int
	if err := held.QueryRowContext(ctx, `SELECT schema_version FROM meta`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("old pin=%d/%v", version, err)
	}
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	meta := getMeta(t, ctx, s)
	decode, projection := selectionBackfillRaceProjection(meta.Incarnation, nil)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, projection); err != nil {
		t.Fatal(err)
	}
	r := sourceReader(t, s, nil)
	want := driver.SelectionSource{Key: demandSourceLock(source), Revision: model.Natural64(1), Present: true, Digest: driver.SelectionDigest(value)}
	if _, err := r.Select(ctx, demandAPIPlan(t, r, want)); err != nil {
		t.Fatalf("migrated legacy boundary: %v", err)
	}
	newRevision := demandAPIWriteSource(t, s, lease, source, []byte("projected"), true)
	deleted := demandAPIWriteSource(t, s, lease, source, nil, false)
	checkProjectedSource(t, s, source, 1, 1, true, want.Digest)
	checkProjectedSource(t, s, source, mustUint(newRevision), mustUint(newRevision), true, driver.SelectionDigest([]byte("projected")))
	checkProjectedSource(t, s, source, mustUint(deleted), mustUint(deleted), false, "")
	if _, err := r.Select(ctx, demandAPIPlan(t, r, want)); err != nil {
		t.Fatalf("held new-reader pin changed: %v", err)
	}
	var end sql.NullString
	var token, digest string
	if err := held.QueryRowContext(ctx, `SELECT source_revision,source_digest,end_revision FROM selection_source_history WHERE source_key=?`, id).Scan(&token, &digest, &end); err != nil {
		t.Fatal(err)
	}
	if token != revKey(1) || digest != want.Digest || end.Valid {
		t.Fatalf("old WAL snapshot changed: %s/%s/%+v", token, digest, end)
	}
	if err := held.QueryRowContext(ctx, `SELECT schema_version FROM meta`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("old version snapshot changed: %d/%v", version, err)
	}
}

func TestDemandSelectionMigrationRefusesInconsistentColumnShape(t *testing.T) {
	for _, name := range []string{"partial_v2", "both_v2", "missing_v3", "wrong_type_v3"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			path, db := demandLegacyDatabase(t, 2)
			switch name {
			case "partial_v2":
				if _, err := db.ExecContext(ctx, `ALTER TABLE key_history ADD COLUMN derived_source_present INTEGER`); err != nil {
					t.Fatal(err)
				}
			case "both_v2":
				if _, err := db.ExecContext(ctx, `ALTER TABLE key_history ADD COLUMN derived_source_present INTEGER; ALTER TABLE key_history ADD COLUMN derived_source_digest TEXT`); err != nil {
					t.Fatal(err)
				}
			case "missing_v3":
				if _, err := db.ExecContext(ctx, `UPDATE meta SET schema_version=3`); err != nil {
					t.Fatal(err)
				}
			case "wrong_type_v3":
				if _, err := db.ExecContext(ctx, `ALTER TABLE key_history ADD COLUMN derived_source_present TEXT; ALTER TABLE key_history ADD COLUMN derived_source_digest TEXT; UPDATE meta SET schema_version=3`); err != nil {
					t.Fatal(err)
				}
			}
			if s, err := (Driver{}).Open(ctx, driver.Config{DSN: path}); !errors.Is(err, driver.ErrUnsupported) {
				if s != nil {
					_ = s.Close()
				}
				t.Fatalf("inconsistent column layout accepted: %v", err)
			}
		})
	}
}

func TestDemandSelectionSchemaFencePreservesExactReplay(t *testing.T) {
	ctx := context.Background()
	s, lease := demandAPIStore(t)
	source := demandSourceKey("replay-source")
	row := driver.DerivedRow{Key: source.Key, Value: []byte("replay-value")}
	tx, header := demandBeginWrite(t, s, lease, []driver.LockKey{demandSourceLock(source)}, true)
	header.Method = "projection.replay"
	header.IdempotencyKey = "projection-replay"
	header.RequestHash = driver.SelectionDigest([]byte("projection-request"))
	if err := tx.PutDerived(ctx, source.Table, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	first := commitSourceTest(t, tx, header)
	held, _ := demandBeginWrite(t, s, lease, []driver.LockKey{demandSourceLock(source)}, true)
	if err := held.PutDerived(ctx, source.Table, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE meta SET schema_version=?`, schemaVersion+1); err != nil {
		t.Fatal(err)
	}
	before := demandPublicationImages(t, s)
	replayed, err := held.Commit(ctx, header)
	if err != nil || !reflect.DeepEqual(replayed, first) {
		t.Fatalf("exact replay reached the new-version fence: %+v/%v", replayed, err)
	}
	if !reflect.DeepEqual(before, demandPublicationImages(t, s)) {
		t.Fatal("replay published projected source/receipt/head")
	}
}

func TestDemandSelectionMetadataScanFailureIsNotAbsence(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	source := demandSourceKey("malformed-sql-type")
	seedDemandSourceKeyHistory(t, s, meta.Incarnation, source, 1, []byte("value"))
	id := keyID(demandSourceLock(source))
	if _, err := s.db.ExecContext(ctx, `UPDATE key_history SET derived_source_present='not-an-integer' WHERE key=?`, id); err != nil {
		t.Fatal(err)
	}
	_, _, err := projectedDerivedSource(ctx, s.db, meta.Incarnation, id, 1)
	var numeric *strconv.NumError
	// database/sql preserves the conversion error's native message; it must
	// not be rewritten into a successful zero observation or legacy fallback.
	if err == nil || (!errors.As(err, &numeric) && !strings.Contains(err.Error(), "converting driver.Value")) {
		t.Fatalf("scan failure rewritten: %v", err)
	}
}

func TestDemandSelectionOfflineHistoryScalarProjectionAndFloor(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial_%t", partial), func(t *testing.T) {
			ctx := context.Background()
			original := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = original.Close() })
			originalLease := getLease(t, ctx, original, driver.LeaseTTL)
			ref := testRef("projection-imported-deleted")
			create, h := beginSourceTest(t, original, originalLease, ref)
			if err := create.PutGraphRecord(ctx, sourceRecord(ref.ID, `{"title":"retained"}`), nil, nil); err != nil {
				t.Fatal(err)
			}
			first := commitSourceTest(t, create, h)
			captured := sourceReader(t, original, nil)
			images, err := captured.Get(ctx, []model.Ref{ref})
			if err != nil || len(images) != 1 {
				t.Fatalf("actual import source=%v/%v", images, err)
			}
			remove, h := beginSourceTest(t, original, originalLease, ref)
			if err := remove.Delete(ctx, ref); err != nil {
				t.Fatal(err)
			}
			last := commitSourceTest(t, remove, h)
			history := driver.OfflineHistory{Source: "sqlite-projection-history", Verified: true, Complete: true, First: 1, Head: 2, Batches: []driver.SourceHistoryBatch{
				{Sequence: 1, CommitIDs: []string{first.Revision.String()}, Changes: []driver.SourceHistoryChange{{After: &images[0]}}},
				{Sequence: 2, CommitIDs: []string{last.Revision.String()}, Changes: []driver.SourceHistoryChange{{Before: &images[0]}}},
			}}
			if partial {
				history.Complete, history.First, history.BaselineVerified = false, 2, true
				history.Baseline = []driver.Record{images[0]}
				history.Gaps = []driver.SourceHistoryGap{{First: 1, Last: 1, Reason: "verified prefix omitted from retained suffix"}}
				history.Batches = history.Batches[1:]
			}
			when := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
			profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: getMeta(t, ctx, original).StoreID, Method: "offline.restore.import", IdempotencyKey: "projection-history", ArchiveDigest: strings.Repeat("b", 64), DerivedDigest: strings.Repeat("c", 64), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.Format(time.RFC3339Nano)}
			if partial {
				profile.HistoryMode = driver.RestoreHistoryModePartial
			}
			profile.HistoryDigest, err = driver.OfflineHistoryDigest(history)
			if err != nil {
				t.Fatal(err)
			}
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			before := getMeta(t, ctx, s)
			marker := model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))
			present, tombstone := demandSourceKey("offline-opaque-"+string([]byte{0xff})), demandSourceKey("offline-tombstone")
			importer, header := demandBeginWrite(t, s, lease, []driver.LockKey{{Kind: driver.LockRecord, Ref: &marker}, demandSourceLock(present), demandSourceLock(tombstone)}, false, ref)
			meta, err := importer.PrepareOfflineHistory(ctx, profile, history, when)
			if err != nil {
				t.Fatal(err)
			}
			var sentinels []driver.Sentinel
			for _, key := range header.LockKeys {
				if key.Kind == driver.LockSentinel {
					sentinels = append(sentinels, key.Sentinel)
				}
			}
			if err := importer.BumpSentinels(ctx, sentinels); err != nil {
				t.Fatal(err)
			}
			if err := importer.PutDerived(ctx, present.Table, []driver.DerivedRow{{Key: tombstone.Key}}, []driver.DerivedRow{{Key: present.Key, Value: nil}}); err != nil {
				t.Fatal(err)
			}
			header.Incarnation, header.Kind, header.Time, header.Method, header.IdempotencyKey, header.Principal = meta.Incarnation, model.ChangesetRestore, when, profile.Method, profile.IdempotencyKey, profile.Admission.Actor
			header.RequestHash, err = profile.RequestHash()
			if err != nil {
				t.Fatal(err)
			}
			entry, err := importer.CommitOfflineHistory(ctx, header)
			if err != nil {
				t.Fatal(err)
			}
			checkProjectedSource(t, s, present, mustUint(entry.Revision), mustUint(entry.Revision), true, driver.SelectionDigest(nil))
			checkProjectedSource(t, s, tombstone, mustUint(entry.Revision), mustUint(entry.Revision), false, "")
			decode, projection := selectionBackfillRaceProjection(meta.Incarnation, nil)
			if err := s.BuildGraphSourceIndexes(ctx, lease, decode, projection); err != nil {
				t.Fatal(err)
			}
			r := sourceReader(t, s, nil)
			for _, source := range []driver.SelectionSource{
				{Key: demandSourceLock(present), Revision: entry.Revision, Present: true, Digest: driver.SelectionDigest(nil)},
				{Key: demandSourceLock(tombstone), Revision: entry.Revision},
			} {
				if _, err := r.Select(ctx, demandAPIPlan(t, r, source)); err != nil {
					t.Fatalf("offline scalar source boundary: %v", err)
				}
			}
			zero := model.Natural64(0)
			view, floorErr := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero})
			if partial {
				if view != nil {
					_ = view.Close()
				}
				var truncated driver.ErrTruncated
				if !errors.As(floorErr, &truncated) || mustUint(truncated.Floor) != 1 {
					t.Fatalf("partial retained floor error=%v", floorErr)
				}
			} else {
				if floorErr != nil {
					t.Fatalf("full history refused its valid revision-zero baseline: %v", floorErr)
				}
				_ = view.Close()
			}
			if view, err := s.Read(ctx, driver.View{Incarnation: before.Incarnation}); err == nil {
				_ = view.Close()
				t.Fatal("old incarnation accepted")
			} else {
				var changed driver.ErrIncarnationChanged
				if !errors.As(err, &changed) {
					t.Fatalf("incarnation error=%v", err)
				}
			}
			if view, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, Branch: model.Natural64(1)}); !errors.Is(err, driver.ErrUnsupported) {
				if view != nil {
					_ = view.Close()
				}
				t.Fatalf("branch error=%v", err)
			}
		})
	}
}

func demandAPIBindFacts(t *testing.T, s *Store, lease driver.ActiveLease, source driver.SelectionSource, facts ...driver.DerivedRowKey) model.Revision {
	t.Helper()
	keys := []driver.LockKey{source.Key}
	for _, fact := range facts {
		keys = append(keys, demandSourceLock(fact))
	}
	tx, header := demandBeginWrite(t, s, lease, keys, true)
	rows := make([]driver.DerivedRow, len(facts))
	for i, fact := range facts {
		rows[i] = demandAPIFactRow(t, tx.meta.Incarnation, source, fact)
	}
	if err := tx.PutSelectionDerived(context.Background(), driver.DerivedAttribute, nil, rows); err != nil {
		t.Fatal(err)
	}
	return commitSourceTest(t, tx, header).Revision
}

func demandCheckFactPin(t *testing.T, s *Store, content model.Revision, fact driver.DerivedRowKey, wantError bool) {
	t.Helper()
	ctx := context.Background()
	r := sourceReader(t, s, nil)
	control := testRef("fact-pin-never-present-control")
	plan := demandAPIPlan(t, r, driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)})
	plan.ContentRevision, plan.ContentCommitted = content, true
	plan.Membership = driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: fact, Kind: driver.SelectionLabelFact}}
	got, err := r.Select(ctx, plan)
	if wantError {
		if !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
			t.Fatalf("removed fact did not refuse at %s: %+v/%v", content.String(), got, err)
		}
	} else if err != nil || got.Total != 0 || got.More {
		t.Fatalf("historical fact lost source at %s: %+v/%v", content.String(), got, err)
	}
}

func TestDemandSelectionProjectedBatchesLegacyAndFactsRollbackTogether(t *testing.T) {
	ctx := context.Background()
	s, lease := demandAPIStore(t)
	const count = 2*keyPublicationBatchSize + 1
	keys := make([]driver.LockKey, count)
	rows := make([]driver.DerivedRow, count)
	for i := range rows {
		key := demandSourceKey(fmt.Sprintf("atomic-source-%03d", i))
		keys[i] = demandSourceLock(key)
		rows[i] = driver.DerivedRow{Key: key.Key, Value: []byte(fmt.Sprintf("before-%d", i))}
	}
	initial, header := demandBeginWrite(t, s, lease, keys, true)
	if err := initial.PutDerived(ctx, driver.DerivedReadyState, nil, rows); err != nil {
		t.Fatal(err)
	}
	r1 := commitSourceTest(t, initial, header).Revision
	meta := getMeta(t, ctx, s)
	legacyID := keyID(keys[0])
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		if _, err := c.ExecContext(ctx, `UPDATE key_history SET derived_source_present=NULL,derived_source_digest=NULL WHERE key=? AND rev=?`, legacyID, revKey(mustUint(r1))); err != nil {
			return err
		}
		_, err := c.ExecContext(ctx, `INSERT INTO selection_source_history(incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest) VALUES(?,?,?,?,?,?,?)`, meta.Incarnation, revKey(0), legacyID, revKey(mustUint(r1)), revKey(mustUint(r1)), 1, driver.SelectionDigest(rows[0].Value))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	facts := []driver.DerivedRowKey{demandFactKey("atomic-fact-a"), demandFactKey("atomic-fact-b")}
	binding := demandAPIBindFacts(t, s, lease, driver.SelectionSource{Key: keys[0], Revision: r1, Present: true, Digest: driver.SelectionDigest(rows[0].Value)}, facts...)
	for i := range rows {
		rows[i].Value = []byte(fmt.Sprintf("after-%d", i))
	}
	changed, header := demandBeginWrite(t, s, lease, keys, true)
	if err := changed.PutDerived(ctx, driver.DerivedReadyState, nil, rows); err != nil {
		t.Fatal(err)
	}
	before := demandPublicationImages(t, s)
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER abort_projection_publication BEFORE INSERT ON changesets BEGIN SELECT RAISE(ABORT,'projected legacy fact abort'); END`); err != nil {
		t.Fatal(err)
	}
	_, err := changed.Commit(ctx, header)
	var native interface{ Code() int }
	if !errors.As(err, &native) || native.Code()&255 != 19 || !strings.Contains(err.Error(), "projected legacy fact abort") {
		t.Fatalf("late native failure=%v", err)
	}
	if !reflect.DeepEqual(before, demandPublicationImages(t, s)) {
		t.Fatal("late abort changed projected columns, legacy/fact intervals, head or receipt")
	}
	if _, err := s.db.ExecContext(ctx, `DROP TRIGGER abort_projection_publication`); err != nil {
		t.Fatal(err)
	}
	recovered := commitSourceTest(t, changed, header)
	for i, key := range keys {
		checkProjectedSource(t, s, *key.Derived, mustUint(recovered.Revision), mustUint(recovered.Revision), true, driver.SelectionDigest(rows[i].Value))
	}
	if n, _, end, _, _, _ := demandSourceHistoryRows(t, s, legacyID); n != 1 || end != revKey(mustUint(recovered.Revision)) {
		t.Fatalf("legacy interval not closed exactly once: %d/%s", n, end)
	}
	for _, fact := range facts {
		var end string
		if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_fact_sets WHERE row_key=? AND start_revision=?`, keyID(demandSourceLock(fact)), revKey(mustUint(binding))).Scan(&end); err != nil {
			t.Fatal(err)
		}
		if end != revKey(mustUint(recovered.Revision)) {
			t.Fatalf("dependent invalidation=%s", end)
		}
		demandCheckFactPin(t, s, binding, fact, false)
		demandCheckFactPin(t, s, recovered.Revision, fact, true)
	}
}

func TestDemandSelectionMultipleDependentsRemovalAndUnboundGap(t *testing.T) {
	ctx := context.Background()
	s, lease := demandAPIStore(t)
	source := demandSourceKey("unbound-source")
	value := []byte("first")
	r1 := demandAPIWriteSource(t, s, lease, source, value, true)
	facts := []driver.DerivedRowKey{demandFactKey("unbound-a"), demandFactKey("unbound-b")}
	firstBinding := demandAPIBindFacts(t, s, lease, driver.SelectionSource{Key: demandSourceLock(source), Revision: r1, Present: true, Digest: driver.SelectionDigest(value)}, facts...)
	keys := []driver.LockKey{demandSourceLock(facts[0]), demandSourceLock(facts[1])}
	remove, header := demandBeginWrite(t, s, lease, keys, true)
	if err := remove.PutSelectionDerived(ctx, driver.DerivedAttribute, []driver.DerivedRow{{Key: facts[0].Key}, {Key: facts[1].Key}}, nil); err != nil {
		t.Fatal(err)
	}
	removed := commitSourceTest(t, remove, header).Revision
	updated := demandAPIWriteSource(t, s, lease, source, []byte("after-gap"), true)
	r := sourceReader(t, s, nil)
	want := driver.SelectionSource{Key: demandSourceLock(source), Revision: updated, Present: true, Digest: driver.SelectionDigest([]byte("after-gap"))}
	if _, err := r.Select(ctx, demandAPIPlan(t, r, want)); err != nil {
		t.Fatalf("no dependents suppressed pure-reader state: %v", err)
	}
	for _, fact := range facts {
		demandCheckFactPin(t, s, firstBinding, fact, false)
		demandCheckFactPin(t, s, removed, fact, true)
		demandCheckFactPin(t, s, updated, fact, true)
	}
	rebound := demandAPIBindFacts(t, s, lease, want, facts...)
	for _, fact := range facts {
		demandCheckFactPin(t, s, firstBinding, fact, false)
		demandCheckFactPin(t, s, rebound, fact, false)
	}
	checkProjectedSource(t, s, source, mustUint(rebound), mustUint(updated), true, want.Digest)
	if count, _, _, _, _, _ := demandSourceHistoryRows(t, s, keyID(want.Key)); count != 0 {
		t.Fatal("rebind duplicated projected source into thin table")
	}
}

func TestDemandSelectionSameCommitSourceAndDependentRebound(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprintf("present_%t", present), func(t *testing.T) {
			ctx := context.Background()
			s, lease := demandAPIStore(t)
			source, fact := demandSourceKey("same-commit-source"), demandFactKey("same-commit-fact")
			r1 := demandAPIWriteSource(t, s, lease, source, []byte("old"), true)
			tx, header := demandBeginWrite(t, s, lease, []driver.LockKey{demandSourceLock(source), demandSourceLock(fact)}, true)
			var del, add []driver.DerivedRow
			digest := ""
			if present {
				add = []driver.DerivedRow{{Key: source.Key, Value: []byte("replacement")}}
				digest = driver.SelectionDigest(add[0].Value)
			} else {
				del = []driver.DerivedRow{{Key: source.Key}}
			}
			if err := tx.PutDerived(ctx, source.Table, del, add); err != nil {
				t.Fatal(err)
			}
			observed := driver.SelectionSource{Key: demandSourceLock(source), Revision: r1, Present: present, Digest: digest}
			row := demandAPIFactRow(t, tx.meta.Incarnation, observed, fact)
			if err := tx.PutSelectionDerived(ctx, fact.Table, nil, []driver.DerivedRow{row}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Select(ctx, demandAPIPlan(t, tx.reader, observed)); err != nil {
				t.Fatalf("staged source lost pinned mutation token: %v", err)
			}
			committed := commitSourceTest(t, tx, header)
			checkProjectedSource(t, s, source, mustUint(committed.Revision), mustUint(committed.Revision), present, digest)
			var actualRevision, actualDigest string
			var actualPresent int
			if err := s.db.QueryRowContext(ctx, `SELECT source_revision,source_present,source_digest FROM selection_fact_sets WHERE row_key=? AND start_revision=?`, keyID(demandSourceLock(fact)), revKey(mustUint(committed.Revision))).Scan(&actualRevision, &actualPresent, &actualDigest); err != nil {
				t.Fatal(err)
			}
			if actualRevision != revKey(mustUint(committed.Revision)) || (actualPresent == 1) != present || actualDigest != digest {
				t.Fatalf("source/dependent rebound diverged: %s/%d/%s", actualRevision, actualPresent, actualDigest)
			}
			demandCheckFactPin(t, s, committed.Revision, fact, false)
		})
	}
}
