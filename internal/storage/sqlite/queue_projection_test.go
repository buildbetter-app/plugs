package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func queueTestStore(t *testing.T) (*Store, driver.ActiveLease) {
	t.Helper()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	return s, getLease(t, context.Background(), s, driver.LeaseTTL)
}

func queueDocument(id, title string) driver.Record {
	doc := pipeline.Document{ID: id, Type: "issue", Project: "queue-project", Fields: map[string]any{"title": title, "status": "todo", "priority": 0}}
	raw, _ := json.Marshal(doc)
	return driver.Record{Ref: testRef(id), Kind: "bead", Body: raw}
}

// Every test publication uses the real producer and its full pre-Head lock
// census. Nil body means typed deletion. No synthetic queue facts are installed.
func queueStage(t *testing.T, s *Store, lease driver.ActiveLease, records ...driver.Record) (*transaction, driver.ChangesetHeader) {
	t.Helper()
	ctx := context.Background()
	keys := map[string]driver.LockKey{}
	bumps := map[driver.Sentinel]bool{}
	plans := make([]*pipeline.SourceProjectionPlan, len(records))
	for i, record := range records {
		var plan *pipeline.SourceProjectionPlan
		var err error
		if record.Body == nil {
			plan, err = pipeline.PrepareSourceDelete(record.Ref, record.Kind)
		} else {
			plan, err = pipeline.PrepareSourceRecord(record)
		}
		if err != nil {
			t.Fatal(err)
		}
		plans[i] = plan
		for _, key := range plan.LockKeys() {
			id, _ := driver.EncodeLockKey(key)
			keys[id] = key
		}
		for _, sentinel := range plan.Sentinels() {
			bumps[sentinel] = true
		}
	}
	ids := make([]string, 0, len(keys))
	for id := range keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	locks := make([]driver.LockRequest, 0, len(ids))
	declared := make([]driver.LockKey, 0, len(ids))
	for _, id := range ids {
		locks = append(locks, driver.LockRequest{Key: keys[id], Mode: driver.LockUpdate})
		declared = append(declared, keys[id])
	}
	writer, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	tx := writer.(*transaction)
	t.Cleanup(func() { _ = tx.Close() })
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		if err := plan.Publish(ctx, tx); err != nil {
			t.Fatal(err)
		}
	}
	sentinels := make([]driver.Sentinel, 0, len(bumps))
	for sentinel := range bumps {
		sentinels = append(sentinels, sentinel)
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return tx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "queue-test"}, LockKeys: declared}
}

func queueRequire(t *testing.T, r driver.QueueProjectionReader, ids ...string) driver.QueueProjection {
	t.Helper()
	got, err := r.QueueProjection(context.Background())
	if err != nil || len(got.Beads) != len(ids) {
		t.Fatalf("complete queue projection: rows=%d want=%v err=%v", len(got.Beads), ids, err)
	}
	for i, id := range ids {
		if got.Beads[i].Bead.Ref.ID != id {
			t.Fatalf("queue row %d=%s want=%s", i, got.Beads[i].Bead.Ref.ID, id)
		}
	}
	return got
}

func TestQueueProjectionCommittedSnapshotAndOverlay(t *testing.T) {
	s, lease := queueTestStore(t)
	ctx := context.Background()
	a, hidden := queueDocument("queue-a", "original"), queueDocument("queue-hidden", "hidden")
	tx, h := queueStage(t, s, lease, a, hidden)
	commitSourceTest(t, tx, h)
	pinned := sourceReader(t, s, nil)
	defer pinned.Close()
	initial := queueRequire(t, pinned, a.Ref.ID, hidden.Ref.ID)
	// QueueFacts survive the actual SQLite JSON/PostgreSQL gob publication.
	if initial.Beads[0].Facts.Version != driver.QueueFactsVersion || initial.Beads[0].Facts.LeasePresent {
		t.Fatal("queue wire metadata lost")
	}
	changed, born := queueDocument(a.Ref.ID, "staged replacement"), queueDocument("queue-born", "staged create")
	tx, h = queueStage(t, s, lease, changed, born, driver.Record{Ref: hidden.Ref, Kind: "bead"})
	projection := queueRequire(t, tx, a.Ref.ID, hidden.Ref.ID)
	images, err := tx.QueueProjectionImages(ctx, projection.View, []model.Ref{a.Ref, hidden.Ref})
	if err != nil || len(images) != 2 || !bytes.Equal(images[0].Body, a.Body) || !bytes.Equal(images[1].Body, hidden.Body) {
		t.Fatalf("queue images included overlay replacement/delete: %+v %v", images, err)
	}
	ordinary, err := tx.Get(ctx, []model.Ref{a.Ref})
	if err != nil || len(ordinary) != 1 || !bytes.Equal(ordinary[0].Body, changed.Body) {
		t.Fatal("ordinary staged Get control did not observe replacement")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// A transaction opened before another commit must make its own fresh
	// terminal snapshot after Head, excluding staged data.
	terminal, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()
	queue := terminal.(driver.QueueProjectionReader)
	if _, err := queue.QueueProjection(ctx); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("queue before Head: %v", err)
	}
	tx, h = queueStage(t, s, lease, born)
	commitSourceTest(t, tx, h)
	lock := driver.LockRequest{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate}
	if _, err := terminal.Lock(ctx, []driver.LockRequest{lock}); err != nil {
		t.Fatal(err)
	}
	if _, err := terminal.Head(ctx); err != nil {
		t.Fatal(err)
	}
	queueRequire(t, queue, a.Ref.ID, born.Ref.ID, hidden.Ref.ID)
	if err := terminal.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	queueRequire(t, pinned, a.Ref.ID, hidden.Ref.ID)
	fresh := sourceReader(t, s, nil)
	defer fresh.Close()
	queueRequire(t, fresh, a.Ref.ID, born.Ref.ID, hidden.Ref.ID)
}

func TestQueueProjectionRejectsHiddenSourceAndCoverageCorruption(t *testing.T) {
	for _, corruption := range []string{"hidden_header_and_both_families", "missing_link", "stale_source", "unready"} {
		t.Run(corruption, func(t *testing.T) {
			s, lease := queueTestStore(t)
			a, hidden := queueDocument("queue-a", "a"), queueDocument("queue-hidden", "hidden")
			tx, h := queueStage(t, s, lease, a, hidden)
			commitSourceTest(t, tx, h)
			control := sourceReader(t, s, nil)
			queueRequire(t, control, a.Ref.ID, hidden.Ref.ID)
			_ = control.Close()
			queueCorrupt(t, s, hidden.Ref, corruption)
			r := sourceReader(t, s, nil)
			defer r.Close()
			got, err := r.QueueProjection(context.Background())
			if !errors.Is(err, driver.ErrSelectionUnavailable) || len(got.Beads) != 0 {
				t.Fatalf("corrupt queue authorized partial domain: %+v %v", got, err)
			}
		})
	}
}

func TestQueueProjectionReadySortWriteKeepsCoverageOnlyWhenDisjoint(t *testing.T) {
	stamp := time.Date(2026, time.November, 5, 0, 0, 0, 0, time.UTC)
	readyValue, err := json.Marshal(pipeline.ReadySortKey{
		Bucket: 0, Rank: 0, CreatedAt: stamp, RankedAt: stamp, Revision: model.Natural64(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, key string
		ready     bool
	}{
		{name: "main-ready-sort", key: "ready/queue-main", ready: true},
		{name: "ordinary-untyped-sort", key: "other/queue-main"},
		{name: "malformed-ready-sort", key: "ready/queue/main"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, lease := queueTestStore(t)
			record := queueDocument("queue-main", "source")
			seed, header := queueStage(t, s, lease, record)
			commitSourceTest(t, seed, header)
			key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: tc.key}
			lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
			tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{lock}, true)
			if err := tx.PutDerived(ctx, key.Table, nil, []driver.DerivedRow{{Key: key.Key, Value: readyValue}}); err != nil {
				t.Fatal(err)
			}
			commitSourceTest(t, tx, header)
			meta := getMeta(t, ctx, s)
			ready, err := selectionFamilyReady(ctx, s.db, meta.Incarnation, sqliteSelectionFamilies()[0], revKey(mustUint(meta.Head)))
			if err != nil || ready != tc.ready {
				t.Fatalf("sort coverage after %s: ready=%t want=%t err=%v", tc.name, ready, tc.ready, err)
			}
			r := sourceReader(t, s, nil)
			defer r.Close()
			if tc.ready {
				queueRequire(t, r, record.Ref.ID)
			} else if projection, err := r.QueueProjection(ctx); !errors.Is(err, driver.ErrSelectionUnavailable) || len(projection.Beads) != 0 {
				t.Fatalf("untyped sort row retained queue coverage: %+v %v", projection, err)
			}
		})
	}

	// A typed bead projection can own a key in the ready/ namespace. A raw
	// replacement cannot inherit that key's coverage, even when its source is
	// deleted in the same commit.
	t.Run("typed-row-shadowed-during-delete", func(t *testing.T) {
		ctx := context.Background()
		s, lease := queueTestStore(t)
		record := queueDocument("queue-shadow", "source")
		key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "ready/" + record.Ref.ID}
		lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
		seed, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{lock}, true, record.Ref)
		if err := seed.PutGraphRecord(ctx, record, nil, nil); err != nil {
			t.Fatal(err)
		}
		row := selectionStageBead(header.Incarnation, record.Ref, record.Body)
		row.Key, row.Index.Row = key.Key, key
		row.Index.Bead.Type = "issue"
		if err := seed.PutSelectionDerived(ctx, key.Table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
		commitSourceTest(t, seed, header)
		before := getMeta(t, ctx, s)
		ready, err := selectionFamilyReady(ctx, s.db, before.Incarnation, sqliteSelectionFamilies()[0], revKey(mustUint(before.Head)))
		if err != nil || !ready {
			t.Fatalf("typed shadow control was not certified: ready=%t err=%v", ready, err)
		}
		tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{lock}, true, record.Ref)
		if err := tx.Delete(ctx, record.Ref); err != nil {
			t.Fatal(err)
		}
		if err := tx.PutDerived(ctx, key.Table, nil, []driver.DerivedRow{{Key: key.Key, Value: readyValue}}); err != nil {
			t.Fatal(err)
		}
		commitSourceTest(t, tx, header)
		after := getMeta(t, ctx, s)
		ready, err = selectionFamilyReady(ctx, s.db, after.Incarnation, sqliteSelectionFamilies()[0], revKey(mustUint(after.Head)))
		if err != nil || ready {
			t.Fatalf("raw replacement shadowed typed row: ready=%t err=%v", ready, err)
		}
	})
}

func queueCorrupt(t *testing.T, s *Store, ref model.Ref, corruption string) {
	t.Helper()
	ctx := context.Background()
	source, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	var derived []string
	for _, table := range []driver.DerivedTable{driver.DerivedSortKey, driver.DerivedReferenceIndex} {
		key, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: ref.ID}})
		derived = append(derived, key)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	switch corruption {
	case "hidden_header_and_both_families":
		exec(`DELETE FROM selection_source_history WHERE source_key=?`, source)
		exec(`DELETE FROM key_history WHERE key IN (?,?)`, derived[0], derived[1])
		exec(`DELETE FROM selection_bead_history WHERE record_key=?`, source)
		exec(`DELETE FROM selection_fact_sets WHERE source_key=?`, source)
	case "missing_link":
		exec(`DELETE FROM key_history WHERE key=?`, derived[1])
	case "stale_source":
		exec(`UPDATE selection_source_history SET source_digest='changed' WHERE source_key=?`, source)
	case "unready":
		exec(`UPDATE selection_family_coverage SET state='unbuilt'`)
	}
}
