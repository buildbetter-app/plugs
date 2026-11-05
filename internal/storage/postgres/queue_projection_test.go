package postgres

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
	s := openTest(t, freshDSN(t))
	return s, testLease(t, s)
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

func TestQueueProjectionCoverageAcrossReadyRankPublication(t *testing.T) {
	for _, mode := range []string{"rank", "unannotated_source", "unannotated_other", "malformed_ready", "shadowed_ready_source", "shadowed_ready_source_deleted"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s, lease := queueTestStore(t)
			record := queueDocument("queue-rank", "ranked source")
			tx, header := queueStage(t, s, lease, record)
			created := commitSourceTest(t, tx, header)
			rankKey, err := pipeline.ReadySortKeyRowKey(record.Ref.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "shadowed_ready_source" || mode == "shadowed_ready_source_deleted" {
				// Typed projection keys are not restricted to the usual source
				// ID. Move the actual annotation into the reserved rank prefix
				// before testing that an untyped replacement still invalidates it.
				oldKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: record.Ref.ID}
				newKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: rankKey}
				r := sourceReader(t, s, nil)
				states, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &oldKey}})
				_ = r.Close()
				if err != nil || len(states) != 1 || !states[0].Present || states[0].Derived == nil || states[0].Derived.Index == nil {
					t.Fatalf("source annotation: %+v %v", states, err)
				}
				row := states[0].Derived.Clone()
				row.Key, row.Index.Row = rankKey, newKey
				move, h := beginTest(t, s, lease, testLock(record.Ref), driver.LockKey{Kind: driver.LockDerived, Derived: &oldKey}, driver.LockKey{Kind: driver.LockDerived, Derived: &newKey})
				if err := move.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, driver.DerivedSortKey, []driver.DerivedRow{{Key: oldKey.Key}}, []driver.DerivedRow{row}); err != nil {
					t.Fatal(err)
				}
				if _, err := move.Commit(ctx, h); err != nil {
					t.Fatal(err)
				}
				_ = move.Close()
			}
			pinned := sourceReader(t, s, nil)
			queueRequire(t, pinned, record.Ref.ID)
			defer pinned.Close()
			key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: rankKey}
			if mode == "unannotated_source" {
				key.Key = record.Ref.ID
			}
			if mode == "unannotated_other" {
				key.Key = "other/" + record.Ref.ID
			}
			if mode == "malformed_ready" {
				key.Key = "ready/queue/rank"
			}
			locks := []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}}
			var sentinels []driver.Sentinel
			if mode == "shadowed_ready_source_deleted" {
				plan, err := pipeline.PrepareSourceDelete(record.Ref, record.Kind)
				if err != nil {
					t.Fatal(err)
				}
				sentinels = plan.Sentinels()
				locks = append(locks, testLock(record.Ref))
				for _, sentinel := range sentinels {
					locks = append(locks, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
				}
			}
			writer, h := beginTest(t, s, lease, locks...)
			if mode == "shadowed_ready_source_deleted" {
				if err := writer.Delete(ctx, record.Ref); err != nil {
					t.Fatal(err)
				}
				if err := writer.BumpSentinels(ctx, sentinels); err != nil {
					t.Fatal(err)
				}
				h.Changes = []model.ChangeRecord{{Ref: record.Ref, Op: model.OpDelete, FromRevision: created.Revision}}
			}
			at := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
			value, err := json.Marshal(pipeline.ReadySortKey{CreatedAt: at, RankedAt: at, Revision: created.Revision})
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.PutDerived(ctx, key.Table, nil, []driver.DerivedRow{{Key: key.Key, Value: value}}); err != nil {
				t.Fatal(err)
			}
			committed, err := writer.Commit(ctx, h)
			if err != nil {
				t.Fatal(err)
			}
			_ = writer.Close()
			current := sourceReader(t, s, nil)
			defer current.Close()
			covered, err := current.selectionCoverage(ctx, current.at)
			if err != nil || covered != (mode == "rank") {
				t.Fatalf("bead family coverage after %s: covered=%v err=%v", mode, covered, err)
			}
			if mode != "rank" {
				if _, err := current.QueueProjection(ctx); !errors.Is(err, driver.ErrSelectionUnavailable) {
					t.Fatalf("untyped source replacement retained coverage: %v", err)
				}
			} else {
				projection := queueRequire(t, current, record.Ref.ID)
				if projection.View.At == nil || projection.View.At.String() != committed.Revision.String() {
					t.Fatalf("rank publication coverage did not advance: %+v", projection.View)
				}
				rows, err := current.DerivedRows(ctx, key.Table, "ready/")
				if err != nil || len(rows) != 1 || rows[0].Key != rankKey || !bytes.Equal(rows[0].Value, value) {
					t.Fatalf("rank publication was omitted: %+v %v", rows, err)
				}
			}
			queueRequire(t, pinned, record.Ref.ID)
		})
	}
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
		exec(`DELETE FROM plug_postgres.selection_sources WHERE key=$1`, []byte(source))
		exec(`DELETE FROM plug_postgres.states WHERE key IN ($1,$2)`, []byte(derived[0]), []byte(derived[1]))
		exec(`DELETE FROM plug_postgres.selection_rows WHERE source_key=$1`, []byte(source))
	case "missing_link":
		exec(`DELETE FROM plug_postgres.states WHERE key=$1`, []byte(derived[1]))
	case "stale_source":
		exec(`UPDATE plug_postgres.selection_sources SET digest=convert_to('changed','UTF8') WHERE key=$1`, []byte(source))
	case "unready":
		exec(`UPDATE plug_postgres.selection_coverage SET ready=FALSE`)
	}
}
