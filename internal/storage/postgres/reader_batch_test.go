package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestMissingRecordReadsDoNotProbeVersionHistory(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	first := seedTest(t, s, lease, "present", `{"value":1}`)
	r := testRead(t, s, nil).(*reader)
	historyScans := func(r *reader) int64 {
		t.Helper()
		var scans int64
		if err := r.sql.QueryRowContext(ctx, `SELECT seq_scan+coalesce(idx_scan,0) FROM pg_stat_xact_user_tables WHERE relid='plug_postgres.versions'::regclass`).Scan(&scans); err != nil {
			t.Fatal(err)
		}
		return scans
	}
	refs := []model.Ref{testRef("present")}
	keys := []driver.LockKey{testLock(refs[0])}
	for i := 0; i < 512; i++ {
		ref := testRef(fmt.Sprintf("absent-%04d", i))
		refs = append(refs, ref)
		keys = append(keys, testLock(ref))
	}
	refs = append(refs, refs[0])
	sort.Slice(keys, func(i, j int) bool { a, _ := keyID(keys[i]); b, _ := keyID(keys[j]); return a < b })
	before := historyScans(r)
	rows, err := r.Get(ctx, refs)
	if err != nil || len(rows) != 2 || rows[0].Ref.ID != "present" || rows[1].Ref.ID != "present" {
		t.Fatalf("current batch lost hits, misses or duplicate order: %+v %v", rows, err)
	}
	states, err := r.Observe(ctx, keys)
	if err != nil || len(states) != len(keys) {
		t.Fatalf("current batch observations: %+v %v", states, err)
	}
	for _, state := range states {
		if state.Key.Ref.ID == "present" {
			if !state.Present || state.Record == nil || state.Revision != first.Revision {
				t.Fatalf("current hit lost its revision: %+v", state)
			}
		} else if state.Present || state.Record != nil || !state.Revision.IsZero() {
			t.Fatalf("current absence acquired a record or revision: %+v", state)
		}
	}
	missingKey, err := recordKey(testRef("single-absent"))
	if err != nil {
		t.Fatal(err)
	}
	if _, exists, err := r.recordAt(ctx, missingKey); err != nil || exists {
		t.Fatalf("single physical absence: exists=%t error=%v", exists, err)
	}
	if got := historyScans(r) - before; got != 0 {
		t.Fatalf("physical absences performed %d version-history scans, want none", got)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	seedTest(t, s, lease, "future", `{"value":2}`)
	past := testRead(t, s, &first.Revision).(*reader)
	before = historyScans(past)
	rows, err = past.Get(ctx, []model.Ref{testRef("future"), testRef("present")})
	if err != nil || len(rows) != 1 || rows[0].Ref.ID != "present" || rows[0].Revision != first.Revision {
		t.Fatalf("historical read disclosed a future row or lost its hit: %+v %v", rows, err)
	}
	if got := historyScans(past) - before; got == 0 {
		t.Fatal("a newer physical row did not use historical reconstruction")
	}
}

func TestBatchedReaderPreservesHistoryMissesAndStagedOverlays(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	a, b, missing := testRef("batch-a"), testRef("batch-b"), testRef("batch-missing")
	fact := driver.LockKey{Kind: driver.LockReadFact, Fact: "reader/batch"}
	derived := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "batch"}}
	keys := []driver.LockKey{testLock(a), testLock(b), testLock(missing), fact, driver.GraphBlockingSentinelLock, derived}
	sort.Slice(keys, func(i, j int) bool { left, _ := keyID(keys[i]); right, _ := keyID(keys[j]); return left < right })
	assertQuery := func(reader driver.Reader, wantIDs []string, wantBody string) {
		t.Helper()
		var cursor *driver.Cursor
		var ids []string
		for pageNumber := 0; ; pageNumber++ {
			if pageNumber > 3 {
				t.Fatal("query paging did not terminate")
			}
			page, err := reader.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Limit: 1, Cursor: cursor})
			if err != nil || page.AsOf.String() != reader.Snapshot().At.String() {
				t.Fatalf("query lost its snapshot: page=%+v err=%v", page, err)
			}
			for _, row := range page.Items {
				ids = append(ids, row.Ref.ID)
				if string(row.Body) != wantBody {
					t.Fatalf("query reconstructed wrong content: %+v", row)
				}
			}
			cursor = page.NextCursor
			if cursor == nil {
				break
			}
		}
		if !reflect.DeepEqual(ids, wantIDs) {
			t.Fatalf("query membership/order=%v want=%v", ids, wantIDs)
		}
	}
	initial := driver.DerivedRow{Key: "batch", Value: []byte("old")}
	tx, header := beginTest(t, s, lease, keys...)
	for _, ref := range []model.Ref{a, b} {
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
			t.Fatal(err)
		}
	}
	assertQuery(tx, []string{a.ID, b.ID}, `{"value":1}`)
	// A revision-zero backing snapshot is empty, but the transaction must
	// retain its own staged records and observations before its first commit.
	if rows, err := tx.Get(ctx, []model.Ref{a, missing, b}); err != nil || len(rows) != 2 || rows[0].Ref.ID != a.ID || rows[1].Ref.ID != b.ID {
		t.Fatalf("revision-zero staged records: %+v %v", rows, err)
	}
	if states, err := tx.Observe(ctx, keys); err != nil {
		t.Fatal(err)
	} else {
		for _, state := range states {
			if state.Key.Kind == driver.LockRecord && (state.Key.Ref.ID == a.ID || state.Key.Ref.ID == b.ID) && (!state.Present || state.Record == nil || string(state.Record.Body) != `{"value":1}`) {
				t.Fatalf("revision-zero staged observation lost: %+v", state)
			}
		}
	}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{initial}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpReadFacts(ctx, []driver.ReadFact{fact.Fact}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	header.Changes = []model.ChangeRecord{{Ref: a, Op: model.OpCreate}, {Ref: b, Op: model.OpCreate}}
	first, err := tx.Commit(ctx, header)
	if err != nil {
		t.Fatal(err)
	}
	tx, header = beginTest(t, s, lease, keys...)
	if err := tx.Put(ctx, driver.Record{Ref: a, Kind: "test", Body: []byte(`{"value":2}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, []driver.DerivedRow{initial}, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpReadFacts(ctx, []driver.ReadFact{fact.Fact}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	header.Changes = []model.ChangeRecord{{Ref: a, Op: model.OpPatch, FromRevision: first.Revision}, {Ref: b, Op: model.OpDelete, FromRevision: first.Revision}}
	second, err := tx.Commit(ctx, header)
	if err != nil {
		t.Fatal(err)
	}
	zero := model.Natural64(0)
	empty := testRead(t, s, &zero)
	assertQuery(empty, nil, "")
	if rows, err := empty.Get(ctx, []model.Ref{a, b, missing}); err != nil || len(rows) != 0 {
		t.Fatalf("revision zero disclosed a later record: %+v %v", rows, err)
	}
	if states, err := empty.Observe(ctx, keys); err != nil {
		t.Fatal(err)
	} else {
		for _, state := range states {
			if state.Present || !state.Revision.IsZero() || state.Record != nil || state.Derived != nil {
				t.Fatalf("revision zero disclosed a later key state: %+v", state)
			}
		}
	}
	for _, historical := range []bool{true, false} {
		var at *model.Revision
		if historical {
			at = &first.Revision
		}
		r := testRead(t, s, at)
		rows, err := r.Get(ctx, []model.Ref{b, missing, a, a})
		if err != nil {
			t.Fatal(err)
		}
		wantCount, wantBody := 2, `{"value":2}`
		if historical {
			wantCount, wantBody = 3, `{"value":1}`
		}
		if len(rows) != wantCount || string(rows[len(rows)-1].Body) != wantBody || rows[len(rows)-1].Ref.ID != a.ID {
			t.Fatalf("history=%t rows=%+v", historical, rows)
		}
		wantIDs := []string{a.ID}
		if historical {
			wantIDs = append(wantIDs, b.ID)
		}
		assertQuery(r, wantIDs, wantBody)
		rows[len(rows)-1].Body[0] = '!'
		if rows[len(rows)-2].Body[0] != '{' {
			t.Fatal("duplicate result bodies alias")
		}
		states, err := r.Observe(ctx, keys)
		if err != nil || len(states) != len(keys) {
			t.Fatalf("history=%t observations=%+v err=%v", historical, states, err)
		}
		for i, state := range states {
			if !reflect.DeepEqual(state.Key, keys[i]) {
				t.Fatal("batch observation order changed")
			}
			if keys[i].Kind == driver.LockRecord && keys[i].Ref.ID == missing.ID {
				if state.Present || !state.Revision.IsZero() || state.Record != nil {
					t.Fatalf("missing record was not retained: %+v", state)
				}
				continue
			}
			wantRevision := second.Revision
			if historical {
				wantRevision = first.Revision
			}
			deleted := !historical && (keys[i].Kind == driver.LockDerived || keys[i].Kind == driver.LockRecord && keys[i].Ref.ID == b.ID)
			if state.Revision.String() != wantRevision.String() || state.Present == deleted || deleted && (state.Record != nil || state.Derived != nil) {
				t.Fatalf("history=%t hit/tombstone revision changed: %+v", historical, state)
			}
		}
	}
	// Transaction reads must still see only their own staged replacements and
	// removals. The batched reader must not substitute any retained read cache.
	tx, _ = beginTest(t, s, lease, keys...)
	if err := tx.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []model.Ref{b, missing} {
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"staged":true}`)}); err != nil {
			t.Fatal(err)
		}
	}
	staged := driver.DerivedRow{Key: "batch", Value: []byte("staged")}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{staged}); err != nil {
		t.Fatal(err)
	}
	assertQuery(tx, []string{b.ID, missing.ID}, `{"staged":true}`)
	rows, err := tx.Get(ctx, []model.Ref{b, a, missing, b})
	if err != nil || len(rows) != 3 || rows[0].Ref.ID != b.ID || rows[1].Ref.ID != missing.ID || rows[2].Ref.ID != b.ID {
		t.Fatalf("staged Get=%+v err=%v", rows, err)
	}
	states, err := tx.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if state.Key.Kind == driver.LockRecord {
			if state.Key.Ref.ID == a.ID {
				if state.Present || state.Record != nil {
					t.Fatal("staged deletion was reused")
				}
			} else if !state.Present || state.Record == nil || string(state.Record.Body) != `{"staged":true}` {
				t.Fatalf("staged record missing: %+v", state)
			}
		} else if state.Key.Kind == driver.LockDerived && (!state.Present || state.Derived == nil || !reflect.DeepEqual(*state.Derived, staged)) {
			t.Fatalf("staged derived row missing: %+v", state)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := tx.Query(canceled, driver.Query{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Query: %v", err)
	}
	if _, err := tx.Get(canceled, []model.Ref{a, b}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Get: %v", err)
	}
	if _, err := tx.Observe(canceled, keys); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Observe: %v", err)
	}
}
