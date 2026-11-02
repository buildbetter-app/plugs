package postgres

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type stateCommitFixture struct {
	keys      []driver.LockKey
	derived   []driver.DerivedRow
	facts     []driver.ReadFact
	sentinels []driver.Sentinel
}

func newStateCommitFixture(t *testing.T, derived, facts, sentinels int) stateCommitFixture {
	t.Helper()
	f := stateCommitFixture{keys: []driver.LockKey{testLock(testRef("batch-a")), testLock(testRef("batch-b"))}}
	for i := 0; i < derived; i++ {
		row := driver.DerivedRow{Key: fmt.Sprintf("batch/%04d", i), Value: []byte(fmt.Sprintf("value-%d", i))}
		f.derived = append(f.derived, row)
		f.keys = append(f.keys, driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: row.Key}})
	}
	for i := 0; i < facts; i++ {
		fact := driver.ReadFact(fmt.Sprintf("batch/fact/%04d", i))
		f.facts = append(f.facts, fact)
		f.keys = append(f.keys, driver.LockKey{Kind: driver.LockReadFact, Fact: fact})
	}
	for i := 0; i < sentinels; i++ {
		sentinel, err := driver.NewBeadSentinel(fmt.Sprintf("batch-%04d", i))
		if err != nil {
			t.Fatal(err)
		}
		f.sentinels = append(f.sentinels, sentinel)
		f.keys = append(f.keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	return f
}

func (f stateCommitFixture) stage(t *testing.T, s *Store, lease driver.ActiveLease, second bool) (driver.WriteTx, driver.ChangesetHeader) {
	t.Helper()
	ctx := context.Background()
	tx, h := beginTest(t, s, lease, f.keys...)
	if err := tx.Put(ctx, driver.Record{Ref: testRef("batch-a"), Kind: "test", Body: []byte(fmt.Sprintf(`{"second":%t}`, second))}); err != nil {
		t.Fatal(err)
	}
	h.Changes = []model.ChangeRecord{{Ref: testRef("batch-a"), Op: model.OpCreate}, {Ref: testRef("batch-b"), Op: model.OpCreate}}
	add, del := f.derived, []driver.DerivedRow(nil)
	if second {
		if err := tx.Delete(ctx, testRef("batch-b")); err != nil {
			t.Fatal(err)
		}
		h.Changes[0].Op, h.Changes[0].FromRevision = model.OpPatch, model.Natural64(1)
		h.Changes[1].Op, h.Changes[1].FromRevision = model.OpDelete, model.Natural64(1)
		add = nil
		for i, row := range f.derived {
			if i%2 == 0 {
				del = append(del, row)
			} else {
				add = append(add, driver.DerivedRow{Key: row.Key, Value: []byte("replacement")})
			}
		}
	} else if err := tx.Put(ctx, driver.Record{Ref: testRef("batch-b"), Kind: "test", Body: []byte(`{"second":false}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, del, add); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpReadFacts(ctx, f.facts); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, f.sentinels); err != nil {
		t.Fatal(err)
	}
	h.Method, h.IdempotencyKey, h.RequestHash = "test.state-batch", fmt.Sprintf("batch-%t", second), fmt.Sprintf("request-%t", second)
	h.Response = model.Commit{Results: []model.OpResult{{Ref: testRef("batch-a"), Record: []byte(fmt.Sprintf(`{"second":%t}`, second))}}}
	return tx, h
}

func stateStatementSizes(t *testing.T, s *Store) []int {
	t.Helper()
	rows, err := s.db.QueryContext(context.Background(), `SELECT size FROM plug_postgres.test_state_statements ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sizes []int
	for rows.Next() {
		var size int
		if err := rows.Scan(&size); err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, size)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return sizes
}

func TestCommitStateStatementsAreBoundedAndPreserveHistory(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	// A statement trigger measures actual engine round trips, independently of
	// the implementation's batching helper. These objects exist only in this
	// disposable database. Row counts also catch dropped or duplicated states.
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE plug_postgres.test_state_statements(id bigserial PRIMARY KEY,size integer NOT NULL);
CREATE FUNCTION plug_postgres.count_state_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN INSERT INTO plug_postgres.test_state_statements(size) SELECT count(*) FROM inserted; RETURN NULL; END$$;
CREATE TRIGGER count_state_statement AFTER INSERT ON plug_postgres.states
REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION plug_postgres.count_state_statement()`)
	if err != nil {
		t.Fatal(err)
	}
	f := newStateCommitFixture(t, 1000, 1000, 1001)
	entries := make([]model.Changeset, 0, 2)
	var firstHeader driver.ChangesetHeader
	previousStatements := 0
	for _, second := range []bool{false, true} {
		tx, h := f.stage(t, s, lease, second)
		entry, err := tx.Commit(ctx, h)
		if err != nil {
			t.Fatal(err)
		}
		if !second {
			firstHeader = h
		}
		entries = append(entries, entry)
		sizes := stateStatementSizes(t, s)
		total := 0
		for _, size := range sizes[previousStatements:] {
			total += size
			// Permit several batch widths without permitting an unbounded
			// single argument payload or returning to scalar publication.
			if size < 1 || size > 512 {
				t.Errorf("unbounded state statement size: %d", size)
			}
		}
		statements := len(sizes) - previousStatements
		if total != 3001 || statements > 32 {
			t.Errorf("state publication second=%t: %d rows in %d INSERT statements; want all 3001 rows in at most 32 bounded statements", second, total, statements)
		}
		t.Logf("state publication revision=%s rows=%d statements=%d", entry.Revision, total, statements)
		previousStatements = len(sizes)
	}
	for i, entry := range entries {
		second := i == 1
		r := testRead(t, s, &entry.Revision)
		states, err := r.Observe(ctx, f.keys)
		if err != nil || len(states) != len(f.keys) {
			t.Fatalf("revision %s observations: %d %v", entry.Revision, len(states), err)
		}
		for j, state := range states {
			key := f.keys[j]
			if !reflect.DeepEqual(state.Key, key) || state.Revision.String() != entry.Revision.String() {
				t.Fatalf("state identity/revision changed at %d: %+v", j, state)
			}
			present := true
			if key.Kind == driver.LockDerived {
				var index int
				if _, err := fmt.Sscanf(key.Derived.Key, "batch/%04d", &index); err != nil {
					t.Fatal(err)
				}
				present = !second || index%2 != 0
				if present {
					want := f.derived[index].Value
					if second {
						want = []byte("replacement")
					}
					if state.Derived == nil || state.Derived.Key != key.Derived.Key || string(state.Derived.Value) != string(want) {
						t.Fatalf("derived image changed: %+v", state)
					}
				}
			} else if key.Kind == driver.LockRecord {
				present = !second || key.Ref.ID != "batch-b"
				if present && (state.Record == nil || string(state.Record.Body) != fmt.Sprintf(`{"second":%t}`, second)) {
					t.Fatalf("record image changed: %+v", state)
				}
			}
			if state.Present != present || !present && (state.Record != nil || state.Derived != nil) {
				t.Fatalf("state presence/tombstone changed: %+v", state)
			}
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	r := testRead(t, s, nil)
	for _, id := range []string{"batch-a", "batch-b"} {
		versions, err := r.Versions(ctx, testRef(id), driver.PageReq{})
		if err != nil || len(versions.Items) != 2 {
			t.Fatalf("durable versions %s: %+v %v", id, versions, err)
		}
		for _, version := range versions.Items {
			if version.Revision.String() == "1" && (!version.Base || version.Deleted || string(version.Body) != `{"second":false}`) || version.Revision.String() == "2" && (version.Deleted != (id == "batch-b") || id == "batch-a" && len(version.Patch) == 0) {
				t.Fatalf("version base/patch/tombstone changed: %+v", version)
			}
		}
	}
	receipt, err := r.IdempotencyByKey(ctx, "system:postgres-test", "", firstHeader.Method, firstHeader.IdempotencyKey)
	if err != nil || receipt == nil || receipt.ResponsePending || receipt.Commit.Revision.String() != "1" || !reflect.DeepEqual(receipt.Changeset, entries[0]) {
		t.Fatalf("durable receipt changed: %+v %v", receipt, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	replay, _ := beginTest(t, s, lease, f.keys...)
	got, err := replay.Commit(ctx, firstHeader)
	if err != nil || !reflect.DeepEqual(got, entries[0]) || len(stateStatementSizes(t, s)) != previousStatements {
		t.Fatalf("replay republished states or changed receipt: %+v %v", got, err)
	}
	meta, err := s.Meta(ctx)
	if err != nil || meta.Head.String() != "2" {
		t.Fatalf("replay advanced head: %+v %v", meta, err)
	}
	t.Log("all 3001 states per revision, history, tombstones, per-record versions and receipt replay preserved")
}

func TestCommitStateBatchFailureRollsBackPublication(t *testing.T) {
	for _, fault := range []string{"second_state_statement", "receipt"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			s := openTest(t, freshDSN(t))
			lease := testLease(t, s)
			// Sequence advancement survives rollback and proves which statement
			// reached the engine; graph/history/receipt rows must not survive.
			stateFailure := ""
			if fault == "second_state_statement" {
				stateFailure = "IF attempt=2 THEN RAISE EXCEPTION 'synthetic later state statement failure'; END IF;"
			}
			_, err := s.db.ExecContext(ctx, `CREATE SEQUENCE plug_postgres.state_statement_attempts;
CREATE FUNCTION plug_postgres.state_statement_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE attempt bigint; BEGIN attempt:=nextval('plug_postgres.state_statement_attempts'); `+stateFailure+` RETURN NULL; END$$;
CREATE TRIGGER state_statement_attempt AFTER INSERT ON plug_postgres.states FOR EACH STATEMENT EXECUTE FUNCTION plug_postgres.state_statement_attempt()`)
			if err != nil {
				t.Fatal(err)
			}
			if fault == "receipt" {
				_, err = s.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.reject_batch_receipt() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'synthetic receipt failure'; END$$;
CREATE TRIGGER reject_batch_receipt BEFORE INSERT ON plug_postgres.receipts FOR EACH ROW EXECUTE FUNCTION plug_postgres.reject_batch_receipt()`)
				if err != nil {
					t.Fatal(err)
				}
			}
			f := newStateCommitFixture(t, 513, 257, 33)
			tx, h := f.stage(t, s, lease, false)
			if _, err := tx.PutBlob(ctx, []byte("batch rollback")); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(ctx, h); err == nil {
				t.Fatal("synthetic engine fault did not fail publication")
			}
			var attempts int
			if err := s.db.QueryRowContext(ctx, `SELECT last_value FROM plug_postgres.state_statement_attempts`).Scan(&attempts); err != nil || attempts < 2 || fault == "second_state_statement" && attempts != 2 {
				t.Fatalf("fault did not reach later state statement: %d %v", attempts, err)
			}
			for _, table := range []string{"records", "versions", "states", "blobs", "changesets", "receipts"} {
				var count int
				if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.`+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("failed publication leaked %s: %d %v", table, count, err)
				}
			}
			meta, err := s.Meta(ctx)
			if err != nil || !meta.Head.IsZero() {
				t.Fatalf("failed publication advanced head: %+v %v", meta, err)
			}
			if fault == "receipt" {
				if _, err := s.db.ExecContext(ctx, `DROP TRIGGER reject_batch_receipt ON plug_postgres.receipts`); err != nil {
					t.Fatal(err)
				}
			}
			if entry := seedTest(t, s, lease, "after-batch-failure", `{}`); entry.Revision.String() != "1" {
				t.Fatalf("failed publication consumed revision: %s", entry.Revision)
			}
			t.Logf("fault=%s attempts=%d; all publication families rolled back, next revision=1", fault, attempts)
		})
	}
}
