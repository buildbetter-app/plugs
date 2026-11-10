package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func openTest(t *testing.T, dsn string) *Store {
	t.Helper()
	s, err := Open(context.Background(), driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func testLease(t *testing.T, s *Store) driver.ActiveLease {
	t.Helper()
	l, err := s.Lease(context.Background(), "engine-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return l
}
func testRead(t *testing.T, s driver.Store, at *model.Revision) driver.Reader {
	t.Helper()
	meta, err := s.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}
func testRef(id string) driver.Ref { return model.RefByID(id, model.Natural64(0)) }
func testLock(ref driver.Ref) driver.LockKey {
	return driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
}

// snapshotSQLApplicationRows reads every physical application table, including
// locks, so a failed cold write cannot hide coordination rows behind a clean
// public snapshot.
func snapshotSQLApplicationRows(t *testing.T, s *Store) map[string][]string {
	t.Helper()
	ctx := context.Background()
	rows, err := s.db.QueryContext(ctx, `SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='plug_postgres' ORDER BY tablename`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(tables) == 0 {
		t.Fatal("missing PostgreSQL application tables")
	}
	snapshot := make(map[string][]string, len(tables))
	for _, name := range tables {
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		values, err := s.db.QueryContext(ctx, `SELECT to_jsonb(t)::text FROM plug_postgres.`+quoted+` AS t ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		var contents []string
		for values.Next() {
			var row string
			if err := values.Scan(&row); err != nil {
				t.Fatal(err)
			}
			contents = append(contents, row)
		}
		if err := values.Err(); err != nil {
			t.Fatal(err)
		}
		if err := values.Close(); err != nil {
			t.Fatal(err)
		}
		snapshot[name] = contents
	}
	return snapshot
}

func beginTest(t *testing.T, s *Store, l driver.ActiveLease, keys ...driver.LockKey) (driver.WriteTx, driver.ChangesetHeader) {
	t.Helper()
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	tx, err := s.Write(context.Background(), l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
	})
	locks := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	if _, err = tx.Lock(context.Background(), locks); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(context.Background()); err != nil {
		t.Fatal(err)
	}
	meta, err := s.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return tx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "postgres-test"}, LockKeys: keys}
}

func seedTest(t *testing.T, s driver.Store, l driver.ActiveLease, id, body string) model.Changeset {
	t.Helper()
	entry, err := conformance.Seed(context.Background(), s, l, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: testRef(id), Kind: "test", Op: model.OpPatch, Body: []byte(body)}})
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestPersistenceAcrossHandlesAndReopen(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	a, b := openTest(t, dsn), openTest(t, dsn)
	l := testLease(t, a)
	ref := testRef("pl-durable")
	derived := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "rank"}}
	fact := driver.LockKey{Kind: driver.LockReadFact, Fact: "durable/predicate"}
	tx, h := beginTest(t, a, l, testLock(ref), fact, derived)
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
		t.Fatal(err)
	}
	blob, err := tx.PutBlob(ctx, []byte("persistent blob"))
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{{Key: "rank", Value: []byte("durable rank")}}); err != nil {
		t.Fatal(err)
	}
	if err = tx.BumpReadFacts(ctx, []driver.ReadFact{fact.Fact}); err != nil {
		t.Fatal(err)
	}
	h.IdempotencyKey, h.Method, h.RequestHash = "durable", "bead.create", "request-1"
	h.ResponsePending = true
	h.AgentLabel = "ignored-authenticated-agent"
	h.Chain = []string{}
	h.Events = []model.EventDescriptor{{
		Index: "0", ChangeIndex: "0", Type: model.EventBeadCreated,
		Resource:   model.AuditResource{Kind: "bead", Ref: ref},
		Provenance: model.EventProvenance{Method: "bead.create", SetBy: map[string]string{"body": "request"}, Cause: model.EventCause{Root: "request-1", Depth: "0", Chain: []string{}}, Chain: []string{}},
	}}
	h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}
	h.Response = model.Commit{Results: []model.OpResult{{Ref: ref, Revision: model.Natural64(0), Record: []byte(`{"value":1}`)}}}
	first, err := tx.Commit(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision.String() != "1" {
		t.Fatal(first.Revision)
	}
	pinned := testRead(t, b, nil)
	records, err := pinned.Get(ctx, []driver.Ref{ref})
	if err != nil || len(records) != 1 {
		t.Fatalf("independent read=%v %v", records, err)
	}
	if _, err = a.SaveCheckpoint(ctx, l, "delivery", 0, []byte("cursor1")); err != nil {
		t.Fatal(err)
	}
	warnings := []model.Warning{{Code: "post_subscriber_failed", Message: "stored after commit"}}
	if err = a.FinalizeResponse(ctx, l, "system:postgres-test", "different-agent", h.Method, h.IdempotencyKey, first.Revision, warnings); err != nil {
		t.Fatal(err)
	}
	seedTest(t, a, l, ref.ID, `{"value":2}`)
	seedTest(t, a, l, "pl-tombstone", `{"value":1}`)
	if _, err = conformance.Seed(ctx, a, l, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: testRef("pl-tombstone"), Kind: "test", Op: model.OpDelete}}); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	// b has independent connections and keeps its RR snapshot when a closes.
	records, err = pinned.Get(ctx, []driver.Ref{ref})
	if err != nil || string(records[0].Body) != `{"value":1}` {
		t.Fatalf("snapshot lost on other pool close: %v %v", records, err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTest(t, dsn)
	freshLease := testLease(t, reopened)
	if freshLease.Epoch() <= l.Epoch() {
		t.Fatal("reopen lost persistent epoch")
	}
	reader := testRead(t, reopened, nil)
	records, err = reader.Get(ctx, []driver.Ref{ref, testRef("pl-tombstone")})
	if err != nil || len(records) != 1 || string(records[0].Body) != `{"value":2}` {
		t.Fatalf("reopened records=%v %v", records, err)
	}
	payload, err := reader.Blob(ctx, blob)
	if err != nil || string(payload) != "persistent blob" {
		t.Fatalf("reopened blob=%s %v", payload, err)
	}
	checkpoint, err := reopened.LoadCheckpoint(ctx, "delivery")
	if err != nil || checkpoint.Generation != 1 || string(checkpoint.Payload) != "cursor1" {
		t.Fatalf("reopened checkpoint=%+v %v", checkpoint, err)
	}
	receipt, err := reader.IdempotencyByKey(ctx, "system:postgres-test", "another-agent", h.Method, h.IdempotencyKey)
	if err != nil || receipt == nil || len(receipt.Commit.Warnings) != 1 || receipt.Commit.Results[0].Revision.String() != "0" || !reflect.DeepEqual(receipt.Changeset, first) {
		t.Fatalf("reopened response=%+v %v", receipt, err)
	}
	states, err := reader.Observe(ctx, []driver.LockKey{fact, derived})
	if err != nil || states[0].Revision.String() != "1" || string(states[1].Derived.Value) != "durable rank" {
		t.Fatalf("reopened facts=%+v %v", states, err)
	}
	versions, err := reader.Versions(ctx, ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 2 {
		t.Fatalf("reopened versions=%+v %v", versions, err)
	}
	old := model.Natural64(1)
	historical, err := testRead(t, reopened, &old).Get(ctx, []driver.Ref{ref})
	if err != nil || string(historical[0].Body) != `{"value":1}` {
		t.Fatalf("reopened history=%v %v", historical, err)
	}
	staleErr := l.Renew(ctx)
	var fenced driver.ErrFenced
	if !errors.As(staleErr, &fenced) {
		t.Fatalf("old holder renewed after reopen: %v", staleErr)
	}
	t.Log("independent pools and reopened database preserve records, tombstones, history, blobs, observations, replay, finalized warnings and checkpoints")
}

func TestSQLFailureAfterStagingRollsBackEveryFamily(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	s := openTest(t, dsn)
	lease := testLease(t, s)
	_, err := s.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.reject_log() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'synthetic late publication failure'; END$$; CREATE TRIGGER reject_log BEFORE INSERT ON plug_postgres.changesets FOR EACH ROW EXECUTE FUNCTION plug_postgres.reject_log()`)
	if err != nil {
		t.Fatal(err)
	}
	beforeRows := snapshotSQLApplicationRows(t, s)
	ref := testRef("pl-rollback")
	fact := driver.LockKey{Kind: driver.LockReadFact, Fact: "rollback/predicate"}
	derived := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "rank"}}
	tx, h := beginTest(t, s, lease, testLock(ref), fact, derived)
	if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
		t.Fatal(err)
	}
	blob, err := tx.PutBlob(ctx, []byte("must roll back"))
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{{Key: "rank", Value: []byte("must roll back")}}); err != nil {
		t.Fatal(err)
	}
	if err = tx.BumpReadFacts(ctx, []driver.ReadFact{fact.Fact}); err != nil {
		t.Fatal(err)
	}
	h.IdempotencyKey, h.Method, h.RequestHash = "rollback", "bead.create", "request"
	h.Chain = []string{}
	h.Events = []model.EventDescriptor{{
		Index: "0", ChangeIndex: "0", Type: model.EventBeadCreated,
		Resource:   model.AuditResource{Kind: "bead", Ref: ref},
		Provenance: model.EventProvenance{Method: "bead.create", SetBy: map[string]string{"body": "request"}, Cause: model.EventCause{Root: "request", Depth: "0", Chain: []string{}}, Chain: []string{}},
	}}
	h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}
	if _, err = tx.Commit(ctx, h); err == nil {
		t.Fatal("engine fault did not fail commit")
	}
	r := testRead(t, s, nil)
	if !r.Snapshot().At.IsZero() {
		t.Fatal("failed SQL transaction advanced head")
	}
	rows, err := r.Query(ctx, driver.Query{})
	if err != nil || len(rows.Items) != 0 {
		t.Fatalf("late failure leaked records: %v %v", rows, err)
	}
	if _, err = r.Blob(ctx, blob); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("late failure leaked blob: %v", err)
	}
	versions, err := r.Versions(ctx, ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 0 {
		t.Fatalf("late failure leaked versions: %v %v", versions, err)
	}
	states, err := r.Observe(ctx, []driver.LockKey{fact, derived})
	if err != nil || states[0].Present || states[1].Present {
		t.Fatalf("late failure leaked derived state: %v %v", states, err)
	}
	receipt, err := r.IdempotencyByKey(ctx, "system:postgres-test", "", h.Method, h.IdempotencyKey)
	if err != nil || receipt != nil {
		t.Fatalf("late failure leaked receipt: %v %v", receipt, err)
	}
	if got := snapshotSQLApplicationRows(t, s); !reflect.DeepEqual(got, beforeRows) {
		t.Fatalf("failed cold publication changed SQL application rows: before=%v after=%v", beforeRows, got)
	}
	reopened := openTest(t, dsn)
	if got := snapshotSQLApplicationRows(t, reopened); !reflect.DeepEqual(got, beforeRows) {
		t.Fatalf("failed cold publication survived reopen: before=%v after=%v", beforeRows, got)
	}
	if _, err = s.db.ExecContext(ctx, `DROP TRIGGER reject_log ON plug_postgres.changesets; DROP FUNCTION plug_postgres.reject_log()`); err != nil {
		t.Fatal(err)
	}
	if entry := seedTest(t, s, lease, ref.ID, `{"value":2}`); entry.Revision.String() != "1" {
		t.Fatalf("SQL rollback consumed revision: %s", entry.Revision)
	}
	t.Log("real PostgreSQL trigger failure after record/version/blob/derived writes publishes none of them and consumes no revision")
}

func TestManyReadersDoNotBlockWriters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	seedTest(t, s, lease, "pl-mvcc", `{"value":1}`)
	readers := make([]driver.Reader, 24)
	for i := range readers {
		readers[i] = testRead(t, s, nil)
	}
	for i := 2; i <= 10; i++ {
		entry, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: testRef("pl-mvcc"), Kind: "test", Op: model.OpPatch, Body: []byte(fmt.Sprintf(`{"value":%d}`, i))}})
		if err != nil || entry.Revision.String() != fmt.Sprint(i) {
			t.Fatalf("writer blocked by readers: %v %v", entry, err)
		}
	}
	for _, r := range readers {
		records, err := r.Get(ctx, []driver.Ref{testRef("pl-mvcc")})
		if err != nil || len(records) != 1 || string(records[0].Body) != `{"value":1}` {
			t.Fatalf("MVCC snapshot advanced: %v %v", records, err)
		}
	}
}
