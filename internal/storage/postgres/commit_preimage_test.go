package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type commitQueryCaptureKey struct{}
type commitQueryEventKey struct{}

type commitQueryEvent struct {
	sql                 string
	argumentTypes       []string
	resultFormatOptions int
	keyType             string
	keysKnown           bool
	keys                int
	rows                int64
	completed           bool
	err                 error
}

type commitQueryCapture struct {
	mu     sync.Mutex
	events []*commitQueryEvent
}

// pgx observes real engine calls made by database/sql. Only the context passed
// to Commit opts in: staging, durable verification, and real lease renewals
// cannot be mistaken for publication preimage work. No SQL is intercepted.
type commitQueryTracer struct{}

func (commitQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	capture, _ := ctx.Value(commitQueryCaptureKey{}).(*commitQueryCapture)
	if capture == nil {
		return ctx
	}
	sql := strings.Join(strings.Fields(data.SQL), " ")
	if !strings.HasPrefix(sql, "SELECT ") || (!strings.Contains(sql, "FROM plug_postgres.records") && !strings.Contains(sql, "FROM plug_postgres.versions")) {
		return ctx
	}
	event := &commitQueryEvent{sql: sql, keys: -1}
	for _, arg := range data.Args {
		event.argumentTypes = append(event.argumentTypes, fmt.Sprintf("%T", arg))
	}
	args := data.Args
	// pgx/stdlib QueryContext prepends its result-format option. Query traces
	// run before pgx consumes that option, so it is not SQL bind parameter $1.
	// Skip only this known prefix; unsupported bind types still fail the oracle.
	if len(args) > 0 {
		if _, ok := args[0].(pgx.QueryResultFormatsByOID); ok {
			event.resultFormatOptions = 1
			args = args[1:]
		}
	}
	if len(args) > 0 {
		event.keyType = fmt.Sprintf("%T", args[0])
		switch keys := args[0].(type) {
		case [][]byte:
			event.keysKnown = true
			event.keys = len(keys)
		case []byte:
			event.keysKnown = true
			event.keys = 1
		}
	}
	capture.mu.Lock()
	capture.events = append(capture.events, event)
	capture.mu.Unlock()
	return context.WithValue(ctx, commitQueryEventKey{}, event)
}

func (commitQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	event, _ := ctx.Value(commitQueryEventKey{}).(*commitQueryEvent)
	if event == nil {
		return
	}
	capture := ctx.Value(commitQueryCaptureKey{}).(*commitQueryCapture)
	capture.mu.Lock()
	event.rows, event.err, event.completed = data.CommandTag.RowsAffected(), data.Err, true
	capture.mu.Unlock()
}

func openCommitQueryTest(t *testing.T) (*Store, driver.ActiveLease) {
	t.Helper()
	config, err := pgx.ParseConfig(freshDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	config.Tracer = commitQueryTracer{}
	dsn := stdlib.RegisterConnConfig(config)
	t.Cleanup(func() { stdlib.UnregisterConnConfig(dsn) })
	s := openTest(t, dsn)
	lease := testLease(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	renewed := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(driver.LeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				renewed <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil {
					if ctx.Err() != nil {
						err = nil
					}
					renewed <- err
					return
				}
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-renewed; err != nil {
			t.Errorf("production-cadence lease renewal failed: %v", err)
		}
	})
	return s, lease
}

func captureCommitQueries(tx driver.WriteTx, h driver.ChangesetHeader) (model.Changeset, error, *commitQueryCapture) {
	capture := &commitQueryCapture{}
	ctx := context.WithValue(context.Background(), commitQueryCaptureKey{}, capture)
	entry, err := tx.Commit(ctx, h)
	return entry, err, capture
}

func requireBoundedCommitQueries(t *testing.T, capture *commitQueryCapture, wantRows int64) {
	t.Helper()
	capture.mu.Lock()
	defer capture.mu.Unlock()
	metadataQueries, metadataRows := 0, int64(0)
	// The bound deliberately permits multiple batch sizes. Count every real
	// record/version SELECT, so scalar historical fallbacks cannot hide behind
	// a bounded new helper while validation or version writing still does N+1.
	if len(capture.events) == 0 || len(capture.events) > 32 {
		t.Fatalf("record/version SELECTs=%d; want 1..32", len(capture.events))
	}
	for _, event := range capture.events {
		if !event.completed || event.err != nil {
			t.Fatalf("record/version query did not complete: %+v", event)
		}
		if strings.Contains(event.sql, "FROM plug_postgres.versions") {
			t.Fatalf("current-head Commit reconstructed a historical preimage: %s", event.sql)
		}
		if strings.Contains(event.sql, "version_count") {
			metadataQueries++
			metadataRows += event.rows
			if !event.keysKnown {
				t.Fatalf("preimage bind width unknown: argument_types=%v key_type=%s", event.argumentTypes, event.keyType)
			}
			if event.keys < 1 || event.keys > 512 || strings.Contains(event.sql, "body") || strings.Contains(event.sql, "ref,") {
				t.Fatalf("unbounded or body-materializing preimage query: %+v", event)
			}
			t.Logf("actual PG Commit preimage argument_types=%v result_format_options=%d key_type=%s keys=%d rows=%d", event.argumentTypes, event.resultFormatOptions, event.keyType, event.keys, event.rows)
		}
	}
	if metadataQueries == 0 || metadataRows != wantRows {
		t.Fatalf("preimage metadata queries=%d rows=%d; want queries>0 rows=%d", metadataQueries, metadataRows, wantRows)
	}
	t.Logf("actual PG Commit record/version SELECTs=%d metadata_queries=%d metadata_rows=%d historical_queries=0", len(capture.events), metadataQueries, metadataRows)
}

func requireRecordCounter(t *testing.T, s *Store, ref driver.Ref, revision, count uint64, deleted bool) {
	t.Helper()
	id, err := recordKey(ref)
	if err != nil {
		t.Fatal(err)
	}
	var gotRevision, gotCount string
	var gotDeleted bool
	if err := s.db.QueryRowContext(context.Background(), `SELECT revision::text,version_count::text,deleted FROM plug_postgres.records WHERE key=$1`, []byte(id)).Scan(&gotRevision, &gotCount, &gotDeleted); err != nil {
		t.Fatal(err)
	}
	if gotRevision != decimal(revision) || gotCount != decimal(count) || gotDeleted != deleted {
		t.Fatalf("record %s revision/count/deleted=%s/%s/%t; want %d/%d/%t", ref.ID, gotRevision, gotCount, gotDeleted, revision, count, deleted)
	}
}

func TestCommitPreimagesBoundedAcrossNewKeyBatches(t *testing.T) {
	ctx := context.Background()
	s, lease := openCommitQueryTest(t)
	seedTest(t, s, lease, "preimage-anchor", `{"anchor":true}`)
	for batch := 0; batch < 2; batch++ {
		if !t.Run(fmt.Sprintf("batch_%d", batch+1), func(t *testing.T) {
			refs := make([]driver.Ref, 1000)
			keys := make([]driver.LockKey, len(refs))
			for i := range refs {
				refs[i] = testRef(fmt.Sprintf("preimage-%04d", batch*1000+i))
				keys[i] = testLock(refs[i])
			}
			tx, h := beginTest(t, s, lease, keys...)
			for _, ref := range refs {
				if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
					t.Fatal(err)
				}
				h.Changes = append(h.Changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
			}
			started := time.Now()
			entry, err, capture := captureCommitQueries(tx, h)
			elapsed := time.Since(started)
			if err != nil {
				t.Fatalf("real 1000-record Commit: %v", err)
			}
			requireBoundedCommitQueries(t, capture, 0)
			if entry.Revision.String() != fmt.Sprint(batch+2) {
				t.Fatalf("new-key publication revision=%s", entry.Revision)
			}
			r := testRead(t, s, nil)
			rows, err := r.Get(ctx, refs)
			if err != nil || len(rows) != len(refs) {
				t.Fatalf("durable new keys=%d err=%v", len(rows), err)
			}
			for i, row := range rows {
				if row.Ref != refs[i] || row.Revision != entry.Revision || string(row.Body) != `{"value":1}` {
					t.Fatalf("new-key durable image changed: %+v", row)
				}
			}
			t.Logf("real new-key batch=%d size=1000 nonzero_head=%d Commit_elapsed=%s", batch+1, batch+1, elapsed)
		}) {
			return
		}
	}
	for _, table := range []string{"records", "versions"} {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.`+table).Scan(&count); err != nil || count != 2001 {
			t.Fatalf("%s rows=%d; want 2001: %v", table, count, err)
		}
	}
}

func TestCommitPreimagesPreserveValidationAndRollback(t *testing.T) {
	ctx := context.Background()
	t.Run("conflict_retry_and_receipt_replay", func(t *testing.T) {
		s, lease := openCommitQueryTest(t)
		seedTest(t, s, lease, "preimage-existing", `{"value":1}`)
		seedTest(t, s, lease, "preimage-unrelated", `{"value":2}`)
		existing, fresh := testRef("preimage-existing"), testRef("preimage-fresh")
		tx, h := beginTest(t, s, lease, testLock(existing), testLock(fresh))
		if err := tx.Put(ctx, driver.Record{Ref: existing, Kind: "test", Body: []byte(`{"value":3}`)}); err != nil {
			t.Fatal(err)
		}
		h.Method, h.IdempotencyKey, h.RequestHash = "test.preimages", "preimage-retry", "preimage-request"
		h.Changes = []model.ChangeRecord{{Ref: existing, Op: model.OpPatch, FromRevision: model.Natural64(0)}}
		_, err, capture := captureCommitQueries(tx, h)
		if !errors.Is(err, driver.ErrConflict) {
			t.Fatalf("stale record revision was accepted: %v", err)
		}
		requireBoundedCommitQueries(t, capture, 1)
		requireRecordCounter(t, s, existing, 1, 1, false)
		// Validation failure leaves this writer usable. Extend its staged set
		// before retrying, so a transaction-scoped preimage cache would be stale.
		if err := tx.Put(ctx, driver.Record{Ref: fresh, Kind: "test", Body: []byte(`{"value":4}`)}); err != nil {
			t.Fatal(err)
		}
		h.Changes[0].FromRevision = model.Natural64(1)
		h.Changes = append(h.Changes, model.ChangeRecord{Ref: fresh, Op: model.OpCreate, FromRevision: model.Natural64(0)})
		entry, err, capture := captureCommitQueries(tx, h)
		if err != nil || entry.Revision.String() != "3" {
			t.Fatalf("retry changed source revision or failed: %+v %v", entry, err)
		}
		requireBoundedCommitQueries(t, capture, 1)
		requireRecordCounter(t, s, existing, 3, 2, false)
		requireRecordCounter(t, s, fresh, 3, 1, false)
		replay, replayHeader := beginTest(t, s, lease, testLock(existing), testLock(fresh))
		if err := replay.Put(ctx, driver.Record{Ref: existing, Kind: "test", Body: []byte(`{"different":true}`)}); err != nil {
			t.Fatal(err)
		}
		replayHeader.Method, replayHeader.IdempotencyKey, replayHeader.RequestHash = h.Method, h.IdempotencyKey, h.RequestHash
		// No Changes describe the different staged body: receipt replay must
		// precede fresh validation and all preimage reads, as before.
		got, err, replayCapture := captureCommitQueries(replay, replayHeader)
		if err != nil || !reflect.DeepEqual(got, entry) || len(replayCapture.events) != 0 {
			t.Fatalf("replay performed fresh preimage work or changed result: %+v %v queries=%d", got, err, len(replayCapture.events))
		}
		requireRecordCounter(t, s, existing, 3, 2, false)
		requireRecordCounter(t, s, fresh, 3, 1, false)
		if meta, err := s.Meta(ctx); err != nil || meta.Head.String() != "3" {
			t.Fatalf("replay consumed a revision: %+v %v", meta, err)
		}
	})

	t.Run("mixed_revisions_delete_and_recreate", func(t *testing.T) {
		s, lease := openCommitQueryTest(t)
		a, b := testRef("preimage-a"), testRef("preimage-b")
		seedTest(t, s, lease, a.ID, `{"value":1}`)
		seedTest(t, s, lease, b.ID, `{"value":2}`)
		tx, h := beginTest(t, s, lease, testLock(a), testLock(b))
		if err := tx.Put(ctx, driver.Record{Ref: a, Kind: "test", Body: []byte(`{"value":3}`)}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Delete(ctx, b); err != nil {
			t.Fatal(err)
		}
		h.Changes = []model.ChangeRecord{{Ref: a, Op: model.OpPatch, FromRevision: model.Natural64(1)}, {Ref: b, Op: model.OpDelete, FromRevision: model.Natural64(2)}}
		if _, err, capture := captureCommitQueries(tx, h); err != nil {
			t.Fatal(err)
		} else {
			requireBoundedCommitQueries(t, capture, 2)
		}
		requireRecordCounter(t, s, a, 3, 2, false)
		requireRecordCounter(t, s, b, 3, 2, true)
		tx, h = beginTest(t, s, lease, testLock(a), testLock(b))
		for _, ref := range []driver.Ref{a, b} {
			if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":4}`)}); err != nil {
				t.Fatal(err)
			}
		}
		h.Changes = []model.ChangeRecord{{Ref: a, Op: model.OpPatch, FromRevision: model.Natural64(3)}, {Ref: b, Op: model.OpCreate, FromRevision: model.Natural64(3)}}
		if _, err, capture := captureCommitQueries(tx, h); err != nil {
			t.Fatal(err)
		} else {
			requireBoundedCommitQueries(t, capture, 2)
		}
		for _, ref := range []driver.Ref{a, b} {
			requireRecordCounter(t, s, ref, 4, 3, false)
		}
		current := testRead(t, s, nil)
		for _, ref := range []driver.Ref{a, b} {
			versions, err := current.Versions(ctx, ref, driver.PageReq{})
			if err != nil || len(versions.Items) != 3 {
				t.Fatalf("mixed history %s: %+v %v", ref.ID, versions, err)
			}
			for _, version := range versions.Items {
				wantBase := version.Revision.String() == "1" || version.Revision.String() == "2" || ref == b && version.Revision.String() == "4"
				wantDeleted := ref == b && version.Revision.String() == "3"
				if version.Base != wantBase || version.Deleted != wantDeleted {
					t.Fatalf("mixed base/tombstone changed: %+v", version)
				}
			}
		}
		for n := uint64(1); n <= 4; n++ {
			at := model.Natural64(n)
			r := testRead(t, s, &at)
			rows, err := r.Get(ctx, []driver.Ref{a, b})
			wantRows := 2
			if n == 1 || n == 3 {
				wantRows = 1
			}
			if err != nil || len(rows) != wantRows {
				t.Fatalf("historical snapshot %d rows=%d err=%v", n, len(rows), err)
			}
			for _, row := range rows {
				value := n
				if row.Ref == a && n == 2 {
					value = 1
				}
				if string(row.Body) != fmt.Sprintf(`{"value":%d}`, value) {
					t.Fatalf("historical snapshot %d body=%s", n, row.Body)
				}
			}
		}
	})

	t.Run("later_version_failure_rolls_back", func(t *testing.T) {
		s, lease := openCommitQueryTest(t)
		existing, fresh := testRef("preimage-rollback-existing"), testRef("preimage-rollback-fresh")
		seedTest(t, s, lease, "preimage-rollback-anchor", `{"anchor":true}`)
		seedTest(t, s, lease, existing.ID, `{"value":2}`)
		if _, err := s.db.ExecContext(ctx, `CREATE SEQUENCE plug_postgres.preimage_version_attempts;
CREATE FUNCTION plug_postgres.reject_preimage_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM nextval('plug_postgres.preimage_version_attempts'); RAISE EXCEPTION 'preimage version fault'; END$$;
CREATE TRIGGER reject_preimage_version BEFORE INSERT ON plug_postgres.versions FOR EACH ROW EXECUTE FUNCTION plug_postgres.reject_preimage_version()`); err != nil {
			t.Fatal(err)
		}
		stage := func() (driver.WriteTx, driver.ChangesetHeader) {
			tx, h := beginTest(t, s, lease, testLock(existing), testLock(fresh))
			for _, ref := range []driver.Ref{existing, fresh} {
				if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":3}`)}); err != nil {
					t.Fatal(err)
				}
			}
			h.Changes = []model.ChangeRecord{{Ref: existing, Op: model.OpPatch, FromRevision: model.Natural64(2)}, {Ref: fresh, Op: model.OpCreate, FromRevision: model.Natural64(0)}}
			return tx, h
		}
		tx, h := stage()
		_, err, capture := captureCommitQueries(tx, h)
		if err == nil || !strings.Contains(err.Error(), "preimage version fault") {
			t.Fatalf("real later version fault was not reached: %v", err)
		}
		requireBoundedCommitQueries(t, capture, 1)
		var attempted bool
		if err := s.db.QueryRowContext(ctx, `SELECT is_called FROM plug_postgres.preimage_version_attempts`).Scan(&attempted); err != nil || !attempted {
			t.Fatalf("engine version statement was not attempted: %t %v", attempted, err)
		}
		requireRecordCounter(t, s, existing, 2, 1, false)
		for _, table := range []string{"records", "versions", "changesets"} {
			var count int
			if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.`+table).Scan(&count); err != nil || count != 2 {
				t.Fatalf("failed publication leaked %s rows=%d: %v", table, count, err)
			}
		}
		if _, err := s.db.ExecContext(ctx, `DROP TRIGGER reject_preimage_version ON plug_postgres.versions`); err != nil {
			t.Fatal(err)
		}
		tx, h = stage()
		entry, err, capture := captureCommitQueries(tx, h)
		if err != nil || entry.Revision.String() != "3" {
			t.Fatalf("fresh publication after rollback: %+v %v", entry, err)
		}
		requireBoundedCommitQueries(t, capture, 1)
		requireRecordCounter(t, s, existing, 3, 2, false)
		requireRecordCounter(t, s, fresh, 3, 1, false)
	})

	t.Run("uint64_counter_exhaustion", func(t *testing.T) {
		s, lease := openCommitQueryTest(t)
		ref := testRef("preimage-exhausted")
		seedTest(t, s, lease, ref.ID, `{"value":1}`)
		id, _ := recordKey(ref)
		if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.records SET version_count=$2 WHERE key=$1`, []byte(id), decimal(math.MaxUint64)); err != nil {
			t.Fatal(err)
		}
		tx, h := beginTest(t, s, lease, testLock(ref))
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":2}`)}); err != nil {
			t.Fatal(err)
		}
		h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpPatch, FromRevision: model.Natural64(1)}}
		_, err, capture := captureCommitQueries(tx, h)
		if !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("uint64 version counter wrapped or changed error: %v", err)
		}
		requireBoundedCommitQueries(t, capture, 1)
		requireRecordCounter(t, s, ref, 1, math.MaxUint64, false)
		if meta, err := s.Meta(ctx); err != nil || meta.Head.String() != "1" {
			t.Fatalf("exhausted counter advanced Head: %+v %v", meta, err)
		}
	})

	t.Run("head_refresh_after_writer_open", func(t *testing.T) {
		s, lease := openCommitQueryTest(t)
		ref := testRef("preimage-head-refresh")
		seedTest(t, s, lease, ref.ID, `{"value":1}`)
		tx, err := s.Write(ctx, lease)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Close()
		// This transaction began at Head1, before another real writer publishes.
		seedTest(t, s, lease, ref.ID, `{"value":2}`)
		key := testLock(ref)
		if _, err := tx.Lock(ctx, []driver.LockRequest{{Key: key, Mode: driver.LockUpdate}}); err != nil {
			t.Fatal(err)
		}
		if head, err := tx.Head(ctx); err != nil || head.String() != "2" {
			t.Fatalf("Head did not pin the current publication: %s %v", head, err)
		}
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":3}`)}); err != nil {
			t.Fatal(err)
		}
		meta, err := s.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		h := driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "postgres-test"}, LockKeys: []driver.LockKey{key}, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpPatch, FromRevision: model.Natural64(2)}}}
		entry, err, capture := captureCommitQueries(tx, h)
		if err != nil || entry.Revision.String() != "3" {
			t.Fatalf("Commit used the writer-open preimage: %+v %v", entry, err)
		}
		requireBoundedCommitQueries(t, capture, 1)
		requireRecordCounter(t, s, ref, 3, 3, false)
	})
}
