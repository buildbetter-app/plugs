package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestIndependentTakeoverDuringEveryPublication(t *testing.T) {
	for _, operation := range []string{"graph", "noop", "checkpoint", "finalizer"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			dsn := freshDSN(t)
			oldStore, newStore := openTest(t, dsn), openTest(t, dsn)
			old := testLease(t, oldStore)
			var publish func() error
			table, event := "changesets", "INSERT"
			switch operation {
			case "graph":
				ref := testRef("pl-racing")
				tx, h := beginTest(t, oldStore, old, testLock(ref))
				h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}
				if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{}`)}); err != nil {
					t.Fatal(err)
				}
				publish = func() error { _, err := tx.Commit(ctx, h); return err }
			case "noop":
				table = "receipts"
				tx, h := beginTest(t, oldStore, old)
				h.Method, h.IdempotencyKey, h.RequestHash = "test.noop", "racing", "hash"
				publish = func() error { _, err := tx.(driver.NoopCommitter).CommitNoop(ctx, h); return err }
			case "checkpoint":
				table = "checkpoints"
				publish = func() error {
					_, err := oldStore.SaveCheckpoint(ctx, old, "racing", 0, []byte("unpublished"))
					return err
				}
			case "finalizer":
				table, event = "receipts", "UPDATE"
				tx, h := beginTest(t, oldStore, old)
				h.Method, h.IdempotencyKey, h.RequestHash = "test.finalize", "racing", "hash"
				h.ResponsePending = true
				entry, err := tx.Commit(ctx, h)
				if err != nil {
					t.Fatal(err)
				}
				publish = func() error {
					return oldStore.FinalizeResponse(ctx, old, "system:postgres-test", "", h.Method, h.IdempotencyKey, entry.Revision, []model.Warning{{Code: "post_subscriber_failed", Message: "must roll back"}})
				}
			}
			// This test-owned SQL gate pauses after the driver's lease-row lock and
			// before its final commit, exercising the actual publication interval.
			_, err := oldStore.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.block_publication() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(246802468); RETURN NEW; END$$; CREATE TRIGGER block_publication BEFORE `+event+` ON plug_postgres.`+table+` FOR EACH ROW EXECUTE FUNCTION plug_postgres.block_publication()`)
			if err != nil {
				t.Fatal(err)
			}
			gate, err := newStore.db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer gate.Close()
			if _, err = gate.ExecContext(ctx, `SELECT pg_advisory_lock(246802468)`); err != nil {
				t.Fatal(err)
			}
			defer gate.ExecContext(context.Background(), `SELECT pg_advisory_unlock(246802468)`)
			completed := make(chan error, 1)
			go func() { completed <- publish() }()
			for {
				var waiting bool
				err = newStore.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objid=246802468 AND NOT granted)`).Scan(&waiting)
				if err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case err := <-completed:
					t.Fatalf("publication ended before SQL gate: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			released := make(chan error, 1)
			go func() { released <- old.Release(ctx) }()
			select {
			case <-old.Lost():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			type acquired struct {
				lease driver.ActiveLease
				err   error
			}
			takeover := make(chan acquired, 1)
			go func() {
				lease, err := newStore.Lease(ctx, "new-independent-pool", driver.LeaseTTL)
				takeover <- acquired{lease, err}
			}()
			select {
			case value := <-takeover:
				t.Fatalf("takeover bypassed publisher's epoch lock: %+v", value)
			case <-time.After(20 * time.Millisecond):
			}
			if _, err = gate.ExecContext(ctx, `SELECT pg_advisory_unlock(246802468)`); err != nil {
				t.Fatal(err)
			}
			var fenced driver.ErrFenced
			if err = <-completed; !errors.As(err, &fenced) {
				t.Fatalf("old publication=%v", err)
			}
			if err = <-released; err != nil {
				t.Fatal(err)
			}
			next := <-takeover
			if next.err != nil {
				t.Fatal(next.err)
			}
			defer next.lease.Release(context.Background())
			if next.lease.Epoch() <= old.Epoch() {
				t.Fatal("takeover reused epoch")
			}
			if _, err = oldStore.Write(ctx, old); !errors.As(err, &fenced) {
				t.Fatalf("old holder still writes: %v", err)
			}
			meta, err := newStore.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expected := "0"
			if operation == "finalizer" {
				expected = "1"
			}
			if meta.Head.String() != expected {
				t.Fatalf("stale publication changed head: %+v", meta)
			}
			if operation == "checkpoint" {
				cp, err := newStore.LoadCheckpoint(ctx, "racing")
				if err != nil || cp.Generation != 0 {
					t.Fatalf("stale checkpoint published: %+v %v", cp, err)
				}
			}
			if operation == "noop" {
				r := testRead(t, newStore, nil)
				value, err := r.IdempotencyByKey(ctx, "system:postgres-test", "", "test.noop", "racing")
				_ = r.Close()
				if err != nil || value != nil {
					t.Fatalf("stale no-op published: %+v %v", value, err)
				}
			}
			if operation == "finalizer" {
				r := testRead(t, newStore, nil)
				value, err := r.IdempotencyByKey(ctx, "system:postgres-test", "", "test.finalize", "racing")
				_ = r.Close()
				if err != nil || value == nil || !value.ResponsePending || len(value.Commit.Warnings) != 0 {
					t.Fatalf("stale warnings published: %+v %v", value, err)
				}
			}
			// A fresh holder immediately gets the previously held record/head locks.
			if _, err = newStore.db.ExecContext(ctx, `DROP TRIGGER block_publication ON plug_postgres.`+table+`; DROP FUNCTION plug_postgres.block_publication()`); err != nil {
				t.Fatal(err)
			}
			seedTest(t, newStore, next.lease, "pl-after-takeover", `{}`)
			t.Log("separate SQL sessions fence the old publisher and serialize takeover across the publication interval")
		})
	}
}
