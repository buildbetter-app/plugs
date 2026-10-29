package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestHistoricalReplayAcrossScheduledTombstones(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "history.db"))
	l := getLease(t, ctx, s, driver.LeaseTTL)
	id := "pl-boundary"
	for i := 1; i <= 100; i++ {
		if i == 33 || i == 65 || i == 97 {
			tx, h := beginRecord(t, ctx, s, l, id)
			if err := tx.Delete(ctx, testRef(id)); err != nil {
				t.Fatal(err)
			}
			h.Changes[0].Op = model.OpDelete
			if _, err := tx.Commit(ctx, h); err != nil {
				t.Fatal(err)
			}
		} else {
			seedRecord(t, ctx, s, l, id, fmt.Sprintf(`{"value":%d}`, i))
		}
	}
	// Force historical reconstruction rather than the optimized current row.
	seedRecord(t, ctx, s, l, "pl-advance", `{}`)
	meta := getMeta(t, ctx, s)
	for _, n := range []uint64{32, 33, 34, 64, 65, 66, 96, 97, 98, 100} {
		at := model.Natural64(n)
		rows, err := getReader(t, ctx, s, meta, &at).Get(ctx, []driver.Ref{testRef(id)})
		if err != nil {
			t.Fatalf("as-of %d: %v", n, err)
		}
		if n == 33 || n == 65 || n == 97 {
			if len(rows) != 0 {
				t.Fatalf("as-of tombstone %d leaked %+v", n, rows)
			}
			continue
		}
		if len(rows) != 1 || string(rows[0].Body) != fmt.Sprintf(`{"value":%d}`, n) {
			t.Fatalf("as-of recreation %d: %+v", n, rows)
		}
	}
	page, err := getReader(t, ctx, s, meta, nil).Versions(ctx, testRef(id), driver.PageReq{Limit: 1000})
	if err != nil || len(page.Items) != 100 {
		t.Fatalf("versions=%d %v", len(page.Items), err)
	}
	diffs := 0
	for i, v := range page.Items {
		if v.Base {
			diffs = 0
		} else if !v.Deleted {
			diffs++
		}
		if diffs > 31 {
			t.Fatalf("unbounded replay at index %d: %d diffs", i, diffs)
		}
		if i%driver.VersionBaseEvery == 0 && !v.Base {
			t.Fatalf("scheduled base missing at %d", i)
		}
	}
	t.Log("real historical replay crossed three scheduled tombstones/recreations with at most 31 forward diffs")
}

func TestIndependentEpochRacesCheckpointFinalizerAndRenewal(t *testing.T) {
	for iteration := 0; iteration < 4; iteration++ {
		t.Run(fmt.Sprint(iteration), func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "metadata.db")
			a := openStore(t, ctx, path)
			b := openStore(t, ctx, path)
			old := getLease(t, ctx, a, driver.LeaseTTL)
			tx, h := beginRecord(t, ctx, a, old, "pl-replay")
			h.IdempotencyKey = "key"
			h.Method = "test.write"
			h.RequestHash = "digest"
			if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-replay"), Kind: "test", Body: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			entry, err := tx.Commit(ctx, h)
			if err != nil {
				t.Fatal(err)
			}
			warnings := []model.Warning{{Code: "post_subscriber_failed", Message: "race result"}}
			start := make(chan struct{})
			results := make(chan error, 3)
			go func() { <-start; _, err := a.SaveCheckpoint(ctx, old, "delivery", 0, []byte("old")); results <- err }()
			go func() {
				<-start
				results <- a.FinalizeResponse(ctx, old, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, warnings)
			}()
			go func() { <-start; results <- old.Renew(ctx) }()
			close(start)
			if err = old.Release(ctx); err != nil {
				t.Fatal(err)
			}
			fresh := getLease(t, ctx, b, driver.LeaseTTL)
			if fresh.Epoch() <= old.Epoch() {
				t.Fatal("epoch reused")
			}
			for i := 0; i < 3; i++ {
				if err := <-results; err != nil {
					requireFenced(t, err)
				}
			}
			cp, err := b.LoadCheckpoint(ctx, "delivery")
			if err != nil || cp.Generation > 1 {
				t.Fatalf("racing checkpoint=%+v %v", cp, err)
			}
			saved, err := b.SaveCheckpoint(ctx, fresh, "delivery", cp.Generation, []byte("new"))
			if err != nil || saved.Generation != cp.Generation+1 {
				t.Fatalf("current epoch checkpoint=%+v %v", saved, err)
			}
			if err = b.FinalizeResponse(ctx, fresh, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, warnings); err != nil {
				t.Fatal(err)
			}
			if _, err = a.SaveCheckpoint(ctx, old, "delivery", saved.Generation, []byte("stale")); err == nil {
				t.Fatal("stale checkpoint accepted")
			} else {
				requireFenced(t, err)
			}
			requireFenced(t, a.FinalizeResponse(ctx, old, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, nil))
			requireFenced(t, old.Renew(ctx))
			meta := getMeta(t, ctx, b)
			receipt, err := getReader(t, ctx, b, meta, nil).IdempotencyByKey(ctx, "system:engine-test", "", h.Method, h.IdempotencyKey)
			if err != nil || receipt == nil || len(receipt.Commit.Warnings) != 1 || meta.Head.String() != "1" {
				t.Fatalf("racing finalization/head=%+v %+v %v", receipt, meta, err)
			}
		})
	}
}

func TestConcurrentRenewalsKeepOneDurableDeadline(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "renew.db"))
	l := getLease(t, ctx, s, driver.LeaseTTL).(*activeLease)
	const workers = 16
	var group sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() { defer group.Done(); errs <- l.Renew(ctx) }()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	row, err := readLease(ctx, s.db)
	if err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	deadline := l.deadline.UnixNano()
	l.mu.Unlock()
	if deadline != row.expires {
		t.Fatalf("timer/durable deadline diverged: %d != %d", deadline, row.expires)
	}
	select {
	case <-l.Lost():
		t.Fatal("renewed lease lost")
	default:
	}
}

func TestCloseReleasesOutstandingResources(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "close.db")
	s := openStore(t, ctx, path)
	l := getLease(t, ctx, s, driver.LeaseTTL)
	r := getReader(t, ctx, s, getMeta(t, ctx, s), nil)
	tx, _ := beginRecord(t, ctx, s, l, "pl-close-store")
	if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-close-store"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.Lost():
	default:
		t.Fatal("store close left live lease")
	}
	reopened := openStore(t, ctx, path)
	next := getLease(t, ctx, reopened, driver.LeaseTTL)
	if !getMeta(t, ctx, reopened).Head.IsZero() {
		t.Fatal("store close implicitly committed")
	}
	entry := seedRecord(t, ctx, reopened, next, "pl-close-store", `{}`)
	if entry.Revision.String() != "1" {
		t.Fatal("close failed to release row/head resources")
	}
}

func TestForeignSchemaAndHardLinksRefuse(t *testing.T) {
	ctx := testContext(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "foreign.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, `CREATE TABLE unrelated(value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err = (Driver{}).Open(ctx, driver.Config{DSN: path}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("foreign schema=%v", err)
	}
	var count int
	if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("foreign schema modified: tables=%d %v", count, err)
	}
	storePath := filepath.Join(dir, "store.db")
	s := openStore(t, ctx, storePath)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias.db")
	if err = os.Link(storePath, alias); err != nil {
		t.Fatal(err)
	}
	if _, err = (Driver{}).Open(ctx, driver.Config{DSN: alias}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("hard-linked database=%v", err)
	}
}
