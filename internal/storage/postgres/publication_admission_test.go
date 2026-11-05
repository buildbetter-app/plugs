package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestPublicationAdmissionWaitsWithoutPoolConnectionsAndPreservesRawTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	publication := driver.WithPublicationAdmission(ctx)
	first, err := s.Write(publication, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	inUse := s.db.Stats().InUse

	// A raw held-row transaction must not queue behind publication admission:
	// claimNext needs to open its own transaction and skip such a row.
	raw, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	const waiters = 8
	results := make(chan error, waiters)
	for range waiters {
		go func() {
			queued, stop := context.WithTimeout(publication, 100*time.Millisecond)
			defer stop()
			tx, err := s.Write(queued, lease)
			if tx != nil {
				_ = tx.Close()
				results <- errors.New("queued publication reserved a transaction")
				return
			}
			results <- err
		}()
	}
	for range waiters {
		if err := <-results; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued publication did not preserve its caller deadline: %v", err)
		}
	}
	if got := s.db.Stats().InUse; got != inUse {
		t.Fatalf("queued publications retained pool connections: before=%d after=%d", inUse, got)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		next, err := s.Write(publication, lease)
		if err != nil {
			t.Fatal(err)
		}
		if err := next.Close(); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := s.Meta(ctx)
	if err != nil || !meta.Head.IsZero() {
		t.Fatalf("admission-only transactions published: %+v %v", meta, err)
	}
}

func TestPreparationAdmissionWaitsWithoutPoolConnectionsAndPreservesRawReaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	view := driver.View{Incarnation: meta.Incarnation}
	preparation := driver.WithPreparationAdmission(ctx)
	var held []driver.Reader
	for range 4 {
		r, err := s.Read(preparation, view)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		held = append(held, r)
	}
	inUse := s.db.Stats().InUse
	raw, err := s.Read(ctx, view)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	const waiters = 8
	results := make(chan error, waiters)
	for range waiters {
		go func() {
			queued, stop := context.WithTimeout(preparation, 100*time.Millisecond)
			defer stop()
			r, err := s.Read(queued, view)
			if r != nil {
				_ = r.Close()
				results <- errors.New("queued preparation reserved a snapshot")
				return
			}
			results <- err
		}()
	}
	for range waiters {
		if err := <-results; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued preparation did not preserve its caller deadline: %v", err)
		}
	}
	if got := s.db.Stats().InUse; got != inUse {
		t.Fatalf("queued preparations retained pool connections: before=%d after=%d", inUse, got)
	}
	for _, r := range held {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		r, err := s.Read(preparation, view)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	meta, err = s.Meta(ctx)
	if err != nil || !meta.Head.IsZero() {
		t.Fatalf("preparation admission published: %+v %v", meta, err)
	}
}

func TestLockPreparationReacquiresFailedReservedConnectionWithoutPublication(t *testing.T) {
	for _, failure := range []string{"closed-handle", "terminated-backend", "canceled-insert"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s := openTest(t, freshDSN(t))
			original := s.lockConn
			key := []byte("b04-reserved-lock-recovery")
			switch failure {
			case "closed-handle":
				if err := original.Close(); err != nil {
					t.Fatal(err)
				}
			case "terminated-backend":
				var pid int
				if err := original.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
					t.Fatal(err)
				}
				var terminated bool
				if err := s.db.QueryRowContext(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil || !terminated {
					t.Fatalf("owned lock backend was not terminated: %t %v", terminated, err)
				}
			case "canceled-insert":
				blocker, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback()
				if _, err := blocker.ExecContext(ctx, "INSERT INTO plug_postgres.locks (key) VALUES ($1)", key); err != nil {
					t.Fatal(err)
				}
				queued, stop := context.WithTimeout(ctx, 100*time.Millisecond)
				err = s.ensureLockKeys(queued, []string{string(key)})
				stop()
				if !errors.Is(err, context.DeadlineExceeded) || s.lockConn != nil {
					t.Fatalf("canceled lock insertion retained its reservation or changed the deadline: conn=%p err=%v", s.lockConn, err)
				}
				if err := blocker.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if err := s.ensureLockKeys(ctx, []string{string(key)}); err != nil {
					t.Fatalf("failed lock reservation was not recovered: %v", err)
				}
			}
			if s.lockConn == nil || s.lockConn == original {
				t.Fatal("failed reserved connection was reused")
			}
			var count int
			if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM plug_postgres.locks WHERE key=$1", key).Scan(&count); err != nil || count != 1 {
				t.Fatalf("lock preparation was not idempotent: count=%d err=%v", count, err)
			}
			meta, err := s.Meta(ctx)
			if err != nil || !meta.Head.IsZero() {
				t.Fatalf("lock recovery published graph state: %+v %v", meta, err)
			}
		})
	}
}
