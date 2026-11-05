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
