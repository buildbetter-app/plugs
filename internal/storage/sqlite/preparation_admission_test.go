package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestPreparationAdmissionWaitsWithoutPoolConnectionsAndPreservesRawReaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opened, err := (Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "preparation.db")})
	if err != nil {
		t.Fatal(err)
	}
	s := opened.(*Store)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
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
			t.Fatalf("queued preparation changed its caller deadline: %v", err)
		}
	}
	if got := s.db.Stats().InUse; got != inUse {
		t.Fatalf("queued preparations retained connections: before=%d after=%d", inUse, got)
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
		future := model.Natural64(1)
		if r, err := s.Read(preparation, driver.View{Incarnation: meta.Incarnation, At: &future}); err == nil || r != nil {
			t.Fatalf("invalid snapshot unexpectedly admitted: %v %v", r, err)
		}
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
		t.Fatalf("preparation admission published graph state: %+v %v", meta, err)
	}
}
