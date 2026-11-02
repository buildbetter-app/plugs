package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// Exercise the actual driver's cancellation boundary, not a simulated busy
// error. A failed BEGIN must not leave the pooled connection owning SQLite's
// writer lock after the publication file lock has been released.
func TestCanceledPublicationBeginReleasesWriter(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "canceled-begin.db")
	a := openStore(t, ctx, path)
	b := openStore(t, ctx, path)
	a.db.SetMaxOpenConns(1)
	t.Cleanup(func() { _, _ = a.db.ExecContext(context.Background(), "ROLLBACK") })
	canceled, beforeBody := 0, 0
	for i := 0; i < 256; i++ {
		call, cancel := context.WithCancel(ctx)
		// Vary the real cancellation boundary around a short native BEGIN.
		// This timer ends work; it never waits for contention or retries it.
		timer := time.AfterFunc(time.Duration(1+i%32)*time.Microsecond, cancel)
		called := false
		err := a.immediate(call, nil, func(c *sql.Conn) error {
			called = true
			_, err := readLease(call, c)
			return err
		})
		timer.Stop()
		cancel()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("publication %d: %T %v", i, err, err)
			}
			canceled++
			if !called {
				beforeBody++
			}
		}
		// The first publisher has returned and released its advisory lock.
		// A different real handle must acquire the native writer immediately.
		if err := b.immediate(ctx, nil, func(c *sql.Conn) error {
			_, err := readLease(ctx, c)
			return err
		}); err != nil {
			t.Fatalf("writer retained after publication %d: body=%v original=%v probe=%T %v", i, called, call.Err(), err, err)
		}
	}
	t.Logf("real canceled publications=%d before-body=%d; separate handle acquired writer after every return", canceled, beforeBody)
}
