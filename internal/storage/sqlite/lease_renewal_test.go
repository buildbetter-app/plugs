package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// The production renewal path is internal/storage/sqlite/lease.go:305-332;
// this test holds the real immediate publication transaction open while that
// path persists a heartbeat independently of the native writer.
func TestSQLiteLeaseRenewalDuringLongWrite(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "lease-renewal.db"))
	lease := getLease(t, ctx, s, time.Second).(*activeLease)
	entered := make(chan struct{})
	release := make(chan struct{})
	published := make(chan error, 1)
	go func() {
		published <- s.immediate(ctx, lease, func(c *sql.Conn) error {
			if _, err := c.ExecContext(ctx, `INSERT INTO checkpoints(key,generation,payload) VALUES(?,?,?)`, "long-renewal", revKey(1), []byte("renewed")); err != nil {
				return err
			}
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	renewCtx, cancel := context.WithTimeout(ctx, time.Second)
	err := lease.Renew(renewCtx)
	cancel()
	if err != nil {
		t.Fatalf("renewal while SQLite write was open: %v", err)
	}
	close(release)
	if err := <-published; err != nil {
		t.Fatalf("long SQLite write: %v", err)
	}
	checkpoint, err := s.LoadCheckpoint(ctx, "long-renewal")
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Generation != 1 || string(checkpoint.Payload) != "renewed" {
		t.Fatalf("long-write checkpoint=%+v", checkpoint)
	}
	t.Logf("SQLite immediate write remained publishable after independent renewal; epoch=%d generation=%d", lease.Epoch(), checkpoint.Generation)
}
