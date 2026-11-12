package postgres

import (
	"context"
	"testing"
	"time"
)

// The production renewal path is internal/storage/postgres/lease.go:198-244;
// the reserved heartbeat connection remains usable while this real checkpoint
// publication transaction waits on an advisory gate.
func TestPostgresLeaseRenewalDuringLongWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s).(*activeLease)
	if _, err := s.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.memory_lease_renewal_hold() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(330079); RETURN NEW; END$$; CREATE TRIGGER memory_lease_renewal_hold BEFORE INSERT ON plug_postgres.checkpoints FOR EACH ROW EXECUTE FUNCTION plug_postgres.memory_lease_renewal_hold()`); err != nil {
		t.Fatal(err)
	}
	gate, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	if _, err := gate.ExecContext(ctx, `SELECT pg_advisory_lock(330079)`); err != nil {
		t.Fatal(err)
	}
	gateHeld := true
	defer func() {
		if gateHeld {
			_, _ = gate.ExecContext(context.Background(), `SELECT pg_advisory_unlock(330079)`)
		}
	}()

	published := make(chan error, 1)
	go func() {
		_, err := s.SaveCheckpoint(ctx, lease, "long-renewal", 0, []byte("renewed"))
		published <- err
	}()
	waitFor := func() {
		t.Helper()
		for {
			var waiting bool
			if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objid=330079 AND NOT granted)`).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
	}
	waitFor()

	renewCtx, renewCancel := context.WithTimeout(ctx, time.Second)
	err = lease.Renew(renewCtx)
	renewCancel()
	if err != nil {
		t.Fatalf("renewal while PostgreSQL write was open: %v", err)
	}
	if _, err := gate.ExecContext(ctx, `SELECT pg_advisory_unlock(330079)`); err != nil {
		t.Fatal(err)
	}
	gateHeld = false
	if err := <-published; err != nil {
		t.Fatalf("long PostgreSQL write: %v", err)
	}
	checkpoint, err := s.LoadCheckpoint(ctx, "long-renewal")
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Generation != 1 || string(checkpoint.Payload) != "renewed" {
		t.Fatalf("long-write checkpoint=%+v", checkpoint)
	}
	t.Logf("PostgreSQL publication remained valid after reserved-connection renewal; epoch=%d generation=%d", lease.Epoch(), checkpoint.Generation)
}
