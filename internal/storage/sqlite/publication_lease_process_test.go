package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSQLitePublicationLeaseRenewalExcludesProcessUntilRealExpiry(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "publication-process.db")
	s := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	cmd := childCommand(t, ctx, path, "publication-takeover")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.WriteFile(path+".finish", []byte("done"), 0600); err != nil {
			t.Error(err)
		}
		if err := cmd.Wait(); err != nil {
			t.Errorf("child=%v\n%s", err, &output)
		}
		if !strings.Contains(output.String(), "child acquired epoch 2 and checkpoint generation 2 after renewed parent really expired") {
			t.Errorf("child takeover evidence missing:\n%s", &output)
		}
		t.Log(output.String())
	})
	if err := waitMarker(ctx, path+".opened"); err != nil {
		t.Fatal(err)
	}
	lease := getLease(t, ctx, s, time.Second).(*activeLease)
	original, err := readLease(ctx, s.db)
	if err != nil {
		t.Fatal(err)
	}
	start, stop := make(chan struct{}), make(chan struct{})
	done := publicationHeartbeat(ctx, lease, start, stop)
	connector := hookPublicationCommit(t, s, path)
	var commitHeld time.Duration
	connector.before = func() {
		started := time.Now()
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		commitHeld = time.Since(started)
	}
	var renewal publicationHeartbeatResult
	connector.after = func() { close(stop); renewal = <-done }
	err = s.immediate(ctx, lease, func(c *sql.Conn) error {
		if _, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "process-publication", revKey(1), []byte("parent")); err != nil {
			return err
		}
		close(start)
		if err := os.WriteFile(path+".acquire", []byte("acquire"), 0600); err != nil {
			return err
		}
		if err := waitMarker(ctx, path+".attempting"); err != nil {
			return err
		}
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		row, err := readLease(ctx, c)
		if err != nil {
			return err
		}
		if row != original || time.Now().UnixNano() < row.expires {
			return fmt.Errorf("test did not cross the unchanged durable expiry under the native writer: epoch=%d expires=%d", row.epoch, row.expires)
		}
		if _, err := os.Stat(path + ".ready"); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("child acquired during exclusive publication: marker error=%v", err)
		}
		return nil
	})
	if err != nil || renewal.err != nil || renewal.successes == 0 || commitHeld < 2*time.Second || connector.steps.Load() != 1 {
		t.Fatalf("long publication=%v heartbeat=%+v commitHeld=%s nativeCommits=%d", err, renewal, commitHeld, connector.steps.Load())
	}
	raw, err := readLease(ctx, observer.db)
	if err != nil || time.Now().UnixNano() < raw.expires {
		t.Fatalf("did not cross the committed SQLite expiry: row=%+v err=%v", raw, err)
	}
	row, err := observer.readLease(ctx, observer.db)
	if err != nil || row.epoch != lease.Epoch() || row.token != original.token || row.expires <= original.expires || time.Now().UnixNano() >= row.expires {
		t.Fatalf("handoff did not persist a fresh parent heartbeat: row=%+v err=%v", row, err)
	}
	// Both a different process and an independent handle must keep waiting
	// after handoff, even though the pre-publication durable expiry passed.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	next, err := observer.Lease(short, "second-connection", driver.LeaseTTL)
	cancel()
	if next != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("independent handle took renewed lease: next=%v err=%v", next, err)
	}
	if _, err := os.Stat(path + ".ready"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child took renewed lease before real expiry: marker error=%v", err)
	}
	// Stop renewing and let the persisted deadline really pass. The same child
	// that was excluded above must now acquire and publish normally.
	if err := waitMarker(ctx, path+".ready"); err != nil {
		t.Fatal(err)
	}
	staleBodyCalled := false
	err = s.immediate(ctx, lease, func(*sql.Conn) error {
		staleBodyCalled = true
		return nil
	})
	requireFenced(t, err)
	if staleBodyCalled {
		t.Fatal("stale publication entered its callback after child takeover")
	}
	requireFenced(t, lease.Renew(ctx))
	if _, err := s.SaveCheckpoint(ctx, lease, "process-publication", 2, []byte("stale")); err == nil {
		t.Fatal("taken-over lease overwrote child publication")
	} else {
		requireFenced(t, err)
	}
	cp, err := observer.LoadCheckpoint(ctx, "process-publication")
	if err != nil || cp.Generation != 2 || string(cp.Payload) != "child" {
		t.Fatalf("child checkpoint=%+v err=%v", cp, err)
	}
	t.Logf("%d heartbeats bridged long publication; child and second handle stayed excluded through durable handoff, then child acquired epoch 2 after real expiry and stale parent was fenced", renewal.successes)
}

func TestSQLitePublicationLeaseWaitingRenewalSeesRegistration(t *testing.T) {
	ctx := testContext(t)
	var observe atomic.Bool
	checkedBeforeRegistration := make(chan struct{})
	// Observe the waiter's real-time expiry check. No time value or deadline
	// is replaced, and it cannot acquire the publication gate we already hold.
	value, err := (Driver{Clock: func() time.Time {
		if observe.CompareAndSwap(true, false) {
			close(checkedBeforeRegistration)
		}
		return time.Now()
	}}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "registration.db")})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	lease := getLease(t, ctx, s, time.Second).(*activeLease)
	gate, err := s.fileLock(ctx, "publication", false, lease.Lost())
	if err != nil {
		t.Fatal(err)
	}
	defer unlockFile(gate)
	// Start Renew before registration while the publication gate is occupied.
	renewal := make(chan error, 1)
	observe.Store(true)
	go func() { renewal <- lease.Renew(ctx) }()
	select {
	case <-checkedBeforeRegistration:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	err = s.immediateLocked(ctx, lease, false, func(c *sql.Conn) error {
		select {
		case err := <-renewal:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		_, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "registration", revKey(1), []byte("renewed"))
		return err
	})
	if err != nil {
		t.Fatalf("Renew already waiting on gate missed publication registration: %v", err)
	}
	select {
	case <-lease.Lost():
		t.Fatal("waiting renewal was fenced instead of bridged")
	default:
	}
	t.Log("Renew admitted before registration completed while the original publication gate and native writer were still held")
}
