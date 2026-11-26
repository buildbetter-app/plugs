package sqlite

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSQLitePublicationLeaseFinalizerOutcomeAfterCommit(t *testing.T) {
	for _, mode := range []string{"expiry", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "finalizer-outcome.db")
			s := openStore(t, ctx, path)
			observer := openStore(t, ctx, path)
			setup := getLease(t, ctx, s, driver.LeaseTTL)
			entry, header := commitReceipt(t, ctx, s, setup, "pl-finalizer-outcome", true)
			meta := getMeta(t, ctx, observer)
			before := lookupReceipt(t, ctx, getReader(t, ctx, observer, meta, nil), header)
			if err := setup.Release(ctx); err != nil {
				t.Fatal(err)
			}
			lease := getLease(t, ctx, s, time.Second).(*activeLease)
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			connector := hookPublicationCommit(t, s, path)
			post := []model.Warning{{Code: "post_subscriber_failed", Message: "confirmed post outcome"}}
			var accepted error
			var during *driver.IdempotencyRecord
			var held time.Duration
			connector.after = func() {
				// This heartbeat arrives AFTER the receipt is durably finalized. The
				// previous implementation needed a fallible lease-only transaction.
				accepted = lease.Renew(ctx)
				during = lookupReceipt(t, ctx, getReader(t, ctx, observer, meta, nil), header)
				if mode == "cancellation" {
					cancel()
					return
				}
				started := time.Now()
				timer := time.NewTimer(2 * time.Second)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
				}
				held = time.Since(started)
			}
			err := s.FinalizeResponse(call, lease, "system:engine-test", "", header.Method, header.IdempotencyKey, entry.Revision, post)
			if accepted != nil || connector.steps.Load() != 1 || mode == "expiry" && held < 2*time.Second {
				t.Fatalf("post-COMMIT interval not exercised: heartbeat=%v commits=%d held=%s", accepted, connector.steps.Load(), held)
			}
			var fenced driver.ErrFenced
			if err != nil || errors.As(err, &fenced) || errors.Is(err, context.Canceled) {
				t.Fatalf("confirmed receipt was reported as rejection: %T %v", err, err)
			}
			after := lookupReceipt(t, ctx, getReader(t, ctx, observer, meta, nil), header)
			want := append(append([]model.Warning{}, before.Commit.Warnings...), post...)
			if during == nil || !reflect.DeepEqual(after, during) || after.ResponsePending ||
				!reflect.DeepEqual(after.Commit.Warnings, want) || !reflect.DeepEqual(after.Commit.Results, before.Commit.Results) ||
				after.Commit.Revision != before.Commit.Revision || after.RequestHash != before.RequestHash ||
				!reflect.DeepEqual(after.Changeset, before.Changeset) || getMeta(t, ctx, observer).Head != meta.Head {
				t.Fatalf("final receipt changed original publication or post outcome: before=%+v during=%+v after=%+v", before, during, after)
			}
			if mode == "expiry" {
				select {
				case <-lease.Lost():
				default:
					t.Fatal("genuinely expired finalizer lease remained live")
				}
				requireFenced(t, lease.Renew(ctx))
				next := getLease(t, ctx, observer, driver.LeaseTTL)
				if next.Epoch() <= lease.Epoch() {
					t.Fatal("takeover did not advance epoch")
				}
				requireFenced(t, s.FinalizeResponse(ctx, lease, "system:engine-test", "", header.Method, header.IdempotencyKey, entry.Revision, post))
				if err := observer.FinalizeResponse(ctx, next, "system:engine-test", "", header.Method, header.IdempotencyKey, entry.Revision, post); err != nil {
					t.Fatal(err)
				}
				if got := lookupReceipt(t, ctx, getReader(t, ctx, observer, meta, nil), header); !reflect.DeepEqual(got, after) {
					t.Fatal("successor retry duplicated warnings or changed finalized receipt")
				}
			} else {
				assertDurableHeartbeat(t, ctx, observer, lease)
			}
			t.Logf("%s after real receipt COMMIT: independent reader saw final warnings and pending=false before interruption; caller received success; no post-COMMIT transaction", mode)
		})
	}
}

func TestSQLiteHeartbeatJournalBindsDurableOwner(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "journal-owner.db")
	s := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	old := getLease(t, ctx, s, driver.LeaseTTL).(*activeLease)
	if err := old.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(s.lockDir, heartbeatFile)
	data, err := os.ReadFile(journalPath)
	if err != nil || len(data) != heartbeatSize {
		t.Fatalf("journal size=%d err=%v", len(data), err)
	}
	if err := old.Release(ctx); err != nil {
		t.Fatal(err)
	}
	next := getLease(t, ctx, observer, driver.LeaseTTL).(*activeLease)
	for _, mode := range []string{"old-epoch", "old-token"} {
		t.Run(mode, func(t *testing.T) {
			stale := append([]byte{}, data...)
			if mode == "old-token" {
				binary.BigEndian.PutUint64(stale[:8], next.epoch)
			}
			// A stale record must be ignored even if its claimed expiry is later
			// than the successor's. Neither token digest nor epoch is optional.
			binary.BigEndian.PutUint64(stale[len(stale)-8:], uint64(time.Now().Add(time.Hour).UnixNano()))
			if err := os.WriteFile(journalPath, stale, 0600); err != nil {
				t.Fatal(err)
			}
			raw, err := readLease(ctx, observer.db)
			if err != nil {
				t.Fatal(err)
			}
			effective, err := observer.readLease(ctx, observer.db)
			if err != nil || effective != raw || effective.epoch != next.epoch {
				t.Fatalf("stale journal extended successor: raw=%+v effective=%+v err=%v", raw, effective, err)
			}
			requireFenced(t, old.Renew(ctx))
		})
	}
	if err := next.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	assertDurableHeartbeat(t, ctx, observer, next)
}

func TestSQLiteHeartbeatJournalInvalidRecordFailsClosed(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "journal-invalid.db")
	s := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	journalPath := filepath.Join(s.lockDir, heartbeatFile)
	if err := os.WriteFile(journalPath, []byte("partial record"), 0600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(journalPath)
	if _, err := observer.readLease(ctx, observer.db); err == nil {
		t.Fatal("malformed journal silently fell back to a stale SQLite expiry")
	}
	if next, err := observer.Lease(ctx, "contender", driver.LeaseTTL); next != nil || err == nil {
		t.Fatalf("malformed journal admitted takeover: next=%v err=%v", next, err)
	}
	if _, err := s.SaveCheckpoint(ctx, lease, "invalid-journal", 0, []byte("refuse")); err == nil {
		t.Fatal("malformed journal admitted publication")
	}
	cp, err := observer.LoadCheckpoint(ctx, "invalid-journal")
	if err != nil || cp.Generation != 0 {
		t.Fatalf("invalid journal published checkpoint: %+v err=%v", cp, err)
	}
}

func TestSQLiteHeartbeatJournalPersistenceFailureLosesLease(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "journal-failure.db")
	s := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL).(*activeLease)
	journalPath := filepath.Join(s.lockDir, heartbeatFile)
	var renewErr error
	err := s.immediate(ctx, lease, func(c *sql.Conn) error {
		if _, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "journal-failure", revKey(1), []byte("must roll back")); err != nil {
			return err
		}
		// Fail the real atomic replacement, not a substituted engine result.
		if err := os.Mkdir(journalPath, 0700); err != nil {
			return err
		}
		renewErr = lease.Renew(ctx)
		return renewErr
	})
	if removeErr := os.Remove(journalPath); removeErr != nil {
		t.Fatal(removeErr)
	}
	if renewErr == nil || !errors.Is(err, renewErr) {
		t.Fatalf("journal persistence failure not returned: renew=%v publication=%v", renewErr, err)
	}
	select {
	case <-lease.Lost():
	default:
		t.Fatal("unpersisted heartbeat left a live owner")
	}
	if got := lease.LossReason(); got != "heartbeat journal update failed" {
		t.Fatalf("lease loss reason = %q", got)
	}
	requireFenced(t, lease.Renew(ctx))
	cp, err := observer.LoadCheckpoint(ctx, "journal-failure")
	if err != nil || cp.Generation != 0 {
		t.Fatalf("failed heartbeat published graph: %+v err=%v", cp, err)
	}
}
