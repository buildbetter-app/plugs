package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSQLitePublicationLeaseTTLWithLiveRenewer(t *testing.T) {
	publicationPastLeaseTTL(t, true)
}

// Ignoring expiry at COMMIT merely because BEGIN was fenced is not sufficient:
// a holder that stops renewing must still lose its publication, even while it
// owns the native writer. This is the safety control for the live-renewer case.
func TestSQLitePublicationLeaseTTLWithoutRenewer(t *testing.T) {
	publicationPastLeaseTTL(t, false)
}

func publicationPastLeaseTTL(t *testing.T, renew bool) {
	t.Helper()
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "publication-lease.db")
	s := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	// Exercise the existing per-lease TTL argument, not a production constant
	// override. The SQL publication deliberately lasts two whole TTLs.
	const ttl = time.Second
	lease := getLease(t, ctx, s, ttl).(*activeLease)
	original, err := readLease(ctx, s.db)
	if err != nil {
		t.Fatal(err)
	}
	lease.mu.Lock()
	initialDeadline := lease.deadline
	lease.mu.Unlock()

	type renewalResult struct {
		attempts, successes int
		err                 error
	}
	var renewal renewalResult
	publicationHeld := make(chan struct{})
	renewalStarted := make(chan time.Time, 1)
	renewalDone := make(chan renewalResult, 1)
	renewCtx, cancelRenew := context.WithCancel(ctx)
	defer cancelRenew()
	if renew {
		go func() {
			var result renewalResult
			defer func() { renewalDone <- result }()
			select {
			case <-publicationHeld:
			case <-renewCtx.Done():
				return
			}
			renewalStarted <- time.Now()
			ticker := time.NewTicker(ttl / 10)
			defer ticker.Stop()
			for {
				result.attempts++
				if err := lease.Renew(renewCtx); err != nil {
					// Cancellation after publication only stops this test's
					// renewer; a real expiry/fence must remain observable.
					if !errors.Is(err, context.Canceled) || renewCtx.Err() == nil {
						result.err = err
					}
					return
				}
				result.successes++
				select {
				case <-renewCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}

	const key = "long-publication"
	payload := []byte("live-holder-publication")
	var entered bool
	var heldFor time.Duration
	err = s.immediate(ctx, lease, func(c *sql.Conn) error {
		// This is the real BEGIN IMMEDIATE / fence / COMMIT boundary used by
		// WriteTx.Commit, restores, and checkpoint publication. No engine
		// response, lock, lease row, or expiry callback is substituted.
		if _, err := c.ExecContext(ctx, `INSERT INTO checkpoints(key,generation,payload) VALUES(?,?,?)`, key, revKey(1), payload); err != nil {
			return err
		}
		entered = true
		started := time.Now()
		close(publicationHeld)
		if renew {
			select {
			case attempted := <-renewalStarted:
				if !attempted.Before(initialDeadline) {
					return fmt.Errorf("renewal did not start before the original lease deadline")
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// A timer makes crossing the real TTL intentional and independent of
		// machine speed or restore size. Do not wait on Lost: it must remain
		// open for a live renewer after the bug is fixed.
		timer := time.NewTimer(2 * ttl)
		defer timer.Stop()
		select {
		case <-timer.C:
			heldFor = time.Since(started)
		case <-ctx.Done():
			return ctx.Err()
		}
		current, err := readLease(ctx, c)
		if err != nil {
			return err
		}
		if current.epoch != original.epoch || current.token != original.token {
			return fmt.Errorf("lease identity changed while its native writer was held")
		}
		return nil
	})
	cancelRenew()
	if renew {
		renewal = <-renewalDone
	}
	if !entered || heldFor < 2*ttl {
		t.Fatalf("did not exercise a long native publication: entered=%v held=%s ttl=%s err=%v", entered, heldFor, ttl, err)
	}
	t.Logf("real SQLite publication held=%s ttl=%s renew=%v attempts=%d successes=%d renewalErr=%v publicationErr=%v epoch=%d unchanged=true", heldFor, ttl, renew, renewal.attempts, renewal.successes, renewal.err, err, original.epoch)

	checkpoint, readErr := observer.LoadCheckpoint(ctx, key)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if renew {
		if err != nil {
			t.Errorf("live holder's own long publication prevented renewal and fenced its commit: %v; want successful COMMIT", err)
		}
		if renewal.attempts == 0 || renewal.successes == 0 || renewal.err != nil {
			t.Errorf("live renewer did not survive its holder's publication: %+v", renewal)
		}
		select {
		case <-lease.Lost():
			t.Error("live renewing holder lost its lease while its own publication held the writer")
		default:
		}
		if checkpoint.Generation != 1 || string(checkpoint.Payload) != string(payload) {
			t.Errorf("independent handle did not observe committed checkpoint: %+v", checkpoint)
		}
		return
	}

	requireFenced(t, err)
	if checkpoint.Generation != 0 || len(checkpoint.Payload) != 0 {
		t.Fatalf("unrenewed holder published after real expiry: %+v", checkpoint)
	}
	select {
	case <-lease.Lost():
	default:
		t.Fatal("unrenewed holder did not signal Lost")
	}
	requireFenced(t, lease.Renew(ctx))
	next := getLease(t, ctx, observer, driver.LeaseTTL)
	if next.Epoch() <= lease.Epoch() {
		t.Fatal("expiry takeover did not advance the lease epoch")
	}
	if _, err := s.SaveCheckpoint(ctx, lease, key, 0, []byte("stale")); err == nil {
		t.Fatal("expired holder published after independent-handle takeover")
	} else {
		requireFenced(t, err)
	}
	if _, err := observer.SaveCheckpoint(ctx, next, key, 0, []byte("new-holder")); err != nil {
		t.Fatal(err)
	}
	t.Logf("without renewal, long publication rolled back; expired Renew and stale publication fenced; independent handle acquired epoch %d and published generation 1", next.Epoch())
}
