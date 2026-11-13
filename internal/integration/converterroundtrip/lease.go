package converterroundtrip

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// startRestoreLeaseRenewal is the one renewal loop used by converter tests and
// by the owned Restore entry point. Tests that call a WithLease entry point
// still own the raw driver lease, so they use the returned stop function to
// drain this loop before releasing or reopening the store.
func startRestoreLeaseRenewal(ctx context.Context, lease driver.ActiveLease) (context.Context, func() error) {
	op, cancel := context.WithCancelCause(ctx)
	var renewalErr error
	done := make(chan struct{})
	var stopOnce sync.Once
	stop := func() error {
		stopOnce.Do(func() {
			cancel(nil)
			<-done
		})
		return renewalErr
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(driver.LeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-op.Done():
				return
			case <-lease.Lost():
				if op.Err() == nil {
					renewalErr = driver.ErrFenced{ExpectedEpoch: lease.Epoch(), ObservedEpoch: lease.Epoch()}
					cancel(renewalErr)
				}
				return
			case <-ticker.C:
				if err := lease.Renew(op); err != nil {
					if op.Err() == nil {
						renewalErr = err
						cancel(err)
					}
					return
				}
			}
		}
	}()
	return op, stop
}

// HoldRestoreLease keeps an owned lease active through restore and publication.
// Loss or renewal failure cancels the operation. Release stops and drains the
// renewal loop before releasing the lease and returns renewal/release errors.
// Callers must call it on every exit and propagate cleanup errors.
func HoldRestoreLease(ctx context.Context, store driver.Store, owner string) (context.Context, driver.ActiveLease, func() error, error) {
	lease, err := store.Lease(ctx, owner, driver.LeaseTTL)
	if err != nil {
		return nil, nil, nil, err
	}
	op, stop := startRestoreLeaseRenewal(ctx, lease)
	return op, lease, func() error {
		return errors.Join(stop(), lease.Release(context.Background()))
	}, nil
}
