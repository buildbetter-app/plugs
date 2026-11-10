package converterroundtrip

import (
	"context"
	"errors"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// HoldRestoreLease keeps an owned lease active through restore and publication.
// Loss or renewal failure cancels the operation. Release stops and drains the
// renewal loop before releasing the lease and returns renewal/release errors.
// Callers must call it on every exit and propagate cleanup errors.
func HoldRestoreLease(ctx context.Context, store driver.Store, owner string) (context.Context, driver.ActiveLease, func() error, error) {
	lease, err := store.Lease(ctx, owner, driver.LeaseTTL)
	if err != nil {
		return nil, nil, nil, err
	}
	op, cancel := context.WithCancelCause(ctx)
	var renewalErr error
	done := make(chan struct{})
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
	return op, lease, func() error {
		cancel(nil)
		<-done
		return errors.Join(renewalErr, lease.Release(context.Background()))
	}, nil
}
