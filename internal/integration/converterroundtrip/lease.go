package converterroundtrip

import (
	"context"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// HoldRestoreLease keeps an owned lease active through restore and publication.
// Loss or renewal failure cancels the operation. Release stops and drains the
// renewal loop before releasing the lease; callers must call it on every exit.
func HoldRestoreLease(ctx context.Context, store driver.Store, owner string) (context.Context, driver.ActiveLease, func() error, error) {
	lease, err := store.Lease(ctx, owner, driver.LeaseTTL)
	if err != nil {
		return nil, nil, nil, err
	}
	op, cancel := context.WithCancel(ctx)
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
				cancel()
				return
			case <-ticker.C:
				if lease.Renew(op) != nil {
					cancel()
					return
				}
			}
		}
	}()
	return op, lease, func() error { cancel(); <-done; return lease.Release(context.Background()) }, nil
}
