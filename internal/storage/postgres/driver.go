package postgres

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type Driver struct{}

func (Driver) Info() driver.Info {
	return driver.Info{Name: "postgres", Version: "1", Capabilities: []string{"transactions", "history", "leases", "checkpoints", "allocator_continuation", "idempotency", "response_finalizer", "noop_receipts"}}
}
func (Driver) Open(ctx context.Context, cfg driver.Config) (driver.Store, error) {
	return Open(ctx, cfg)
}

var _ driver.Driver = Driver{}
var _ driver.Store = (*Store)(nil)
