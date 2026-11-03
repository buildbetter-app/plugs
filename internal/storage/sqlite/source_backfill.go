package sqlite

import (
	"context"
	"database/sql"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

func (s *Store) BuildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder) error {
	value, err := s.Write(ctx, lease)
	if err != nil {
		return err
	}
	defer value.Close()
	if _, err = value.Lock(ctx, driver.LockSet{{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate}}); err != nil {
		return err
	}
	if _, err = value.Head(ctx); err != nil {
		return err
	}
	t := value.(*transaction)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err = t.check(ctx); err != nil {
		return err
	}
	return s.immediate(ctx, t.lease, func(c *sql.Conn) error {
		meta, err := readMeta(ctx, c)
		if err != nil {
			return err
		}
		return sourceindex.SQLite.Backfill(ctx, c, meta, decode)
	})
}

var _ driver.GraphSourceIndexBuilder = (*Store)(nil)
