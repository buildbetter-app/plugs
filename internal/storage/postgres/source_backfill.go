package postgres

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

func (s *Store) BuildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder, projects ...driver.GraphSourceProjection) error {
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
	build, selectionErr := t.prepareSelectionBackfill(ctx, projects)
	if selectionErr != nil && !sourceindex.IsSelectionBackfillUnavailable(selectionErr) {
		return selectionErr
	}
	hooks := []sourceindex.BackfillSelection{}
	if build != nil {
		hooks = append(hooks, build.hooks())
	}
	backfillErr := sourceindex.Postgres.Backfill(ctx, t.sql, t.meta, decode, hooks...)
	// Only the shared typed result proves the graph lifecycle completed despite
	// optional selection refusal. An arbitrary graph error must still roll back.
	if backfillErr != nil && !sourceindex.IsSelectionBackfillRefused(backfillErr) {
		return backfillErr
	}
	if err = t.publish(ctx, ""); err != nil {
		return err
	}
	if selectionErr != nil {
		return sourceindex.NewSelectionBackfillRefused(selectionErr)
	}
	return backfillErr
}

var _ driver.GraphSourceIndexBuilder = (*Store)(nil)
