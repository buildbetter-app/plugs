package sqlite

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The caller holds Head. Commit repeats this over its BEGIN IMMEDIATE view,
// including receipts and checkpoints that do not advance the graph head.
func restoreEmpty(ctx context.Context, q queryer) error {
	m, err := readMeta(ctx, q)
	if err != nil {
		return err
	}
	if !m.Head.IsZero() || !m.HistoryFloor.IsZero() {
		return driver.ErrRestoreTargetNotEmpty
	}
	var occupied bool
	err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM objects) OR EXISTS(SELECT 1 FROM records)
 OR EXISTS(SELECT 1 FROM versions) OR EXISTS(SELECT 1 FROM key_history)
 OR EXISTS(SELECT 1 FROM blobs) OR EXISTS(SELECT 1 FROM changesets)
 OR EXISTS(SELECT 1 FROM replays) OR EXISTS(SELECT 1 FROM checkpoints)`).Scan(&occupied)
	if err == nil && occupied {
		return driver.ErrRestoreTargetNotEmpty
	}
	return err
}

func (t *transaction) PrepareOfflineRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	if !t.hasHead || t.restore != nil || len(t.records)+len(t.derived)+len(t.bumps)+len(t.blobs) != 0 {
		return driver.Meta{}, fmt.Errorf("offline restore requires Head before staging")
	}
	if err := restoreEmpty(ctx, t.tx); err != nil {
		return driver.Meta{}, err
	}
	m, err := driver.NewOfflineRestoreMeta(t.meta, source)
	if err != nil {
		return driver.Meta{}, err
	}
	t.restore = &m
	return m, nil
}

var _ driver.OfflineRestoreTx = (*transaction)(nil)
