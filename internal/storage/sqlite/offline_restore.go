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
 OR EXISTS(SELECT 1 FROM replays) OR EXISTS(SELECT 1 FROM checkpoints)
 OR EXISTS(SELECT 1 FROM allocator_states)`).Scan(&occupied)
	if err == nil && occupied {
		return driver.ErrRestoreTargetNotEmpty
	}
	return err
}

func (t *transaction) PrepareOfflineRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.prepareRestore(ctx, source)
}

func (t *transaction) prepareRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	if !t.hasHead || t.restore != nil || len(t.records)+len(t.derived)+len(t.bumps)+len(t.blobs)+len(t.allocators)+len(t.parentCounters) != 0 {
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
	t.invalidateReadyOrderPublication()
	return m, nil
}

func (t *transaction) PrepareNativeRestore(ctx context.Context, profile driver.NativeRestoreProfile) (driver.Meta, error) {
	bound, err := profile.Detached()
	if err != nil {
		return driver.Meta{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.nativeAllocatorLocks(bound.AllocatorBinding); err != nil {
		return driver.Meta{}, err
	}
	meta, err := t.prepareRestore(ctx, bound.StoreID)
	if err == nil {
		t.nativeRestore = &bound
	}
	return meta, err
}

var _ driver.OfflineRestoreTx = (*transaction)(nil)
var _ driver.NativeRestoreTx = (*transaction)(nil)

func (t *transaction) PrepareRestoreImport(ctx context.Context, profile driver.RestoreImportProfile) (driver.Meta, error) {
	bound, err := profile.Detached()
	if err != nil {
		return driver.Meta{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	meta, err := t.prepareRestore(ctx, bound.SourceStoreID)
	if err == nil {
		t.importRestore = &bound
	}
	return meta, err
}

var _ driver.RestoreImportTx = (*transaction)(nil)
