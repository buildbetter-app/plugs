package postgres

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Head serializes preparation and publication against all other writers. Lock
// placeholders and the active lease are not user data; receipts/checkpoints are.
func (t *transaction) restoreEmpty(ctx context.Context) error {
	m, err := readMeta(ctx, t.sql, "")
	if err != nil {
		return err
	}
	if !m.Head.IsZero() || !m.HistoryFloor.IsZero() {
		return driver.ErrRestoreTargetNotEmpty
	}
	var occupied bool
	err = t.sql.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM plug_postgres.records) OR EXISTS(SELECT 1 FROM plug_postgres.versions) OR EXISTS(SELECT 1 FROM plug_postgres.states) OR EXISTS(SELECT 1 FROM plug_postgres.blobs) OR EXISTS(SELECT 1 FROM plug_postgres.changesets) OR EXISTS(SELECT 1 FROM plug_postgres.receipts) OR EXISTS(SELECT 1 FROM plug_postgres.checkpoints) OR EXISTS(SELECT 1 FROM plug_postgres.allocator_states)`).Scan(&occupied)
	if err == nil && occupied {
		return driver.ErrRestoreTargetNotEmpty
	}
	return err
}
func (t *transaction) prepareRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	if !t.hasHead || t.restore != nil || len(t.records)+len(t.derived)+len(t.bumps)+len(t.blobs)+len(t.allocators)+len(t.parentCounters) != 0 {
		return driver.Meta{}, fmt.Errorf("offline restore requires Head before staging")
	}
	if err := t.restoreEmpty(ctx); err != nil {
		return driver.Meta{}, err
	}
	m, err := driver.NewOfflineRestoreMeta(t.meta, source)
	if err == nil {
		t.restore = &m
	}
	return m, err
}
func (t *transaction) PrepareOfflineRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.prepareRestore(ctx, source)
}
func (t *transaction) PrepareRestoreImport(ctx context.Context, p driver.RestoreImportProfile) (driver.Meta, error) {
	bound, err := p.DetachedCoreOnly()
	if err != nil {
		return driver.Meta{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	m, err := t.prepareRestore(ctx, bound.SourceStoreID)
	if err == nil {
		t.importRestore = &bound
	}
	return m, err
}
func (t *transaction) PrepareNativeRestore(ctx context.Context, p driver.NativeRestoreProfile) (driver.Meta, error) {
	bound, err := p.Detached()
	if err != nil {
		return driver.Meta{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.nativeAllocatorLocks(bound.AllocatorBinding); err != nil {
		return driver.Meta{}, err
	}
	m, err := t.prepareRestore(ctx, bound.StoreID)
	if err == nil {
		t.nativeRestore = &bound
	}
	return m, err
}

var _ driver.OfflineRestoreTx = (*transaction)(nil)
var _ driver.RestoreImportTx = (*transaction)(nil)
var _ driver.NativeRestoreTx = (*transaction)(nil)
