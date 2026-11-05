package memory

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *Store) restoreEmpty() error {
	v := s.state
	if !v.meta.Head.IsZero() || !v.meta.HistoryFloor.IsZero() || len(v.records)+len(v.kinds)+len(v.versions)+len(v.keys)+len(v.keyHistory)+len(v.blobs)+len(v.log)+len(v.replays)+len(v.allocators)+len(v.parentCounters)+len(s.checkpoints) != 0 || v.allocatorHistory != nil {
		return driver.ErrRestoreTargetNotEmpty
	}
	return nil
}

func (t *transaction) PrepareOfflineRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	return t.prepareRestore(ctx, source)
}

func (t *transaction) prepareRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	s := t.store
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	if !t.hasHead || s.head != t || t.restore != nil || len(t.records)+len(t.derived)+len(t.bumps)+len(t.blobs)+len(t.allocators)+len(t.parentCounters)+len(t.checkpointWrites)+len(t.checkpointGuards) != 0 {
		return driver.Meta{}, fmt.Errorf("offline restore requires Head before staging")
	}
	if err := s.restoreEmpty(); err != nil {
		return driver.Meta{}, err
	}
	m, err := driver.NewOfflineRestoreMeta(s.state.meta, source)
	if err != nil {
		return driver.Meta{}, err
	}
	t.restore = &m
	return m, nil
}

func (t *transaction) PrepareNativeRestore(ctx context.Context, profile driver.NativeRestoreProfile) (driver.Meta, error) {
	bound, err := profile.Detached()
	if err != nil {
		return driver.Meta{}, err
	}
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
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
	bound, err := profile.DetachedCoreOnly()
	if err != nil {
		return driver.Meta{}, err
	}
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, err := t.prepareRestore(ctx, bound.SourceStoreID)
	if err == nil {
		t.importRestore = &bound
	}
	return meta, err
}

var _ driver.RestoreImportTx = (*transaction)(nil)
