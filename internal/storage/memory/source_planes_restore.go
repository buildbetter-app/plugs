package memory

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The caller already owns the complete lock set and Head. Core-only and
// offline-history preparation deliberately keep their separate refusals.
func (t *transaction) PrepareSourcePlaneImport(ctx context.Context, profile driver.RestoreImportProfile) (driver.Meta, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	if len(t.checkpointWrites)+len(t.checkpointGuards) != 0 || t.offlineHistory != nil {
		return driver.Meta{}, driver.ErrUnsupported
	}
	bound, err := driver.PrepareSourcePlaneImportProfile(profile, t.store.state.meta, driver.CheckpointLifetimeProcess)
	if err != nil {
		return driver.Meta{}, err
	}
	meta, err := t.prepareRestore(ctx, bound.SourceStoreID)
	if err != nil {
		return driver.Meta{}, err
	}
	t.importRestore = &bound
	t.sourcePlaneImport = driver.NewSourcePlaneImportPublication(bound, meta)
	return meta, nil
}

func (t *transaction) BindSourcePlaneImport(ctx context.Context, binding driver.SourcePlaneCommitBindingV1) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	return t.sourcePlaneImport.Bind(binding)
}

var _ driver.SourcePlaneRestoreTx = (*transaction)(nil)
