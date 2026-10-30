package memory

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *Store) restoreEmpty() error {
	v := s.state
	if !v.meta.Head.IsZero() || !v.meta.HistoryFloor.IsZero() || len(v.records)+len(v.kinds)+len(v.versions)+len(v.keys)+len(v.keyHistory)+len(v.blobs)+len(v.log)+len(v.replays)+len(s.checkpoints) != 0 {
		return driver.ErrRestoreTargetNotEmpty
	}
	return nil
}

func (t *transaction) PrepareOfflineRestore(ctx context.Context, source driver.StoreID) (driver.Meta, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	if !t.hasHead || s.head != t || t.restore != nil || len(t.records)+len(t.derived)+len(t.bumps)+len(t.blobs) != 0 {
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

var _ driver.OfflineRestoreTx = (*transaction)(nil)
