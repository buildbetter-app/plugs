package memory

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (t *transaction) snapshotReader(ctx context.Context) (*reader, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return nil, err
	}
	view := t.base.fork()
	for id, record := range t.records {
		if record == nil {
			delete(view.records, id)
		} else {
			view.records[id] = copyRecord(*record)
			view.kinds[id] = record.Kind
		}
	}
	for hash, data := range t.blobs {
		view.blobs[hash] = blob{bytes: data}
	}
	for id, value := range t.derived {
		view.keys[id] = value
	}
	return newReader(view, driver.View{Incarnation: view.meta.Incarnation}, s.now)
}

func (t *transaction) Snapshot() driver.View {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	at := t.snapshotAt
	return driver.View{At: &at, Branch: model.Natural64(0), Incarnation: t.incarnation}
}

func (t *transaction) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	r, err := t.snapshotReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.Get(ctx, refs)
}
func (t *transaction) Blob(ctx context.Context, hash driver.Hash) ([]byte, error) {
	r, err := t.snapshotReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.Blob(ctx, hash)
}
func (t *transaction) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	r, err := t.snapshotReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.Observe(ctx, keys)
}
func (t *transaction) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r, err := t.snapshotReader(ctx)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	defer r.Close()
	return r.Query(ctx, q)
}
func (t *transaction) Versions(ctx context.Context, ref driver.Ref, req driver.PageReq) (driver.Page[driver.Version], error) {
	r, err := t.snapshotReader(ctx)
	if err != nil {
		return driver.Page[driver.Version]{}, err
	}
	defer r.Close()
	return r.Versions(ctx, ref, req)
}
func (t *transaction) Changesets(ctx context.Context, cursor *driver.Cursor, limit int) ([]model.Changeset, error) {
	r, err := t.snapshotReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.Changesets(ctx, cursor, limit)
}
func (t *transaction) ChangesetByKey(ctx context.Context, principal, agent, method, key string) (*model.Changeset, error) {
	r, err := t.snapshotReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.ChangesetByKey(ctx, principal, agent, method, key)
}
func (t *transaction) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	r, err := t.snapshotReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.IdempotencyByKey(ctx, principal, agent, method, key)
}

var _ driver.WriteTx = (*transaction)(nil)
