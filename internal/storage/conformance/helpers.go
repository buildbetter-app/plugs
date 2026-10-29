package conformance

import (
	"context"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func ref(id string) driver.Ref { return model.RefByID(id, model.Natural64(0)) }
func recordLock(id string) driver.LockKey {
	value := ref(id)
	return driver.LockKey{Kind: driver.LockRecord, Ref: &value}
}
func update(keys ...driver.LockKey) []driver.LockRequest {
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	result := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		result[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	return result
}
func lockKeys(requests []driver.LockRequest) []driver.LockKey {
	keys := make([]driver.LockKey, len(requests))
	for i, r := range requests {
		keys[i] = r.Key
	}
	return keys
}
func header(meta driver.Meta, requests []driver.LockRequest, changes ...model.ChangeRecord) driver.ChangesetHeader {
	return driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "conformance"}, LockKeys: lockKeys(requests), Changes: changes}
}
func begin(t *testing.T, ctx context.Context, s driver.Store, lease driver.ActiveLease, requests []driver.LockRequest) driver.WriteTx {
	t.Helper()
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	if _, err = tx.Lock(ctx, requests); err != nil {
		t.Fatal(err)
	}
	return tx
}
func read(t *testing.T, ctx context.Context, s driver.Store, meta driver.Meta, at *model.Revision) driver.Reader {
	t.Helper()
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}
func put(t *testing.T, ctx context.Context, s driver.Store, lease driver.ActiveLease, meta driver.Meta, id, body string) model.Changeset {
	t.Helper()
	requests := update(recordLock(id))
	tx := begin(t, ctx, s, lease, requests)
	states, err := tx.Observe(ctx, lockKeys(requests))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Put(ctx, driver.Record{Ref: ref(id), Kind: "test", Body: []byte(body)}); err != nil {
		t.Fatal(err)
	}
	entry, err := tx.Commit(ctx, header(meta, requests, model.ChangeRecord{Ref: ref(id), Op: model.OpPatch, FromRevision: states[0].Revision, ChangedPaths: []string{"/value"}}))
	if err != nil {
		t.Fatal(err)
	}
	return entry
}
