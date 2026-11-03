package sourceactivation

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeOntologyFirst restores a real ontology-only current image, then adds
// its first bead. Startup must leave a fresh restore target unoccupied, and
// empty graph certification must respect the restored incarnation's floor.
func NativeOntologyFirst(t *testing.T, store driver.Store, lease driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	if err := pipeline.BuildSourceIndexes(ctx, store, lease); err != nil {
		t.Fatal(err)
	}
	profile := driver.NativeRestoreProfile{StoreID: "ontology-only-native", Principal: model.PrincipalRef{Kind: "user", ID: "operator"}, Method: "admin.restore.finish", IdempotencyKey: "native-ontology-first", RequestHash: strings.Repeat("a", 64)}
	snapshot, err := driver.NewAllocatorSnapshot(driver.Meta{StoreID: profile.StoreID, Incarnation: "source-ontology-only"}, model.Natural64(1), model.Natural64(0), driver.AllocatorInventory{})
	if err != nil {
		t.Fatal(err)
	}
	profile.AllocatorBinding, err = snapshot.Binding()
	if err != nil {
		t.Fatal(err)
	}
	fence, err := driver.NativeAllocatorRestoreLock()
	if err != nil {
		t.Fatal(err)
	}
	ontology := model.RefByID("ontology/global", model.Natural64(0))
	keys := []driver.LockKey{fence, driver.GraphBlockingSentinelLock, {Kind: driver.LockRecord, Ref: &ontology}}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks := make(driver.LockSet, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	meta, err := tx.(driver.NativeRestoreTx).PrepareNativeRestore(ctx, profile)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.(driver.NativeAllocatorRestoreTx).RestoreNativeAllocators(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err = tx.Put(ctx, driver.Record{Ref: ontology, Kind: "ontology", Body: []byte(`{"version":1}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: profile.Principal, Method: profile.Method, IdempotencyKey: profile.IdempotencyKey, RequestHash: profile.RequestHash, LockKeys: keys, Changes: []model.ChangeRecord{{Ref: ontology, Op: model.OpCreate}}}); err != nil {
		t.Fatal(err)
	}
	bead := model.RefByID("pl-native-first", model.Natural64(0))
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: bead, Kind: "bead", Op: model.OpCreate, Body: []byte(`{"id":"pl-native-first","type":"issue","fields":{"status":"todo"}}`), Graph: &conformance.FixtureGraphSource{}}}); err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if rows, more, err := r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, nil, 0); err != nil || more || len(rows) != 0 {
		t.Fatalf("native ontology-first Parent coverage unavailable: %v", err)
	}
	if rows, more, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, nil, nil, nil, 0); err != nil || more || len(rows) != 0 {
		t.Fatalf("native ontology-first Links coverage unavailable: %v", err)
	}
	zero := model.Natural64(0)
	if r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero}); err == nil {
		_ = r.Close()
		t.Fatal("native ontology-only activation invented revision-zero history")
	} else {
		var truncated driver.ErrTruncated
		if !errors.As(err, &truncated) {
			t.Fatal(err)
		}
	}
}
