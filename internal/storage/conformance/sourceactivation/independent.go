package sourceactivation

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func ParentOnlyUpgrade(t *testing.T, store driver.Store, lease driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID("pl-parent-only", model.Natural64(0))
	bead, err := driver.NewBeadSentinel(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock, {Kind: driver.LockRecord, Ref: &ref}, {Kind: driver.LockSentinel, Sentinel: bead}}
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
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.(driver.HierarchySourceWriter).PutHierarchyRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{"id":"pl-parent-only","type":"issue","fields":{"status":"todo"}}`)}, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking, bead}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "parent-only-upgrade"}, LockKeys: keys, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}}); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.BuildSourceIndexes(ctx, store, lease); err != nil {
		t.Fatal(err)
	}
	zero := model.Natural64(0)
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if rows, more, err := r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, nil, 0); err != nil || len(rows) != 0 || more {
		t.Fatalf("link backfill discarded known Parent history: %v", err)
	}
	if rows, more, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, nil, nil, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("link backfill invented revision-zero coverage: %v", err)
	}
	current, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	if rows, more, err := current.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, nil, nil, nil, 0); err != nil || len(rows) != 0 || more {
		t.Fatalf("complete-empty outgoing declarations unavailable after backfill: %v", err)
	}
}

// GapRefuses is called after simulating an older publisher advancing Head
// without its source markers. Startup must not bless that genuinely unknown gap.
func GapRefuses(t *testing.T, store driver.Store, lease driver.ActiveLease) {
	t.Helper()
	calls := 0
	err := store.(driver.GraphSourceIndexBuilder).BuildGraphSourceIndexes(context.Background(), lease, func(driver.Record) (driver.GraphSourceMetadata, error) {
		calls++
		return driver.GraphSourceMetadata{}, nil
	})
	if !errors.Is(err, driver.ErrUnsupported) || calls != 0 {
		t.Fatalf("startup bridged stale coverage: decoded=%d err=%v", calls, err)
	}
}
