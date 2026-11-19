package sourceactivation

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// AllocatorCache runs the same boundary census and content hash oracle against
// every real engine, including actual restore into the freshly opened target.
func AllocatorCache(t *testing.T, store driver.Store, lease driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	prefixes := []string{"demo", "demo-a", "demo.x", "demo-"}
	var keys []driver.LockKey
	var profiles []driver.OfflineAllocator
	for _, prefix := range prefixes {
		key, err := driver.AllocatorLock(prefix)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
		profiles = append(profiles, driver.NewHashAllocator(prefix, 3, 8, 0.25))
	}
	// EncodeLockKey is the driver's canonical ordering for these allocator facts.
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	var locks driver.LockSet
	for _, key := range keys {
		locks = append(locks, driver.LockRequest{Key: key, Mode: driver.LockUpdate})
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
	target, err := tx.(driver.OfflineRestoreTx).PrepareOfflineRestore(ctx, "population-boundary-source")
	if err != nil {
		t.Fatal("fresh target restore refused", err)
	}
	if err := tx.(driver.AllocatorContinuation).RestoreAllocators(ctx, profiles); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: target.Incarnation, Kind: model.ChangesetRestore, Principal: model.PrincipalRef{Kind: "system", ID: "plug-converter"}, Method: "offline.restore", IdempotencyKey: "population-boundary-restore", RequestHash: strings.Repeat("0", 64), Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}}, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(1700000000, 0).UTC()
	principal := model.PrincipalRef{Kind: "user", ID: "population"}
	creator, err := driver.PrincipalKey(principal)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"demo-a", "demo-a-b", "demo-a.1", "demo-", "demo--tail", "demo.x-tail", "a/b-c", " demo-tail", "demo/a-b"}
	changes := make([]conformance.FixtureChange, 0, len(ids))
	occupied := map[string]bool{}
	beads := map[string]bool{}
	for _, id := range ids {
		kind := "bead"
		if strings.ContainsRune(id, '/') || strings.TrimSpace(id) != id {
			kind = "test"
		} else {
			beads[id] = true
		}
		occupied[id] = true
		body, err := json.Marshal(pipeline.Document{ID: id, Project: "other", Type: "issue", Fields: map[string]any{"title": "boundary"}})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: model.Ref{ID: id}, Kind: kind, Op: model.OpCreate, Body: body})
	}
	collision, err := driver.HashCandidate("demo", driver.HashInput{Title: "boundary-0", Creator: creator, Timestamp: stamp}, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	occupied[collision] = true
	changes = append(changes, conformance.FixtureChange{Ref: model.Ref{ID: collision}, Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)})
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	w := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithClock(func() time.Time { return stamp }))
	for i, prefix := range []string{"demo", "demo", "demo-a", "demo-a", "demo.x", "demo.x", "demo-", "demo-"} {
		var count uint64
		for id := range beads {
			tail, ok := strings.CutPrefix(id, prefix+"-")
			if ok && tail != "" && !strings.ContainsRune(tail, '.') {
				count++
			}
		}
		title := fmt.Sprintf("boundary-%d", i)
		input := driver.HashInput{Title: title, Creator: creator, Timestamp: stamp}
		a := driver.NewHashAllocator(prefix, 3, 8, 0.25)
		var candidates []string
		expected, err := a.MintHashID(input, count, func(id string) (bool, error) { candidates = append(candidates, id); return occupied[id], nil })
		if err != nil {
			t.Fatal(err)
		}
		work := &allocatorCacheWork{counts: map[driver.ReadWorkKind]int64{}}
		observed := driver.WithReadWorkObserver(ctx, work)
		result, err := w.Apply(observed, principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Key: "new", Type: "issue", Fields: map[string]any{"title": title}}}}}, model.WriteMeta{Method: "tx.apply", Project: prefix, IdempotencyKey: fmt.Sprintf("%032x", i+1)})
		if err != nil {
			t.Fatal(err)
		}
		got := result.Results[0].Ref.ID
		if got != expected {
			t.Fatalf("prefix=%s got=%s census=%s count=%d candidates=%v", prefix, got, expected, count, candidates)
		}
		if i%2 == 1 && work.counts[driver.ReadAllocatorDecodes] != 0 {
			t.Fatalf("warm own commit census=%v", work.counts)
		}
		beads[got] = true
		occupied[got] = true
		t.Logf("prefix=%s census_population=%d candidates=%v allocated=%s allocator_decodes=%d", prefix, count, candidates, got, work.counts[driver.ReadAllocatorDecodes])
	}
}

type allocatorCacheWork struct {
	mu     sync.Mutex
	counts map[driver.ReadWorkKind]int64
}

func (w *allocatorCacheWork) ObserveReadWork(k driver.ReadWorkKind, n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.counts[k] += n
}
