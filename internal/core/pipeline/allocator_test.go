package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func seedAllocator(t *testing.T, store driver.Store, lease driver.ActiveLease, value driver.OfflineAllocator) {
	t.Helper()
	ctx := context.Background()
	key, err := driver.AllocatorLock(value.Prefix)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	restore, ok := tx.(driver.OfflineRestoreTx)
	if !ok {
		t.Fatal("store does not expose offline restore")
	}
	const sourceID driver.StoreID = "allocator-seed-source"
	meta, err := restore.PrepareOfflineRestore(ctx, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	continuation, ok := tx.(driver.AllocatorContinuation)
	if !ok {
		t.Fatal("store does not expose allocator continuation")
	}
	if err = continuation.RestoreAllocators(ctx, []driver.OfflineAllocator{value}); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(struct {
		Source driver.StoreID
		Value  driver.OfflineAllocator
	}{sourceID, value})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	hash := hex.EncodeToString(digest[:])
	if _, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: model.PrincipalRef{Kind: "system", ID: "plug-converter"}, Method: "offline.restore", IdempotencyKey: "allocator-seed-" + hash, RequestHash: hash, Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}}, LockKeys: []driver.LockKey{key}}); err != nil {
		t.Fatal(err)
	}
}

func pipelineCreate(t *testing.T, w *Writer, key string, n int) model.Commit {
	t.Helper()
	request := meta(n)
	request.Project = "demo"
	commit, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create(key)}}, request)
	if err != nil {
		t.Fatal(err)
	}
	return commit
}

func TestRestoredAllocatorFeedsOrdinaryCreate(t *testing.T) {
	h := setup(t)
	seedAllocator(t, h.store, h.lease, driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8, Children: map[string]uint64{"demo-7": 4}})
	commit := pipelineCreate(t, h.writer(), "next", 90)
	if got := commit.Results[0].Ref.ID; got != "demo-9" {
		t.Fatalf("restored sequential allocator produced %q", got)
	}
	reader := h.read(t)
	value, err := reader.(driver.AllocatorReader).LoadAllocator(context.Background(), "demo")
	if err != nil || value.LastID != 9 || value.Children["demo-7"] != 4 {
		t.Fatalf("allocator after create=%+v err=%v", value, err)
	}
}

func TestAllocatorRejectsDuplicateWithoutPublication(t *testing.T) {
	s := memory.New()
	t.Cleanup(func() { _ = s.Close() })
	l, err := s.Lease(context.Background(), "allocator-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := s.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	key, _ := driver.AllocatorLock("demo")
	tx, err := s.Write(context.Background(), l)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(context.Background(), driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(context.Background()); err != nil {
		t.Fatal(err)
	}
	continuation := tx.(driver.AllocatorContinuation)
	value := driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8}
	if err = continuation.RestoreAllocators(context.Background(), []driver.OfflineAllocator{value, value}); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("duplicate restore error=%v", err)
	}
	if got, err := s.Meta(context.Background()); err != nil || !got.Head.IsZero() {
		t.Fatalf("duplicate restore published head=%+v err=%v", got, err)
	}
	if _, err = continuation.LoadAllocator(context.Background(), "demo"); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("duplicate restore staged state=%v", err)
	}
	_ = meta
}

func TestAllocatorRejectsExistingPrefixWithoutWrite(t *testing.T) {
	h := setup(t)
	seedAllocator(t, h.store, h.lease, driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8})
	before, err := h.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	key, err := driver.AllocatorLock("demo")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.store.Write(context.Background(), h.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(context.Background(), driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = tx.(driver.AllocatorContinuation).RestoreAllocators(context.Background(), []driver.OfflineAllocator{{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 99}})
	if !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("existing prefix error=%v", err)
	}
	after, err := h.store.Meta(context.Background())
	if err != nil || after.Head.String() != before.Head.String() {
		t.Fatalf("existing prefix changed head before=%+v after=%+v err=%v", before, after, err)
	}
	loaded, err := tx.(driver.AllocatorReader).LoadAllocator(context.Background(), "demo")
	if err != nil || loaded.LastID != 8 {
		t.Fatalf("existing prefix changed staged state=%+v err=%v", loaded, err)
	}
}

func TestHashAllocatorRefusesCreateWithoutFallback(t *testing.T) {
	h := setup(t)
	seedAllocator(t, h.store, h.lease, driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorHash, HashLength: 8})
	fallbackUsed := false
	writer := h.writer(WithIDGenerator(func() (string, error) {
		fallbackUsed = true
		return "should-not-be-used", nil
	}))
	request := meta(91)
	request.Project = "demo"
	_, err := writer.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("hash-refusal")}}, request)
	if !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("hash allocator error=%v", err)
	}
	if fallbackUsed {
		t.Fatal("hash allocator fell back to a random ID")
	}
	reader := h.read(t)
	if rows, err := reader.Query(context.Background(), driver.Query{Filter: map[string]any{"project": "demo"}, Limit: 10}); err != nil || len(rows.Items) != 0 {
		t.Fatalf("hash refusal wrote records=%+v err=%v", rows.Items, err)
	}
}

func TestConcurrentRestoredCreatesAdvanceDistinctIDs(t *testing.T) {
	h := setup(t)
	seedAllocator(t, h.store, h.lease, driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8})
	w1, w2 := h.writer(), h.writer()
	var wg sync.WaitGroup
	ids := make(chan string, 2)
	errCh := make(chan error, 2)
	for i, w := range []*Writer{w1, w2} {
		wg.Add(1)
		go func(i int, w *Writer) {
			defer wg.Done()
			request := meta(100 + i)
			request.Project = "demo"
			commit, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create(fmt.Sprintf("concurrent-%d", i))}}, request)
			if err != nil {
				errCh <- err
				return
			}
			ids <- commit.Results[0].Ref.ID
		}(i, w)
	}
	wg.Wait()
	close(ids)
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 2 || !seen["demo-9"] || !seen["demo-10"] {
		t.Fatalf("concurrent IDs=%v", seen)
	}
}

func TestSQLiteAllocatorSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "allocator.db")
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "sqlite-allocator", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	seedAllocator(t, store, lease, driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8})
	writer := New(store, lease, WithOntology(testOntology{}), WithIDGenerator(func() (string, error) { return "should-not-be-used", nil }))
	request := meta(110)
	request.Project = "demo"
	commit, err := writer.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{create("sqlite-next")}}, request)
	if err != nil || commit.Results[0].Ref.ID != "demo-9" {
		t.Fatalf("first sqlite create=%+v err=%v", commit, err)
	}
	if err = lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedLease, err := reopened.Lease(ctx, "sqlite-allocator-reopened", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedLease.Release(ctx)
	writer = New(reopened, reopenedLease, WithOntology(testOntology{}), WithIDGenerator(func() (string, error) { return "should-not-be-used", nil }))
	request = meta(111)
	request.Project = "demo"
	commit, err = writer.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{create("sqlite-next-2")}}, request)
	if err != nil || commit.Results[0].Ref.ID != "demo-10" {
		t.Fatalf("reopened sqlite create=%+v err=%v", commit, err)
	}
}

func TestAllocatorReplacementSeedIdentityAndInitializedRefusal(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var store driver.Store = memory.New()
			if kind == "sqlite" {
				store.Close()
				var err error
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "seed.db")})
				if err != nil {
					t.Fatal(err)
				}
			}
			defer store.Close()
			lease, err := store.Lease(ctx, "allocator-replacement", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			seed := driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8}
			seedAllocator(t, store, lease, seed)
			after, err := store.Meta(ctx)
			if err != nil || after.StoreID != "allocator-seed-source" || after.Incarnation == before.Incarnation || after.Head.String() != "1" || after.HistoryFloor.String() != "1" {
				t.Fatalf("allocator replacement identity=%+v %v", after, err)
			}
			payload, err := json.Marshal(struct {
				Source driver.StoreID
				Value  driver.OfflineAllocator
			}{"allocator-seed-source", seed})
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(payload)
			hash := hex.EncodeToString(digest[:])
			reader, err := store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := reader.IdempotencyByKey(ctx, "system:plug-converter", "", "offline.restore", "allocator-seed-"+hash)
			reader.Close()
			if err != nil || receipt == nil || receipt.RequestHash != hash || receipt.ResponsePending || receipt.Changeset.Incarnation != after.Incarnation || receipt.Changeset.Kind != model.ChangesetRestore {
				t.Fatalf("allocator replacement receipt=%+v %v", receipt, err)
			}
			key, err := driver.AllocatorLock("other")
			if err != nil {
				t.Fatal(err)
			}
			tx, err := store.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			if _, err := tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			_, err = tx.(driver.OfflineRestoreTx).PrepareOfflineRestore(ctx, "new-source")
			if !errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
				t.Fatalf("initialized identity-preserving injection=%v", err)
			}
			if got, err := tx.(driver.AllocatorReader).LoadAllocator(ctx, "other"); !errors.Is(err, driver.ErrNotFound) {
				t.Fatalf("initialized refusal staged allocator=%+v %v", got, err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := store.Meta(ctx)
			if err != nil || got != after {
				t.Fatalf("initialized refusal changed identity/history: %+v %v", got, err)
			}
		})
	}
}
