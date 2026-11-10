package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestHashAllocatorMintsCollisionFreeBatchAndReplays(t *testing.T) {
	h := setup(t)
	seed := driver.NewHashAllocator("demo", 3, 8, 0.25)
	seedAllocator(t, h.store, h.lease, seed)
	w := h.writer(WithIDGenerator(func() (string, error) {
		return "fallback-must-not-run", nil
	}))
	request := meta(92)
	request.Project = "demo"
	cs := model.ChangeSet{Ops: []model.Op{create("first"), create("second")}}
	commit, err := w.Apply(context.Background(), by, cs, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(commit.Results) != 2 || commit.Results[0].Ref.ID == commit.Results[1].Ref.ID {
		t.Fatalf("hash batch IDs=%+v", commit.Results)
	}
	for _, result := range commit.Results {
		if !strings.HasPrefix(result.Ref.ID, "demo-") || len(strings.TrimPrefix(result.Ref.ID, "demo-")) != 3 {
			t.Fatalf("hash ID=%q", result.Ref.ID)
		}
	}
	replay, err := w.Apply(context.Background(), by, cs, request)
	if err != nil || !reflect.DeepEqual(commit, replay) {
		t.Fatalf("hash replay commit=%+v err=%v replay=%+v", commit, err, replay)
	}
	reader := h.read(t)
	value, err := reader.(driver.AllocatorReader).LoadAllocator(context.Background(), "demo")
	if err != nil || value.Mode != driver.AllocatorHash || value.MinHashLength != 3 || value.MaxHashLength != 8 {
		t.Fatalf("hash allocator after batch=%+v err=%v", value, err)
	}
}

func TestHashAllocatorUsesPersistedCreationTimestamp(t *testing.T) {
	h := setup(t)
	seedAllocator(t, h.store, h.lease, driver.NewHashAllocator("demo", 3, 8, 0.25))
	var ticks uint64
	w := h.writer(WithClock(func() time.Time {
		ticks++
		return h.now.Add(time.Duration(ticks) * time.Nanosecond)
	}))
	commit := pipelineCreate(t, w, "advancing-clock", 94)
	rows, err := h.read(t).Get(context.Background(), []model.Ref{commit.Results[0].Ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("created record: %+v %v", rows, err)
	}
	doc, err := DecodeDocument(rows[0].Body)
	if err != nil || doc.CreatedAt == nil {
		t.Fatalf("created timestamp: %+v %v", doc, err)
	}
	creator, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	want, err := driver.HashCandidate("demo", driver.HashInput{Title: "initial", Creator: creator, Timestamp: *doc.CreatedAt}, 3, 0)
	if err != nil || want != doc.ID {
		t.Fatalf("ID %q does not hash its stored creation tuple: want=%s err=%v", doc.ID, want, err)
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

func TestSeededRandomAllocatorCollisionReplayAndDriverChildren(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var store driver.Store = memory.New()
			if kind == "sqlite" {
				store.Close()
				var err error
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "random.db")})
				if err != nil {
					t.Fatal(err)
				}
			}
			defer store.Close()
			lease, err := store.Lease(ctx, "random-allocator", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			seed := driver.NewRandomAllocator("demo")
			seedAllocator(t, store, lease, seed)
			s := "demo-" + strings.Repeat("a", 32)
			f := "demo-" + strings.Repeat("1", 32)
			ids := []string{s, s, f}
			calls := 0
			w := New(store, lease, WithOntology(testOntology{}), func(c *config) {
				c.newAllocatorID = func(a driver.OfflineAllocator) (string, error) {
					if a.Mode != driver.AllocatorRandom || a.Prefix != "demo" {
						t.Fatalf("fallback instead of live seed: %+v", a)
					}
					if calls >= len(ids) {
						return "", fmt.Errorf("unexpected entropy draw")
					}
					id := ids[calls]
					calls++
					return id, nil
				}
			})
			first := pipelineCreate(t, w, "source32", 501)
			if first.Results[0].Ref.ID != s {
				t.Fatal(first)
			}
			second := pipelineCreate(t, w, "collision", 502)
			if second.Results[0].Ref.ID != f || calls != 3 {
				t.Fatalf("collision retry %v calls%d", second, calls)
			}
			replay := pipelineCreate(t, w, "collision", 502)
			if replay.Revision != second.Revision || calls != 3 {
				t.Fatalf("replay reminted: %v calls%d", replay, calls)
			}
			child := s + "." + strings.Repeat("2", 32)
			ref := model.Ref{ID: child}
			key, _ := driver.AllocatorLock("demo")
			tx, err := store.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			if _, err = tx.Lock(ctx, driver.LockSet{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate}, {Key: key, Mode: driver.LockUpdate}}); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			allocator := tx.(driver.AllocatorContinuation)
			if err = allocator.AllocateChild(ctx, s, child); err != nil {
				t.Fatal(err)
			}
			if err = allocator.AllocateChild(ctx, s, child); !errors.Is(err, driver.ErrConflict) {
				t.Fatalf("reserved collision accepted: %v", err)
			}
			if err = tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			current, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := store.Read(ctx, driver.View{Incarnation: current.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			got, err := reader.(driver.AllocatorReader).LoadAllocator(ctx, "demo")
			if err != nil || got.Mode != driver.AllocatorRandom || got.LastID != 0 || len(got.Children) != 0 {
				t.Fatalf("random seed mutated: %+v %v", got, err)
			}
		})
	}
}

func TestPreparedImportBindsCommentsAndRejectsForgedCommit(t *testing.T) {
	for _, admission := range []driver.RestoreImportAdmission{
		{Mode: "authenticated", Actor: by, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)},
		{Mode: "anonymous-local", Actor: model.PrincipalRef{Kind: "anonymous", ID: "local"}, Scope: "local-agent", Authority: "trusted-human-presence", AdmissionDigest: strings.Repeat("a", 64)},
		{Mode: "anonymous-local", Actor: model.PrincipalRef{Kind: "anonymous", ID: "local"}, Scope: "", Authority: "trusted-human-presence", AdmissionDigest: strings.Repeat("a", 64)},
	} {
		t.Run(admission.Mode+"/"+admission.Scope, func(t *testing.T) { testPreparedImportBinding(t, admission) })
	}
}

func testPreparedImportBinding(t *testing.T, admission driver.RestoreImportAdmission) {
	ctx := context.Background()
	profile := driver.RestoreImportProfile{Admission: admission, SourceStoreID: "import-source", Method: "offline.restore.import", IdempotencyKey: "prepared-import-key", ArchiveDigest: strings.Repeat("b", 64), DerivedDigest: strings.Repeat("c", 64)}
	actor := profile.Admission.Actor
	now := time.Date(2026, 10, 30, 19, 30, 0, 0, time.UTC)
	comment, err := NewImportedComment(profile, "old body", "  source user  ", now.Add(-time.Hour), now)
	if err != nil || comment.SourceAuthor != "source user" || comment.Principal.ID != actor.ID {
		t.Fatalf("import comment: %+v %v", comment, err)
	}
	for _, author := range []string{"a\u200db", "a\x00b", strings.Repeat("界", 257)} {
		if _, err := NewImportedComment(profile, "", author, now, now); err == nil {
			t.Fatalf("invalid author accepted: %q", author)
		}
	}
	legacy := Document{ID: "old", Type: "task", Fields: map[string]any{}, Comments: []Comment{{Body: "legacy", Principal: actor, Time: now}}}
	body, _ := json.Marshal(legacy)
	if _, err := DecodeDocument(body); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var store driver.Store = memory.New()
			if kind == "sqlite" {
				store.Close()
				var err error
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "profile.db")})
				if err != nil {
					t.Fatal(err)
				}
			}
			defer store.Close()
			lease, err := store.Lease(ctx, "prepared-import", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			tx, err := store.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			ref := model.Ref{ID: "source-legacy"}
			key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
			allocatorKey, _ := driver.AllocatorLock("source")
			sentinel, _ := driver.NewBeadSentinel(ref.ID)
			sentinelKey := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
			if _, err = tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}, {Key: allocatorKey, Mode: driver.LockUpdate}, {Key: sentinelKey, Mode: driver.LockUpdate}}); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			meta, err := tx.(driver.RestoreImportTx).PrepareRestoreImport(ctx, profile)
			if err != nil {
				t.Fatal(err)
			}
			if err = tx.(driver.AllocatorContinuation).RestoreAllocators(ctx, []driver.OfflineAllocator{driver.NewRandomAllocator("source")}); err != nil {
				t.Fatal(err)
			}
			if err = tx.BumpSentinels(ctx, []driver.Sentinel{sentinel}); err != nil {
				t.Fatal(err)
			}
			// Preparation must retain a detached binding even if the caller mutates it.
			document := Document{ID: ref.ID, Type: "task", Fields: map[string]any{}, Comments: []Comment{comment}}
			body, _ := json.Marshal(document)
			if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}); err != nil {
				t.Fatal(err)
			}
			hash, err := profile.RequestHash()
			if err != nil {
				t.Fatal(err)
			}
			header := driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Time: now, Principal: actor, AgentLabel: profile.AgentLabel(), Method: profile.Method, IdempotencyKey: profile.IdempotencyKey, RequestHash: hash, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)}}, LockKeys: []driver.LockKey{key, allocatorKey, sentinelKey}}
			forged := header
			forged.Principal = model.PrincipalRef{Kind: "user", ID: "forged"}
			if _, err = tx.Commit(ctx, forged); err == nil {
				t.Fatal("forged actor accepted")
			}
			if got, _ := store.Meta(ctx); !got.Head.IsZero() {
				t.Fatal("forged commit published")
			}
			forgedComment := comment
			forgedComment.Principal = model.PrincipalRef{Kind: "user", ID: "forged"}
			document.Comments = []Comment{forgedComment}
			badBody, _ := json.Marshal(document)
			if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: badBody}); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Commit(ctx, header); err == nil {
				t.Fatal("forged imported comment accepted")
			}
			if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}); err != nil {
				t.Fatal(err)
			}
			for name, mutate := range map[string]func(*driver.ChangesetHeader){
				"method": func(h *driver.ChangesetHeader) { h.Method = "offline.restore" },
				"key":    func(h *driver.ChangesetHeader) { h.IdempotencyKey += "other" },
				"scope": func(h *driver.ChangesetHeader) {
					q := profile
					q.Admission.Scope = "other-scope"
					h.RequestHash, _ = q.RequestHash()
				},
				"archive": func(h *driver.ChangesetHeader) {
					q := profile
					q.ArchiveDigest = strings.Repeat("d", 64)
					h.RequestHash, _ = q.RequestHash()
				},
				"derived": func(h *driver.ChangesetHeader) {
					q := profile
					q.DerivedDigest = strings.Repeat("d", 64)
					h.RequestHash, _ = q.RequestHash()
				},
				"authorization-context": func(h *driver.ChangesetHeader) { h.Principal.Caveats = map[string]any{"scope": "other"} },
				"anonymous-label":       func(h *driver.ChangesetHeader) { h.AgentLabel = "forged-label" },
			} {
				bad := header
				mutate(&bad)
				if _, err = tx.Commit(ctx, bad); err == nil {
					t.Fatalf("changed %s accepted", name)
				}
			}
			for name, mutate := range map[string]func(*Comment){
				"origin":       func(c *Comment) { c.Origin = "" },
				"restore-time": func(c *Comment) { c.Time = c.Time.Add(time.Second) },
				"source-time":  func(c *Comment) { c.SourceTime = nil },
			} {
				bad := comment
				mutate(&bad)
				document.Comments = []Comment{bad}
				badBody, _ := json.Marshal(document)
				if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: badBody}); err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Commit(ctx, header); err == nil {
					t.Fatalf("changed comment %s accepted", name)
				}
			}
			if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}); err != nil {
				t.Fatal(err)
			}
			profile.Admission.Scope = "mutated-after-prepare"
			if _, err = tx.Commit(ctx, header); err != nil {
				t.Fatal(err)
			}
			current, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := store.Read(ctx, driver.View{Incarnation: current.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			receipt, err := reader.IdempotencyByKey(ctx, actor.Kind+":"+actor.ID, header.AgentLabel, header.Method, header.IdempotencyKey)
			if err != nil || receipt == nil || receipt.RequestHash != hash || receipt.Changeset.Principal.ID != actor.ID {
				t.Fatalf("receipt binding: %+v %v", receipt, err)
			}
			profile.Admission.Scope = admission.Scope
		})
	}
}

// These calls exercise driver consumption directly: snapshot collision
// detection in Writer cannot mask a missing engine membership/lock guard.
func TestRandomAllocatorDriverRefusalsPublishNothing(t *testing.T) {
	ctx := context.Background()
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			var store driver.Store = memory.New()
			if engine == "sqlite" {
				store.Close()
				var err error
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "refusals.db")})
				if err != nil {
					t.Fatal(err)
				}
			}
			defer store.Close()
			lease, err := store.Lease(ctx, "direct-random-refusals", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			seedAllocator(t, store, lease, driver.NewRandomAllocator("demo"))
			source := "demo-" + strings.Repeat("a", 32)
			writer := New(store, lease, WithOntology(testOntology{}), func(c *config) {
				c.newAllocatorID = func(driver.OfflineAllocator) (string, error) { return source, nil }
			})
			pipelineCreate(t, writer, "durable-source", 601)
			for _, scenario := range []string{"durable", "staged", "missing-candidate-lock", "shared-candidate-lock", "cancelled"} {
				t.Run(scenario, func(t *testing.T) {
					candidate := "demo-" + strings.Repeat("1", 32)
					if scenario == "durable" {
						candidate = source
					}
					ref := model.Ref{ID: candidate}
					allocatorKey, _ := driver.AllocatorLock("demo")
					locks := driver.LockSet{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate}, {Key: allocatorKey, Mode: driver.LockUpdate}}
					if scenario == "missing-candidate-lock" {
						locks = locks[1:]
					}
					if scenario == "shared-candidate-lock" {
						locks[0].Mode = driver.LockShare
					}
					if scenario == "staged" {
						// The preceding real Writer create now establishes source
						// coverage after allocator-only history. Stage the collision
						// with its complete metadata/lock, not an illegal untyped Put.
						locks = append(locks, driver.LockRequest{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate})
						sort.Slice(locks, func(i, j int) bool {
							a, _ := driver.EncodeLockKey(locks[i].Key)
							b, _ := driver.EncodeLockKey(locks[j].Key)
							return a < b
						})
					}
					before, err := store.Meta(ctx)
					if err != nil {
						t.Fatal(err)
					}
					reader, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					beforeRecords, err := reader.Get(ctx, []model.Ref{ref})
					if err != nil {
						t.Fatal(err)
					}
					beforeAllocator, err := reader.(driver.AllocatorReader).LoadAllocator(ctx, "demo")
					reader.Close()
					if err != nil {
						t.Fatal(err)
					}
					tx, err := store.Write(ctx, lease)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Close()
					beforeFacts, err := tx.Lock(ctx, locks)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = tx.Head(ctx); err != nil {
						t.Fatal(err)
					}
					if scenario == "staged" {
						body, _ := json.Marshal(Document{ID: candidate, Type: "task", Fields: map[string]any{"title": "must roll back"}})
						if err = PutSourceRecord(ctx, tx, driver.Record{Ref: ref, Kind: "bead", Body: body}); err != nil {
							t.Fatal(err)
						}
					}
					callCtx := ctx
					if scenario == "cancelled" {
						var cancel context.CancelFunc
						callCtx, cancel = context.WithCancel(ctx)
						cancel()
					}
					err = tx.(driver.AllocatorContinuation).AllocateTopLevel(callCtx, "demo", candidate)
					if err == nil {
						t.Fatalf("%s allocation accepted", scenario)
					}
					if (scenario == "durable" || scenario == "staged") && !errors.Is(err, driver.ErrConflict) {
						t.Fatalf("collision error=%v", err)
					}
					if scenario == "cancelled" && !errors.Is(err, context.Canceled) {
						t.Fatalf("cancel error=%v", err)
					}
					if err = tx.Rollback(ctx); err != nil {
						t.Fatal(err)
					}
					after, err := store.Meta(ctx)
					if err != nil || after != before {
						t.Fatalf("refusal changed metadata: %+v %+v %v", before, after, err)
					}
					reader, err = store.Read(ctx, driver.View{Incarnation: after.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					afterRecords, err := reader.Get(ctx, []model.Ref{ref})
					if err != nil {
						t.Fatal(err)
					}
					afterAllocator, err := reader.(driver.AllocatorReader).LoadAllocator(ctx, "demo")
					reader.Close()
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(beforeRecords, afterRecords) || !reflect.DeepEqual(beforeAllocator, afterAllocator) {
						t.Fatalf("refusal published records or allocator: %v -> %v", beforeRecords, afterRecords)
					}
					verify, err := store.Write(ctx, lease)
					if err != nil {
						t.Fatal(err)
					}
					afterFacts, err := verify.Lock(ctx, locks)
					verify.Close()
					if err != nil || !reflect.DeepEqual(beforeFacts, afterFacts) {
						t.Fatalf("refusal advanced fact/record revision: %v -> %v (%v)", beforeFacts, afterFacts, err)
					}
				})
			}
		})
	}
}
