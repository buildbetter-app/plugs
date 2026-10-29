package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type allocatorTestOntology struct{}

func (allocatorTestOntology) Normalize(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
	return nil, nil
}

func (allocatorTestOntology) Validate(context.Context, port.ProposedChange, port.SnapshotReader) error {
	return nil
}

func restoreAllocator(t *testing.T, s *Store, lease driver.ActiveLease, value driver.OfflineAllocator) {
	t.Helper()
	ctx := context.Background()
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key, err := driver.AllocatorLock(value.Prefix)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.Write(ctx, lease)
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
	continuation, ok := tx.(driver.AllocatorContinuation)
	if !ok {
		t.Fatal("postgres writer does not expose allocator continuation")
	}
	if err = continuation.RestoreAllocators(ctx, []driver.OfflineAllocator{value}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(ctx, driver.ChangesetHeader{
		Incarnation: meta.Incarnation,
		Kind:        model.ChangesetRestore,
		Principal:   model.PrincipalRef{Kind: "system", ID: "allocator-test"},
		LockKeys:    []driver.LockKey{key},
	}); err != nil {
		t.Fatal(err)
	}
}

func allocatorValue(t *testing.T, r driver.Reader, prefix string) driver.OfflineAllocator {
	t.Helper()
	value, err := r.(driver.AllocatorReader).LoadAllocator(context.Background(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func pipelineCreate(t *testing.T, w *pipeline.Writer, n int) model.Commit {
	t.Helper()
	commit, err := w.Apply(context.Background(), model.PrincipalRef{Kind: "user", ID: "allocator-test"}, model.ChangeSet{Ops: []model.Op{{
		Kind:   model.OpCreate,
		Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "allocator"}},
	}}}, model.WriteMeta{Method: "tx.apply", IdempotencyKey: fmt.Sprintf("%032x", n), Project: "demo", Return: "full"})
	if err != nil {
		t.Fatal(err)
	}
	return commit
}

func TestPostgresAllocatorPipelineReconnectAndConcurrentRetry(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	s := openTest(t, dsn)
	lease := testLease(t, s)
	restoreAllocator(t, s, lease, driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8, Children: map[string]uint64{"demo-8": 3}})

	var fallbackCalls atomic.Int32
	newWriter := func() *pipeline.Writer {
		return pipeline.New(s, lease,
			pipeline.WithOntology(allocatorTestOntology{}),
			pipeline.WithIDGenerator(func() (string, error) {
				fallbackCalls.Add(1)
				return "fallback", nil
			}),
		)
	}
	writers := []*pipeline.Writer{newWriter(), newWriter()}
	commits := make(chan model.Commit, len(writers))
	errs := make(chan error, len(writers))
	var wg sync.WaitGroup
	for i, writer := range writers {
		wg.Add(1)
		go func(i int, writer *pipeline.Writer) {
			defer wg.Done()
			commit, err := writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "allocator-test"}, model.ChangeSet{Ops: []model.Op{{
				Kind:   model.OpCreate,
				Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "concurrent"}},
			}}}, model.WriteMeta{Method: "tx.apply", IdempotencyKey: fmt.Sprintf("%032x", 100+i), Project: "demo", Return: "full"})
			if err != nil {
				errs <- err
				return
			}
			commits <- commit
		}(i, writer)
	}
	wg.Wait()
	close(commits)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for commit := range commits {
		if len(commit.Results) != 1 {
			t.Fatalf("concurrent commit=%+v", commit)
		}
		seen[commit.Results[0].Ref.ID] = true
	}
	if len(seen) != 2 || !seen["demo-9"] || !seen["demo-10"] {
		t.Fatalf("concurrent allocator IDs=%v", seen)
	}
	if fallbackCalls.Load() != 0 {
		t.Fatalf("allocator unexpectedly used fallback generator %d times", fallbackCalls.Load())
	}
	value := allocatorValue(t, testRead(t, s, nil), "demo")
	if value.LastID != 10 || value.Children["demo-8"] != 3 {
		t.Fatalf("concurrent allocator state=%+v", value)
	}

	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTest(t, dsn)
	reopenedLease := testLease(t, reopened)
	commit := pipelineCreate(t, pipeline.New(reopened, reopenedLease,
		pipeline.WithOntology(allocatorTestOntology{}),
		pipeline.WithIDGenerator(func() (string, error) { return "fallback-after-reopen", nil }),
	), 111)
	if got := commit.Results[0].Ref.ID; got != "demo-11" {
		t.Fatalf("reopened allocator ID=%q", got)
	}
	value = allocatorValue(t, testRead(t, reopened, nil), "demo")
	if value.LastID != 11 {
		t.Fatalf("reopened allocator state=%+v", value)
	}
}

func TestPostgresAllocatorChildDetachedAndOverflow(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	restoreAllocator(t, s, lease, driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8, Children: map[string]uint64{"demo-8": 3}})
	key, err := driver.AllocatorLock("demo")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	continuation := tx.(driver.AllocatorContinuation)
	if err = continuation.AllocateChild(ctx, "demo-8", "demo-8.4"); err != nil {
		t.Fatal(err)
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "allocator-test"}, LockKeys: []driver.LockKey{key}}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Close(); err != nil {
		t.Fatal(err)
	}

	r := testRead(t, s, nil)
	value := allocatorValue(t, r, "demo")
	if value.Children["demo-8"] != 4 {
		t.Fatalf("child counter=%+v", value.Children)
	}
	value.Children["demo-8"] = 99
	again := allocatorValue(t, r, "demo")
	if again.Children["demo-8"] != 4 {
		t.Fatalf("allocator read was not detached: %+v", again.Children)
	}

	// A maxed child counter and top-level counter both fail closed without
	// changing the durable state.
	maxValue := driver.OfflineAllocator{Prefix: "max", Mode: driver.AllocatorSequential, LastID: ^uint64(0), Children: map[string]uint64{"max-1": ^uint64(0)}}
	restoreAllocator(t, s, lease, maxValue)
	maxKey, _ := driver.AllocatorLock("max")
	tx, err = s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, driver.LockSet{{Key: maxKey, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	continuation = tx.(driver.AllocatorContinuation)
	if err = continuation.AllocateTopLevel(ctx, "max", "max-18446744073709551616"); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("top-level overflow=%v", err)
	}
	if err = continuation.AllocateChild(ctx, "max-1", "max-1.18446744073709551616"); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("child overflow=%v", err)
	}
	loaded, err := continuation.LoadAllocator(ctx, "max")
	if err != nil || loaded.LastID != ^uint64(0) || loaded.Children["max-1"] != ^uint64(0) {
		t.Fatalf("overflow changed staged state=%+v err=%v", loaded, err)
	}
}

func TestPostgresAllocatorRollbackAndDuplicateRefusal(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	value := driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8}
	restoreAllocator(t, s, lease, value)
	metaBefore, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}

	key, _ := driver.AllocatorLock("demo")
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	continuation := tx.(driver.AllocatorContinuation)
	if err = continuation.RestoreAllocators(ctx, []driver.OfflineAllocator{value, value}); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("duplicate restore=%v", err)
	}
	if err = tx.Close(); err != nil {
		t.Fatal(err)
	}
	metaAfterDuplicate, err := s.Meta(ctx)
	if err != nil || metaAfterDuplicate.Head.String() != metaBefore.Head.String() {
		t.Fatalf("duplicate restore published head=%+v err=%v", metaAfterDuplicate, err)
	}

	// An already-admitted prefix is also refused before any staged update.
	tx, err = s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err = tx.(driver.AllocatorContinuation).RestoreAllocators(ctx, []driver.OfflineAllocator{{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 99}}); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("existing restore=%v", err)
	}
	loaded, err := tx.(driver.AllocatorContinuation).LoadAllocator(ctx, "demo")
	if err != nil || loaded.LastID != 8 {
		t.Fatalf("existing restore changed state=%+v err=%v", loaded, err)
	}
	if err = tx.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = s.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.reject_allocator_commit() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'allocator rollback'; END$$; CREATE TRIGGER reject_allocator_commit BEFORE INSERT ON plug_postgres.changesets FOR EACH ROW EXECUTE FUNCTION plug_postgres.reject_allocator_commit()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS reject_allocator_commit ON plug_postgres.changesets; DROP FUNCTION IF EXISTS plug_postgres.reject_allocator_commit()`)
	})

	record := testRef("demo-9")
	tx, h := beginTest(t, s, lease, key, testLock(record))
	if err = tx.Put(ctx, driver.Record{Ref: record, Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
		t.Fatal(err)
	}
	if err = tx.(driver.AllocatorContinuation).AllocateTopLevel(ctx, "demo", "demo-9"); err != nil {
		t.Fatal(err)
	}
	h.Changes = []model.ChangeRecord{{Ref: record, Op: model.OpCreate}}
	if _, err = tx.Commit(ctx, h); err == nil {
		t.Fatal("synthetic late publication unexpectedly committed")
	}

	metaAfter, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if metaAfter.Head.String() != metaBefore.Head.String() {
		t.Fatalf("rollback advanced head from %s to %s", metaBefore.Head, metaAfter.Head)
	}
	r := testRead(t, s, nil)
	rows, err := r.Get(ctx, []driver.Ref{record})
	if err != nil || len(rows) != 0 {
		t.Fatalf("rolled-back record=%v err=%v", rows, err)
	}
	rolledBack := allocatorValue(t, r, "demo")
	if rolledBack.LastID != 8 {
		t.Fatalf("rolled-back allocator=%+v", rolledBack)
	}
}

func TestPostgresAllocatorHashAndStaleLeaseRefuse(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	s := openTest(t, dsn)
	lease := testLease(t, s)
	restoreAllocator(t, s, lease, driver.OfflineAllocator{Prefix: "hash", Mode: driver.AllocatorHash, HashLength: 8})
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	old, err := s.Lease(ctx, "stale", 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	<-old.Lost()
	other := openTest(t, dsn)
	fresh := testLease(t, other)
	defer fresh.Release(ctx)
	if _, err = s.Write(ctx, old); err == nil {
		t.Fatal("stale lease opened a writer")
	} else {
		var fenced driver.ErrFenced
		if !errors.As(err, &fenced) {
			t.Fatalf("stale lease error=%v", err)
		}
	}
	if value := allocatorValue(t, testRead(t, other, nil), "hash"); value.Mode != driver.AllocatorHash {
		t.Fatalf("stale lease changed hash state=%+v", value)
	}
}

func TestPostgresAllocatorSchemaOneAdditiveInitialization(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	s := openTest(t, dsn)
	if _, err := s.db.ExecContext(ctx, `DROP TABLE plug_postgres.allocator_states`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTest(t, dsn)
	var table string
	if err := reopened.db.QueryRowContext(ctx, `SELECT to_regclass('plug_postgres.allocator_states')`).Scan(&table); err != nil {
		t.Fatal(err)
	}
	if table != "plug_postgres.allocator_states" {
		t.Fatalf("additive allocator table=%q", table)
	}
	lease := testLease(t, reopened)
	restoreAllocator(t, reopened, lease, driver.OfflineAllocator{Prefix: "legacy", Mode: driver.AllocatorSequential, LastID: 4})
	if value := allocatorValue(t, testRead(t, reopened, nil), "legacy"); value.LastID != 4 {
		t.Fatalf("allocator after additive initialization=%+v", value)
	}
}
