package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func guardPath(s *Store, key driver.LockKey) string {
	return guardStripePath(s.lockDir, guardStripe(key))
}

func guardDescriptorCount(t *testing.T, s *Store) int {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, fd := range fds {
		path, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(path, s.lockDir+string(os.PathSeparator)) {
			count++
		}
	}
	return count
}

func guardVMACount(t *testing.T, s *Store) int {
	t.Helper()
	data, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Fatal(err)
	}
	prefix := s.lockDir + string(os.PathSeparator)
	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, prefix) {
			count++
		}
	}
	return count
}

func guardRequests(count int) driver.LockSet {
	requests := make(driver.LockSet, count)
	for i := range requests {
		ref := testRef(fmt.Sprintf("pl-guard-%06d", i))
		requests[i] = driver.LockRequest{
			Key:  driver.LockKey{Kind: driver.LockRecord, Ref: &ref},
			Mode: driver.LockUpdate,
		}
	}
	return requests
}

func restoreGuardRequests(count int) driver.LockSet {
	requests := make(driver.LockSet, 0, count*4+1)
	for i := range count {
		id := fmt.Sprintf("pl-restore-%06d", i)
		ref := testRef(id)
		bead, err := driver.NewBeadSentinel(id)
		if err != nil {
			panic(err)
		}
		closure, err := driver.NewClosureSentinel(id)
		if err != nil {
			panic(err)
		}
		requests = append(requests,
			driver.LockRequest{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate},
			driver.LockRequest{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead}, Mode: driver.LockUpdate},
			driver.LockRequest{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: closure}, Mode: driver.LockUpdate},
			driver.LockRequest{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: id + ".self"}}, Mode: driver.LockUpdate},
		)
	}
	requests = append(requests, driver.LockRequest{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate})
	sort.Slice(requests, func(i, j int) bool { return keyID(requests[i].Key) < keyID(requests[j].Key) })
	return requests
}

func collidingGuardKeys(t *testing.T) []driver.LockKey {
	t.Helper()
	first := testKey("pl-stripe-000000")
	stripe := guardStripe(first)
	keys := []driver.LockKey{first}
	for i := 1; i < guardStripeCount*256; i++ {
		key := testKey(fmt.Sprintf("pl-stripe-%06d", i))
		if guardStripe(key) == stripe {
			keys = append(keys, key)
			if len(keys) == 3 {
				return keys
			}
		}
	}
	t.Fatal("could not find three colliding guard keys")
	return nil
}

func TestGuardStripePlanUsesNumericOrder(t *testing.T) {
	requests := guardRequests(2048)
	for i := range requests {
		requests[i].Mode = driver.LockShare
	}
	stripes, indexes := planGuardStripes(requests)
	if len(stripes) < 2 || len(stripes) > guardStripeCount || len(indexes) != len(requests) {
		t.Fatalf("stripe plan sizes: stripes=%d indexes=%d requests=%d", len(stripes), len(indexes), len(requests))
	}
	for i, stripe := range stripes {
		if i > 0 && stripes[i-1].stripe >= stripe.stripe {
			t.Fatalf("stripe acquisition order is not strictly increasing: %+v", stripes)
		}
		if !stripe.share || stripe.skipLocked {
			t.Fatalf("shared-only stripe has incorrect mode: %+v", stripe)
		}
	}
	for i, request := range requests {
		if indexes[i] != guardStripe(request.Key) {
			t.Fatalf("request %d mapped to stripe %d, want %d", i, indexes[i], guardStripe(request.Key))
		}
	}
}

func TestCollidingGuardModesUseOneHandleAndStrongestMode(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "guard-stripe-modes.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	keys := collidingGuardKeys(t)
	for _, first := range []driver.LockMode{driver.LockShare, driver.LockUpdate, driver.LockSkipLocked} {
		for _, second := range []driver.LockMode{driver.LockShare, driver.LockUpdate, driver.LockSkipLocked} {
			t.Run(string(first)+"/"+string(second), func(t *testing.T) {
				requests := driver.LockSet{{Key: keys[0], Mode: first}, {Key: keys[1], Mode: second}}
				stripes, _ := planGuardStripes(requests)
				share := first == driver.LockShare && second == driver.LockShare
				skipLocked := first == driver.LockSkipLocked && second == driver.LockSkipLocked
				if len(stripes) != 1 || stripes[0].share != share || stripes[0].skipLocked != skipLocked {
					t.Fatalf("incorrect combined stripe modes: %+v", stripes)
				}
				value, err := s.Write(ctx, lease)
				if err != nil {
					t.Fatal(err)
				}
				tx := value.(*transaction)
				defer tx.Close()
				beforeFD, beforeVMA := guardDescriptorCount(t, s), guardVMACount(t, s)
				states, err := tx.Lock(ctx, requests)
				if err != nil || len(states) != 2 || len(tx.guards) != 1 {
					t.Fatalf("colliding acquisition states=%d handles=%d err=%v", len(states), len(tx.guards), err)
				}
				if !reflect.DeepEqual(tx.acquired, requests) {
					t.Fatalf("stripe promotion changed per-key modes: got=%+v want=%+v", tx.acquired, requests)
				}
				if got := guardDescriptorCount(t, s); got != beforeFD {
					t.Fatalf("colliding stripe retained descriptors: before=%d after=%d", beforeFD, got)
				}
				if got := guardVMACount(t, s); got != beforeVMA+1 {
					t.Fatalf("colliding stripe mappings: before=%d after=%d, want one added", beforeVMA, got)
				}
				for i, request := range requests {
					if keyID(states[i].Key) != keyID(request.Key) {
						t.Fatalf("state %d lost input key order", i)
					}
					if request.Mode == driver.LockShare {
						if err := tx.Put(ctx, driver.Record{Ref: *request.Key.Ref, Kind: "test", Body: []byte(`{}`)}); err == nil {
							t.Fatal("stripe promotion authorized a write under a share lock")
						}
					}
				}
				guardProbe(t, ctx, guardPath(s, keys[0]), false, false)
				guardProbe(t, ctx, guardPath(s, keys[1]), true, share)
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				if got := guardVMACount(t, s); got != beforeVMA {
					t.Fatalf("colliding stripe release retained VMAs: before=%d after=%d", beforeVMA, got)
				}
				guardProbe(t, ctx, guardPath(s, keys[0]), false, true)
			})
		}
	}
}

func TestCollidingSkipLockedGuardsOmitBusyStripe(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "guard-stripe-skip.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	keys := collidingGuardKeys(t)
	holder, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err := holder.Lock(ctx, driver.LockSet{{Key: keys[0], Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	requests := driver.LockSet{{Key: keys[1], Mode: driver.LockSkipLocked}, {Key: keys[2], Mode: driver.LockSkipLocked}}
	value, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	tx := value.(*transaction)
	defer tx.Close()
	beforeFD, beforeVMA := guardDescriptorCount(t, s), guardVMACount(t, s)
	states, err := tx.Lock(ctx, requests)
	if err != nil || len(states) != 0 || len(tx.acquired) != 0 || len(tx.guards) != 0 {
		t.Fatalf("busy skip-only stripe: states=%d acquired=%d handles=%d err=%v", len(states), len(tx.acquired), len(tx.guards), err)
	}
	if got := guardDescriptorCount(t, s); got != beforeFD {
		t.Fatalf("skipped stripe retained descriptors: before=%d after=%d", beforeFD, got)
	}
	if got := guardVMACount(t, s); got != beforeVMA {
		t.Fatalf("skipped stripe retained mappings: before=%d after=%d", beforeVMA, got)
	}
	if err := tx.Put(ctx, driver.Record{Ref: *keys[1].Ref, Kind: "test", Body: []byte(`{}`)}); err == nil {
		t.Fatal("omitted colliding key authorized a write")
	}
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	nextValue, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	next := nextValue.(*transaction)
	defer next.Close()
	states, err = next.Lock(ctx, requests)
	if err != nil || len(states) != 2 || len(next.guards) != 1 {
		t.Fatalf("released skip-only stripe: states=%d handles=%d err=%v", len(states), len(next.guards), err)
	}
}

func TestLargeCompleteLockSetUsesBoundedDescriptors(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "guard-descriptor.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	requests := guardRequests(20000)
	before := guardDescriptorCount(t, s)

	value, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	tx := value.(*transaction)
	defer tx.Close()
	states, err := tx.Lock(ctx, requests)
	if err != nil {
		t.Fatalf("real SQLite complete set of %d keys: %T %v; context=%v", len(requests), err, err, ctx.Err())
	}
	if len(states) != len(requests) {
		t.Fatalf("states=%d, want %d", len(states), len(requests))
	}
	after := guardDescriptorCount(t, s)
	t.Logf("actual complete guard keys=%d; lock-directory descriptors before=%d held=%d", len(states), before, after)
	if after-before > 1 {
		t.Fatalf("guard acquisition retained %d descriptors for %d keys, want at most one", after-before, len(requests))
	}
	if err := driver.ValidateLockSet(tx.acquired); err != nil {
		t.Fatalf("acquired lock order: %v", err)
	}
	if len(tx.acquired) != len(requests) {
		t.Fatalf("acquired=%d, want %d", len(tx.acquired), len(requests))
	}
	if _, err := tx.Lock(ctx, requests); err == nil {
		t.Fatal("second complete lock acquisition succeeded")
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if after := guardDescriptorCount(t, s); after != before {
		t.Fatalf("close retained descriptors: before=%d after=%d", before, after)
	}

	// Closing the transaction must release every guard so a fresh writer can
	// acquire the identical complete set immediately.
	next, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if _, err := next.Lock(ctx, requests); err != nil {
		t.Fatalf("lock set after close: %v", err)
	}
}

func TestCompleteRestoreGuardFamiliesUseBoundedDescriptorsAndMappings(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "restore-guard-capacity.db"))
	requests := restoreGuardRequests(20000)
	if err := driver.ValidateLockSet(requests); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 80001 {
		t.Fatalf("restore guard requests=%d, want 80001", len(requests))
	}
	stripes := make(map[int]bool)
	for _, request := range requests {
		stripes[guardStripe(request.Key)] = true
	}
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	value, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	tx := value.(*transaction)
	defer tx.Close()
	beforeFD, beforeVMA := guardDescriptorCount(t, s), guardVMACount(t, s)
	states, err := tx.Lock(ctx, requests)
	if err != nil {
		t.Fatalf("restore guard acquisition failed after %d keys: %v", len(states), err)
	}
	if len(states) != len(requests) || len(tx.acquired) != len(requests) {
		t.Fatalf("restore guard acquisition states=%d acquired=%d want %d", len(states), len(tx.acquired), len(requests))
	}
	if len(tx.guards) != len(stripes) || len(tx.guards) > 1024 {
		t.Fatalf("restore guard handles=%d, want %d distinct stripes and at most 1024", len(tx.guards), len(stripes))
	}
	afterFD, afterVMA := guardDescriptorCount(t, s), guardVMACount(t, s)
	t.Logf("actual restore guard set records=%d bead_sentinels=%d closure_sentinels=%d ancestor_rows=%d graph_sentinel=1 total=%d descriptors=%d vmas=%d", 20000, 20000, 20000, 20000, len(requests), afterFD-beforeFD, afterVMA-beforeVMA)
	if afterFD-beforeFD > 1 {
		t.Fatalf("restore guard acquisition retained %d descriptors for %d keys", afterFD-beforeFD, len(requests))
	}
	if afterVMA-beforeVMA != len(stripes) || afterVMA-beforeVMA > 1024 {
		t.Fatalf("restore guard mappings=%d, want %d distinct stripes and at most 1024", afterVMA-beforeVMA, len(stripes))
	}
	// A second open must conflict on every held stripe, not just a sample.
	for stripe := range stripes {
		f, ok, err := tryFileLock(guardStripePath(s.lockDir, stripe), false)
		unlockFile(f)
		if err != nil || ok {
			t.Fatalf("restore stripe %d lost exclusion: acquired=%v err=%v", stripe, ok, err)
		}
	}
	guardProbe(t, ctx, guardPath(s, requests[0].Key), false, false)
	guardProbe(t, ctx, guardPath(s, requests[len(requests)-1].Key), true, false)
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if got := guardDescriptorCount(t, s); got != beforeFD {
		t.Fatalf("restore guard release retained descriptors: before=%d after=%d", beforeFD, got)
	}
	if got := guardVMACount(t, s); got != beforeVMA {
		t.Fatalf("restore guard release retained VMAs: before=%d after=%d", beforeVMA, got)
	}
	guardProbe(t, ctx, guardPath(s, requests[0].Key), false, true)
	guardProbe(t, ctx, guardPath(s, requests[len(requests)-1].Key), false, true)
}

func TestCommitReleasesGuardMappings(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "commit-guard-release.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	beforeFD, beforeVMA := guardDescriptorCount(t, s), guardVMACount(t, s)
	tx, header := beginRecord(t, ctx, s, lease, "pl-guard-commit-release")
	header.Method = "test.guard.commit"
	header.IdempotencyKey = "guard-commit-release"
	header.RequestHash = "guard-commit-release"
	if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-guard-commit-release"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if got := guardDescriptorCount(t, s); got != beforeFD+1 {
		t.Fatalf("commit guard acquisition retained unexpected descriptors: before=%d after=%d", beforeFD, got)
	}
	if got := guardVMACount(t, s); got <= beforeVMA {
		t.Fatalf("commit guard acquisition retained no VMA: before=%d after=%d", beforeVMA, got)
	}
	if _, err := tx.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
	if got := guardDescriptorCount(t, s); got != beforeFD {
		t.Fatalf("commit retained descriptors: before=%d after=%d", beforeFD, got)
	}
	if got := guardVMACount(t, s); got != beforeVMA {
		t.Fatalf("commit retained VMAs: before=%d after=%d", beforeVMA, got)
	}
}

func TestCanceledPartialLockAcquisitionReleasesAllRanges(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "guard-cancel.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	first, held := testKey("pl-guard-cancel-first"), testKey("pl-guard-cancel-held")

	holderValue, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	holder := holderValue.(*transaction)
	defer holder.Close()
	if _, err := holder.Lock(ctx, driver.LockSet{{Key: held, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}

	contenderValue, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	contender := contenderValue.(*transaction)
	defer contender.Close()
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, err = contender.Lock(short, driver.LockSet{
		{Key: first, Mode: driver.LockUpdate},
		{Key: held, Mode: driver.LockUpdate},
	})
	cancel()
	if err == nil {
		t.Fatal("blocked partial lock acquisition succeeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked lock error=%T %v, want context deadline", err, err)
	}
	guardProbe(t, ctx, guardPath(s, first), false, true)
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}

	retryCtx, retryCancel := context.WithTimeout(ctx, time.Second)
	defer retryCancel()
	states, err := contender.Lock(retryCtx, driver.LockSet{
		{Key: first, Mode: driver.LockUpdate},
		{Key: held, Mode: driver.LockUpdate},
	})
	if err != nil {
		t.Fatalf("retry after canceled partial acquisition: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("retry states=%d, want 2", len(states))
	}
}

// Losing the lease after the entry fence must never publish a successful
// complete set whose mappings the lease watcher is already waiting to release.
func TestLockRejectsLeaseLostDuringGuardAcquisition(t *testing.T) {
	ctx := testContext(t)
	var loseOnFence atomic.Bool
	var lease *activeLease
	value, err := (Driver{Clock: func() time.Time {
		now := time.Now()
		if loseOnFence.Swap(false) {
			lease.signalLost("test fence")
		}
		return now
	}}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "guard-lost-acquisition.db")})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease = getLease(t, ctx, s, driver.LeaseTTL).(*activeLease)
	writer, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	tx := writer.(*transaction)
	defer tx.Close()
	requests := restoreGuardRequests(2)
	beforeFD, beforeVMA := guardDescriptorCount(t, s), guardVMACount(t, s)

	// fence checks Lost before calling the production Clock seam. Signal loss
	// at that point while Lock owns the transaction mutex: Close is queued,
	// and the real stripe flock/mmap and SQL paths still have to unwind.
	loseOnFence.Store(true)
	states, lockErr := tx.Lock(ctx, requests)
	if lockErr == nil {
		_ = tx.Close()
		t.Fatalf("Lock published %d states after lease loss; its guard mappings were already queued for release", len(states))
	}
	requireFenced(t, lockErr)
	if len(states) != 0 {
		t.Fatalf("fenced acquisition returned %d states", len(states))
	}
	tx.mu.Lock()
	locked, acquired, adopted := tx.locked, len(tx.acquired), len(tx.guards)
	afterFD, afterVMA := guardDescriptorCount(t, s), guardVMACount(t, s)
	tx.mu.Unlock()
	if locked || acquired != 0 || adopted != 0 {
		t.Fatalf("fenced acquisition adopted a partial set: locked=%v acquired=%d guards=%d", locked, acquired, adopted)
	}
	if got := afterFD; got != beforeFD {
		t.Fatalf("fenced acquisition retained descriptors: before=%d after=%d", beforeFD, got)
	}
	if got := afterVMA; got != beforeVMA {
		t.Fatalf("fenced acquisition retained VMAs: before=%d after=%d", beforeVMA, got)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	for _, request := range requests {
		guardProbe(t, ctx, guardPath(s, request.Key), false, true)
	}
	t.Logf("lease loss rejected the complete %d-key set and released every original flock/mapping", len(requests))
}

func TestCompleteLockStatesMatchPinnedObserver(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "guard-state-equivalence.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	live := driver.DerivedRow{Key: "guard/live", Value: []byte("original")}
	deleted := driver.DerivedRow{Key: "guard/deleted", Value: []byte("deleted")}
	opaque := driver.DerivedRow{Key: "guard/opaque-\xff", Value: []byte("opaque")}
	initial := []driver.DerivedRow{live, deleted, opaque}
	batchKeys := make([]driver.LockKey, 300)
	for i := range batchKeys {
		row := driver.DerivedRow{Key: fmt.Sprintf("guard/batch-%03d", i), Value: []byte(fmt.Sprintf("value-%d", i))}
		batchKeys[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: row.Key}}
		if i%2 == 0 {
			initial = append(initial, row)
		}
	}
	prefixCommit(t, ctx, s, lease, 1, nil, initial)
	// Open before subsequent commits: Lock must repin after taking every guard.
	writer, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	live.Value = []byte("current")
	prefixCommit(t, ctx, s, lease, 2, []driver.DerivedRow{deleted}, []driver.DerivedRow{live})
	seedRecord(t, ctx, s, lease, "pl-guard-deleted", `{"old":true}`)
	remove, header := beginRecord(t, ctx, s, lease, "pl-guard-deleted")
	if err := remove.Delete(ctx, testRef("pl-guard-deleted")); err != nil {
		t.Fatal(err)
	}
	header.Changes[0].Op = model.OpDelete
	if _, err := remove.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}

	fact := driver.ReadFact("guard/equivalence")
	factWriter, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer factWriter.Close()
	factKeys := []driver.LockKey{testKey("pl-guard-live"), {Kind: driver.LockReadFact, Fact: fact}}
	factLocks := driver.LockSet{{Key: factKeys[0], Mode: driver.LockUpdate}, {Key: factKeys[1], Mode: driver.LockUpdate}}
	if _, err := factWriter.Lock(ctx, factLocks); err != nil {
		t.Fatal(err)
	}
	if _, err := factWriter.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := factWriter.Put(ctx, driver.Record{Ref: testRef("pl-guard-live"), Kind: "test", Body: []byte(`{ "number": 1e+03, "nested": [1, 2] }`)}); err != nil {
		t.Fatal(err)
	}
	if err := factWriter.BumpReadFacts(ctx, []driver.ReadFact{fact}); err != nil {
		t.Fatal(err)
	}
	if _, err := factWriter.Commit(ctx, driver.ChangesetHeader{
		Incarnation: getMeta(t, ctx, s).Incarnation, Kind: model.ChangesetWrite,
		Principal: model.PrincipalRef{Kind: "system", ID: "guard-state-test"}, LockKeys: factKeys,
		Changes: []model.ChangeRecord{{Ref: testRef("pl-guard-live"), Op: model.OpCreate, FromRevision: model.Natural64(0)}},
	}); err != nil {
		t.Fatal(err)
	}

	skipped := testKey("pl-guard-skipped")
	holder, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err := holder.Lock(ctx, driver.LockSet{{Key: skipped, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	sentinel, _ := driver.NewClosureSentinel("pl-prefix")
	keys := []driver.LockKey{
		testKey("pl-prefix"), testKey("pl-guard-live"), testKey("pl-guard-deleted"), testKey("pl-guard-missing"), skipped,
		{Kind: driver.LockReadFact, Fact: fact}, {Kind: driver.LockReadFact, Fact: "guard/missing"},
		{Kind: driver.LockSentinel, Sentinel: sentinel}, driver.GraphBlockingSentinelLock,
		{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: live.Key}},
		{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: deleted.Key}},
		{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "guard/missing"}},
		{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: opaque.Key}},
	}
	keys = append(keys, batchKeys...)
	sort.Slice(keys, func(i, j int) bool { return keyID(keys[i]) < keyID(keys[j]) })
	requests := make(driver.LockSet, len(keys))
	selected := make([]driver.LockKey, 0, len(keys)-1)
	for i, key := range keys {
		mode := driver.LockUpdate
		if keyID(key) == keyID(skipped) {
			mode = driver.LockSkipLocked
		} else {
			selected = append(selected, key)
		}
		requests[i] = driver.LockRequest{Key: key, Mode: mode}
	}
	observer := getReader(t, ctx, s, getMeta(t, ctx, s), nil)
	want, err := observer.Observe(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	got, err := writer.Lock(ctx, requests)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("complete locked states differ from independent pinned scalar observation:\ngot=%+v\nwant=%+v", got, want)
	}
	staged := driver.DerivedRow{Key: live.Key, Value: []byte("staged")}
	if err := writer.PutDerived(ctx, driver.DerivedAttribute, nil, []driver.DerivedRow{staged}); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: staged.Key}}})
	if err != nil || len(state) != 1 || state[0].Derived == nil || string(state[0].Derived.Value) != "staged" {
		t.Fatalf("locked writer lost staged derived overlay: %+v %v", state, err)
	}
	t.Logf("complete %d selected states match pinned scalar observation across multiple batches with live/deleted/missing records, facts, sentinels, populated/missing/opaque derived rows and a skipped guard", len(got))
}

func guardProbe(t *testing.T, ctx context.Context, path string, share, acquired bool) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestLegacyGuardProtocolChild$", "-test.v")
	mode := "update"
	if share {
		mode = "share"
	}
	cmd.Env = append(os.Environ(), "PLUG_GUARD_PROBE_PATH="+path, "PLUG_GUARD_PROBE_MODE="+mode)
	output, err := cmd.CombinedOutput()
	want := fmt.Sprintf("legacy guard acquired=%v", acquired)
	if err != nil || !strings.Contains(string(output), want) {
		t.Fatalf("legacy process probe want %q: %v\n%s", want, err, output)
	}
}

func TestLegacyGuardProtocolChild(t *testing.T) {
	path := os.Getenv("PLUG_GUARD_PROBE_PATH")
	if path == "" {
		return
	}
	f, ok, err := tryFileLock(path, os.Getenv("PLUG_GUARD_PROBE_MODE") == "share")
	if err != nil {
		t.Fatal(err)
	}
	defer unlockFile(f)
	t.Logf("legacy guard acquired=%v", ok)
}

func TestLegacyGuardKernelMappingLifetime(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "kernel-lifetime.db"))
	path := guardPath(s, testKey("pl-kernel-lifetime"))
	f, ok, err := tryFileLock(path, false)
	if err != nil || !ok {
		t.Fatalf("legacy flock: acquired=%v err=%v", ok, err)
	}
	defer unlockFile(f)
	// Linux retains the open file description through the mapping. No mapped
	// byte is accessed: guard files may remain empty, exactly as before.
	mapping, err := syscall.Mmap(int(f.Fd()), 0, 1, syscall.PROT_NONE, syscall.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if mapping != nil {
			_ = syscall.Munmap(mapping)
		}
	}()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if count := guardDescriptorCount(t, s); count != 0 {
		t.Fatalf("closed mapped flock retained %d descriptors", count)
	}
	guardProbe(t, ctx, path, false, false)
	guardProbe(t, ctx, path, true, false)
	if err := syscall.Munmap(mapping); err != nil {
		t.Fatal(err)
	}
	mapping = nil
	guardProbe(t, ctx, path, false, true)
	t.Log("actual separate legacy flock processes excluded after descriptor close; munmap released exact original guard")
}

func TestGuardModesPreserveLegacyInterprocessProtocol(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "legacy-protocol.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	keys := []driver.LockKey{
		testKey("pl-legacy-record"),
		{Kind: driver.LockReadFact, Fact: "guard/legacy:full-key"},
		driver.GraphBlockingSentinelLock,
		{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: "legacy:row"}},
	}
	for _, key := range keys {
		for _, mode := range []driver.LockMode{driver.LockShare, driver.LockUpdate, driver.LockSkipLocked} {
			t.Run(keyID(key)+"/"+string(mode), func(t *testing.T) {
				tx, err := s.Write(ctx, lease)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Close()
				states, err := tx.Lock(ctx, driver.LockSet{{Key: key, Mode: mode}})
				if err != nil || len(states) != 1 || keyID(states[0].Key) != keyID(key) {
					t.Fatalf("lock states=%v err=%v", states, err)
				}
				guardProbe(t, ctx, guardPath(s, key), false, false)
				guardProbe(t, ctx, guardPath(s, key), true, mode == driver.LockShare)
				if err := tx.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
				guardProbe(t, ctx, guardPath(s, key), false, true)
			})
		}
	}
}
