package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func guardPath(s *Store, key driver.LockKey) string {
	hash := sha256.Sum256([]byte("guard:" + keyID(key)))
	return filepath.Join(s.lockDir, hex.EncodeToString(hash[:]))
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

func TestCompleteRestoreGuardFamiliesMeasureDescriptorAndVMACapacity(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "restore-guard-capacity.db"))
	requests := restoreGuardRequests(20000)
	if err := driver.ValidateLockSet(requests); err != nil {
		t.Fatal(err)
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
	if len(states) != len(requests) || len(tx.guards) != len(requests) {
		t.Fatalf("restore guard acquisition states=%d handles=%d want %d", len(states), len(tx.guards), len(requests))
	}
	afterFD, afterVMA := guardDescriptorCount(t, s), guardVMACount(t, s)
	t.Logf("actual restore guard set records=%d bead_sentinels=%d closure_sentinels=%d ancestor_rows=%d graph_sentinel=1 total=%d descriptors=%d vmas=%d", 20000, 20000, 20000, 20000, len(requests), afterFD-beforeFD, afterVMA-beforeVMA)
	if afterFD-beforeFD > 1 {
		t.Fatalf("restore guard acquisition retained %d descriptors for %d keys", afterFD-beforeFD, len(requests))
	}
	if afterVMA-beforeVMA != len(requests) {
		t.Fatalf("restore guard mappings=%d, want one VMA per key (%d)", afterVMA-beforeVMA, len(requests))
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if got := guardDescriptorCount(t, s); got != beforeFD {
		t.Fatalf("restore guard release retained descriptors: before=%d after=%d", beforeFD, got)
	}
	if got := guardVMACount(t, s); got != beforeVMA {
		t.Fatalf("restore guard release retained VMAs: before=%d after=%d", beforeVMA, got)
	}
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
			lease.signalLost()
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
	// and the real per-key flock/mmap and SQL paths still have to unwind.
	loseOnFence.Store(true)
	states, lockErr := tx.Lock(ctx, requests)
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if lockErr == nil {
		t.Fatalf("Lock published %d states after lease loss; its guard mappings were already queued for release", len(states))
	}
	requireFenced(t, lockErr)
	if len(states) != 0 {
		t.Fatalf("fenced acquisition returned %d states", len(states))
	}
	if got := guardDescriptorCount(t, s); got != beforeFD {
		t.Fatalf("fenced acquisition retained descriptors: before=%d after=%d", beforeFD, got)
	}
	if got := guardVMACount(t, s); got != beforeVMA {
		t.Fatalf("fenced acquisition retained VMAs: before=%d after=%d", beforeVMA, got)
	}
	for _, request := range requests {
		guardProbe(t, ctx, guardPath(s, request.Key), false, true)
	}
	t.Logf("lease loss rejected the complete %d-key set and released every original flock/mapping", len(requests))
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
