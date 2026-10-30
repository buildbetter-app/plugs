package runtime

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestLockOrderIsGlobalAndDeterministic(t *testing.T) {
	ref := testRef("e5-lock")
	keys := queueKeys(ref, true)
	if err := driver.ValidateLockKeys(keys); err != nil {
		t.Fatal(err)
	}
	if len(keys) < 4 {
		t.Fatalf("lock set=%v", keys)
	}
	unsorted := append([]driver.LockKey(nil), keys...)
	unsorted[0], unsorted[1] = unsorted[1], unsorted[0]
	if err := driver.ValidateLockKeys(unsorted); err == nil {
		t.Fatal("unsorted lock set accepted")
	}
	duplicate := append([]driver.LockKey(nil), keys...)
	duplicate[1] = duplicate[0]
	if err := driver.ValidateLockKeys(duplicate); err == nil {
		t.Fatal("duplicate lock set accepted")
	}
}

func TestProbeRunNamespaceAndClaimCountAreFailClosed(t *testing.T) {
	first := newRunID()
	second := newRunID()
	if first == second {
		t.Fatalf("generated run IDs collided: %q", first)
	}
	if _, err := (ProbeConfig{RunID: "bad run"}).normalized(); err == nil {
		t.Fatal("invalid run ID accepted")
	}
	c := &counters{}
	if err := requireCurrentClaims(c, 1); err == nil || !strings.Contains(err.Error(), "current run claimed 0 records") {
		t.Fatalf("zero current claims accepted: %v", err)
	}
	c.claimed.Store(1)
	if err := requireCurrentClaims(c, 1); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryE5ProbeRunsConcurrentClaimersAndMutators(t *testing.T) {
	report, err := RunMemory(context.Background(), ProbeConfig{
		Records: 200, Claimers: 200, Mutators: 200, Deadline: 45 * time.Second, Seed: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Claimed != 200 || report.Mutations < 200 {
		t.Fatalf("report=%+v", report)
	}
	if !report.NoDeadlock || report.Deadlocks != 0 || !report.StaleLeaseClosed {
		t.Fatalf("concurrency safety report=%+v", report)
	}
	if report.Conflicts == 0 || report.Retries == 0 {
		t.Fatalf("retry instrumentation did not observe the injected conflict: %+v", report)
	}
	if report.Failover.Choice != "managed-ha-postgres" || !report.Failover.RequiresNewIncarnation || !report.Failover.ObservedEpochIncrease {
		t.Fatalf("failover choice/report=%+v", report.Failover)
	}
	t.Logf("E5 memory: claimed=%d mutated=%d examined=%d skipped=%d retries=%d fenced=%d duration=%s", report.Claimed, report.Mutations, report.CandidatesExamined, report.SkippedLocked, report.Retries, report.Fenced, report.Duration)
}

func TestPostgresE5ProbeWhenDSNIsProvided(t *testing.T) {
	dsn := os.Getenv("E5_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set E5_POSTGRES_DSN to run the disposable PostgreSQL lane")
	}
	report, err := RunPostgres(context.Background(), dsn, ProbeConfig{Records: 200, Claimers: 200, Mutators: 200, Deadline: 90 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if report.Claimed != 200 || report.Mutations < 200 || !report.NoDeadlock || !report.StaleLeaseClosed {
		t.Fatalf("postgres report=%+v", report)
	}
	t.Logf("E5 postgres: claimed=%d mutated=%d retries=%d fenced=%d duration=%s", report.Claimed, report.Mutations, report.Retries, report.Fenced, report.Duration)
}

func TestStaleLeaseNeverPublishesAWrite(t *testing.T) {
	// The full probe exercises this path after the 200-agent run.  Keep a
	// focused assertion here so a future harness optimization cannot remove the
	// fail-closed check while retaining a green stress count.
	report, err := RunMemory(context.Background(), ProbeConfig{Records: 2, Claimers: 2, Mutators: 2, Deadline: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if report.Fenced < 1 {
		t.Fatal(errors.New("stale lease was not fenced"))
	}
}

func TestRetryCountersConflictThenSuccess(t *testing.T) {
	c := &counters{}
	calls := 0
	err := retry(context.Background(), c, func() error {
		calls++
		if calls == 1 {
			return driver.ErrConflict
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || c.attempts.Load() != 2 || c.retries.Load() != 1 || c.conflicts.Load() != 1 || c.mutations.Load() != 1 {
		t.Fatalf("conflict then success counters: calls=%d attempts=%d retries=%d conflicts=%d mutations=%d", calls, c.attempts.Load(), c.retries.Load(), c.conflicts.Load(), c.mutations.Load())
	}
}

func TestRetryCountersMultipleConflictsThenSuccess(t *testing.T) {
	c := &counters{}
	calls := 0
	err := retry(context.Background(), c, func() error {
		calls++
		if calls < 3 {
			return driver.ErrConflict
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || c.attempts.Load() != 3 || c.retries.Load() != 2 || c.conflicts.Load() != 2 || c.mutations.Load() != 1 {
		t.Fatalf("multiple conflicts then success counters: calls=%d attempts=%d retries=%d conflicts=%d mutations=%d", calls, c.attempts.Load(), c.retries.Load(), c.conflicts.Load(), c.mutations.Load())
	}
}

func TestRetryCountersConflictThenNonRetryableError(t *testing.T) {
	c := &counters{}
	calls := 0
	want := errors.New("validation failed")
	err := retry(context.Background(), c, func() error {
		calls++
		if calls == 1 {
			return driver.ErrConflict
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("error=%v, want %v", err, want)
	}
	if calls != 2 || c.attempts.Load() != 2 || c.retries.Load() != 1 || c.conflicts.Load() != 1 || c.mutations.Load() != 0 {
		t.Fatalf("conflict then nonretryable counters: calls=%d attempts=%d retries=%d conflicts=%d mutations=%d", calls, c.attempts.Load(), c.retries.Load(), c.conflicts.Load(), c.mutations.Load())
	}
}

func TestRetryCountersInitialPreCommitFailure(t *testing.T) {
	c := &counters{}
	want := errors.New("pre-commit validation failed")
	err := retry(context.Background(), c, func() error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error=%v, want %v", err, want)
	}
	if c.attempts.Load() != 1 || c.retries.Load() != 0 || c.conflicts.Load() != 0 || c.mutations.Load() != 0 {
		t.Fatalf("initial nonretryable counters: attempts=%d retries=%d conflicts=%d mutations=%d", c.attempts.Load(), c.retries.Load(), c.conflicts.Load(), c.mutations.Load())
	}
}

func TestCommitErrorCountersIncludeClaimConflict(t *testing.T) {
	c := &counters{}
	if err := countError(c, driver.ErrConflict); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("countError returned %v", err)
	}
	if c.conflicts.Load() != 1 {
		t.Fatalf("claim conflict was not counted: conflicts=%d", c.conflicts.Load())
	}
	if err := countError(c, errors.New("serialization failure")); err == nil {
		t.Fatal("serialization error unexpectedly nil")
	}
	if c.conflicts.Load() != 2 {
		t.Fatalf("serialization conflict was not counted: conflicts=%d", c.conflicts.Load())
	}
}

func TestRetryCountersExhaustedBudget(t *testing.T) {
	c := &counters{}
	calls := 0
	err := retry(context.Background(), c, func() error {
		calls++
		return driver.ErrConflict
	})
	if err == nil || !strings.Contains(err.Error(), "retry budget exhausted") {
		t.Fatalf("error=%v, want exhausted budget", err)
	}
	if calls != 3 || c.attempts.Load() != 3 || c.retries.Load() != 2 || c.conflicts.Load() != 3 || c.mutations.Load() != 0 {
		t.Fatalf("exhausted counters: calls=%d attempts=%d retries=%d conflicts=%d mutations=%d", calls, c.attempts.Load(), c.retries.Load(), c.conflicts.Load(), c.mutations.Load())
	}
}

func TestMemoryE5ProbeCountsInjectedAttempt(t *testing.T) {
	report, err := RunMemory(context.Background(), ProbeConfig{Records: 1, Claimers: 1, Mutators: 1, Deadline: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// Claimed and successful mutation operations each have at least one
	// execution. The deliberately failed stale commit is one additional
	// executed attempt and must therefore be represented in this denominator.
	if report.Attempts < report.Claimed+report.Mutations+1 {
		t.Fatalf("injected attempt omitted: attempts=%d claimed=%d mutations=%d report=%+v", report.Attempts, report.Claimed, report.Mutations, report)
	}
	if report.Conflicts < 1 || report.Retries < 1 {
		t.Fatalf("injected conflict counters=%+v", report)
	}
}

func testRef(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
