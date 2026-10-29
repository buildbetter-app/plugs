package runtime

import (
	"context"
	"errors"
	"os"
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

func testRef(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
