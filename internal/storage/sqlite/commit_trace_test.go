package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestCommitTraceSeparatesPublicationAndFailureWithoutChangingHead(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "commit-trace.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	tx, header := beginRecord(t, ctx, s, lease, "trace-success")
	if err := tx.Put(ctx, driver.Record{Ref: testRef("trace-success"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	trace := &CommitTrace{}
	if _, err := tx.Commit(WithCommitTrace(ctx, trace), header); err != nil {
		t.Fatal(err)
	}
	samples := trace.Snapshot()
	for _, phase := range []CommitPhase{CommitPhaseTxLock, CommitPhasePublicationLock, CommitPhaseBegin,
		CommitPhaseRecords, CommitPhaseSelection, CommitPhaseSelectionSources,
		CommitPhaseSelectionInvalidate, CommitPhaseSelectionHistory, CommitPhaseSQLiteCommit, CommitPhaseFinish} {
		sample, ok := commitTraceSample(samples, phase)
		if !ok || sample.Failed || sample.Duration < 0 {
			t.Fatalf("successful Commit phase %s missing or failed: %+v", phase, samples)
		}
	}
	if sample, _ := commitTraceSample(samples, CommitPhaseRecords); sample.Staged != 1 {
		t.Fatalf("record phase staged count = %d, want 1", sample.Staged)
	}
	if len(samples) > len(trace.samples) {
		t.Fatalf("successful Commit trace exceeded fixed capacity: %d", len(samples))
	}
	// Snapshot must detach the request-local data from later callers.
	samples[0].Phase = "tampered"
	if trace.Snapshot()[0].Phase == "tampered" {
		t.Fatal("trace snapshot aliases internal samples")
	}
	head := getMeta(t, ctx, s).Head
	bad, badHeader := beginRecord(t, ctx, s, lease, "trace-refusal")
	if err := bad.Put(ctx, driver.Record{Ref: testRef("trace-refusal"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	badHeader.Changes = nil // staged record has no declared ChangeRecord
	failed := &CommitTrace{}
	if _, err := bad.Commit(WithCommitTrace(ctx, failed), badHeader); err == nil {
		t.Fatal("undeclared staged record committed")
	}
	failedSamples := failed.Snapshot()
	for _, phase := range []CommitPhase{CommitPhaseGuards, CommitPhaseBody} {
		sample, ok := commitTraceSample(failedSamples, phase)
		if !ok || !sample.Failed {
			t.Fatalf("failed Commit phase %s absent: %+v", phase, failedSamples)
		}
	}
	if _, ok := commitTraceSample(failedSamples, CommitPhaseSQLiteCommit); ok {
		t.Fatal("failed validation reached SQLite COMMIT")
	}
	if sample, ok := commitTraceSample(failedSamples, CommitPhaseRollback); !ok || sample.Duration < 0 {
		t.Fatalf("failed Commit rollback phase absent: %+v", failedSamples)
	}
	if got := getMeta(t, ctx, s).Head; got != head {
		t.Fatalf("failed Commit advanced Head: %s -> %s", head, got)
	}
}

func commitTraceSample(samples []CommitPhaseSample, phase CommitPhase) (CommitPhaseSample, bool) {
	for _, sample := range samples {
		if sample.Phase == phase {
			return sample, true
		}
	}
	return CommitPhaseSample{}, false
}
