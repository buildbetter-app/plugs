package sqlite

import (
	"context"
	"sync"
	"time"
)

// CommitPhase names bounded, request-local measurements of SQLite Commit.
// These samples describe elapsed time and staged input size; they do not
// claim SQL row visits, lock ownership, or a cause for an expired lease.
type CommitPhase string

const (
	CommitPhaseTxLock              CommitPhase = "transaction_lock"
	CommitPhaseValidate            CommitPhase = "validate"
	CommitPhasePublicationLock     CommitPhase = "publication_lock"
	CommitPhaseConnection          CommitPhase = "connection"
	CommitPhaseBegin               CommitPhase = "begin_immediate"
	CommitPhaseFenceBefore         CommitPhase = "fence_before"
	CommitPhaseBody                CommitPhase = "body"
	CommitPhaseReplay              CommitPhase = "replay"
	CommitPhaseGuards              CommitPhase = "guards_and_validation"
	CommitPhaseEncode              CommitPhase = "encode_changeset"
	CommitPhasePrepareSource       CommitPhase = "prepare_source"
	CommitPhaseAllocatorJournal    CommitPhase = "allocator_journal"
	CommitPhaseRecords             CommitPhase = "records_and_versions"
	CommitPhaseSourcePublish       CommitPhase = "source_publish"
	CommitPhaseDerivedKeys         CommitPhase = "derived_keys"
	CommitPhaseSelection           CommitPhase = "selection"
	CommitPhaseSelectionPrepare    CommitPhase = "selection_prepare"
	CommitPhaseSelectionSources    CommitPhase = "selection_sources"
	CommitPhaseSelectionInvalidate CommitPhase = "selection_source_invalidation"
	CommitPhaseSelectionHistory    CommitPhase = "selection_source_history"
	CommitPhaseSelectionClose      CommitPhase = "selection_close_rows"
	CommitPhaseSelectionInsert     CommitPhase = "selection_insert_rows"
	CommitPhaseSelectionCoverage   CommitPhase = "selection_coverage"
	CommitPhaseTail                CommitPhase = "tail_and_replay"
	CommitPhaseFenceAfter          CommitPhase = "fence_after"
	CommitPhaseSQLiteCommit        CommitPhase = "sqlite_commit"
	CommitPhaseRollback            CommitPhase = "rollback_cleanup"
	CommitPhaseFinish              CommitPhase = "transaction_cleanup"
)

// CommitPhaseSample contains no source identifiers or payload bytes. Staged
// counts are input cardinalities, not the number of rows visited by SQLite.
type CommitPhaseSample struct {
	Phase    CommitPhase
	Duration time.Duration
	Staged   int
	Failed   bool
}

// CommitTrace holds one Commit's bounded phase samples. It is optional and
// remains in memory; Commit never persists or logs it. Use a fresh trace for
// each Commit and call Snapshot after Commit returns.
type CommitTrace struct {
	mu      sync.Mutex
	samples [32]CommitPhaseSample
	count   int
}

type commitTraceKey struct{}

// WithCommitTrace attaches an optional diagnostic to one Commit call. A nil
// trace leaves the context and ordinary Commit behavior unchanged.
func WithCommitTrace(ctx context.Context, trace *CommitTrace) context.Context {
	if trace == nil {
		return ctx
	}
	return context.WithValue(ctx, commitTraceKey{}, trace)
}

func commitTraceFromContext(ctx context.Context) *CommitTrace {
	trace, _ := ctx.Value(commitTraceKey{}).(*CommitTrace)
	return trace
}

func traceStart(trace *CommitTrace) time.Time {
	if trace == nil {
		return time.Time{}
	}
	return time.Now()
}

func (trace *CommitTrace) record(phase CommitPhase, started time.Time, staged int, err error) {
	if trace == nil || started.IsZero() {
		return
	}
	sample := CommitPhaseSample{Phase: phase, Duration: time.Since(started), Staged: staged, Failed: err != nil}
	trace.mu.Lock()
	if trace.count < len(trace.samples) {
		trace.samples[trace.count] = sample
		trace.count++
	}
	trace.mu.Unlock()
}

// Snapshot returns detached samples in completion order. Nested phases may
// overlap their enclosing body or selection duration.
func (trace *CommitTrace) Snapshot() []CommitPhaseSample {
	if trace == nil {
		return nil
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return append([]CommitPhaseSample(nil), trace.samples[:trace.count]...)
}

type commitPhaseTracker struct {
	trace   *CommitTrace
	phase   CommitPhase
	started time.Time
	staged  int
}

func (tracker *commitPhaseTracker) next(phase CommitPhase, staged int) {
	if tracker.trace == nil {
		return
	}
	tracker.trace.record(tracker.phase, tracker.started, tracker.staged, nil)
	tracker.phase, tracker.staged, tracker.started = phase, staged, time.Now()
}

func (tracker *commitPhaseTracker) finish(err error) {
	if tracker.trace != nil {
		tracker.trace.record(tracker.phase, tracker.started, tracker.staged, err)
	}
}
