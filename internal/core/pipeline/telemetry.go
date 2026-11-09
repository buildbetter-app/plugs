package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Phase is the fixed set of write-pipeline lanes. Keeping this vocabulary
// closed means an operation's identifiers, project names, and callback names
// can never become metric labels.
type Phase string

const (
	PhaseAdmission Phase = "admission"
	PhaseApply     Phase = "apply"
	PhaseReplay    Phase = "replay"
	PhasePrepare   Phase = "prepare"
	PhasePre       Phase = "pre"
	PhaseGraph     Phase = "graph"
	PhaseClaims    Phase = "claims"
	PhaseChanges   Phase = "changes"
	PhaseAudit     Phase = "audit"
	PhaseCommit    Phase = "commit"

	// Commit subphases partition transact before transaction cleanup. The
	// existing PhaseCommit still measures the complete transact call.
	PhaseCommitProjections Phase = "commit_projections"
	PhaseCommitLockPlan    Phase = "commit_lock_plan"
	PhaseCommitWrite       Phase = "commit_write"
	PhaseCommitLock        Phase = "commit_lock"
	PhaseCommitHead        Phase = "commit_head"
	PhaseCommitGuards      Phase = "commit_guards"
	PhaseCommitPrepareRows Phase = "commit_prepare_rows"
	PhaseCommitSources     Phase = "commit_sources"
	PhaseCommitAncestors   Phase = "commit_ancestors"
	PhaseCommitEvents      Phase = "commit_events"
	PhaseCommitDriver      Phase = "commit_driver"
	PhaseCommitNoop        Phase = "commit_noop"
	PhasePost              Phase = "post"
	PhaseResponse          Phase = "response"
	phaseOther             Phase = "other"
)

var phases = [...]Phase{
	PhaseAdmission,
	PhaseApply,
	PhaseReplay,
	PhasePrepare,
	PhasePre,
	PhaseGraph,
	PhaseClaims,
	PhaseChanges,
	PhaseAudit,
	PhaseCommit,
	PhaseCommitProjections,
	PhaseCommitLockPlan,
	PhaseCommitWrite,
	PhaseCommitLock,
	PhaseCommitHead,
	PhaseCommitGuards,
	PhaseCommitPrepareRows,
	PhaseCommitSources,
	PhaseCommitAncestors,
	PhaseCommitEvents,
	PhaseCommitDriver,
	PhaseCommitNoop,
	PhasePost,
	PhaseResponse,
	phaseOther,
}

// Result is the fixed set of pipeline outcomes used by lane and write
// counters. It deliberately does not contain a model error code or callback
// identity: those values are either private or unbounded.
type Result string

const (
	ResultSuccess  Result = "success"
	ResultCommit   Result = "commit"
	ResultReplay   Result = "replay"
	ResultRetry    Result = "retry"
	ResultRefusal  Result = "refusal"
	ResultCanceled Result = "canceled"
	ResultConflict Result = "conflict"
	ResultPending  Result = "pending"
	ResultWarning  Result = "warning"
	ResultDryRun   Result = "dry_run"
	ResultError    Result = "error"
	resultOther    Result = "other"
)

var results = [...]Result{
	ResultSuccess,
	ResultCommit,
	ResultReplay,
	ResultRetry,
	ResultRefusal,
	ResultCanceled,
	ResultConflict,
	ResultPending,
	ResultWarning,
	ResultDryRun,
	ResultError,
	resultOther,
}

// ConflictReason is also closed. A conflict counter never contains a record,
// project, request, or storage error string.
type ConflictReason string

const (
	ConflictStaleSnapshot ConflictReason = "stale_snapshot"
	ConflictDriver        ConflictReason = "driver"
	conflictOther         ConflictReason = "other"
)

var conflictReasons = [...]ConflictReason{
	ConflictStaleSnapshot,
	ConflictDriver,
	conflictOther,
}

// Observer is the internal, non-durable telemetry seam for a Writer. An
// observer receives only bounded enums and a duration; it never sees a write
// request, response, error, principal, or record body. Implementations must be
// concurrency safe and return promptly without I/O. Calls run outside storage
// transactions; an observer panic cannot change a write's result.
type Observer interface {
	ObservePhase(Phase, time.Duration, Result)
	ObserveConflict(ConflictReason)
	ObserveOutcome(Result)
}

type laneSample struct {
	count uint64
	sumMS float64
}

// Collector is an in-process bounded observer. Its arrays are sized by the
// closed vocabularies above, so repeated high-cardinality requests cannot grow
// memory or Prometheus series cardinality.
type Collector struct {
	mu        sync.Mutex
	lanes     [len(phases)][len(results)]laneSample
	conflicts [len(conflictReasons)]uint64
	outcomes  [len(results)]uint64
}

func NewCollector() *Collector { return &Collector{} }

// NewWithTelemetry composes a Writer with a fresh bounded collector. The
// collector has no goroutines and needs no shutdown call.
func NewWithTelemetry(store driver.Store, lease driver.ActiveLease, options ...Option) (*Writer, *Collector) {
	collector := NewCollector()
	options = append(options, WithObserver(collector))
	return New(store, lease, options...), collector
}

func (c *Collector) ObservePhase(phase Phase, duration time.Duration, result Result) {
	if c == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	pi := phaseIndex(phase)
	ri := resultIndex(result)
	c.mu.Lock()
	c.lanes[pi][ri].count++
	c.lanes[pi][ri].sumMS += float64(duration) / float64(time.Millisecond)
	c.mu.Unlock()
}

func (c *Collector) ObserveConflict(reason ConflictReason) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.conflicts[conflictIndex(reason)]++
	c.mu.Unlock()
}

func (c *Collector) ObserveOutcome(result Result) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.outcomes[resultIndex(result)]++
	c.mu.Unlock()
}

// Snapshot is a copy of the collector state. The maps contain only the fixed
// phase/result/reason values and are safe for callers to inspect or compare.
type Snapshot struct {
	LaneCount  map[Phase]map[Result]uint64
	LaneMillis map[Phase]map[Result]float64
	Conflicts  map[ConflictReason]uint64
	Outcomes   map[Result]uint64
}

func (c *Collector) Snapshot() Snapshot {
	result := Snapshot{
		LaneCount:  make(map[Phase]map[Result]uint64, len(phases)),
		LaneMillis: make(map[Phase]map[Result]float64, len(phases)),
		Conflicts:  make(map[ConflictReason]uint64, len(conflictReasons)),
		Outcomes:   make(map[Result]uint64, len(results)),
	}
	if c == nil {
		return result
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for pi, phase := range phases {
		counts := make(map[Result]uint64, len(results))
		millis := make(map[Result]float64, len(results))
		for ri, value := range results {
			counts[value] = c.lanes[pi][ri].count
			millis[value] = c.lanes[pi][ri].sumMS
		}
		result.LaneCount[phase] = counts
		result.LaneMillis[phase] = millis
	}
	for i, reason := range conflictReasons {
		result.Conflicts[reason] = c.conflicts[i]
	}
	for i, value := range results {
		result.Outcomes[value] = c.outcomes[i]
	}
	return result
}

// SeriesCount is the fixed number of exposed time series: a sum and count per
// lane, plus conflict and outcome counters. It cannot grow with request data.
func (c *Collector) SeriesCount() int {
	return 2*len(phases)*len(results) + len(conflictReasons) + len(results)
}

// Prometheus returns deterministic text exposition without a client library
// or network exporter. Zero-valued fixed series are included intentionally so
// the output's shape is stable across hosts and workloads.
func (c *Collector) Prometheus() string {
	var builder strings.Builder
	_ = c.Render(&builder)
	return builder.String()
}

// Render writes deterministic Prometheus text. lane_ms is a cumulative
// summary without quantiles; its sum and count permit aggregation across
// scrapes and processes without storing individual observations.
func (c *Collector) Render(w io.Writer) error {
	if w == nil {
		return errors.New("pipeline telemetry: nil exposition writer")
	}
	snapshot := c.Snapshot()
	if _, err := fmt.Fprintln(w, "# HELP lane_ms Write-pipeline lane duration in milliseconds."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE lane_ms summary"); err != nil {
		return err
	}
	for _, phase := range phases {
		for _, result := range results {
			count := snapshot.LaneCount[phase][result]
			labels := fmt.Sprintf(`phase="%s",result="%s"`, phase, result)
			if _, err := fmt.Fprintf(w, "lane_ms_sum{%s} %.6f\n", labels, snapshot.LaneMillis[phase][result]); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "lane_ms_count{%s} %d\n", labels, count); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintln(w, "# HELP write_conflict_total Write attempts that observed a bounded conflict."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE write_conflict_total counter"); err != nil {
		return err
	}
	for _, reason := range conflictReasons {
		if _, err := fmt.Fprintf(w, "write_conflict_total{reason=\"%s\"} %d\n", reason, snapshot.Conflicts[reason]); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w, "# HELP write_outcome_total Completed write-pipeline outcomes."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "# TYPE write_outcome_total counter"); err != nil {
		return err
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(w, "write_outcome_total{result=\"%s\"} %d\n", result, snapshot.Outcomes[result]); err != nil {
			return err
		}
	}
	return nil
}

// ServeHTTP is a reusable renderer only. Composition deliberately does not
// mount it as a public route; an operator may wrap it in a separately
// authorized endpoint later without adding a second writer or exporter here.
func (c *Collector) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := c.Render(w); err != nil {
		http.Error(w, "telemetry unavailable", http.StatusInternalServerError)
	}
}

func phaseIndex(value Phase) int {
	for i, candidate := range phases {
		if candidate == value {
			return i
		}
	}
	return len(phases) - 1
}

func resultIndex(value Result) int {
	for i, candidate := range results {
		if candidate == value {
			return i
		}
	}
	return len(results) - 1
}

func conflictIndex(value ConflictReason) int {
	for i, candidate := range conflictReasons {
		if candidate == value {
			return i
		}
	}
	return len(conflictReasons) - 1
}

func pendingResponse(err error) bool {
	var typed *model.Error
	if errors.As(err, &typed) && typed != nil && typed.Data.Code == "busy" {
		_, known := typed.Data.Details["committedRevision"]
		return known
	}
	return false
}

func classifyPhaseResult(ctx context.Context, err error) Result {
	if err == nil {
		return ResultSuccess
	}
	if pendingResponse(err) {
		return ResultPending
	}
	// Callback failures may wrap caller cancellation in subscriber_unavailable.
	// Classify the observation using the lane context without changing that
	// established business error or treating successful post-commit work as canceled.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		return ResultCanceled
	}
	if errors.Is(err, errStale) || errors.Is(err, driver.ErrConflict) {
		return ResultRetry
	}
	var typed *model.Error
	if errors.As(err, &typed) && typed != nil {
		switch typed.Data.Code {
		case "write_conflict", "stale_snapshot":
			return ResultConflict
		case "internal_error", "subscriber_unavailable", "authz_unavailable", "busy":
			return ResultError
		default:
			return ResultRefusal
		}
	}
	return ResultError
}

func classifyOutcome(ctx context.Context, outcome port.WriteOutcome, err error, committed, dryRun bool) Result {
	if err == nil {
		if outcome.Replayed {
			return ResultReplay
		}
		if dryRun {
			return ResultDryRun
		}
		return ResultCommit
	}
	if committed {
		// The graph transaction is already durable. A lost or delayed response
		// is reported as pending rather than as a second failed write.
		return ResultPending
	}
	result := classifyPhaseResult(ctx, err)
	if result == ResultRetry {
		return ResultConflict
	}
	return result
}

// deferredPhaseTimings records only fixed, straight-line transaction stages.
// No observer runs from next or finish: transact flushes after its rollback and
// close defers. Each reached stage is recorded once; unreached stages stay zero.
// The closed phase vocabulary bounds both this buffer and exported labels.
type deferredPhaseTimings struct {
	enabled bool
	phase   Phase
	started time.Time
	samples [len(phases)]struct {
		phase    Phase
		duration time.Duration
		result   Result
	}
	count int
}

func (d *deferredPhaseTimings) next(phase Phase) {
	if !d.enabled {
		return
	}
	now := time.Now()
	d.record(now, ResultSuccess)
	d.phase, d.started = phase, now
}

func (d *deferredPhaseTimings) record(ended time.Time, result Result) {
	if d.phase == "" {
		return
	}
	sample := &d.samples[d.count]
	sample.phase, sample.duration, sample.result = d.phase, ended.Sub(d.started), result
	d.count++
	d.phase = ""
}

func (d *deferredPhaseTimings) finish(ctx context.Context, err error) {
	if d.enabled && d.phase != "" {
		d.record(time.Now(), classifyPhaseResult(ctx, err))
	}
}

func (d *deferredPhaseTimings) flush(w *Writer) {
	for _, sample := range d.samples[:d.count] {
		w.observePhaseResult(sample.phase, sample.duration, sample.result)
	}
}

func (w *Writer) observePhaseSince(ctx context.Context, phase Phase, started time.Time, err error) {
	w.observePhaseResult(phase, time.Since(started), classifyPhaseResult(ctx, err))
}

func (w *Writer) observePhaseResult(phase Phase, duration time.Duration, result Result) {
	if w == nil || w.cfg.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	w.cfg.observer.ObservePhase(phase, duration, result)
}

func (w *Writer) observeConflict(reason ConflictReason) {
	if w == nil || w.cfg.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	w.cfg.observer.ObserveConflict(reason)
}

func (w *Writer) observeOutcome(result Result) {
	if w == nil || w.cfg.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	w.cfg.observer.ObserveOutcome(result)
}
