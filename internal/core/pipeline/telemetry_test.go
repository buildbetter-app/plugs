package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (h *harness) telemetryWriter(options ...Option) (*Writer, *Collector) {
	base := []Option{
		WithClock(func() time.Time { return h.now }),
		WithIDGenerator(func() (string, error) { return fmt.Sprintf("pl-%d", h.ids.Add(1)), nil }),
		WithOntology(testOntology{}),
	}
	return NewWithTelemetry(h.store, h.lease, append(base, options...)...)
}

func TestGenericClaimReleaseRefusalPrecedesCancellation(t *testing.T) {
	for _, entry := range []string{"Apply", "ApplyWithOutcome"} {
		t.Run(entry, func(t *testing.T) {
			h := setup(t)
			writer, collector := h.telemetryWriter()
			before := collector.Snapshot()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m := meta(1)
			m.Method = "bead.release"
			cs := model.ChangeSet{Ops: []model.Op{create("generic-release")}}
			var err error
			if entry == "Apply" {
				_, err = writer.Apply(ctx, by, cs, m)
			} else {
				_, err = writer.ApplyWithOutcome(ctx, by, cs, m)
			}
			code(t, err, "invalid_params")
			if !reflect.DeepEqual(collector.Snapshot(), before) {
				t.Fatal("generic release entered pipeline telemetry before its public refusal")
			}

			// The protected entry point must still honor caller cancellation.
			_, err = writer.ApplyClaimRelease(ctx, by, model.Ref{ID: "pl-claimed"}, "", m)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("protected release error=%v, want context.Canceled", err)
			}
			if got := collector.Snapshot().Outcomes[ResultCanceled]; got != 1 {
				t.Fatalf("protected cancellation outcomes=%d want 1", got)
			}
			assertEmpty(t, h)
		})
	}
}

func TestCollectorObservesSuccessfulCommitAndDeterministicExposition(t *testing.T) {
	h := setup(t)
	w, collector := h.telemetryWriter()
	meta := meta(1)
	meta.Project = "private-project-telemetry"
	result, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpCreate, Create: &model.CreateOp{
			Key:    "private-request-key",
			Type:   "issue",
			Fields: map[string]any{"title": "private-payload-text"},
		}},
	}}, meta)
	if err != nil {
		t.Fatal(err)
	}
	if result.Revision.String() != "1" {
		t.Fatalf("revision=%s", result.Revision)
	}

	snapshot := collector.Snapshot()
	if got := snapshot.Outcomes[ResultCommit]; got != 1 {
		t.Fatalf("commit outcomes=%d want 1", got)
	}
	if got := snapshot.LaneCount[PhaseApply][ResultCommit]; got != 1 {
		t.Fatalf("apply commit observations=%d want 1", got)
	}
	if got := snapshot.LaneCount[PhaseCommit][ResultSuccess]; got != 1 {
		t.Fatalf("commit success observations=%d want 1", got)
	}

	exposition := collector.Prometheus()
	if exposition != collector.Prometheus() {
		t.Fatal("Prometheus exposition is not deterministic")
	}
	for _, private := range []string{"private-project-telemetry", "private-request-key", "private-payload-text", "nikhil", result.Results[0].Ref.ID} {
		if strings.Contains(exposition, private) {
			t.Fatalf("private value %q appeared in exposition", private)
		}
	}
	if !strings.Contains(exposition, "# TYPE lane_ms summary\n") || !strings.Contains(exposition, `lane_ms_sum{phase="commit",result="success"}`) || !strings.Contains(exposition, `write_conflict_total{reason="stale_snapshot"}`) {
		t.Fatalf("exposition omitted fixed metrics:\n%s", exposition)
	}

	response := httptest.NewRecorder()
	collector.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 || response.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" || response.Body.String() != exposition {
		t.Fatalf("HTTP exposition code=%d content-type=%q body matches=%t", response.Code, response.Header().Get("Content-Type"), response.Body.String() == exposition)
	}
}

func TestCollectorCardinalityStaysBoundedForHighCardinalityInputs(t *testing.T) {
	h := setup(t)
	w, collector := h.telemetryWriter()
	initial := metricSeries(t, collector.Prometheus())
	for i := 0; i < 64; i++ {
		writeMeta := meta(100 + i)
		writeMeta.Project = fmt.Sprintf("project-%d-with-private-value", i)
		_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{
			{Kind: model.OpCreate, Create: &model.CreateOp{
				Key:    fmt.Sprintf("request-key-%d", i),
				Type:   "issue",
				Fields: map[string]any{"title": fmt.Sprintf("payload-%d", i)},
			}},
		}}, writeMeta)
		if err != nil {
			t.Fatal(err)
		}
		// Unknown observer enum strings are folded into the fixed "other"
		// buckets, even if an internal caller accidentally passes private text.
		collector.ObservePhase(Phase(writeMeta.Project), time.Millisecond, Result(writeMeta.IdempotencyKey))
		collector.ObserveConflict(ConflictReason(writeMeta.Project))
		collector.ObserveOutcome(Result(writeMeta.Project))
	}
	after := collector.Prometheus()
	if got := metricSeries(t, after); !reflect.DeepEqual(got, initial) {
		t.Fatal("requests changed the exposed metric names/labels")
	}
	if len(initial) != collector.SeriesCount() || len(initial) != 327 {
		t.Fatalf("actual series=%d reported=%d want 327", len(initial), collector.SeriesCount())
	}
	for _, private := range []string{"project-", "request-key-", "payload-", "nikhil", "00000000000000000000000000000064"} {
		if strings.Contains(after, private) {
			t.Fatalf("private input %q leaked into metrics", private)
		}
	}
}

func metricSeries(t *testing.T, exposition string) []string {
	t.Helper()
	var series []string
	seen := map[string]bool{}
	for _, line := range strings.Split(exposition, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, " ")
		n, err := strconv.ParseFloat(value, 64)
		if !ok || err != nil || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) || seen[name] {
			t.Fatalf("invalid or duplicate metric sample: %s", line)
		}
		seen[name] = true
		series = append(series, name)
	}
	return series
}

func TestCollectorObservesStaleFactRetry(t *testing.T) {
	h := setup(t)
	ordinary := h.writer()
	seed := apply(t, ordinary, model.ChangeSet{Ops: []model.Op{create("watched"), create("target")}}, meta(1))
	watched, target := seed.Results[0].Ref, seed.Results[1].Ref
	var calls atomic.Int32
	w, collector := h.telemetryWriter(WithValidators(ValidatorBinding{ID: "reads-policy", Validator: validatorFunc(func(ctx context.Context, _ port.ProposedChange, reader port.SnapshotReader) (port.Verdict, error) {
		if _, err := reader.Get(ctx, []model.Ref{watched}); err != nil {
			return port.Verdict{}, err
		}
		if calls.Add(1) == 1 {
			if _, err := ordinary.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(watched, map[string]any{"title": "changed-during-validation"})}}, meta(2)); err != nil {
				return port.Verdict{}, err
			}
		}
		return port.Verdict{Allow: true}, nil
	})}))

	result, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(target, map[string]any{"title": "committed-after-retry"})}}, meta(3))
	if err != nil || result.Revision.String() != "3" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("validator calls=%d want 2", calls.Load())
	}
	snapshot := collector.Snapshot()
	if got := snapshot.Conflicts[ConflictStaleSnapshot]; got != 1 {
		t.Fatalf("stale conflicts=%d want 1", got)
	}
	if got := snapshot.LaneCount[PhaseCommit][ResultRetry]; got != 1 {
		t.Fatalf("commit retries=%d want 1", got)
	}
	if got := snapshot.Outcomes[ResultCommit]; got != 1 {
		t.Fatalf("commit outcomes=%d want 1", got)
	}
}

func TestCollectorObservesValidatorRefusal(t *testing.T) {
	h := setup(t)
	w, collector := h.telemetryWriter(WithValidators(ValidatorBinding{ID: "deny", Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
		return port.Verdict{Allow: false}, nil
	})}))
	_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("refused")}}, meta(1))
	code(t, err, "ontology_guard_failed")

	snapshot := collector.Snapshot()
	if got := snapshot.Outcomes[ResultRefusal]; got != 1 {
		t.Fatalf("refusal outcomes=%d want 1", got)
	}
	if got := snapshot.LaneCount[PhasePre][ResultRefusal]; got != 1 {
		t.Fatalf("pre refusals=%d want 1", got)
	}
	if got := snapshot.LaneCount[PhaseCommit][ResultSuccess]; got != 0 {
		t.Fatalf("refused write committed=%d", got)
	}
}

func TestCollectorObservesCancellationDuringPrePhase(t *testing.T) {
	h := setup(t)
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, collector := h.telemetryWriter(WithValidators(ValidatorBinding{ID: "wait", Validator: validatorFunc(func(ctx context.Context, _ port.ProposedChange, _ port.SnapshotReader) (port.Verdict, error) {
		close(started)
		<-ctx.Done()
		return port.Verdict{}, ctx.Err()
	})}))
	done := make(chan error, 1)
	go func() {
		_, err := w.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{create("canceled")}}, meta(1))
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("validator did not start")
	}
	cancel()
	select {
	case err := <-done:
		// Preserve the established callback error while observing the canceled
		// caller context. Telemetry must not rewrite business errors.
		code(t, err, "subscriber_unavailable")
	case <-time.After(3 * time.Second):
		t.Fatal("canceled write did not return")
	}

	snapshot := collector.Snapshot()
	if got := snapshot.Outcomes[ResultCanceled]; got != 1 {
		t.Fatalf("canceled outcomes=%d want 1", got)
	}
	if got := snapshot.LaneCount[PhasePre][ResultCanceled]; got != 1 {
		t.Fatalf("pre cancellations=%d want 1", got)
	}
	assertEmpty(t, h)
}

func TestCollectorSumsDurationsAndPropagatesRendererErrors(t *testing.T) {
	collector := NewCollector()
	collector.ObservePhase(PhasePre, time.Millisecond, ResultSuccess)
	collector.ObservePhase(PhasePre, 2*time.Millisecond, ResultSuccess)
	collector.ObservePhase(PhasePre, -time.Millisecond, ResultSuccess)
	text := collector.Prometheus()
	if !strings.Contains(text, "lane_ms_sum{phase=\"pre\",result=\"success\"} 3.000000\n") || !strings.Contains(text, "lane_ms_count{phase=\"pre\",result=\"success\"} 3\n") {
		t.Fatal("summary did not accumulate durations and counts")
	}
	if err := collector.Render(errorWriter{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("render error=%v", err)
	}
	// Caller changes to snapshots cannot corrupt the next scrape.
	snapshot := collector.Snapshot()
	snapshot.LaneCount[PhasePre][ResultSuccess] = 99
	if collector.Prometheus() != text {
		t.Fatal("snapshot mutation changed the collector")
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestCollectorCountsConcurrentSameKeyWritesOncePerCall(t *testing.T) {
	h := setup(t)
	w, collector := h.telemetryWriter()
	cs, m := model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1)
	// Commit before the concurrent replays, so the test asserts exact durable
	// counts independently of scheduling and the four-attempt contention bound.
	first := apply(t, w, cs, m)
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			outcome, err := w.ApplyWithOutcome(context.Background(), by, cs, m)
			if err != nil || !outcome.Replayed || outcome.Commit.Revision.String() != "1" {
				t.Errorf("concurrent replay=%+v err=%v", outcome, err)
			}
			_ = collector.Prometheus()
		})
	}
	wg.Wait()
	snapshot := collector.Snapshot()
	if snapshot.Outcomes[ResultCommit] != 1 || snapshot.Outcomes[ResultReplay] != 12 || snapshot.LaneCount[PhaseCommit][ResultSuccess] != 1 || snapshot.LaneCount[PhaseReplay][ResultSuccess] != 13 {
		t.Fatalf("concurrent observations=%+v", snapshot)
	}
	assertSingleGraphWrite(t, h, first)
}

func TestCollectorObservesDefaultWriterAndSurvivesObserverPanic(t *testing.T) {
	h := setup(t)
	w := h.writer()
	collector, ok := w.cfg.observer.(*Collector)
	if !ok {
		t.Fatal("default writer has no bounded collector")
	}
	apply(t, w, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
	if collector.Snapshot().Outcomes[ResultCommit] != 1 {
		t.Fatal("default writer did not emit its committed outcome")
	}
	apply(t, h.writer(WithObserver(panickingObserver{})), model.ChangeSet{Ops: []model.Op{create("other")}}, meta(2))
	if h.read(t).Snapshot().At.String() != "2" {
		t.Fatal("observer panic changed commit atomicity")
	}
}

type panickingObserver struct{}

func (panickingObserver) ObservePhase(Phase, time.Duration, Result) { panic("observer fault") }
func (panickingObserver) ObserveConflict(ConflictReason)            { panic("observer fault") }
func (panickingObserver) ObserveOutcome(Result)                     { panic("observer fault") }

func TestCollectorObservesDriverConflictExhaustion(t *testing.T) {
	h := setup(t)
	w, collector := h.telemetryWriter()
	w.store = conflictStore{Store: h.store}
	_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
	code(t, err, "write_conflict")
	snapshot := collector.Snapshot()
	if snapshot.Conflicts[ConflictDriver] != 4 || snapshot.Outcomes[ResultConflict] != 1 || snapshot.LaneCount[PhaseCommit][ResultRetry] != 4 {
		t.Fatalf("exhausted observations=%+v", snapshot)
	}
	assertEmpty(t, h)
}

type conflictStore struct{ driver.Store }

func (conflictStore) Write(context.Context, driver.ActiveLease) (driver.WriteTx, error) {
	return nil, driver.ErrConflict
}

func TestCollectorObservesIndeterminateOwnCommitResolvedByReceipt(t *testing.T) {
	h := setup(t)
	w, collector := h.telemetryWriter()
	w.store = lostAckStore{Store: h.store}
	outcome, err := w.ApplyWithOutcome(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
	if err != nil || outcome.Replayed {
		t.Fatalf("resolved outcome=%+v err=%v", outcome, err)
	}
	snapshot := collector.Snapshot()
	if snapshot.Outcomes[ResultReplay] != 0 || snapshot.Outcomes[ResultCommit] != 1 || snapshot.LaneCount[PhaseReplay][ResultSuccess] != 2 || snapshot.LaneCount[PhaseCommit][ResultError] != 1 {
		t.Fatalf("resolved observations=%+v", snapshot)
	}
	assertSingleGraphWrite(t, h, outcome.Commit)
}

type lostAckStore struct{ driver.Store }
type lostAckTx struct{ driver.WriteTx }

func (s lostAckStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	return lostAckTx{WriteTx: tx}, err
}

func (tx lostAckTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	entry, err := tx.WriteTx.Commit(ctx, header)
	if err != nil {
		return entry, err
	}
	return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: header.IdempotencyKey}
}

func TestCollectorKeepsCommittedPostFailureSeparateFromCancellation(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			h := setup(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w, collector := h.telemetryWriter(WithPostSubscribers(PostBinding{ID: "private-post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
				cancel()
				return errors.New("private-post-error-text")
			})}))
			if pending {
				w.store = &finalizerFault{Store: h.store, always: true}
			}
			commit, err := w.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
			want := ResultCommit
			if pending {
				code(t, err, "busy")
				want = ResultPending
			} else if err != nil || len(commit.Warnings) != 1 {
				t.Fatalf("post response=%+v err=%v", commit, err)
			}
			snapshot := collector.Snapshot()
			if snapshot.Outcomes[want] != 1 || snapshot.Outcomes[ResultCanceled] != 0 || snapshot.LaneCount[PhaseCommit][ResultSuccess] != 1 || snapshot.LaneCount[PhasePost][ResultWarning] != 1 {
				t.Fatalf("post observations=%+v", snapshot)
			}
			if strings.Contains(collector.Prometheus(), "private-post") {
				t.Fatal("post callback identity/error leaked into metrics")
			}
		})
	}
}
