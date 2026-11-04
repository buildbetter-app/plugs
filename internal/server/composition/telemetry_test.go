package composition

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSQLiteCompositionEmitsPipelineTelemetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry.db")
	f := startRuntime(t, Config{Driver: "sqlite", DSN: path, Project: "sqlite-private-project", Address: DefaultAddress})
	if f.r.Metrics == nil {
		t.Fatal("composed runtime did not install a telemetry collector")
	}
	created, err := f.client.Call(t.Context(), "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "sqlite-private-payload"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	object := resultObject(t, created)
	results := object["results"].([]any)
	ref := results[0].(map[string]any)["ref"].(map[string]any)
	id := ref["id"].(string)

	snapshot := f.r.Metrics.Snapshot()
	if got := snapshot.Outcomes[pipeline.ResultCommit]; got != 1 {
		t.Fatalf("commit outcomes=%d want 1", got)
	}
	exposition := f.r.Metrics.Prometheus()
	if !strings.Contains(exposition, `write_outcome_total{result="commit"} 1`) || strings.Contains(exposition, id) || strings.Contains(exposition, "sqlite-private-payload") || strings.Contains(exposition, "sqlite-private-project") {
		t.Fatalf("unexpected SQLite exposition:\n%s", exposition)
	}
	response, err := http.Get("http://" + f.server.Addr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("composition exposed an unconfigured metrics route: %d", response.StatusCode)
	}
	metrics := f.r.Metrics
	f.stop(t)
	if metrics.Prometheus() != exposition {
		t.Fatal("closing the runtime changed its in-process metrics")
	}
}

// This fixture uses the production composition helper with one real SQLite
// store, lease, and writer. Callback options exercise paths the default
// runtime does not install, without expanding its exported configuration.
func TestSQLiteTelemetryCompositionRetriesRefusesAndCancels(t *testing.T) {
	ctx := context.Background()
	store, err := openStore(ctx, Config{Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "lanes.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	lease, err := store.Lease(ctx, "telemetry-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(ctx); err != nil {
			t.Error(err)
		}
	})
	by := model.PrincipalRef{Kind: "user", ID: "private-telemetry-principal"}
	meta := func(n int) model.WriteMeta {
		return model.WriteMeta{Method: "tx.apply", Project: "private-telemetry-project", IdempotencyKey: fmt.Sprintf("%032x", n)}
	}
	patch := func(ref model.Ref, title string) model.ChangeSet {
		return model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": title}}}}}
	}
	var writer *pipeline.Writer
	var watched model.Ref
	var retries atomic.Int32
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	validator := telemetryValidator(func(call context.Context, p port.ProposedChange, reader port.SnapshotReader) (port.Verdict, error) {
		switch p.Meta.IdempotencyKey {
		case meta(3).IdempotencyKey:
			if _, err := reader.Get(call, []model.Ref{watched}); err != nil {
				return port.Verdict{}, err
			}
			if retries.Add(1) == 1 {
				if _, err := writer.Apply(ctx, by, patch(watched, "private-interference"), meta(2)); err != nil {
					return port.Verdict{}, err
				}
			}
		case meta(4).IdempotencyKey:
			return port.Verdict{Violations: []model.Violation{{Guard: "private-guard", Message: "private-refusal-text"}}}, nil
		case meta(5).IdempotencyKey:
			cancel()
			return port.Verdict{}, call.Err()
		}
		return port.Verdict{Allow: true}, nil
	})
	writer, metrics := newPipelineWriter(store, lease,
		pipeline.WithOntology(service.NewOntology(nil)),
		pipeline.WithValidators(pipeline.ValidatorBinding{ID: "private-binding-name", Validator: validator}),
	)
	seed, err := writer.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "private-watched-title"}}},
		{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "private-target-title"}}},
	}}, meta(1))
	if err != nil {
		t.Fatal(err)
	}
	watched, target := seed.Results[0].Ref, seed.Results[1].Ref
	result, err := writer.Apply(ctx, by, patch(target, "private-committed-title"), meta(3))
	if err != nil || result.Revision.String() != "3" || retries.Load() != 2 {
		t.Fatalf("retry result=%+v calls=%d err=%v", result, retries.Load(), err)
	}
	for _, tc := range []struct {
		n    int
		ctx  context.Context
		code model.ErrorCode
	}{
		{4, ctx, "ontology_guard_failed"},
		{5, cancelCtx, "subscriber_unavailable"},
	} {
		_, err := writer.Apply(tc.ctx, by, patch(target, "private-unpublished-title"), meta(tc.n))
		var typed *model.Error
		if !errors.As(err, &typed) || typed.Data.Code != tc.code {
			t.Fatalf("request %d error=%v want %s", tc.n, err, tc.code)
		}
	}
	metadata, err := store.Meta(ctx)
	if err != nil || metadata.Head.String() != "3" {
		t.Fatalf("refusal/cancellation changed committed head: %+v %v", metadata, err)
	}
	snapshot := metrics.Snapshot()
	wantLanes := map[pipeline.Phase]map[pipeline.Result]uint64{
		pipeline.PhaseAdmission:         {pipeline.ResultSuccess: 6},
		pipeline.PhaseReplay:            {pipeline.ResultSuccess: 6},
		pipeline.PhasePrepare:           {pipeline.ResultSuccess: 6},
		pipeline.PhasePre:               {pipeline.ResultSuccess: 4, pipeline.ResultRefusal: 1, pipeline.ResultCanceled: 1},
		pipeline.PhaseGraph:             {pipeline.ResultSuccess: 4},
		pipeline.PhaseClaims:            {pipeline.ResultSuccess: 4},
		pipeline.PhaseChanges:           {pipeline.ResultSuccess: 4},
		pipeline.PhaseAudit:             {pipeline.ResultSuccess: 4},
		pipeline.PhaseCommit:            {pipeline.ResultSuccess: 3, pipeline.ResultRetry: 1},
		pipeline.PhaseCommitProjections: {pipeline.ResultSuccess: 4},
		pipeline.PhaseCommitLockPlan:    {pipeline.ResultSuccess: 4},
		pipeline.PhaseCommitWrite:       {pipeline.ResultSuccess: 4},
		pipeline.PhaseCommitLock:        {pipeline.ResultSuccess: 3, pipeline.ResultRetry: 1},
		pipeline.PhaseCommitHead:        {pipeline.ResultSuccess: 3},
		pipeline.PhaseCommitGuards:      {pipeline.ResultSuccess: 3},
		pipeline.PhaseCommitPrepareRows: {pipeline.ResultSuccess: 3},
		pipeline.PhaseCommitSources:     {pipeline.ResultSuccess: 3},
		pipeline.PhaseCommitAncestors:   {pipeline.ResultSuccess: 3},
		pipeline.PhaseCommitEvents:      {pipeline.ResultSuccess: 3},
		pipeline.PhaseCommitDriver:      {pipeline.ResultSuccess: 3},
		pipeline.PhaseResponse:          {pipeline.ResultSuccess: 3},
		pipeline.PhaseApply:             {pipeline.ResultCommit: 3, pipeline.ResultRefusal: 1, pipeline.ResultCanceled: 1},
	}
	for phase, counts := range snapshot.LaneCount {
		for result, count := range counts {
			if want := wantLanes[phase][result]; count != want {
				t.Errorf("%s/%s observations=%d want %d", phase, result, count, want)
			}
		}
	}
	wantOutcomes := map[pipeline.Result]uint64{pipeline.ResultCommit: 3, pipeline.ResultRefusal: 1, pipeline.ResultCanceled: 1}
	for result, count := range snapshot.Outcomes {
		if count != wantOutcomes[result] {
			t.Errorf("outcome %s=%d want %d", result, count, wantOutcomes[result])
		}
	}
	for reason, count := range snapshot.Conflicts {
		want := uint64(0)
		if reason == pipeline.ConflictStaleSnapshot {
			want = 1
		}
		if count != want {
			t.Errorf("conflict %s=%d want %d", reason, count, want)
		}
	}
	exposition := metrics.Prometheus()
	if strings.Contains(exposition, "private-") || strings.Contains(exposition, watched.ID) || strings.Contains(exposition, target.ID) || exposition != metrics.Prometheus() {
		t.Fatal("SQLite metrics leaked private input or rendered nondeterministically")
	}
}

type telemetryValidator func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error)

func (f telemetryValidator) Validate(ctx context.Context, p port.ProposedChange, reader port.SnapshotReader) (port.Verdict, error) {
	return f(ctx, p, reader)
}
