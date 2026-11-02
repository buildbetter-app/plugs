package cp7latency_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
)

func TestRealProductCP7100K(t *testing.T) {
	checkDeclaration(t)
	engine := os.Getenv("CP7_LATENCY_ENGINE")
	if engine != "sqlite" && engine != "postgres" {
		t.Fatal("Bazel target must declare its real engine")
	}
	f := newFixture(t, engine, corpusSize)
	for _, divisor := range []int{1, 100, 1000} {
		name := map[int]string{1: "100pct", 100: "1pct", 1000: "0.1pct"}[divisor]
		t.Run(name, func(t *testing.T) {
			for _, op := range []string{"Q4Ready", "Q5ClaimNext", "Q6GroupedCount"} {
				t.Run(op, func(t *testing.T) {
					f.compose(t)
					cold := f.observe(t, engine, divisor, op, "cold", 0)
					for i := 0; i < warmups; i++ {
						f.observe(t, engine, divisor, op, "warmup", i)
					}
					measured := make([]sample, 0, measuredSamples)
					for i := 0; i < measuredSamples; i++ {
						measured = append(measured, f.observe(t, engine, divisor, op, "measured", i))
					}
					result, err := summarize(engine, divisor, op, f.corpusHash, cold, measured)
					if err != nil {
						t.Fatal(err)
					}
					emit(t, "CP7_CASE", result)
				})
			}
		})
	}
}

func (f *fixture) observe(t *testing.T, engine string, divisor int, op, phase string, index int) sample {
	t.Helper()
	before, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p := f.user(divisor)
	if op == "Q5ClaimNext" {
		p = f.agent(divisor, divisor != 1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestDeadline)
	defer cancel()
	meta := model.WriteMeta{Method: "bead.claimNext", Project: projectName, Return: "full", IdempotencyKey: requestKey(op, divisor, phase, index)}
	var ready service.BeadPage
	var grouped viewops.Result
	var outcome *port.WriteOutcome
	f.store.counts.reset()
	started := time.Now()
	switch op {
	case "Q4Ready":
		ready, err = f.app.Service.Ready(ctx, p, service.ReadyRequest{Project: projectName, Limit: pageSize})
	case "Q5ClaimNext":
		outcome, err = f.app.Service.ClaimNextWithOutcome(ctx, p, service.ClaimNextRequest{Project: projectName}, meta)
	case "Q6GroupedCount":
		grouped, err = f.views.Run(ctx, p, groupedRequest())
	default:
		t.Fatalf("undeclared operation %q", op)
	}
	elapsed := time.Since(started)
	status, code := classify(err, ctx.Err())
	s := sample{Engine: engine, FractionDivisor: divisor, Corpus: f.n, ExpectedAuthorized: f.n / divisor, Operation: op, Phase: phase, Index: index,
		ElapsedMS: float64(elapsed.Microseconds()) / 1000, Status: status, Code: code, Counts: f.store.counts.value(), TargetMS: 50}
	// Even an oracle failure retains its raw timed observation.
	defer func() { emit(t, "CP7_SAMPLE", s) }()
	if op == "Q6GroupedCount" {
		s.TargetMS = 250
	}
	if err != nil {
		s.Error = err.Error()
		var typed *model.Error
		if errors.As(err, &typed) {
			s.ErrorData = &typed.Data
		}
		if len(ready.Items) != 0 || ready.Next != nil || outcome != nil || len(grouped.Items) != 0 || len(grouped.Groups) != 0 || grouped.NextCursor != nil {
			s.Status = "correctness_error"
			t.Fatal("refused request published a partial result")
		}
	}
	validationStarted := time.Now()
	after, metaErr := f.store.Meta(context.Background())
	if metaErr != nil {
		s.Status = "correctness_error"
		t.Fatal(metaErr)
	}
	if (op != "Q5ClaimNext" || status != "success") && after.Head.String() != before.Head.String() {
		s.Status = "correctness_error"
		t.Fatal("read/refused request unexpectedly committed a changeset")
	}
	if status == "success" {
		s.Status = "correctness_error"
		s.Error = "post-call correctness validation failed; see named assertion"
		switch op {
		case "Q4Ready":
			f.checkPage(t, ready, divisor)
		case "Q5ClaimNext":
			f.checkClaim(t, p, outcome, meta, divisor)
		case "Q6GroupedCount":
			f.checkGrouped(t, grouped, divisor)
		}
		s.Status = status
		s.Error = ""
	}
	s.TargetPass = status == "success" && s.ElapsedMS < s.TargetMS
	s.ValidationMS = float64(time.Since(validationStarted).Microseconds()) / 1000
	if op == "Q5ClaimNext" && status == "success" {
		resetStarted := time.Now()
		f.resetClaim(t, outcome.Commit.Results[0].Ref)
		s.ResetMS = float64(time.Since(resetStarted).Microseconds()) / 1000
	}
	if status == "correctness_error" {
		t.Fatalf("unexpected product error retained above: %v", err)
	}
	return s
}
