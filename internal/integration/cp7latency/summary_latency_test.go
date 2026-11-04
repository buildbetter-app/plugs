package cp7latency_test

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

//go:embed summary-workload.json
var summaryWorkload []byte

func checkSummaryDeclaration(t *testing.T) {
	t.Helper()
	checkDeclaration(t)
	var got struct {
		Schema    string   `json:"schema"`
		Original  string   `json:"original_workload_sha256"`
		Engines   []string `json:"engines"`
		Fractions []int    `json:"fractions"`
		Corpus    int      `json:"beads"`
		Operation string   `json:"operation"`
		Fields    []string `json:"fields"`
		GroupBy   []string `json:"group_by"`
		Cold      int      `json:"cold_first_requests"`
		Warmups   int      `json:"warmups"`
		Samples   int      `json:"samples"`
		Deadline  int      `json:"request_deadline_ms"`
		Target    int      `json:"target_ms"`
	}
	if err := json.Unmarshal(summaryWorkload, &got); err != nil {
		t.Fatal(err)
	}
	if got.Schema != "plug-real-cp7-type-summary/v1" || got.Original != fmt.Sprintf("%x", sha256.Sum256(workload)) ||
		!reflect.DeepEqual(got.Engines, []string{"sqlite", "postgres"}) || !reflect.DeepEqual(got.Fractions, []int{1, 100, 1000}) ||
		got.Corpus != corpusSize || got.Operation != "Q6TypeSummary" || got.Fields == nil || len(got.Fields) != 0 ||
		!reflect.DeepEqual(got.GroupBy, []string{"type"}) || got.Cold != 1 || got.Warmups != warmups || got.Samples != measuredSamples ||
		got.Deadline != int(requestDeadline/time.Millisecond) || got.Target != 250 {
		t.Fatal("summary matrix differs from separate declaration")
	}
	t.Logf("CP7_SUMMARY_WORKLOAD sha256=%x declaration=%s", sha256.Sum256(summaryWorkload), summaryWorkload)
}

func observeSummary(t *testing.T, f *fixture, views *viewops.Service, engine string, divisor int, phase string, index int) sample {
	t.Helper()
	before, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var counts counters
	ctx, cancel := context.WithTimeout(context.Background(), requestDeadline)
	defer cancel()
	observed := driver.WithReadWorkObserver(ctx, &counts)
	start := time.Now()
	got, err := views.Run(observed, f.user(divisor), summaryRequest())
	elapsed := time.Since(start)
	status, code := classify(err, ctx.Err())
	s := sample{Engine: engine, FractionDivisor: divisor, Corpus: f.n, ExpectedAuthorized: f.n / divisor,
		Operation: "Q6TypeSummary", Phase: phase, Index: index, ElapsedMS: float64(elapsed.Microseconds()) / 1000,
		Status: status, Code: code, Counts: counts.value(), Work: counts.work(), SQLExecutorWork: "UNMEASURED", TargetMS: 250}
	defer func() { emit(t, "CP7_SUMMARY_SAMPLE", s) }()
	if err != nil {
		s.Error = err.Error()
		var typed *model.Error
		if errors.As(err, &typed) {
			s.ErrorData = &typed.Data
		}
		if refusedOutput(service.BeadPage{}, nil, got) {
			s.Status = "correctness_error"
			t.Fatal("refused summary emitted partial result")
		}
	} else {
		s.Status = "correctness_error"
		if got.Count != nil || len(got.Items) != 0 || got.NextCursor != nil || len(got.Groups) != 1 ||
			got.Groups[0].Key != "issue" || got.Groups[0].Count != f.n/divisor || len(got.Groups[0].Items) != 0 || got.Groups[0].Elided != nil {
			t.Fatalf("summary differs from independent complete population: %+v", got)
		}
		work := counts.work()
		if work.Selections != 1 || counts.value().Queries != 0 || work.SelectionSQL != 1 || work.SelectionControls != 1 ||
			work.AggregateGroups != 1 || work.Payloads != 0 || work.Projections != 0 {
			t.Fatalf("summary did not execute the compiled no-payload aggregate: api=%+v work=%+v", counts.value(), work)
		}
		s.Status = "success"
	}
	after, metaErr := f.store.Meta(context.Background())
	if metaErr != nil || before.Head.String() != after.Head.String() {
		s.Status = "correctness_error"
		t.Fatalf("summary changed source head: %v", metaErr)
	}
	s.TargetPass = s.Status == "success" && s.ElapsedMS < s.TargetMS
	if s.Status == "correctness_error" {
		t.Fatalf("unexpected summary product error retained above: %v", err)
	}
	return s
}

func TestRealProductCP7SeparateTimedTypeSummary100K(t *testing.T) {
	checkSummaryDeclaration(t)
	engine := os.Getenv("CP7_LATENCY_ENGINE")
	if engine != "sqlite" && engine != "postgres" {
		t.Fatalf("undeclared engine %q", engine)
	}
	f := newFixture(t, engine, corpusSize)
	for _, divisor := range []int{1, 100, 1000} {
		t.Run(fmt.Sprintf("divisor%d", divisor), func(t *testing.T) {
			f.compose(t)
			views := qualificationViews(t, f, nil)
			cold := observeSummary(t, f, views, engine, divisor, "cold", 0)
			for i := 0; i < warmups; i++ {
				observeSummary(t, f, views, engine, divisor, "warmup", i)
			}
			samples := make([]sample, 0, measuredSamples)
			for i := 0; i < measuredSamples; i++ {
				samples = append(samples, observeSummary(t, f, views, engine, divisor, "measured", i))
			}
			result, err := summarize(engine, divisor, "Q6TypeSummary", f.corpusHash, cold, samples)
			if err != nil {
				t.Fatal(err)
			}
			result.WorkloadSHA256 = fmt.Sprintf("%x", sha256.Sum256(summaryWorkload))
			emit(t, "CP7_SUMMARY_CASE", result)
		})
	}
}
