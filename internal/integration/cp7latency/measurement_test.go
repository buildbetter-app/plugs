package cp7latency_test

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

//go:embed workload.json
var workload []byte

const (
	warmups         = 3
	measuredSamples = 20
	requestDeadline = 30 * time.Second
)

func requestKey(op string, divisor int, phase string, index int) string {
	// Stable full-scope keys never reuse a timed receipt.
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%s/%d", op, divisor, phase, index))))[:32]
}

type sample struct {
	Engine             string           `json:"engine"`
	FractionDivisor    int              `json:"fraction_divisor"`
	Corpus             int              `json:"corpus"`
	ExpectedAuthorized int              `json:"expected_authorized"`
	Operation          string           `json:"operation"`
	Phase              string           `json:"phase"`
	Index              int              `json:"index"`
	ElapsedMS          float64          `json:"elapsed_ms"`
	Status             string           `json:"status"`
	Code               model.ErrorCode  `json:"error_code,omitempty"`
	Error              string           `json:"error,omitempty"`
	ErrorData          *model.ErrorData `json:"error_data,omitempty"`
	Counts             readCounts       `json:"reader_api"`
	Work               executedReadWork `json:"executed_read_work"`
	SQLExecutorWork    string           `json:"sql_executor_work"`
	TargetMS           float64          `json:"target_ms"`
	TargetPass         bool             `json:"target_pass"`
	ValidationMS       float64          `json:"validation_ms"`
	ResetMS            float64          `json:"reset_ms"`
}

type caseResult struct {
	Engine             string         `json:"engine"`
	FractionDivisor    int            `json:"fraction_divisor"`
	Operation          string         `json:"operation"`
	WorkloadSHA256     string         `json:"workload_sha256"`
	CorpusSHA256       string         `json:"corpus_sha256"`
	Corpus             int            `json:"corpus"`
	ExpectedAuthorized int            `json:"expected_authorized"`
	Cold               sample         `json:"cold_first_request"`
	Warmups            int            `json:"warmups"`
	Samples            int            `json:"samples"`
	Outcomes           map[string]int `json:"outcomes"`
	ResponseP95MS      float64        `json:"response_p95_ms"`
	SuccessfulP95MS    *float64       `json:"successful_latency_p95_ms"`
	TargetMS           float64        `json:"target_ms"`
	TargetPass         bool           `json:"target_pass"`
	Status             string         `json:"status"`
}

// A refused/deadline response is not a successful latency observation. Known
// write-pre budget exhaustion remains a baseline performance RED. All other
// errors fail correctness, even if a later sample might have succeeded.
func classify(err, requestErr error) (string, model.ErrorCode) {
	if err == nil {
		return "success", ""
	}
	// A joined deadline must not hide an independent failure. Ordinary
	// wrapping preserves the classification of the returned cause.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		status, code := "", model.ErrorCode("")
		for _, cause := range joined.Unwrap() {
			next, nextCode := classify(cause, requestErr)
			if next == "correctness_error" {
				return next, nextCode
			}
			if status != "" && (status != next || code != nextCode) {
				return "correctness_error", code
			}
			status, code = next, nextCode
		}
		if status != "" {
			return status, code
		}
		return "correctness_error", ""
	}
	if cause := errors.Unwrap(err); cause != nil {
		return classify(cause, requestErr)
	}
	var value *model.Error
	if errors.As(err, &value) {
		if value.Data.Code == "authz_postfilter_cap" || value.Data.Code == "scope_too_large" {
			return "cap_refusal", value.Data.Code
		}
		if value.Data.Code == "busy" && value.Message == "write pre phases exceeded their deadline" {
			return "write_pre_deadline", value.Data.Code
		}
		// viewops maps a real canceled/deadline cause to this exact busy
		// form. Require the request deadline as well; unrelated typed errors
		// remain correctness failures even when the context has expired.
		if value.Data.Code == "busy" && value.Message == "" && value.Data.Retryable && value.Data.Hint == "view read was canceled or exceeded its deadline" && errors.Is(requestErr, context.DeadlineExceeded) {
			return "request_deadline", value.Data.Code
		}
		return "correctness_error", value.Data.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "request_deadline", ""
	}
	return "correctness_error", ""
}

func p95(values []float64) float64 {
	if len(values) == 0 {
		panic("p95 needs observations")
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	return ordered[int(math.Ceil(.95*float64(len(ordered))))-1]
}

func summarize(engine string, divisor int, op, corpusHash string, cold sample, observations []sample) (caseResult, error) {
	r := caseResult{Engine: engine, FractionDivisor: divisor, Operation: op,
		WorkloadSHA256: fmt.Sprintf("%x", sha256.Sum256(workload)), CorpusSHA256: corpusHash,
		Corpus: corpusSize, ExpectedAuthorized: corpusSize / divisor, Cold: cold, Warmups: warmups,
		Samples: len(observations), Outcomes: map[string]int{}, Status: "RED", TargetMS: 50}
	if op == "Q6GroupedCount" {
		r.TargetMS = 250
	}
	if len(observations) != measuredSamples {
		return r, fmt.Errorf("need exactly %d measured samples, got %d", measuredSamples, len(observations))
	}
	values := make([]float64, 0, len(observations))
	for index, s := range observations {
		if s.Engine != engine || s.FractionDivisor != divisor || s.Operation != op || s.Phase != "measured" || s.Index != index || s.Corpus != corpusSize || s.ExpectedAuthorized != corpusSize/divisor {
			return r, errors.New("sample matrix identity/order mismatch")
		}
		if s.Status == "correctness_error" {
			return r, errors.New("correctness error cannot be summarized as valid baseline")
		}
		r.Outcomes[s.Status]++
		values = append(values, s.ElapsedMS)
	}
	r.ResponseP95MS = p95(values)
	if r.Outcomes["success"] == measuredSamples {
		r.SuccessfulP95MS = &r.ResponseP95MS
		r.TargetPass = r.ResponseP95MS < r.TargetMS
		if r.TargetPass {
			r.Status = "PASS"
		}
	}
	return r, nil
}

func emit(t *testing.T, kind string, value any) {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s %s", kind, b)
}

func checkDeclaration(t *testing.T) {
	t.Helper()
	type declaration struct {
		Schema         string   `json:"schema"`
		Engines        []string `json:"engines"`
		Beads          int      `json:"beads"`
		MaxParentDepth int      `json:"max_parent_depth"`
		ChainLength    int      `json:"chain_length"`
		Project        string   `json:"project"`
		Type           string   `json:"type"`
		TitlePrefix    string   `json:"title_prefix"`
		Priority       int      `json:"priority"`
		Status         string   `json:"status"`
		SeedBatch      int      `json:"seed_batch"`
		Fractions      []int    `json:"fractions"`
		Populations    []int    `json:"populations"`
		Operations     []string `json:"operations"`
		PageLimit      int      `json:"page_limit"`
		Query          string   `json:"query"`
		GroupBy        []string `json:"group_by"`
		Fields         []string `json:"fields"`
		Sort           []string `json:"sort"`
		Cold           int      `json:"cold_first_requests"`
		Samples        int      `json:"samples"`
		Warmups        int      `json:"warmups"`
		Deadline       int      `json:"request_deadline_ms"`
		PageTarget     int      `json:"page_and_full_claim_target_ms"`
		CountTarget    int      `json:"grouped_count_target_ms"`
		OrdinaryCap    int      `json:"ordinary_candidate_cap"`
		PostgresImage  string   `json:"postgres_image"`
	}
	var d declaration
	if err := json.Unmarshal(workload, &d); err != nil {
		t.Fatal(err)
	}
	want := declaration{
		Schema: "plug-real-cp7-latency/v1", Engines: []string{"sqlite", "postgres"},
		Beads: corpusSize, MaxParentDepth: 10, ChainLength: 11, Project: projectName,
		Type: "issue", TitlePrefix: "CP7 bead ", Priority: 0, Status: "todo", SeedBatch: seedBatch,
		Fractions: []int{1, 100, 1000}, Populations: []int{100000, 1000, 100},
		Operations: []string{"Q4Ready", "Q5ClaimNext", "Q6GroupedCount"}, PageLimit: pageSize,
		Query: viewQuery, GroupBy: []string{"type"}, Fields: []string{"id", "type", "fields.title", "parent"}, Sort: []string{"id"},
		Cold: 1, Samples: measuredSamples, Warmups: warmups, Deadline: int(requestDeadline / time.Millisecond),
		PageTarget: 50, CountTarget: 250, OrdinaryCap: 20000, PostgresImage: postgresImage,
	}
	if !reflect.DeepEqual(d, want) {
		t.Fatal("executable workload differs from declaration")
	}
	t.Logf("CP7_WORKLOAD sha256=%x declaration=%s", sha256.Sum256(workload), workload)
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
	ctx = driver.WithReadWorkObserver(ctx, &f.store.counts)
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
		ElapsedMS: float64(elapsed.Microseconds()) / 1000, Status: status, Code: code, Counts: f.store.counts.value(), Work: f.store.counts.work(), SQLExecutorWork: "UNMEASURED", TargetMS: 50}
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
