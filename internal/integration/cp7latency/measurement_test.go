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
	var value *model.Error
	if errors.As(err, &value) {
		if value.Data.Code == "authz_postfilter_cap" || value.Data.Code == "scope_too_large" {
			return "cap_refusal", value.Data.Code
		}
		if value.Data.Code == "busy" && value.Message == "write pre phases exceeded their deadline" {
			return "write_pre_deadline", value.Data.Code
		}
		if requestErr != nil || errors.Is(err, context.DeadlineExceeded) {
			return "request_deadline", value.Data.Code
		}
		return "correctness_error", value.Data.Code
	}
	if requestErr != nil || errors.Is(err, context.DeadlineExceeded) {
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
