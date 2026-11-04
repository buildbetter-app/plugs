package rehearsal

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// The public fixture includes successful closes, a retry, a failed task and
// read calls. Its task denominator is independent of its RPC call count.
func metricCompletenessFixture(t *testing.T, repeats, unknownCalls int) Metrics {
	t.Helper()
	var samples []Sample
	for repeat := 0; repeat < repeats; repeat++ {
		samples = append(samples, Sample{Method: "memory.list"})
		for task := 0; task < 8; task++ {
			samples = append(samples, Sample{Method: "bead.ready", Latency: time.Millisecond})
			if task == 0 {
				samples = append(samples, Sample{Method: "bead.close", Code: "write_conflict", Retryable: true, Latency: time.Millisecond})
			}
			close := Sample{Method: "bead.close", Completed: true, Latency: time.Millisecond}
			if task == 7 {
				close.Code, close.Completed = "forbidden", false
			}
			samples = append(samples, close)
		}
	}
	for i := 0; i < unknownCalls; i++ {
		samples = append(samples, Sample{Method: "bead.ready", Code: "method_not_found", Latency: time.Millisecond})
	}
	metrics, err := Summarize(samples, 8*repeats, 7*repeats)
	if err != nil {
		t.Fatal(err)
	}
	return metrics
}

func metricCompletenessTargets() map[string]time.Duration {
	return map[string]time.Duration{
		"bead.close":  50 * time.Millisecond,
		"bead.ready":  50 * time.Millisecond,
		"memory.list": 50 * time.Millisecond,
	}
}

func TestMetricCompletenessPublicSampleControl(t *testing.T) {
	baseline := metricCompletenessFixture(t, 2, 0)
	shadow := metricCompletenessFixture(t, 1, 0)
	if shadow.Tasks != 8 || shadow.Completed != 7 || len(shadow.Rows) != 3 {
		t.Fatalf("task denominator or method identity changed: %+v", shadow)
	}
	wantRows := []Row{
		{Method: "bead.close", Calls: 9, Success: 7, RetryableError: 1, TerminalError: 1, Completed: 7, ErrorCodes: map[string]int{"write_conflict": 1, "forbidden": 1}, P50MS: 1, P95MS: 1, P99MS: 1},
		{Method: "bead.ready", Calls: 8, Success: 8, ErrorCodes: map[string]int{}, P50MS: 1, P95MS: 1, P99MS: 1},
		{Method: "memory.list", Calls: 1, Success: 1, ErrorCodes: map[string]int{}},
	}
	if !reflect.DeepEqual(shadow.Rows, wantRows) {
		t.Fatalf("actual success/failure counts differ:\n got %+v\nwant %+v", shadow.Rows, wantRows)
	}
	before, err := json.Marshal([]Metrics{baseline, shadow})
	if err != nil {
		t.Fatal(err)
	}
	if issues := Compare(baseline, shadow, metricCompletenessTargets()); len(issues) != 0 {
		t.Fatalf("same rates with different call/task denominators refused: %v", issues)
	}
	after, err := json.Marshal([]Metrics{baseline, shadow})
	if err != nil || string(after) != string(before) {
		t.Fatal("comparison changed source rows or denominators")
	}
}

func TestMetricCompletenessRejectsMissingAndMismatchedCounters(t *testing.T) {
	withUnknown := metricCompletenessFixture(t, 1, 2)
	if issues := Compare(withUnknown, withUnknown, metricCompletenessTargets()); !reflect.DeepEqual(issues, []string{"bead.ready:unknown_method_or_flag"}) {
		t.Fatalf("valid unknown-call measurement lost its distinct refusal: %v", issues)
	}
	for _, test := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"missing-unknown-counter", func(row map[string]any) { delete(row, "unknown_method_or_flag") }},
		{"zero-unknown-counter", func(row map[string]any) { row["unknown_method_or_flag"] = 0 }},
		{"undercounted-unknown-counter", func(row map[string]any) { row["unknown_method_or_flag"] = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(withUnknown)
			if err != nil {
				t.Fatal(err)
			}
			var document map[string]any
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatal(err)
			}
			for _, value := range document["rows"].([]any) {
				row := value.(map[string]any)
				if row["method"] == "bead.ready" {
					test.change(row)
				}
			}
			raw, err = json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			var inconsistent Metrics
			if err := json.Unmarshal(raw, &inconsistent); err != nil {
				t.Fatal(err)
			}
			// The error-code observations and all other counters remain intact.
			for i, row := range inconsistent.Rows {
				row.Unknown = withUnknown.Rows[i].Unknown
				if !reflect.DeepEqual(row, withUnknown.Rows[i]) {
					t.Fatal("fixture changed a counter other than unknown-call coverage")
				}
			}
			if issues := Compare(inconsistent, inconsistent, metricCompletenessTargets()); !reflect.DeepEqual(issues, []string{"invalid_or_missing_baseline_or_shadow_metrics"}) {
				t.Fatalf("inconsistent transcript accepted as a valid comparison: %v", issues)
			}
		})
	}
}

func TestMetricCompletenessKeepsMissingSamplesDistinct(t *testing.T) {
	baseline := metricCompletenessFixture(t, 1, 0)
	shadow := metricCompletenessFixture(t, 1, 0)
	shadow.Rows = shadow.Rows[:2] // Omit the measured memory.list row.
	if issues := Compare(baseline, shadow, metricCompletenessTargets()); !reflect.DeepEqual(issues, []string{"memory.list:missing_method_coverage"}) {
		t.Fatalf("missing samples lost their coverage refusal: %v", issues)
	}
	if issues := Compare(baseline, Metrics{}, metricCompletenessTargets()); !reflect.DeepEqual(issues, []string{"invalid_or_missing_baseline_or_shadow_metrics"}) {
		t.Fatalf("empty metrics passed: %v", issues)
	}
}

func TestMetricCompletenessPreservesUnknownFlagAndTransportDistinction(t *testing.T) {
	for _, test := range []struct {
		code        string
		wantUnknown int
	}{
		{"caller_unknown_flag", 1},
		{"transport_error", 0},
	} {
		t.Run(test.code, func(t *testing.T) {
			metrics, err := Summarize([]Sample{{Method: "bead.ready", Code: test.code, Latency: time.Millisecond}}, 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			row := metrics.Rows[0]
			if row.Unknown != test.wantUnknown || row.TerminalError != 1 || row.Success != 0 || !reflect.DeepEqual(row.ErrorCodes, map[string]int{"transport_error": 1}) {
				t.Fatalf("existing sanitized error distinction changed: %+v", row)
			}
			issues := Compare(metrics, metrics, map[string]time.Duration{"bead.ready": 50 * time.Millisecond})
			if test.wantUnknown == 0 && len(issues) != 0 || test.wantUnknown != 0 && !reflect.DeepEqual(issues, []string{"bead.ready:unknown_method_or_flag"}) {
				t.Fatalf("valid sanitized error measurement misclassified: %v", issues)
			}
		})
	}
}
