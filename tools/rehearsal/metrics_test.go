package rehearsal

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func measured(t *testing.T, calls, errors, completed int, latency time.Duration, code string) Metrics {
	t.Helper()
	samples := make([]Sample, calls)
	for i := range samples {
		samples[i] = Sample{Method: "bead.ready", Latency: latency}
		if i < errors {
			samples[i].Code = code
		}
	}
	metrics, err := Summarize(samples, 100, completed)
	if err != nil {
		t.Fatal(err)
	}
	return metrics
}

func TestMetricsRejectErrorCodeDriftUnknownCallsMissingTargetsAndCompletionDrift(t *testing.T) {
	base := measured(t, 100, 10, 100, time.Millisecond, "conflict")
	targets := map[string]time.Duration{"bead.ready": 50 * time.Millisecond}
	if issues := Compare(base, measured(t, 200, 20, 95, time.Millisecond, "conflict"), targets); len(issues) != 0 {
		t.Fatal(issues)
	}
	for _, test := range []struct {
		name    string
		shadow  Metrics
		targets map[string]time.Duration
		want    string
	}{
		{"per-code", measured(t, 100, 10, 100, time.Millisecond, "forbidden"), targets, "error_rate_increased:forbidden"},
		{"rate", measured(t, 100, 11, 100, time.Millisecond, "conflict"), targets, "error_rate_increased:conflict"},
		{"completion", measured(t, 100, 10, 94, time.Millisecond, "conflict"), targets, "task_completion_outside"},
		{"p95-exact-boundary", measured(t, 100, 10, 100, 50*time.Millisecond, "conflict"), targets, "p95_target_failed"},
		{"missing-target", base, nil, "latency_target_unset"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if issues := strings.Join(Compare(base, test.shadow, test.targets), " "); !strings.Contains(issues, test.want) {
				t.Fatal(issues)
			}
		})
	}
	unknown, err := Summarize([]Sample{{Method: "private-input-must-not-appear", Latency: time.Millisecond}}, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(unknown)
	if strings.Contains(string(raw), "private-input") || unknown.Rows[0].Unknown != 1 {
		t.Fatal(string(raw))
	}
	if issues := strings.Join(Compare(unknown, unknown, map[string]time.Duration{"unknown": time.Second}), " "); !strings.Contains(issues, "unknown_method_or_flag") {
		t.Fatal(issues)
	}
	invalid := measured(t, 1, 0, 100, time.Millisecond, "")
	invalid.Rows[0].P95MS = math.NaN()
	if issues := strings.Join(Compare(base, invalid, targets), " "); !strings.Contains(issues, "invalid_or_missing") {
		t.Fatal(issues)
	}
	missing := measured(t, 1, 0, 100, time.Millisecond, "")
	missing.Rows[0].Method = "memory.list"
	if issues := strings.Join(Compare(base, missing, targets), " "); !strings.Contains(issues, "missing_method_coverage") {
		t.Fatal(issues)
	}
	if issues := strings.Join(Compare(base, base, map[string]time.Duration{"bead.ready": time.Second, "agent.brief": time.Second}), " "); !strings.Contains(issues, "agent.brief:missing_method_coverage") {
		t.Fatal(issues)
	}
}

func completeGateEvidence(t *testing.T, now time.Time) GateEvidence {
	t.Helper()
	metrics := measured(t, 10, 0, 100, time.Millisecond, "")
	end := utcDay(now)
	e := GateEvidence{SourceSHA: strings.Repeat("a", 40), SourceTree: strings.Repeat("b", 40), ShadowStart: end.AddDate(0, 0, -14), ShadowEnd: end, LatencyTargets: map[string]time.Duration{"bead.ready": 50 * time.Millisecond}, ReferenceBaseline: true, Failover: true, Rollback: true, PlatformSafety: true, AuthenticatedStaging: true}
	for i := 0; i < 3; i++ {
		e.Rehearsals = append(e.Rehearsals, Receipt{SourceSHA: e.SourceSHA, SourceTree: e.SourceTree, Digest: strings.Repeat(string(rune('a'+i)), 64), Finished: end.AddDate(0, 0, i-3).Add(23 * time.Hour), Clean: true})
	}
	for i := 0; i < 7; i++ {
		e.LastWeek = append(e.LastWeek, ShadowDay{Date: end.AddDate(0, 0, i-7), Baseline: metrics, Shadow: metrics})
	}
	return e
}

func TestGateChecksActualNightlyStreakFreshnessAtExecutionAndShadowWindows(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	e := completeGateEvidence(t, now)
	if waits := EvaluateGate(now, e); len(waits) != 0 {
		t.Fatal(waits)
	}
	// At exactly seven days the latest receipt remains fresh; one second
	// later the same immutable receipts must fail at the new gate execution.
	boundary := e.Rehearsals[2].Finished.Add(7 * 24 * time.Hour)
	if waits := EvaluateGate(boundary, e); len(waits) != 0 {
		t.Fatal(waits)
	}
	if waits := strings.Join(EvaluateGate(boundary.Add(time.Second), e), " "); !strings.Contains(waits, "latest_rehearsal_older_than_seven_days_at_gate_execution") {
		t.Fatal(waits)
	}
	for _, test := range []struct {
		name   string
		change func(*GateEvidence)
		want   string
	}{
		{"same-session", func(e *GateEvidence) {
			for i := range e.Rehearsals {
				e.Rehearsals[i].Finished = now.Add(-time.Duration(i+1) * time.Minute)
			}
		}, "not_three_consecutive_nights"},
		{"missed-night", func(e *GateEvidence) { e.Rehearsals[0].Finished = e.Rehearsals[0].Finished.AddDate(0, 0, -1) }, "not_three_consecutive_nights"},
		{"future", func(e *GateEvidence) { e.Rehearsals[2].Finished = now.Add(time.Hour) }, "future_rehearsal"},
		{"changed-head", func(e *GateEvidence) { e.SourceSHA = strings.Repeat("c", 40) }, "unbound"},
		{"duplicate-receipt", func(e *GateEvidence) { e.Rehearsals[1].Digest = e.Rehearsals[0].Digest }, "duplicate"},
		{"short-shadow", func(e *GateEvidence) { e.ShadowStart = e.ShadowEnd.Add(-time.Hour) }, "shadow_fortnight"},
		{"missing-day", func(e *GateEvidence) { e.LastWeek = e.LastWeek[1:] }, "seven_complete"},
		{"wrong-week", func(e *GateEvidence) { e.LastWeek[0].Date = e.LastWeek[0].Date.AddDate(0, 0, -1) }, "outside_last_week"},
		{"unqualified-staging", func(e *GateEvidence) { e.AuthenticatedStaging = false }, "authenticated_staging_unqualified"},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := completeGateEvidence(t, now)
			test.change(&e)
			if waits := strings.Join(EvaluateGate(now, e), " "); !strings.Contains(waits, test.want) {
				t.Fatal(waits)
			}
		})
	}
}
