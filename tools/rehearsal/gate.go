package rehearsal

import (
	"regexp"
	"sort"
	"time"
)

type Receipt struct {
	SourceSHA  string    `json:"source_sha"`
	SourceTree string    `json:"source_tree"`
	Digest     string    `json:"receipt_sha256"`
	Finished   time.Time `json:"finished"`
	Clean      bool      `json:"clean"`
}

type ShadowDay struct {
	Date     time.Time `json:"date"`
	Baseline Metrics   `json:"baseline"`
	Shadow   Metrics   `json:"shadow"`
}

type GateEvidence struct {
	SourceSHA            string                   `json:"source_sha"`
	SourceTree           string                   `json:"source_tree"`
	Rehearsals           []Receipt                `json:"rehearsals"`
	ShadowStart          time.Time                `json:"shadow_start"`
	ShadowEnd            time.Time                `json:"shadow_end"`
	LastWeek             []ShadowDay              `json:"last_week"`
	LatencyTargets       map[string]time.Duration `json:"latency_targets_ns"`
	ReferenceBaseline    bool                     `json:"reference_baseline"`
	Failover             bool                     `json:"managed_failover"`
	Rollback             bool                     `json:"complete_rollback"`
	PlatformSafety       bool                     `json:"platform_safety"`
	AuthenticatedStaging bool                     `json:"authenticated_staging"`
}

var sha40 = regexp.MustCompile(`^[0-9a-f]{40}$`)
var receiptDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func utcDay(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// EvaluateGate is called with the current clock at every gate execution.
// It validates receipts supplied by a trusted host; it does not authenticate
// those receipts or authorize staging, a switch, release or deployment.
func EvaluateGate(now time.Time, evidence GateEvidence) []string {
	waits := []string{}
	if now.IsZero() || !sha40.MatchString(evidence.SourceSHA) || !sha40.MatchString(evidence.SourceTree) {
		waits = append(waits, "missing_current_head_or_clock")
	}
	runs := append([]Receipt(nil), evidence.Rehearsals...)
	sort.Slice(runs, func(i, j int) bool { return runs[i].Finished.Before(runs[j].Finished) })
	if len(runs) != 3 {
		waits = append(waits, "three_clean_nightly_rehearsals_required")
	} else {
		seen := map[string]bool{}
		for i, run := range runs {
			if !run.Clean || run.SourceSHA != evidence.SourceSHA || run.SourceTree != evidence.SourceTree || !receiptDigestPattern.MatchString(run.Digest) || seen[run.Digest] || run.Finished.IsZero() || run.Finished.After(now) {
				waits = append(waits, "unclean_unbound_duplicate_or_future_rehearsal")
			}
			seen[run.Digest] = true
			if i > 0 && !utcDay(run.Finished).Equal(utcDay(runs[i-1].Finished).AddDate(0, 0, 1)) {
				waits = append(waits, "rehearsals_are_not_three_consecutive_nights")
			}
		}
		if now.Sub(runs[2].Finished) > 7*24*time.Hour {
			waits = append(waits, "latest_rehearsal_older_than_seven_days_at_gate_execution")
		}
	}
	if evidence.ShadowStart.IsZero() || evidence.ShadowEnd.IsZero() || evidence.ShadowEnd.Sub(evidence.ShadowStart) < 14*24*time.Hour || evidence.ShadowEnd.After(now) {
		waits = append(waits, "complete_shadow_fortnight_required")
	}
	week := append([]ShadowDay(nil), evidence.LastWeek...)
	sort.Slice(week, func(i, j int) bool { return week[i].Date.Before(week[j].Date) })
	if len(week) != 7 {
		waits = append(waits, "seven_complete_last_week_metric_days_required")
	} else {
		for i, day := range week {
			if !utcDay(day.Date).Equal(utcDay(evidence.ShadowEnd).AddDate(0, 0, i-7)) || day.Date.After(now) {
				waits = append(waits, "shadow_day_outside_last_week")
			}
			waits = append(waits, Compare(day.Baseline, day.Shadow, evidence.LatencyTargets)...)
		}
	}
	for name, pass := range map[string]bool{"reference_baseline": evidence.ReferenceBaseline, "managed_failover": evidence.Failover, "complete_rollback": evidence.Rollback, "platform_safety": evidence.PlatformSafety, "authenticated_staging": evidence.AuthenticatedStaging} {
		if !pass {
			waits = append(waits, name+"_unqualified")
		}
	}
	sort.Strings(waits)
	return waits
}
