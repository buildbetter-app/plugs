// Package rehearsal measures disposable caller runs and evaluates evidence
// windows. It never treats synthetic runs as staging or switch approval.
package rehearsal

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

// Sample contains no request, result, principal, resource ID or error message.
type Sample struct {
	Method    string
	Code      string
	Retryable bool
	Latency   time.Duration
	Completed bool
}

type Row struct {
	Method         string         `json:"method"`
	Calls          int            `json:"calls"`
	Success        int            `json:"success"`
	RetryableError int            `json:"retryable_error"`
	TerminalError  int            `json:"terminal_error"`
	Unknown        int            `json:"unknown_method_or_flag"`
	Completed      int            `json:"completion_count"`
	ErrorCodes     map[string]int `json:"error_codes"`
	P50MS          float64        `json:"p50_ms"`
	P95MS          float64        `json:"p95_ms"`
	P99MS          float64        `json:"p99_ms"`
}

type Metrics struct {
	Rows      []Row `json:"rows"`
	Tasks     int   `json:"tasks"`
	Completed int   `json:"completion_count"`
}

func Summarize(samples []Sample, tasks, completed int) (Metrics, error) {
	if tasks <= 0 || completed < 0 || completed > tasks || len(samples) == 0 {
		return Metrics{}, fmt.Errorf("nonempty samples and valid task denominators required")
	}
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		return Metrics{}, err
	}
	rows := map[string]*Row{}
	latencies := map[string][]time.Duration{}
	for _, sample := range samples {
		if sample.Latency < 0 {
			return Metrics{}, fmt.Errorf("negative latency")
		}
		method := sample.Method
		_, known := catalog.Descriptor(method)
		if !known {
			method = "unknown"
		}
		row := rows[method]
		if row == nil {
			row = &Row{Method: method, ErrorCodes: map[string]int{}}
			rows[method] = row
		}
		row.Calls++
		latencies[method] = append(latencies[method], sample.Latency)
		code := sample.Code
		if !known || code == "method_not_found" || code == "caller_unknown_flag" {
			row.Unknown++
			if !known {
				code = "method_not_found"
			}
		}
		if code == "" {
			row.Success++
			if sample.Completed {
				row.Completed++
			}
			continue
		}
		if catalog.NewError(code, "", nil).Data.Code != code {
			code = "transport_error"
		}
		row.ErrorCodes[code]++
		if sample.Retryable {
			row.RetryableError++
		} else {
			row.TerminalError++
		}
	}
	result := Metrics{Tasks: tasks, Completed: completed}
	for method, row := range rows {
		values := latencies[method]
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		percentile := func(p float64) float64 {
			return float64(values[int(math.Ceil(p*float64(len(values))))-1]) / float64(time.Millisecond)
		}
		row.P50MS, row.P95MS, row.P99MS = percentile(.50), percentile(.95), percentile(.99)
		result.Rows = append(result.Rows, *row)
	}
	sort.Slice(result.Rows, func(i, j int) bool { return result.Rows[i].Method < result.Rows[j].Method })
	return result, nil
}

// Compare enforces each error-code rate, coverage, absolute completion delta
// and explicit per-method p95 targets. A missing measurement never passes.
func Compare(baseline, shadow Metrics, targets map[string]time.Duration) []string {
	issues := []string{}
	if !validMetrics(baseline) || !validMetrics(shadow) {
		return []string{"invalid_or_missing_baseline_or_shadow_metrics"}
	}
	if math.Abs(float64(baseline.Completed)/float64(baseline.Tasks)-float64(shadow.Completed)/float64(shadow.Tasks)) > .05+1e-12 {
		issues = append(issues, "task_completion_outside_five_percentage_points")
	}
	baseRows := map[string]Row{}
	shadowRows := map[string]Row{}
	for _, row := range baseline.Rows {
		baseRows[row.Method] = row
	}
	for _, row := range shadow.Rows {
		shadowRows[row.Method] = row
	}
	methods := map[string]bool{}
	for method := range baseRows {
		methods[method] = true
	}
	for method := range shadowRows {
		methods[method] = true
	}
	catalog, _ := wire.DefaultCatalog()
	for method := range targets {
		if _, known := catalog.Descriptor(method); !known && method != "unknown" {
			issues = append(issues, "invalid_latency_target_method")
			continue
		}
		methods[method] = true
	}
	for method := range methods {
		base, baseOK := baseRows[method]
		row, shadowOK := shadowRows[method]
		if !baseOK || !shadowOK || base.Calls <= 0 || row.Calls <= 0 {
			issues = append(issues, method+":missing_method_coverage")
			continue
		}
		if base.Unknown > 0 || row.Unknown > 0 {
			issues = append(issues, method+":unknown_method_or_flag")
		}
		for code, count := range row.ErrorCodes {
			if float64(count)/float64(row.Calls) > float64(base.ErrorCodes[code])/float64(base.Calls) {
				issues = append(issues, method+":error_rate_increased:"+code)
			}
		}
		target := targets[method]
		if target <= 0 {
			issues = append(issues, method+":latency_target_unset")
		} else if row.P95MS >= float64(target)/float64(time.Millisecond) {
			issues = append(issues, method+":p95_target_failed")
		}
	}
	sort.Strings(issues)
	return issues
}

func validMetrics(metrics Metrics) bool {
	if metrics.Tasks <= 0 || metrics.Completed < 0 || metrics.Completed > metrics.Tasks || len(metrics.Rows) == 0 {
		return false
	}
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		return false
	}
	seen := map[string]bool{}
	for _, row := range metrics.Rows {
		_, known := catalog.Descriptor(row.Method)
		if (!known && row.Method != "unknown") || seen[row.Method] || row.Calls <= 0 || row.Success < 0 || row.Completed < 0 || row.Completed > row.Success || row.RetryableError < 0 || row.TerminalError < 0 || row.Unknown < 0 || row.Unknown > row.Calls || row.Success+row.RetryableError+row.TerminalError != row.Calls {
			return false
		}
		seen[row.Method] = true
		if row.Method == "unknown" && row.Unknown == 0 {
			return false
		}
		errors := 0
		for code, count := range row.ErrorCodes {
			if count <= 0 || (code != "transport_error" && catalog.NewError(code, "", nil).Data.Code != code) {
				return false
			}
			errors += count
		}
		if errors != row.RetryableError+row.TerminalError {
			return false
		}
		if math.IsNaN(row.P50MS) || math.IsNaN(row.P95MS) || math.IsNaN(row.P99MS) || math.IsInf(row.P50MS, 0) || math.IsInf(row.P95MS, 0) || math.IsInf(row.P99MS, 0) || row.P50MS < 0 || row.P95MS < row.P50MS || row.P99MS < row.P95MS {
			return false
		}
	}
	return true
}
