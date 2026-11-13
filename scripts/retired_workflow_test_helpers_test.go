package scripts_test

import (
	"fmt"
	"strings"
	"testing"
)

// The GitHub Actions workflow policy tests were retired with the workflows.
// A few retained local policy tests still share names from the removed helper
// files; keep only the constants they need to compile.
const (
	bazelDoltIntJobName  = "bazel-dolt-int"
	bazelDoltRaceJobName = "bazel-dolt-race"
	bazelDoltMemberIf    = "${{ needs.rbe.outputs.mode != 'skip' && needs.rbe.outputs.mode != 'remote' }}"
	bazelProxiedMemberIf = "${{ needs.rbe.outputs.enabled == 'true' && needs.rbe.outputs.mode != 'remote' }}"
	bazelDoltIntJobIf    = "${{ needs.rbe.outputs.mode == 'remote' && inputs.integration != 'off' }}"
	bazelForkFarmValue   = "${{ inputs.fork-farm == 'authorized' && github.event_name == 'pull_request_target' && inputs.checkout-sha != '' }}"
	refusedProbeURL      = "https://127.0.0.1:1"
)

// No retained test enumerates workflow analytics jobs. The map remains empty
// so workflow-only helper code can stay out of the executable test surface.
var ciAnalyticsLaneJobs = map[string]string{}

func bazelRCConfigLines(rc, name string) []string {
	var out []string
	for _, raw := range strings.Split(rc, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		head, rest, ok := strings.Cut(line, " ")
		cmd, cfg, hasCfg := strings.Cut(head, ":")
		if ok && hasCfg && cfg == name {
			out = append(out, fmt.Sprintf("%s %s", cmd, strings.TrimSpace(rest)))
		}
	}
	return out
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := map[string]int{}
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		counts[s]--
	}
	for _, n := range counts {
		if n != 0 {
			return false
		}
	}
	return true
}

func bazelCallOutputValue(t *testing.T, name string, jobs map[string]string) string {
	t.Helper()
	if v, ok := jobs[name]; ok {
		return v
	}
	return ""
}
