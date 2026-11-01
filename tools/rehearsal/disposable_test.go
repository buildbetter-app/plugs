package rehearsal

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestThreeDisposableRehearsals(t *testing.T) {
	sha, tree := os.Getenv("R1_SOURCE_SHA"), os.Getenv("R1_SOURCE_TREE")
	if sha == "" || tree == "" {
		sha, tree = strings.Repeat("a", 40), strings.Repeat("b", 40)
		t.Log("provenance=synthetic unit markers; hosted wrapper receipt binds test source")
	}
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			report, err := RunDisposable(ctx, kind, sha, tree)
			if err != nil {
				t.Fatal(err)
			}
			if !report.ResourcesCleaned || report.Preparation != "PASS" || report.Qualification != "WAIT" || report.FactoryWorkers != sampledFactoryWorkers || report.FactoryTasks != sampledFactoryTaskCount || len(report.Runs) != 3 {
				t.Fatalf("invalid prep state: %+v", report)
			}
			for _, run := range report.Runs {
				if !run.FreshEmptyTarget || !run.RestoredComparison || !run.NewIncarnation || !run.AuthenticatedMCP || !run.ServiceAdminRefused || run.FactoryWorkers != sampledFactoryWorkers || run.FactoryTasks != sampledFactoryTaskCount || run.Baseline.Completed != sampledFactoryTaskCount || run.Shadow.Completed != sampledFactoryTaskCount || len(run.Baseline.Rows) != 8 || len(run.Shadow.Rows) != 8 {
					t.Fatalf("missing functional evidence: %+v", run)
				}
				for _, digest := range []string{run.Rollback.ImageDigest, run.Rollback.PromptDigest, run.Rollback.PluginDigest, run.Rollback.DataDumpDigest} {
					if len(digest) != 64 {
						t.Fatalf("rollback digest is not a SHA-256: %q", digest)
					}
				}
				if !run.Rollback.InputsFrozen || run.Rollback.Software != "WAIT_pinned_bundle_canary_only_no_redeployment" || run.Rollback.Complete != "WAIT_full_image_prompt_plugin_data_rollback_requires_redeployment_and_beads_readback" || run.Rollback.Data != run.Rollback.ReverseConversion {
					t.Fatalf("rollback boundary changed: %+v", run.Rollback)
				}
				for _, row := range run.Shadow.Rows {
					if row.TerminalError != 0 || row.RetryableError != 0 || row.Unknown != 0 {
						t.Fatalf("caller failure: %+v", row)
					}
				}
				for _, wait := range run.ComparisonWaits {
					if !strings.HasSuffix(wait, ":latency_target_unset") {
						t.Fatalf("unexpected comparison issue: %s", wait)
					}
				}
				if run.ReverseConversion != "WAIT_native_head_reverse_profile_refused" {
					t.Fatalf("reverse boundary changed; independently review before qualification: %s", run.ReverseConversion)
				}
			}
			waits := strings.Join(report.GateWaits, " ")
			if !strings.Contains(waits, "not_three_consecutive_nights") || !strings.Contains(waits, "authenticated_staging_unqualified") {
				t.Fatal(waits)
			}
			raw, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "synthetic caller context") || strings.Contains(string(raw), "service:rehearsal") || strings.Contains(string(raw), "user:rehearsal-admin") {
				t.Fatal("report contains payload/principal")
			}
			t.Logf("I8_PREP_REPORT %s", raw)
		})
	}
}

func TestDisposableRunnerRefusesExternalResourceConfiguration(t *testing.T) {
	if _, err := RunDisposable(context.Background(), "postgres", strings.Repeat("a", 40), strings.Repeat("b", 40)); err == nil {
		t.Fatal("unowned Postgres path accepted")
	}
	if _, err := RunDisposable(context.Background(), "sqlite", "HEAD", strings.Repeat("b", 40)); err == nil {
		t.Fatal("unbound source accepted")
	}
}
