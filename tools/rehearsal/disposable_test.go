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
		t.Fatal("R1_SOURCE_SHA and R1_SOURCE_TREE are required; synthetic source markers cannot qualify a rollback transition")
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
				if !run.Rollback.InputsFrozen || run.Rollback.Software != "PASS_digest_verified_old_candidate_old" || run.Rollback.Complete != "PASS_executable_image_prompt_plugin_data_transition" || run.Rollback.Data != "PASS_snapshot_restore_public_readback" || run.Rollback.ReverseConversion == "PASS" {
					t.Fatalf("rollback boundary changed: %+v", run.Rollback)
				}
				if !run.Rollback.CandidatePinned || !run.Rollback.OldToCandidate || !run.Rollback.CandidatePublicReadback || !run.Rollback.CandidateStopped || !run.Rollback.CandidateToOld || !run.Rollback.RollbackPublicReadback || !run.Rollback.AtomicReplacements || !run.Rollback.IncarnationBoundary || !run.Rollback.CleanupVerified || !run.Rollback.WrongDigestRefused || !run.Rollback.PinMismatchRefused {
					t.Fatalf("missing deployable transition evidence: %+v", run.Rollback)
				}
				if run.Rollback.OldBundle.SourceSHA != sha || run.Rollback.OldBundle.SourceTree != tree || run.Rollback.CandidateBundle.SourceSHA != sha || run.Rollback.CandidateBundle.SourceTree != tree || run.Rollback.RollbackBundle.SourceSHA != sha || run.Rollback.RollbackBundle.SourceTree != tree {
					t.Fatalf("bundle provenance is not bound to the candidate: %+v", run.Rollback)
				}
				for _, digest := range []string{run.Rollback.OldBundle.ExecutableDigest, run.Rollback.OldBundle.ImageDigest, run.Rollback.OldBundle.PromptDigest, run.Rollback.OldBundle.PluginDigest, run.Rollback.CandidateBundle.ExecutableDigest, run.Rollback.CandidateBundle.ImageDigest, run.Rollback.CandidateBundle.PromptDigest, run.Rollback.CandidateBundle.PluginDigest, run.Rollback.RollbackBundle.ExecutableDigest, run.Rollback.RollbackBundle.ImageDigest, run.Rollback.RollbackBundle.PromptDigest, run.Rollback.RollbackBundle.PluginDigest} {
					if len(digest) != 64 {
						t.Fatalf("bundle identity is not a SHA-256: %q", digest)
					}
				}
				if run.Rollback.OldIncarnation == "" || run.Rollback.CandidateIncarnation == "" || run.Rollback.RollbackIncarnation == "" || run.Rollback.OldIncarnation == run.Rollback.CandidateIncarnation || run.Rollback.CandidateIncarnation == run.Rollback.RollbackIncarnation {
					t.Fatalf("runtime incarnation transition was not proven: %+v", run.Rollback)
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
	if _, err := RunDisposable(context.Background(), "sqlite", strings.Repeat("a", 40), strings.Repeat("b", 40)); err == nil {
		t.Fatal("synthetic source markers accepted")
	}
}

func TestRuntimeBundleValidationAndAtomicReplacementFailClosed(t *testing.T) {
	if err := ensureWorkRoot(); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(workRoot, "plug-swarm-i8-bundle-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	data := []byte("disposable-data")
	bundle, err := buildRuntimeBundle(root, "candidate", "0123456789abcdef0123456789abcdef01234567", "89abcdef0123456789abcdef0123456789abcdef", data)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeBundle(bundle); err != nil {
		t.Fatal(err)
	}
	serviceRoot := filepath.Join(root, "service")
	if err := installRuntimeFiles(serviceRoot, bundle); err != nil {
		t.Fatal(err)
	}
	promptPath := filepath.Join(serviceRoot, runtimePromptName)
	promptBefore, err := os.ReadFile(promptPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bundle.PromptPath, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeBundle(bundle); err == nil {
		t.Fatal("wrong prompt digest accepted")
	}
	if got, readErr := os.ReadFile(promptPath); readErr != nil || string(got) != string(promptBefore) {
		t.Fatalf("failed validation changed the installed prompt: %v", readErr)
	}
	bundle.PromptPath = filepath.Join(root, "missing-prompt")
	if err := validateRuntimeBundle(bundle); err == nil {
		t.Fatal("missing pinned artifact accepted")
	}
}
