package rehearsal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestThreeDisposableRehearsals(t *testing.T) {
	sha, tree := testSourcePins(t)
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			report, err := RunDisposable(ctx, kind, sha, tree)
			if err != nil {
				t.Fatal(err)
			}
			if !report.ResourcesCleaned || report.Preparation != "PASS" || report.Qualification != "WAIT" || report.FactoryWorkers != sampledFactoryWorkers || report.FactoryTasks != sampledFactoryTaskCount || len(report.Runs) != 3 {
				t.Fatalf("invalid prep state: %+v", report)
			}
			if report.RuntimeRollback == nil || !completeRuntimeRollback(*report.RuntimeRollback, sha, tree) {
				t.Fatal("caller runner did not consume the complete owned rollback and cleanup")
			}
			for _, run := range report.Runs {
				if !run.FreshEmptyTarget || !run.RestoredComparison || !run.NewIncarnation || !run.AuthenticatedMCP || !run.ServiceAdminRefused || !run.TranscriptCompared || run.TranscriptExchanges != 3 || run.FactoryWorkers != sampledFactoryWorkers || run.FactoryTasks != sampledFactoryTaskCount || run.Baseline.Completed != sampledFactoryTaskCount || run.Shadow.Completed != sampledFactoryTaskCount || len(run.Baseline.Rows) != 8 || len(run.Shadow.Rows) != 8 {
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

func testSourcePins(t *testing.T) (string, string) {
	t.Helper()
	_, sha, tree, err := sourceCheckout()
	if err != nil {
		t.Fatal(err)
	}
	return sha, tree
}

func TestDeployableRollbackTransition(t *testing.T) {
	sha, tree := testSourcePins(t)
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			report, err := RunDeployableRollback(ctx, kind, sha, tree)
			if err != nil {
				t.Fatalf("actual transition failed: %v; report=%+v", err, report)
			}
			if report.Software != "PASS_digest_verified_old_candidate_old" || report.Complete != "PASS_executable_image_prompt_plugin_data_transition" || report.Data != "PASS_snapshot_restore_public_readback" || !strings.HasPrefix(report.ReverseConversion, "WAIT_") {
				t.Fatalf("qualification boundary: %+v", report)
			}
			if !report.InputsFrozen || !report.CandidatePinned || !report.OldToCandidate || !report.CandidatePublicReadback || !report.CandidateStopped || !report.CandidateToOld || !report.RollbackPublicReadback || !report.AtomicReplacements || !report.IncarnationBoundary || !report.CleanupVerified || !report.WrongDigestRefused || !report.PinMismatchRefused {
				t.Fatalf("missing actual transition evidence: %+v", report)
			}
			if report.OldBundle.ExecutableDigest != report.RollbackBundle.ExecutableDigest || report.OldBundle.ImageDigest != report.RollbackBundle.ImageDigest || report.OldBundle.PromptDigest != report.RollbackBundle.PromptDigest || report.OldBundle.PluginDigest != report.RollbackBundle.PluginDigest || report.RollbackBundle.DataDigest != report.CandidateBundle.DataDigest {
				t.Fatal("rollback did not restore exact old artifacts and checkpoint")
			}
			if len(report.Observations) != 3 {
				t.Fatal("missing actual runtime observations")
			}
			seenContainers := map[string]bool{}
			seenIncarnations := map[string]bool{}
			for i, b := range []RuntimeBundle{report.OldBundle, report.CandidateBundle, report.RollbackBundle} {
				if b.SourceSHA != sha || b.SourceTree != tree {
					t.Fatal("runtime is not pinned to tested source")
				}
				for _, digest := range []string{b.ExecutableDigest, b.ImageDigest, b.ImageArchiveDigest, b.PromptDigest, b.PluginDigest, b.DataDigest} {
					if len(digest) != 64 {
						t.Fatal("missing real artifact digest")
					}
				}
				ob := report.Observations[i]
				if seenContainers[ob.ContainerID] || ob.ContainerID == "" || seenIncarnations[ob.RestoredIncarnation] || ob.RestoredIncarnation == "" {
					t.Fatal("runtime/incarnation reused")
				}
				seenContainers[ob.ContainerID] = true
				seenIncarnations[ob.RestoredIncarnation] = true
				if ob.ImageID != "sha256:"+b.ImageDigest || ob.ExecutableDigest != b.ExecutableDigest || ob.PromptDigest != b.PromptDigest || ob.PluginDigest != b.PluginDigest || ob.DataDigest != b.DataDigest || !ob.PublicDataReadback || !ob.ReceiptReplayed || !ob.PromptCanary || !ob.PluginCanary || (i > 0 && !ob.StaleCursorRefused) {
					t.Fatalf("running identity/public boundary mismatch: %+v", ob)
				}
			}
			raw, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(raw, []byte("Token")) || bytes.Contains(raw, []byte("plg_s_")) || bytes.Contains(raw, []byte("plg_k_")) {
				t.Fatal("runtime receipt exposed a token")
			}
			t.Logf("I8_RUNTIME_TRANSITION driver=%s same_tree_edition_profiles_only=true synthetic_admin_presence=true %s", kind, raw)
		})
	}
}

func TestRuntimeBundleValidationAndAtomicReplacementFailClosed(t *testing.T) {
	sha, tree := testSourcePins(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := ensureWorkRoot(); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(workRoot, "plug-swarm-bundle-")
	if err != nil {
		t.Fatal(err)
	}
	d := newOwnedDocker(root)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := d.cleanup(ctx); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	fixture, err := openFixture(ctx, "memory", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.rpc.Call(ctx, "memory.save", map[string]any{"key": "bundle-test", "content": "public disposable data"}, ""); err != nil {
		t.Fatal(err)
	}
	snapshot, _, dumpErr := captureDump(ctx, fixture.adminRPC)
	if err = errors.Join(dumpErr, fixture.close()); err != nil {
		t.Fatal(err)
	}
	old, err := buildRuntimeBundle(ctx, d, "old", sha, tree, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := buildRuntimeBundle(ctx, d, "candidate", sha, tree, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	service := filepath.Join(root, "service")
	if err = installRuntimeFiles(service, old, "memory", nil); err != nil {
		t.Fatal(err)
	}
	before, err := os.Readlink(filepath.Join(service, "current"))
	if err != nil {
		t.Fatal(err)
	}
	assertUnchanged := func(t *testing.T) {
		t.Helper()
		after, e := os.Readlink(filepath.Join(service, "current"))
		if e != nil || before != after {
			t.Fatal("failed install changed active generation")
		}
		for name, want := range map[string]string{runtimeExecutableName: old.ExecutableDigest, runtimePromptName: old.PromptDigest, runtimePluginName: old.PluginDigest, runtimeDataName: old.DataDigest} {
			got, e := fileDigest(filepath.Join(service, "current", name))
			if e != nil || got != want {
				t.Fatalf("partial replacement of %s", name)
			}
		}
		staged, e := os.ReadDir(filepath.Join(service, "generations"))
		if e != nil || len(staged) != 1 {
			t.Fatal("failed install left an owned partial generation")
		}
	}
	for _, point := range []string{runtimeExecutableName, runtimePromptName, runtimePluginName, runtimeDataName, "activate"} {
		t.Run("partial-"+point, func(t *testing.T) {
			injected := errors.New("injected partial install failure")
			err := installRuntimeFiles(service, candidate, "memory", func(stage string) error {
				if stage == point {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatal("partial replacement failure hidden")
			}
			assertUnchanged(t)
		})
	}
	for _, field := range []string{"executable", "image-archive", "image-pin", "prompt", "plugin", "data", "sha", "tree", "missing", "symlink"} {
		t.Run("wrong-"+field, func(t *testing.T) {
			bad := candidate
			switch field {
			case "executable":
				bad.ExecutableDigest = strings.Repeat("0", 64)
			case "image-archive":
				bad.ImageArchiveDigest = strings.Repeat("0", 64)
			case "image-pin":
				bad.ImageDigest = strings.Repeat("0", 64)
			case "prompt":
				bad.PromptDigest = strings.Repeat("0", 64)
			case "plugin":
				bad.PluginDigest = strings.Repeat("0", 64)
			case "data":
				bad.DataDigest = strings.Repeat("0", 64)
			case "sha":
				bad.SourceSHA = strings.Repeat("a", 40)
			case "tree":
				bad.SourceTree = strings.Repeat("b", 40)
			case "missing":
				bad.PromptPath = filepath.Join(root, "missing")
			case "symlink":
				bad.PromptPath = filepath.Join(root, "prompt-link")
				if err := os.Symlink(candidate.PromptPath, bad.PromptPath); err != nil {
					t.Fatal(err)
				}
			}
			if err := installRuntimeFiles(service, bad, "memory", nil); err == nil {
				t.Fatal("wrong/missing artifact accepted")
			}
			assertUnchanged(t)
		})
	}
	if err = installRuntimeFiles(service, candidate, "memory", nil); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{runtimeExecutableName: candidate.ExecutableDigest, runtimePromptName: candidate.PromptDigest, runtimePluginName: candidate.PluginDigest, runtimeDataName: candidate.DataDigest} {
		got, e := fileDigest(filepath.Join(service, "current", name))
		if e != nil || got != want {
			t.Fatal("complete candidate tuple was not activated")
		}
	}
}

func TestTransitionFailuresNeverQualifyOrStartAfterFailedStop(t *testing.T) {
	for _, point := range []string{"stop", "install", "start", "readback", "rollback"} {
		t.Run(point, func(t *testing.T) {
			var calls []string
			injected := errors.New("injected " + point + " failure")
			step := func(name string) func() error {
				return func() error {
					calls = append(calls, name)
					if name == point || (point == "rollback" && name == "readback") {
						return injected
					}
					return nil
				}
			}
			err := performRuntimeTransition(step("stop"), step("install"), step("start"), step("readback"), step("rollback"))
			if !errors.Is(err, injected) {
				t.Fatal("failed transition reported success")
			}
			wants := map[string][]string{
				"stop": {"stop"}, "install": {"stop", "install", "rollback"}, "start": {"stop", "install", "start", "rollback"},
				"readback": {"stop", "install", "start", "readback", "rollback"}, "rollback": {"stop", "install", "start", "readback", "rollback"},
			}
			if !reflect.DeepEqual(calls, wants[point]) {
				t.Fatalf("unsafe successor/recovery sequence: %v", calls)
			}
		})
	}
	for _, point := range []string{"runtime-stop", "owned-cleanup", "directory-cleanup"} {
		t.Run(point, func(t *testing.T) {
			report := Rollback{Software: "PASS_digest_verified_old_candidate_old", Complete: "PASS_executable_image_prompt_plugin_data_transition"}
			failures := []error{nil, nil, nil}
			index := map[string]int{"runtime-stop": 0, "owned-cleanup": 1, "directory-cleanup": 2}[point]
			injected := errors.New("injected " + point + " failure")
			failures[index] = injected
			err := finishRuntimeReport(&report, nil, failures[0], failures[1], failures[2])
			if !errors.Is(err, injected) || report.CleanupVerified || report.Software != "BLOCK" || report.Data != "BLOCK" || report.Complete != "BLOCK" {
				t.Fatalf("cleanup failure masked: %+v", report)
			}
		})
	}
}

func TestSourcePinsFailClosed(t *testing.T) {
	sha, tree := testSourcePins(t)
	for _, pins := range [][2]string{{strings.Repeat("a", 40), strings.Repeat("b", 40)}, {"HEAD", tree}, {sha, strings.Repeat("b", 40)}} {
		if _, err := RunDisposable(context.Background(), "sqlite", pins[0], pins[1]); err == nil {
			t.Fatal("synthetic/unbound preparation source accepted")
		}
		if _, err := RunDeployableRollback(context.Background(), "sqlite", pins[0], pins[1]); err == nil {
			t.Fatal("synthetic/unbound runtime source accepted")
		}
	}
	t.Setenv("R1_SOURCE_SHA", strings.Repeat("a", 40))
	if _, _, _, err := sourceCheckout(); err == nil {
		t.Fatal("wrong expected source pin accepted")
	}
}

func TestActualFailedTransitionRecoveryAndRollbackRemainBlocked(t *testing.T) {
	sha, tree := testSourcePins(t)
	for _, phase := range []string{"candidate-canary", "rollback-canary"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			corrupted := false
			report, err := runDeployableRollback(ctx, "memory", sha, tree, func(stage string, h *ownedRuntime) error {
				if stage != phase {
					return nil
				}
				corrupted = true
				// Corrupt the actual active generation after the real restore. The
				// public canary must refuse this digest, and recovery must reinstall the
				// untouched old bundle before any recovery readback can pass.
				return os.WriteFile(filepath.Join(h.root, "current", runtimePromptName), []byte("corrupt installed prompt"), 0600)
			})
			if !corrupted || err == nil || report.Software != "BLOCK" || report.Complete != "BLOCK" || report.Data != "BLOCK" || !report.CleanupVerified {
				t.Fatalf("real failed transition hidden: err=%v report=%+v", err, report)
			}
			if phase == "candidate-canary" {
				if !report.RecoveryAttempted || !report.RecoveryVerified || report.RecoveredObservation == nil || !report.RecoveredObservation.PublicDataReadback || !report.RecoveredObservation.PromptCanary || !report.RecoveredObservation.PluginCanary {
					t.Fatalf("real old recovery missing: %+v", report)
				}
			} else if report.RollbackPublicReadback || report.CandidateToOld || !report.CandidateStopped {
				t.Fatalf("failed final rollback qualified: %+v", report)
			}
			raw, e := json.Marshal(report)
			if e != nil {
				t.Fatal(e)
			}
			t.Logf("I8_EXPECTED_RUNTIME_FAILURE phase=%s %s", phase, raw)
		})
	}
}

func TestOwnedCleanupLeavesDifferentOwnerNetwork(t *testing.T) {
	if err := ensureWorkRoot(); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(workRoot, "plug-swarm-cleanup-")
	if err != nil {
		t.Fatal(err)
	}
	otherRoot, err := os.MkdirTemp(workRoot, "plug-swarm-untouched-")
	if err != nil {
		t.Fatal(err)
	}
	owned, other := newOwnedDocker(root), newOwnedDocker(otherRoot)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := owned.cleanup(cleanupCtx); err != nil {
			t.Error(err)
		}
		if err := other.cleanup(cleanupCtx); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(otherRoot); err != nil {
			t.Error(err)
		}
	})
	if err = owned.createNetwork(ctx); err != nil {
		t.Fatal(err)
	}
	if err = other.createNetwork(ctx); err != nil {
		t.Fatal(err)
	}
	if err = owned.owned(ctx, "network", other.network); err == nil {
		t.Fatal("other owner accepted")
	}
	if err = owned.cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if err = other.owned(ctx, "network", other.network); err != nil {
		t.Fatal("cleanup removed another owner's network")
	}
	t.Log("I8_OWNED_CLEANUP own selection empty; different-owner internal network remains")
}
