package scripts_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// worker-env-autosync.yml closes the #7419 gap: nightly.yml's rbe-worker-
// env-sync job only opens a ci/infra issue for a human to fix a gascity
// re-pin (#7387's ~100-minute stall); this instead runs tools/rbe/worker-
// env-sync every 15 minutes and, on a real mismatch (exit 1), copies
// gascity main's manifest and pin (tools/rbe/worker-env-sync-copy) and
// opens or updates a fixed-branch PR labeled status/needs-review-auto, which
// lands through the automated review and the merge queue.
const (
	workerEnvAutosyncWorkflow = "worker-env-autosync.yml"
	workerEnvAutosyncJob      = "autosync"
	workerEnvAutosyncBranch   = "bot/worker-env-sync"
	workerEnvSyncCopy         = "tools/rbe/worker-env-sync-copy"
)

// TestWorkerEnvAutosyncWorkflowTriggersOnScheduleAndDispatchOnly: the App
// private key this job mints is a secret of the autofix environment
// (deployment-branch policy: main only), and the only events that can ever
// run on main without a fork in the loop are schedule and workflow_dispatch
// -- pull_request (or pull_request_target) here would hand a fork a path to
// it.

// TestWorkerEnvAutosyncWorkflowPermissionsAreMinimal: least privilege.
// Top-level and the job's own grant both stay read-only: every write this
// job makes (the push, the PR, the label) goes through one of
// the two minted App tokens, never the job's own GITHUB_TOKEN permissions,
// so there is nothing here for either App token's role to leak into.

// TestWorkerEnvAutosyncWorkflowShape: the job compares with gascity main,
// gates every write step on a real mismatch (rc == 1, never rc == 2's
// "unknown" or rc == 0's "in step"), mints two App tokens only once a
// mismatch needs a write, pushes the fixed branch with push-token alone,
// and opens/labels the PR with pr-token alone (so it is not a
// GITHUB_TOKEN PR - those never trigger CI).

// TestWorkerEnvAutosyncWorkflowNeverInterpolatesExpressionsInRun: every
// `${{ }}` GitHub would otherwise substitute directly into a shell script
// is instead assigned to a step's `env:` first, so a value that happens to
// contain shell metacharacters (a PR title, a run URL) is data the shell
// sees through a variable, never text GitHub splices into the script
// before bash ever runs it.

// TestWorkerEnvAutosyncWorkflowNeverArmsAutoMerge: the sync PR lands the
// way any other PR does. Its status/needs-review-auto label hands it to the
// automated review workflow, and the merge workflow queues it once that
// review passes, so no step merges it or arms auto-merge, and its body
// names the label rather than promising an auto-merge nothing arms.
// Arming cannot work here: GitHub's enablePullRequestAutoMerge wants
// contents:write as well as pull-requests:write on one token (pr-token
// alone failed with "Resource not accessible by integration" on the run
// that opened #7746), the push-plus-PR token the two-token split rules
// out. Nor should it: main requires only the CI and PR Risk gates
// (engdocs/CI_REQUIRED_CHECK_TOPOLOGY.md), so an armed PR would join the
// merge queue without the review.

// TestWorkerEnvSyncCopyAgainstFixture runs the real tools/rbe/worker-env-
// sync-copy against a scratch checkout and a local gascity (reusing
// rbe_worker_env_test.go's fixture): it overwrites the manifest and the pin
// byte for byte, leaves everything else untouched, and fails loudly rather
// than writing anything when gascity's platforms/BUILD.bazel carries no
// pin to copy.
func TestWorkerEnvSyncCopyAgainstFixture(t *testing.T) {
	// worker-env-sync-copy runs only in worker-env-autosync.yml, on
	// ubuntu-latest. It edits the pin with GNU `sed -i -E`; BSD sed (macOS)
	// takes -E as -i's backup suffix, so the {64} interval is read as a
	// basic regex and never matches, and the pin is left stale. Its owner
	// keeps it Linux-only rather than porting it.
	if runtime.GOOS != "linux" {
		t.Skipf("worker-env-sync-copy is Linux-CI-only (worker-env-autosync.yml) and uses GNU sed -i -E; GOOS=%s", runtime.GOOS)
	}
	f := newRBEWorkerEnvFixture(t)
	stalePin := rbeTestPin(rbeTestStaleManifest)
	beads := f.beads(t, rbeTestStaleManifest, stalePin)
	rbeTestWrite(t, filepath.Join(beads, workerEnvSyncCopy), readPolicyFile(t, bazelPolicyRoot(t), workerEnvSyncCopy), 0o755)
	// A file the copy must never touch.
	rbeTestWrite(t, filepath.Join(beads, "tools/rbe/untouched.txt"), "keep\n", 0o644)

	code, out := f.run(t, beads, workerEnvSyncCopy+" main\n")
	if code != 0 {
		t.Fatalf("worker-env-sync-copy: exit %d\n%s", code, out)
	}
	if !strings.Contains(out, "copied gascity main's worker-env ("+f.pin+")") {
		t.Errorf("worker-env-sync-copy output %q, want it to report the copied pin %s", out, f.pin)
	}
	gotManifest, err := os.ReadFile(filepath.Join(beads, rbeWorkerEnvManifest))
	if err != nil || string(gotManifest) != f.manifest {
		t.Errorf("worker-env.txt after copy = %q, %v; want gascity's manifest %q", gotManifest, err, f.manifest)
	}
	gotBuild, err := os.ReadFile(filepath.Join(beads, rbeWorkerPlatformBuild))
	if err != nil || !strings.Contains(string(gotBuild), "\"worker-env\": \""+f.pin+"\",") {
		t.Errorf("platforms/BUILD.bazel after copy = %q, %v; want the pin %s", gotBuild, err, f.pin)
	}
	untouched, err := os.ReadFile(filepath.Join(beads, "tools/rbe/untouched.txt"))
	if err != nil || string(untouched) != "keep\n" {
		t.Errorf("worker-env-sync-copy touched an unrelated file: %q, %v", untouched, err)
	}

	// gascity's platforms/BUILD.bazel with no pin at all: nothing copied.
	noPinGascity := t.TempDir()
	rbeTestWrite(t, filepath.Join(noPinGascity, "main/tools/rbe/worker-env.txt"), "arch x86_64\n", 0o644)
	rbeTestWrite(t, filepath.Join(noPinGascity, "main/platforms/BUILD.bazel"), "platform(\n    name = \"rbe_worker\",\n)\n", 0o644)
	beadsNoPin := f.beads(t, rbeTestStaleManifest, stalePin)
	rbeTestWrite(t, filepath.Join(beadsNoPin, workerEnvSyncCopy), readPolicyFile(t, bazelPolicyRoot(t), workerEnvSyncCopy), 0o755)
	cmd := "WORKER_ENV_SYNC_URL=file://" + noPinGascity + " " + workerEnvSyncCopy + " main\n"
	code, out = f.run(t, beadsNoPin, cmd)
	if code == 0 || strings.Contains(out, "copied gascity") {
		t.Errorf("worker-env-sync-copy with no gascity pin: exit %d\n%s, want a failure and nothing copied", code, out)
	}
	stillStale, err := os.ReadFile(filepath.Join(beadsNoPin, rbeWorkerEnvManifest))
	if err != nil || string(stillStale) != rbeTestStaleManifest {
		t.Errorf("worker-env-sync-copy wrote despite no gascity pin: %q, %v", stillStale, err)
	}
}
