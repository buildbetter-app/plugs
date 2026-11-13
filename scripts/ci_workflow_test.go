package scripts_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestPullRequestWorkflowsTriggerOnHotfixBranches is the #7148 regression.
// pr.yml's own comment states why release/** is in the pull_request
// trigger: "release-prep and release-fix PRs are based on the release
// branch, not on main. Without it they run almost no CI and the first
// full signal is the tag build, where a failure burns the tag." A hotfix
// backport PR (hotfix/1.3.1 and its siblings) is in exactly that position
// — based on a branch other than main — so it needs the same trigger,
// alongside release/**, not instead of it.

// TestReleaseCrossCompileRunsInItsOwnBazelLane pins the release-target
// cross-compilation gate into bazel.yml's bazel-release-cross lane, whose
// job.status ci-gate requires (BAZEL_RELEASE_CROSS), and keeps it from
// running twice: not back in bazel-pure (where it was a step on the longest
// cached lane), and not as the retired pr.yml `go build` job beside it.

// TestReleaseTargetCrossCompilationMatrixMatchesGoreleaser keeps
// scripts/ci/release-targets.txt (the single source of truth for the
// platforms scripts/ci/bazel-release-cross-compile.sh builds in bazel.yml's
// pure-Go lane) and the set of shipped release targets in lockstep. Without
// a guard a newly added release target -- the way freebsd/amd64 once was --
// is silently uncovered while a green cross-compilation check still stands.
// That is worse than having no check at all, because the check's existence
// implies the coverage it has quietly lost.

type goreleaserBuild struct {
	ID     string   `yaml:"id"`
	GOOS   []string `yaml:"goos"`
	GOARCH []string `yaml:"goarch"`
	Env    []string `yaml:"env"`
}

func readGoreleaserBuilds(t *testing.T) []goreleaserBuild {
	t.Helper()

	path := filepath.Join(sourceRepoRoot(t), ".goreleaser.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var config struct {
		Builds []goreleaserBuild `yaml:"builds"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(config.Builds) == 0 {
		t.Fatalf("%s declares no builds", path)
	}
	return config.Builds
}

// Exclude read-permission coverage is required, not best effort: Bazel's
// PR-core lane (bazel.yml's `test`, the only PR run of PR Core's tests)
// requires it, and pr.yml's gate requires that lane.

// Every run step in the three-host preflight/doc-freshness jobs invokes Go,
// and the jobs restore a non-race GOCACHE precisely so those invocations are
// incremental. A run step without GOCACHE silently falls back to the runner's
// empty default cache: "Exercise native init gateway credential shell" did,
// and cold-compiled ./cmd/bd on every PR (210-240s of a ~370s macos-latest
// leg, ~120s on Windows) right after the hook-timeout step had compiled the
// same test binary into the restored cache.

// main.yml's test job runs the macOS -race -short suite on Blacksmith macOS
// (blacksmithMacOSLabel, the image pr.yml's same-repo macOS legs use), and
// restores the module and non-race caches main.yml's
// blacksmith-macos-go-build-cache Blacksmith leg seeds: the same path and a
// restore-keys prefix of that saver's key. It writes only its own race cache.

// TestEmbeddedDoltBatchApplySuiteRunsNonRaceUnderBazel pins the non-race
// embedded batch-apply variant that replaced nightly.yml's retired go test
// step: every race lane shrinks or skips the 1000-item apply, the 712-item
// statement-count baseline and the 458-issue batch-create equivalence
// scenarios (raceEnabled), so this target is their only full-size run. It
// must stay non-race (go_test_race_off over embeddeddolt_test; race inflates
// the embedded engine's own wall-clock), set BEADS_TEST_EMBEDDED_DOLT=1 (the
// gate skipUnlessEmbeddedDolt checks), select exactly these tests, and stay in
// the embedded tier (tag embedded), which bazel.yml runs on every PR, merge
// group, push to main and night.

// TestNightlyRunsNoGoTestOnARunner: nightly.yml's only test run is the
// fresh Bazel call on rbe-west (ga-96smfk.22); its retired full-test,
// bazel-go-test-json and measure jobs ran go test on GitHub runners.

func assertNoUnmanagedGoCacheSteps(t *testing.T, workflows map[string]ciWorkflow, managed map[string]map[string]bool) {
	t.Helper()

	for workflowName, workflow := range workflows {
		for jobName, job := range workflow.Jobs {
			for _, step := range job.Steps {
				family := actionFamily(step.Uses)
				if isForbiddenGoCacheActionFamily(family) {
					t.Errorf("%s job %q has forbidden monolithic cache step %q", workflowName, jobName, step.Name)
					continue
				}
				if isGoCacheActionFamily(family) && !managed[workflowName][jobName] {
					t.Errorf("%s job %q has unmanaged cache step %q", workflowName, jobName, step.Name)
				}
			}
		}
	}
}

const (
	setupGoActionFamily         = "actions/setup-go"
	setupNodeActionFamily       = "actions/setup-node"
	setupPythonActionFamily     = "actions/setup-python"
	cacheMonolithicActionFamily = "actions/cache"
	cacheRestoreActionFamily    = "actions/cache/restore"
	cacheSaveActionFamily       = "actions/cache/save"
	setupGoSHA                  = "b7ad1dad31e06c5925ef5d2fc7ad053ef454303e"
	setupNodeSHA                = "820762786026740c76f36085b0efc47a31fe5020"
	setupPythonSHA              = "5fda3b95a4ea91299a34e894583c3862153e4b97"
	cacheSHA                    = "55cc8345863c7cc4c66a329aec7e433d2d1c52a9"
	goCacheSchema               = "v2"
	goBaseTag                   = "gms_pure_go"
	goModuleCachePath           = "~/go/pkg/mod"
	goModuleCacheRestoreID      = "restore-go-module-cache"
	macOSRunner                 = "macos-latest"
)

type goCacheStep struct {
	name        string
	id          string
	family      string
	key         string
	restoreKeys string
	path        string
	ifCondition string
}

func goModuleCacheKey() string {
	return "beads-go-mod-" + goCacheSchema + "-${{ runner.os }}-${{ runner.arch }}-go-${{ steps.setup-go.outputs.go-version }}-${{ hashFiles('go.mod', 'go.sum') }}"
}

func goModuleCacheRestoreKeys() string {
	return "beads-go-mod-" + goCacheSchema + "-${{ runner.os }}-${{ runner.arch }}-go-${{ steps.setup-go.outputs.go-version }}-"
}

func goBuildCachePrefix(profile string) string {
	// The base tag identifies the cache topology. Go's content-addressed
	// cache includes compiler options, so extra tags can safely share it.
	return "beads-go-build-" + goCacheSchema + "-${{ runner.os }}-${{ runner.arch }}-go-${{ steps.setup-go.outputs.go-version }}-base-" + goBaseTag + "-" + profile + "-"
}

func goBuildCacheKey(profile string) string {
	return goBuildCachePrefix(profile) + "${{ github.sha }}"
}

// goCacheDateSuffix is the key suffix for the day-bucketed caches saved by
// main.yml's push-to-main-only Blacksmith seeders (blacksmith-go-build-cache,
// test-windows): bounded by go.sum content plus a UTC calendar
// day, not by commit sha, so an ordinary day of push traffic cannot exhaust
// Blacksmith's 25 GB/week/repo cache quota the way a brand-new multi-GB entry
// per push did (F7b review fix S7; measured ~1.74-2.4GB/push under the old
// per-sha scheme). Each seeder's own "Compute cache date" step (id:
// cache-date) in main.yml computes the day half; restore-keys are unaffected,
// since they are already valid prefixes of this format too (they end right
// before where the sha/hash segment used to start). Pinned by
// TestBlacksmithSaverCacheKeysAreNotPerCommit.
func goCacheDateSuffix() string {
	return "${{ hashFiles('go.sum') }}-${{ steps.cache-date.outputs.today }}"
}

func goBuildCacheDayBucketedKey(profile string) string {
	return goBuildCachePrefix(profile) + goCacheDateSuffix()
}

// mainRestoreBuildCacheDayBucketedIf is mainRestoreBuildCacheIf with a
// day-bucketed key (S7), for main.yml's blacksmith-go-build-cache and
// test-windows seeders.
func mainRestoreBuildCacheDayBucketedIf(profile, condition string) goCacheStep {
	step := mainRestoreBuildCacheIf(profile, condition)
	step.key = goBuildCacheDayBucketedKey(profile)
	return step
}

// saveBuildCacheDayBucketedAfterFailureIf is saveBuildCacheAfterFailureIf
// with a day-bucketed key (S7), for main.yml's blacksmith-go-build-cache
// seeder.
func saveBuildCacheDayBucketedAfterFailureIf(profile, condition string) goCacheStep {
	step := saveBuildCacheAfterFailureIf(profile, condition)
	step.key = goBuildCacheDayBucketedKey(profile)
	return step
}

// saveBuildCacheDayBucketed is saveBuildCache with a day-bucketed key (S7),
// for main.yml's test-windows seeder.
func saveBuildCacheDayBucketed(profile string) goCacheStep {
	step := saveBuildCache(profile)
	step.key = goBuildCacheDayBucketedKey(profile)
	return step
}

func goBuildCachePath(profile string) string { return "${{ runner.temp }}/go-cache/" + profile }

func goBuildCacheRestoreID(profile string) string {
	return "restore-" + profile + "-go-build-cache"
}

func cacheMissCondition(restoreID string) string {
	return "steps." + restoreID + ".outputs.cache-hit != 'true'"
}

func combineConditions(conditions ...string) string {
	var nonEmpty []string
	for _, condition := range conditions {
		if condition != "" {
			nonEmpty = append(nonEmpty, condition)
		}
	}
	return strings.Join(nonEmpty, " && ")
}

func failureSurvivingCacheSaveCondition(conditions ...string) string {
	return "${{ !cancelled() && " + combineConditions(conditions...) + " }}"
}

func restoreModuleCache() goCacheStep {
	return goCacheStep{name: "Restore Go module cache", family: cacheRestoreActionFamily, key: goModuleCacheKey(), restoreKeys: goModuleCacheRestoreKeys(), path: goModuleCachePath}
}

func mainRestoreModuleCache() goCacheStep {
	step := restoreModuleCache()
	step.id = goModuleCacheRestoreID
	return step
}

func saveModuleCache() goCacheStep { return saveModuleCacheIf("") }

func saveModuleCacheIf(condition string) goCacheStep {
	return goCacheStep{name: "Save Go module cache", family: cacheSaveActionFamily, key: goModuleCacheKey(), path: goModuleCachePath, ifCondition: combineConditions(condition, cacheMissCondition(goModuleCacheRestoreID))}
}

// saveModuleCacheAfterFailure is saveModuleCache with a failure-surviving
// condition, for saver jobs where a flaky mid-build step should not
// prevent the module cache fetched before it from being saved.
func saveModuleCacheAfterFailure() goCacheStep {
	step := saveModuleCache()
	step.ifCondition = failureSurvivingCacheSaveCondition(cacheMissCondition(goModuleCacheRestoreID))
	return step
}

func restoreBuildCache(profile string) goCacheStep {
	return restoreBuildCacheIf(profile, "")
}

func restoreBuildCacheIf(profile, condition string) goCacheStep {
	return goCacheStep{name: "Restore " + profile + " Go build cache", family: cacheRestoreActionFamily, key: goBuildCacheKey(profile), restoreKeys: goBuildCachePrefix(profile), path: goBuildCachePath(profile), ifCondition: condition}
}

func mainRestoreBuildCache(profile string) goCacheStep {
	return mainRestoreBuildCacheIf(profile, "")
}

func mainRestoreBuildCacheIf(profile, condition string) goCacheStep {
	step := restoreBuildCacheIf(profile, condition)
	step.id = goBuildCacheRestoreID(profile)
	return step
}

func saveBuildCache(profile string) goCacheStep { return saveBuildCacheIf(profile, "") }

func saveBuildCacheIf(profile, condition string) goCacheStep {
	return goCacheStep{name: "Save " + profile + " Go build cache", family: cacheSaveActionFamily, key: goBuildCacheKey(profile), path: goBuildCachePath(profile), ifCondition: combineConditions(condition, cacheMissCondition(goBuildCacheRestoreID(profile)))}
}

// saveBuildCacheAfterFailureIf is saveBuildCache with a failure-surviving
// condition, optionally on an arbitrary matrix condition, for main.yml's
// blacksmith-go-build-cache job (F7b), which gates its race/non-race saves on
// matrix.flavor instead of matrix.os.
func saveBuildCacheAfterFailureIf(profile, condition string) goCacheStep {
	step := saveBuildCache(profile)
	step.ifCondition = failureSurvivingCacheSaveCondition(condition, cacheMissCondition(goBuildCacheRestoreID(profile)))
	return step
}

func assertGoCacheInventory(t *testing.T, job ciWorkflowJob, want []goCacheStep) {
	t.Helper()

	var got []ciWorkflowStep
	for _, step := range job.Steps {
		if isGoCacheActionFamily(actionFamily(step.Uses)) {
			got = append(got, step)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("cache steps = %d, want %d; got %+v", len(got), len(want), got)
	}
	for i, expected := range want {
		step := got[i]
		if step.Name != expected.name || step.ID != expected.id || actionFamily(step.Uses) != expected.family || step.With["key"] != expected.key || step.With["restore-keys"] != expected.restoreKeys || step.With["path"] != expected.path || step.If != expected.ifCondition {
			t.Errorf("cache step %d = {name:%q id:%q family:%q key:%q restore-keys:%q path:%q if:%q}, want {name:%q id:%q family:%q key:%q restore-keys:%q path:%q if:%q}", i, step.Name, step.ID, actionFamily(step.Uses), step.With["key"], step.With["restore-keys"], step.With["path"], step.If, expected.name, expected.id, expected.family, expected.key, expected.restoreKeys, expected.path, expected.ifCondition)
		}
	}
}

func assertStepsBefore(t *testing.T, job ciWorkflowJob, before, after []string) {
	t.Helper()

	for _, beforeName := range before {
		beforeIndex := job.stepIndex(t, beforeName)
		for _, afterName := range after {
			afterIndex := job.stepIndex(t, afterName)
			if beforeIndex >= afterIndex {
				t.Errorf("step %q index %d must precede %q index %d", beforeName, beforeIndex, afterName, afterIndex)
			}
		}
	}
}

func assertGoCacheEnv(t *testing.T, job ciWorkflowJob, stepName, profile string) {
	t.Helper()
	if got := job.step(t, stepName).Env["GOCACHE"]; got != goBuildCachePath(profile) {
		t.Errorf("step %q GOCACHE = %q, want %q", stepName, got, goBuildCachePath(profile))
	}
}

func assertStepRunsExactly(t *testing.T, job ciWorkflowJob, stepName, want string) {
	t.Helper()
	if got := job.step(t, stepName).Run; got != want {
		t.Errorf("step %q run = %q, want %q", stepName, got, want)
	}
}

func assertStepEnvValue(t *testing.T, job ciWorkflowJob, stepName, key, want string) {
	t.Helper()
	if got := job.step(t, stepName).Env[key]; got != want {
		t.Errorf("step %q env %s = %q, want %q", stepName, key, got, want)
	}
}

// matrixIncludeHosts returns the sorted, de-duplicated set of matrix.include
// os values -- used by tests covering pr-preflight-platforms and
// check-doc-freshness-platforms now that F7b restructured them from a
// top-level os: list into an include:-only "matrix marker" shape.
func matrixIncludeHosts(include []ciWorkflowMatrixInclude) []string {
	seen := make(map[string]bool)
	var hosts []string
	for _, tuple := range include {
		if !seen[tuple.OS] {
			seen[tuple.OS] = true
			hosts = append(hosts, tuple.OS)
		}
	}
	sort.Strings(hosts)
	return hosts
}

func assertGoCacheWriter(t *testing.T, workflow ciWorkflow, wantJob, wantRunner, wantStep, wantCondition string) {
	t.Helper()

	var writers []string
	for jobName, job := range workflow.Jobs {
		for _, step := range job.Steps {
			if job.RunsOn == wantRunner && actionFamily(step.Uses) == cacheSaveActionFamily && step.Name == wantStep && step.If == wantCondition {
				writers = append(writers, jobName+"/"+step.Name)
			}
		}
	}
	want := wantJob + "/" + wantStep
	if len(writers) != 1 || writers[0] != want {
		t.Errorf("cache writer %q with if %q = %v, want exactly [%s]", wantStep, wantCondition, writers, want)
	}
}

var actionPin = regexp.MustCompile(`^[0-9a-f]{40}$`)

func actionFamily(uses string) string {
	family, _, found := strings.Cut(uses, "@")
	if !found {
		return uses
	}
	return family
}

func isGoCacheActionFamily(family string) bool {
	return family == cacheMonolithicActionFamily || family == cacheRestoreActionFamily || family == cacheSaveActionFamily
}

func isForbiddenGoCacheActionFamily(family string) bool {
	return family == cacheMonolithicActionFamily
}

func assertPinnedGoCacheActions(t *testing.T, workflowName string, workflow ciWorkflow) {
	t.Helper()

	allowed := map[string]string{
		setupGoActionFamily:      setupGoSHA,
		cacheRestoreActionFamily: cacheSHA,
		cacheSaveActionFamily:    cacheSHA,
	}
	for jobName, job := range workflow.Jobs {
		for _, step := range job.Steps {
			family := actionFamily(step.Uses)
			wantSHA, managed := allowed[family]
			if isForbiddenGoCacheActionFamily(family) {
				managed = true
			}
			if !managed {
				continue
			}
			_, sha, found := strings.Cut(step.Uses, "@")
			if !found || !actionPin.MatchString(sha) {
				t.Errorf("%s job %q step %q action %q is not pinned to exactly 40 lowercase hex characters", workflowName, jobName, step.Name, step.Uses)
			}
			if isForbiddenGoCacheActionFamily(family) {
				t.Errorf("%s job %q step %q uses forbidden monolithic action %q", workflowName, jobName, step.Name, family)
				continue
			}
			if !found || !actionPin.MatchString(sha) {
				continue
			}
			if sha != wantSHA {
				t.Errorf("%s job %q step %q action %q has SHA %q, want released SHA %q", workflowName, jobName, step.Name, family, sha, wantSHA)
			}
			if family == setupGoActionFamily && step.With["cache"] != "false" {
				t.Errorf("%s job %q setup-go cache = %q, want false", workflowName, jobName, step.With["cache"])
			}
		}
	}
}

// TestWorkflowsInstallPinnedDolt keeps the Dolt CLI under test pinned. Every
// workflow used to install it by piping dolthub/dolt's releases/latest
// install.sh, so the binary under test changed whenever upstream published —
// including backports, which can move "latest" backwards. When Dolt 2.3.0
// landed it regressed CALL DOLT_RESET('--hard'): roughly one freshly created
// database in twenty comes up with the procedure permanently broken
// ("Error 1105 (HY000): context canceled"), which made
// TestFreshBootstrapHealIncarnation fail on a coin flip. See
// scripts/ci/install-dolt.sh for the per-version measurements. The CLI is now
// pinned to the same release as the container image, so the two halves of
// every server-mode test (the per-test sql-server doltserver.Start launches,
// and the shared container) can never drift apart.

// TestPinnedDoltCLIMatchesContainerImage keeps the CLI pin, the sql-server
// container pin and the hermetic Bazel dolt (tools/bazel/dolt.bzl) on the same
// Dolt release. Server-mode tests run both at once
// against the same databases; a drifting pair tests a combination no release
// ever shipped.

// TestPinnedDoltCLIAtLeastRecommendedMin guards doltversion.RecommendedMin
// against the pinned Dolt CLI version (scripts/ci/install-dolt.sh) drifting
// below it (#7500). TestPinnedDoltCLIMatchesContainerImage holds the container
// image and tools/bazel/dolt.bzl to that same pin, so this covers every CI
// Dolt. A pin lowered below RecommendedMin would mean every CI run exercises a
// Dolt CLI the package's own policy already warns is below the matrix-tested
// floor.

// TestUpdateVendorHashWorkflowBuildsIndependently guards #7499: the dependabot
// vendorHash job must not take scripts/update-nix-vendorhash.sh's exit status
// on trust. A script that wrongly exits 0 on a broken build (as it did on
// #7441) is caught only if an unconditional `nix build .#default` runs after
// it and before the job reports a match or pushes the hash.

func captureOne(t *testing.T, pattern, body, source string) string {
	t.Helper()

	matches := regexp.MustCompile(pattern).FindStringSubmatch(body)
	if matches == nil {
		t.Fatalf("%s does not match %s", source, pattern)
	}
	return matches[1]
}

// F3: package-mcp and package-npm, the two lanes bazel-gate.sh does not
// know about at all - their skip reason is the caller's package-gates
// input, never the rbe job's mode.
var bazelPackageJobs = map[string]bool{bazelPackageMCPJobName: true, bazelPackageNPMJobName: true}

const (
	uploadArtifactSHA   = "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
	downloadArtifactSHA = "3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
	checkoutSHA         = "3d3c42e5aac5ba805825da76410c181273ba90b1"
	// Same SHA/comment already used in this repo for this action (rbe-
	// prewarm's mint step, update-flake-lock.yml).
	appTokenActionSHA   = "bcd2ba49218906704ab6c1aa796996da409d3eb1"
	bazelCacheKeyPrefix = "bazel-repo-v4-${{ runner.os }}-"
	bazelCacheKey       = bazelCacheKeyPrefix + "${{ hashFiles('.bazelversion', 'MODULE.bazel', 'MODULE.bazel.lock') }}"
	bazelCachePath      = "${{ runner.temp }}/bazel-ci-cache"
	// Save only from a push to main that missed the exact key: the content is
	// fixed by the key, so re-saving every push only churns the quota.
	bazelCacheSaveIf = "${{ always() && github.event_name == 'push' && github.ref == 'refs/heads/main' && " +
		"steps.bazel.outcome == 'success' && steps.bazel.outputs.cache-hit != 'true' }}"
)

// bazel.yml's jobs: the rbe job that decides the execution mode, the
// --config=ci lane, one job per CI job a Bazel config mirrors, and the two
// package gates (F3), sorted alphabetically to match TestBazelWorkflowJobsAndExecutionMode's sort.Strings.
var bazelJobNames = []string{bazelCmdDoltJobName, bazelDoltIntJobName, bazelDoltRaceJobName, bazelDoltJobName, bazelEmbedJobName, bazelIntegJobName, bazelProxiedJobName, bazelPureJobName, bazelReleaseCrossJobName, bazelServerJobName, bazelJobName, bazelPackageMCPJobName, bazelPackageNPMJobName, bazelRBEJobName, bazelRBEPrewarmJobName, bazelRRCSeedJob, bazelRRCVerifyJob}

// The lanes that only run remotely (skipped unless the rbe job chose remote);
// bazel-integration runs remotely or with the read-only cache (bazelIntegIf);
// every other lane also runs locally.
var bazelRemoteOnlyJobs = map[string]bool{bazelEmbedJobName: true, bazelProxiedJobName: true, bazelServerJobName: true}

// The rbe job's decision step reads exactly one secret, and only to test it
// for emptiness: its env value is a boolean, not the secret.
const (
	bazelRBESecretPath  = ".jobs." + bazelRBEJobName + ".steps[0].env.HAS_EXECUTOR"
	bazelRBESecretValue = "${{ secrets.RBE_WEST_EXECUTOR != '' }}"
)

// rbe-prewarm reads its one app secret (the private key; the App's public
// client-id is a literal) in exactly two places, mirroring the rbe job's own
// emptiness-test/mint pattern above: an env boolean (never an `if:`) gating
// the mint step, and the mint step's own `with:`
// (the only step in bazel.yml allowed to read a secret outside a
// setup-bazel env, alongside bazelRBESecretPath/Value). Step indices are
// steps[0]=credential check, [1]=mint, [2]=dispatch, [3]=record result (no
// checkout step: B1, security review of bdef342d5).
const (
	bazelRBEPrewarmHasAppPath  = ".jobs." + bazelRBEPrewarmJobName + ".steps[0].env.HAS_POOL_APP"
	bazelRBEPrewarmHasAppValue = "${{ secrets.RBE_POOL_APP_PRIVATE_KEY != '' }}"
	bazelRBEPrewarmKeyPath     = ".jobs." + bazelRBEPrewarmJobName + ".steps[1].with.private-key"
	bazelRBEPrewarmKeyValue    = "${{ secrets.RBE_POOL_APP_PRIVATE_KEY }}"
	// The job-level continue-on-error path itself: see
	// TestBazelWorkflowJobsAndExecutionMode's comment on the same exception.
	bazelRBEPrewarmContinueOnErrorPath = ".jobs." + bazelRBEPrewarmJobName + ".continue-on-error"
)

// rbe-ci-bep-analytics-design.md (S3) adds two steps to every lane job
// ("CI analytics summary" and "Upload CI analytics summary"): reporting
// only (the extracted summary feeds a follow-on analysis pipeline, nothing
// in bazel.yml or pr.yml reads its output), so both carry a deliberate
// step-level continue-on-error - the only step-level exception
// TestBazelWorkflowSecretsAndFailureSurface allows, matched below by job,
// step name and content (ciAnalyticsContinueOnErrorPaths) rather than by a
// single fixed path the way rbe-prewarm's job-level one is, since every
// lane job has its own copy of these two steps at a different index.

// ciAnalyticsContinueOnErrorPaths returns the walkYAML path of every CI
// analytics step's continue-on-error key, so
// TestBazelWorkflowSecretsAndFailureSurface can allow exactly those and
// nothing else. Scoped three ways, not just by step name: the job must be
// one of ciAnalyticsLaneJobs (ci_analytics_workflow_test.go), the summary
// step's run must actually invoke ci_analytics_extract.py, and the upload
// step's artifact name must start with "ci-analytics-" - so a future step
// that happens to share one of these two names, in a job this design never
// touched, can't ride along on the exception unnoticed.
func ciAnalyticsContinueOnErrorPaths(t *testing.T) map[string]bool {
	t.Helper()
	paths := map[string]bool{}
	for name, job := range readCIWorkflow(t, bazelWorkflowName).Jobs {
		if _, ok := ciAnalyticsLaneJobs[name]; !ok {
			continue
		}
		for i, step := range job.Steps {
			switch {
			case step.Name == "CI analytics summary" && strings.Contains(step.Run, "ci_analytics_extract.py"):
			case step.Name == "Upload CI analytics summary" && strings.HasPrefix(step.With["name"], "ci-analytics-"):
			default:
				continue
			}
			paths[fmt.Sprintf(".jobs.%s.steps[%d].continue-on-error", name, i)] = true
		}
	}
	return paths
}

// bazelServedTimingsStepName is bazel-embedded's per-run upload of the served
// targets' test.xml (bazel-embedded-served-testxml), which their pinned
// shard manifests are sized from (backend/http/BUILD.bazel). Telemetry like
// the CI analytics upload: nothing in bazel.yml, pr.yml or the gate reads it,
// so an artifact service 5xx or stall must not turn the required embedded
// lane red or hold it to the job timeout. Its continue-on-error is the one
// TestBazelWorkflowSecretsAndFailureSurface allows here, and only on this
// exact step (bazelServedTimingsContinueOnErrorPath).
const bazelServedTimingsStepName = "Upload served test timings"

// bazelServedTimingsContinueOnErrorPath returns the walkYAML path of the
// served-timings upload's continue-on-error key, after checking the step is
// still only that upload: upload-artifact, the fixed artifact name, test.xml
// under backend/http only, a 2-minute timeout.
func bazelServedTimingsContinueOnErrorPath(t *testing.T) string {
	t.Helper()
	job := readCIWorkflow(t, bazelWorkflowName).job(t, bazelEmbedJobName)
	for i, step := range job.Steps {
		if step.Name != bazelServedTimingsStepName {
			continue
		}
		if !strings.HasPrefix(step.Uses, "actions/upload-artifact@") ||
			step.With["name"] != "bazel-embedded-served-testxml" ||
			step.With["path"] != "bazel-testlogs/backend/http/**/test.xml" ||
			step.TimeoutMinutes != 2 || fmt.Sprint(step.ContinueOnError) != "true" {
			t.Errorf("%s step %q = %+v; want a continue-on-error, 2-minute upload-artifact of bazel-testlogs/backend/http/**/test.xml named bazel-embedded-served-testxml", bazelEmbedJobName, step.Name, step)
		}
		return fmt.Sprintf(".jobs.%s.steps[%d].continue-on-error", bazelEmbedJobName, i)
	}
	t.Fatalf("%s has no %q step", bazelEmbedJobName, bazelServedTimingsStepName)
	return ""
}

// bazelDoltIntTestContinueOnErrorPath: bazel-dolt-int's `bazel test` step's
// continue-on-error key path, one of the step-level exceptions
// TestBazelWorkflowSecretsAndFailureSurface allows (must-fix 2). Unlike the
// CI analytics steps' (reporting only, read by nothing), this one is load
// bearing: the always() "Classify tier results" step right after it (which
// itself carries no continue-on-error) reads the test step's outcome and rc
// output and decides job.status per-tier, so the test step's own
// continue-on-error does not hide a failure from pr.yml's ci-gate, it only
// moves which step's exit code determines it. Matched by job and step name
// (not a fixed index), since a future step added above it would shift it.
func bazelDoltIntTestContinueOnErrorPath(t *testing.T) string {
	t.Helper()
	job := readCIWorkflow(t, bazelWorkflowName).job(t, bazelDoltIntJobName)
	for i, step := range job.Steps {
		if step.ID == "test" {
			return fmt.Sprintf(".jobs.%s.steps[%d].continue-on-error", bazelDoltIntJobName, i)
		}
	}
	t.Fatalf("%s has no step with id \"test\"", bazelDoltIntJobName)
	return ""
}

// bazelDoltIntCheckContinueOnErrorPaths: bazel-dolt-int's two per-tier
// check_testcases.py steps' continue-on-error key paths (should-fix 2,
// PR #7535 review), the same load-bearing shape as
// bazelDoltIntTestContinueOnErrorPath above: each step's own continue-on-
// error keeps a zero-testcase shard in one tier from failing the job
// directly, but the always() "Classify tier results" step folds its outcome
// into that tier's result through --check, so nothing is hidden from
// pr.yml's ci-gate, only moved.
func bazelDoltIntCheckContinueOnErrorPaths(t *testing.T) map[string]bool {
	t.Helper()
	job := readCIWorkflow(t, bazelWorkflowName).job(t, bazelDoltIntJobName)
	paths := map[string]bool{}
	for i, step := range job.Steps {
		if step.ID == "check-ss" || step.ID == "check-cd" {
			paths[fmt.Sprintf(".jobs.%s.steps[%d].continue-on-error", bazelDoltIntJobName, i)] = true
		}
	}
	if len(paths) != 2 {
		t.Fatalf("%s: want 2 check-ss/check-cd steps, found %d", bazelDoltIntJobName, len(paths))
	}
	return paths
}

// rbe-fork: the lanes' setup-bazel env that asks it for a certificate (modes
// fork-ro and fork-rw only), and the mint's status URL the rbe job probes.
const (
	bazelForkRemoteValue = "${{ startsWith(needs.rbe.outputs.mode, 'fork-') && 'true' || '' }}"
	bazelMintStatusURL   = "https://rbe-mint.ops.gascity.com:8444/v1/status?repo="
)

// The only triggers bazel.yml may have. pull_request_target (and
// workflow_run) would run with secrets in the context of fork PRs. PRs and
// merge groups reach it only through pr.yml's call, so it runs once per PR.
var bazelWorkflowTriggers = []string{"push", "workflow_call", "workflow_dispatch"}

// What pr.yml's ci-gate does with each bazel.yml lane (every job but the rbe
// job, whose rbe-mode output the gate reads): a gated lane has a
// CI_GATE_REQUIRED id read from its workflow_call output; an advisory lane is
// not part of pr.yml's call at all (the call's inputs turn it off, checked by
// TestBazelGateSimulation), for the recorded reason. A new job in bazel.yml
// must be added to one of the two (TestBazelLaneIsGatedAlongsideLegacy).
var bazelLaneGateIDs = map[string]string{
	bazelJobName:     "BAZEL_TEST",
	bazelPureJobName: "BAZEL_PURE",
	// Every release target cross-compiled, cgo off (and nogo over each).
	bazelReleaseCrossJobName: "BAZEL_RELEASE_CROSS",
	bazelEmbedJobName:        "BAZEL_EMBEDDED",
	bazelDoltJobName:         "BAZEL_DOLTSERVER",
	// Remote-only PR Risk tiers (fork and Dependabot PRs rely on
	// pr-risk.yml's legacy jobs, like the embedded tier's).
	bazelProxiedJobName: "BAZEL_PROXIED",
	bazelServerJobName:  "BAZEL_SERVER_STORAGE",
	// main.yml's integration jobs, required on PRs although their legacy
	// twins run only on push to main: remotely on same-repo PRs, locally
	// with the read-only cache (mode cache) on fork and Dependabot PRs.
	bazelIntegJobName: "BAZEL_INTEGRATION",
	// F3: never skippable on pr.yml (package-gates is always "on" there), so
	// bazel-gate.sh's skip list never names them.
	bazelPackageMCPJobName: "PACKAGE_MCP",
	bazelPackageNPMJobName: "PACKAGE_NPM",
}

// bazelFlagGate: a lane pr.yml's ci-gate requires only while a committed
// top-level pr.yml flag is "true" (the WINDOWS_PREBUILT_REQUIRED pattern):
// the lane runs in pr.yml's call and always reports, its id is read into the
// gate step's env but is never in the static CI_GATE_REQUIRED list, and the
// run script adds it when the flag is "true". Until then it is advisory, so
// it carries job-level continue-on-error (a failure must not turn the call's
// aggregate, BAZEL, red); once required, its own id still reddens the gate.
type bazelFlagGate struct {
	id   string // its CI_GATE_REQUIRED id
	flag string // pr.yml's top-level env flag
	want string // the flag's committed value: flipping it is a reviewed change
}

var bazelFlagGatedLanes = map[string]bazelFlagGate{
	// The cmd/bd Dolt-server tier (the Dolt-gated cmd/bd tests no other lane
	// runs), advisory until its first clean runs.
	bazelCmdDoltJobName: {id: "BAZEL_CMD_DOLT", flag: "BAZEL_CMD_DOLT_REQUIRED", want: "true"},
}

// bazelFlagGateRun is the run-script fragment that adds a flag-gated lane's
// id to CI_GATE_REQUIRED.
func bazelFlagGateRun(g bazelFlagGate) string {
	return "if [[ \"$" + g.flag + "\" == \"true\" ]]; then\n" +
		"  CI_GATE_REQUIRED=\"$CI_GATE_REQUIRED " + g.id + "\"\n" +
		"fi\n"
}

// rbe-prewarm: not a build/test lane at all (no Bazel config mirrors it), so
// it has nothing for ci-gate to require; it is advisory by design
// (continue-on-error, best-effort pre-warm dispatch) and reports only so
// TestBazelLaneIsGatedAlongsideLegacy's generic shape check still applies.
var bazelAdvisoryLanes = map[string]string{
	bazelRBEPrewarmJobName: "best-effort pre-warm dispatch to gastownhall/gascity; never gates or blocks a lane",
}

// bazel-integration's if: remote (modes remote, fork-ro, fork-rw: enabled),
// or local with the read-only cache (mode cache: fork and Dependabot PRs
// while rbe-fork is closed, whose only PR-time integration run it is; a
// warm run builds and tests only what the PR changed), never plain local
// (rbe=off: a cold tagged race build of //... on one runner); and off when a
// caller passes integration: "off" (pr.yml and bazel-farm.yml pass "on"). A
// string input: on push and dispatch it is null, and null != 'off', so the
// lane keeps running on main.
const bazelIntegIf = "${{ (needs.rbe.outputs.enabled == 'true' || needs.rbe.outputs.mode == 'cache') && inputs.integration != 'off' }}"

// bazelServerStorageIf: like bazelIntegIf, bazel-server-storage honors the
// `integration` input (an explicit "off" skips it too, gastownhall/beads
// #7486 review), but unlike bazel-integration/bazel-cmd-dolt it is
// remote-only (wantRemoteOnlyIf), not mode cache -- there is no PR-time
// warm-cache run of this tier, only remote or nothing. Mode remote minus:
// there bazel-dolt-int runs this tier instead (item 5 A2,
// bazel_dolt_int_test.go).
const bazelServerStorageIf = "${{ needs.rbe.outputs.enabled == 'true' && needs.rbe.outputs.mode != 'remote' && inputs.integration != 'off' }}"

// bazelCmdDoltMemberIf: bazel-cmd-dolt's pre-union rule (bazelIntegIf) minus
// mode remote, where bazel-dolt-int runs this tier instead (item 5 A2,
// bazel_dolt_int_test.go).
const bazelCmdDoltMemberIf = "${{ (needs.rbe.outputs.enabled == 'true' || needs.rbe.outputs.mode == 'cache') && inputs.integration != 'off' && needs.rbe.outputs.mode != 'remote' }}"

// F3: package-mcp and package-npm's if. Unlike every other lane's, it does
// not read the rbe job's outputs at all - only the caller's package-gates
// input - so these two never skip because of execution mode, only because a
// caller left package-gates at its default "off".
const bazelPackageGatesIf = "${{ inputs.package-gates == 'on' }}"

// F3: the package gates' runners (bazelPackageRunsOn, in
// ci_blacksmith_runner_test.go with the other runner sizes) are sized for
// pytest/npm on the runner itself, not for a Bazel client alone.
// Mode remote only: the gates take no rbe-fork certificate, so fork modes
// (enabled too) build bd with go build on a GitHub-hosted runner, as before.

// rbe-prewarm's bespoke if (TestBazelWorkflowJobsAndExecutionMode): mode
// remote only (B1, security review of bdef342d5 - was remote or fork-rw).
// fork-rw is reached only by a fork or Dependabot pull_request run, which
// never carries a workflow_call secret regardless of this if (GitHub does
// not forward repository/App secrets to a fork's pull_request event), so
// that half of the old condition always found no-app and no-op'd; it also
// cost an idle runner-minute for nothing. bazel-farm.yml no longer forwards
// the app secrets either (TestBazelFarmWorkflowSecurity), removing the only
// other path that could have reached fork-rw with the credential attached.
// Its runs-on still reuses the exact same mode-ternary every other lane
// uses (wantRunsOn): TestBazelRBEJobDecidesOnce forbids re-deriving
// fork/event facts outside the rbe job.
const bazelRBEPrewarmIf = "${{ needs.rbe.outputs.mode == 'remote' }}"

// F3: the rbe job's own runner (not gated by its own outputs - it decides
// them). Same-repo PRs and merge_group/push/dispatch/schedule (never forks)
// get Blacksmith; a pull_request_target farm run and any fork or Dependabot
// PR stay GitHub-hosted. Pinned verbatim by TestSameRepoBlacksmithRunners.
const wantRBERunsOn = "${{ (github.event_name == 'push' || github.event_name == 'workflow_dispatch' || github.event_name == 'schedule' || github.event_name == 'merge_group' || (github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository && github.actor != 'dependabot[bot]')) && 'blacksmith-2vcpu-ubuntu-2404' || 'ubuntu-latest' }}"

// The sameRepoBlacksmith{2,4,8}vcpu consts and the real-expression evaluator
// that checks them now live in ci_blacksmith_runner_test.go (shared with
// F7c; see that file's doc comment for the API F7c rebases onto).

// F7c: used to define its own sameRepoBlacksmith4vcpu here (same expression
// as sameRepoBlacksmith2vcpu with the 4 vCPU label, for advisory jobs that
// compile Go). F7a independently defined the same const; both are now served
// by the single shared sameRepoBlacksmith4vcpu in ci_blacksmith_runner_test.go.

// main.yml's push-to-main-only Windows and macOS savers, on the exact labels
// pr.yml's Windows and macOS legs run on (no GitHub-hosted leg is left to
// seed, ga-96smfk.22).
const mainBlacksmithWindows4vcpuRunsOn = blacksmithWindows4vcpuRunsOn
const mainBlacksmithMacOSGoBuildCacheRunsOn = "${{ '" + blacksmithMacOSLabel + "' }}"

// main.yml's push-to-main-only macOS test job, on Blacksmith macOS.
const mainBlacksmithMacOSTestRunsOn = "${{ '" + blacksmithMacOSLabel + "' }}"

// pr.yml's call of bazel.yml: exactly these inputs (review D1 v2 N3). An rbe
// override would put every PR in local mode and ungate the embedded tier
// while the gate stays self-consistent; integration: "off" would drop the
// required integration lane (explicit "on", so the pin and the gate
// simulation, which reads these inputs, do not depend on the default).
var bazelPRCallWith = map[string]string{
	"build-artifact-name": "bazel-ci-build-artifacts",
	"integration":         "on",
	// F3: only pr.yml opts in; bazel-farm.yml and nightly.yml keep the
	// default "off" (TestBazelGateSimulation's other callers).
	"package-gates": "on",
	// Mirrors pr.yml's BAZEL_CMD_DOLT_REQUIRED (a `with:` cannot read env):
	// true leaves //cmd/bd:bd_test to the then-required bazel-cmd-dolt
	// (TestBazelIntegrationExcludesBdTestOnlyWhereCmdDoltCovers).
	"cmd-dolt-required": bazelFlagGatedLanes[bazelCmdDoltJobName].want,
}

// The call's aggregate result (needs.bazel.result, through bazel-gate.sh).
const bazelAggregateGateID = "BAZEL"

// The gate script and the rbe job's execution modes.
const bazelGateScript = ".github/scripts/bazel-gate.sh"

var bazelRBEModes = []string{"remote", "fork-ro", "fork-rw", "cache", "local", "skip"}

// bazelRemoteModes: the modes whose lanes execute on rbe-west (the rbe job's
// enabled output is "true" in exactly these): remote with the CI secrets,
// fork-ro and fork-rw with an rbe-fork-mint certificate.
var bazelRemoteModes = map[string]bool{"remote": true, "fork-ro": true, "fork-rw": true}

// bazelModeEnabled: the rbe job's enabled output for a mode.
func bazelModeEnabled(mode string) string { return strconv.FormatBool(bazelRemoteModes[mode]) }

// The four RBE secrets plus the rbe-prewarm job's "bazel-allocator" GitHub
// App private key (it mints its own gastownhall/gascity installation
// token from it, inline in the job's own dispatch step - see bazel.yml's
// rbe-prewarm job), the full set bazel.yml declares under workflow_call and
// the only secrets pr.yml or nightly.yml may hand it
// (TestBazelLaneIsGatedAlongsideLegacy below).
var bazelCallSecrets = map[string]string{
	"RBE_WEST_EXECUTOR":        "${{ secrets.RBE_WEST_EXECUTOR }}",
	"RBE_TLS_CERT":             "${{ secrets.RBE_TLS_CERT }}",
	"RBE_TLS_KEY":              "${{ secrets.RBE_TLS_KEY }}",
	"RBE_TLS_CA":               "${{ secrets.RBE_TLS_CA }}",
	"RBE_POOL_APP_PRIVATE_KEY": "${{ secrets.RBE_POOL_APP_PRIVATE_KEY }}",
}

// bazel-farm.yml's pull_request_target run must never carry the
// gastownhall/gascity app credential (B1, security review of bdef342d5): an
// allowlisted fork author's own PR code runs under that trust tier, and the
// app secrets mint a token with write access to another organization's
// repository. Only pr.yml and nightly.yml (same-repo/trusted triggers only)
// pass the RBE_POOL_APP_PRIVATE_KEY secret; bazel-farm.yml passes exactly these
// four.
var bazelFarmCallSecrets = map[string]string{
	"RBE_WEST_EXECUTOR": "${{ secrets.RBE_WEST_EXECUTOR }}",
	"RBE_TLS_CERT":      "${{ secrets.RBE_TLS_CERT }}",
	"RBE_TLS_KEY":       "${{ secrets.RBE_TLS_KEY }}",
	"RBE_TLS_CA":        "${{ secrets.RBE_TLS_CA }}",
}

type ciCompositeAction struct {
	Runs struct {
		Using string           `yaml:"using"`
		Steps []ciWorkflowStep `yaml:"steps"`
	} `yaml:"runs"`
}

func readSetupBazelAction(t *testing.T) ciCompositeAction {
	t.Helper()
	path := filepath.Join(sourceRepoRoot(t), setupBazelActionDir, "action.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var action ciCompositeAction
	if err := yaml.Unmarshal(data, &action); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if action.Runs.Using != "composite" {
		t.Fatalf("%s runs.using = %q, want composite", path, action.Runs.Using)
	}
	return action
}

// bazel.yml's jobs, their runner, rbe-west gate and skip rules, and the
// setup-bazel env. The skip rules are also what pr.yml's ci-gate accepts as a
// skip (TestBazelGateSimulation).

// F3: bazelPackageRunsOn's comment and the package-mcp job's runner size
// are both premised on mcp_pytest() in package-mcp.sh passing an explicit
// worker count (not "-n auto", which would reintroduce the CPU-count/
// cgroup-quota mismatch the explicit count was pinned to avoid): the
// default 8, or BEADS_MCP_PYTEST_WORKERS, which bazel.yml's package-mcp sets
// to 16 exactly when it runs on its 8 vCPU Blacksmith runner
// (TestBlacksmithBazelRunnerSizes). It is also premised on pytest-xdist
// itself being a locked dev dependency so that worker count is reproducible
// across CI runs. Pin both here directly against the script and the MCP
// package's manifest/lock.

// F3: package-mcp and package-npm's job definitions (including the very
// detect step this script's output feeds) moved into bazel.yml, so a PR that
// edits only bazel.yml's package-gate logic must still self-trigger both
// gates the same way editing pr.yml/main.yml/pr-risk.yml does; otherwise a
// change to the job that runs the gates could ship without the gates having
// run against it. No other policy test reads this script's content.

// F3: the exact "same-repo PR, or merge_group" Blacksmith expression, pinned
// once per job so a future edit cannot drift one copy from another. Reads no
// repo variable, so the rollback is a plain revert. The rbe job's own
// version additionally covers push, workflow_dispatch and schedule (never
// forks); every other job below reads only merge_group and pull_request.

// TestBlacksmithJobsReadNoSecrets: F7a's rule that a job whose runs-on can
// select a Blacksmith label never reads a secret, in any form (job env, job
// if:, step env, step with:, step if:, or a step's run: body interpolating
// ${{ secrets.X }}). Blacksmith's ternary already falls back to ubuntu-latest
// for forks and Dependabot, but the no-secrets rule is a second, independent
// backstop: even a same-repo Blacksmith run should never need repository
// secrets for a cache-free, read-only check.
//
// Review N-4 (2026-10-03): this also now scans job.If and step.If, since a
// conditional guarded on a secret (e.g. an if: expression testing
// secrets.FOO) would otherwise slip past the env/with/run checks below. It
// does not yet inspect
// container/services credentials or resolve matrix markers in runs-on
// (neither ciWorkflowJob field exists today); that is deferred to F7b, which
// is where container/services and matrix-aware runs-on resolution land.

// TestSameRepoBlacksmithExpressionSemantics now lives in
// ci_blacksmith_runner_test.go, which evaluates the real
// sameRepoBlacksmith{2,4,8}vcpu expression strings with a small GitHub
// Actions expression evaluator instead of a hand-written Go mirror
// (review SF-3). That file is shared with F7c.

// TestPRCIGateFastChecksTokens pins pr.yml's fast-checks job, the base-diff
// checks no Bazel target can hold (check-migration-hygiene.sh's delta half
// and the .beads/issues.jsonl guard): each step's id, its outputs entry, and
// the CI_GATE_REQUIRED token/env mapping, plus the FAST_CHECKS backstop
// token (the job's own .result), which gives ci-gate.sh one clear line if the
// whole job is lost before any step reports. The job's former tree checks
// are Bazel tests (TestPRPolicyChecksRunAsBazelTargets); their tokens must
// not linger.

// TestPRPolicyChecksRunAsBazelTargets: every repository guard pr.yml's
// fast-checks, PR Policy and doc-freshness jobs used to run on a GitHub
// runner is a Bazel test (ga-96smfk.20), with no tag, so bazel.yml's `test`
// lane (--config=ci, remote) runs it and CI Gate / Required gates on it, or,
// for the integration-tagged doc-freshness suite, the integration lane.

// Review N-6 (2026-10-03): test-windows-small's dbproxy-server leg must keep
// building CGO-disabled, same as before the fold.

// stepContinueOnError reports whether a step's continue-on-error is truthy
// (the field unmarshals as `any` because the YAML schema also allows an
// expression string, which GitHub itself treats as whatever it evaluates to
// - this test only cares about the literal `true` every current step uses).
func stepContinueOnError(step ciWorkflowStep) bool {
	v, ok := step.ContinueOnError.(bool)
	return ok && v
}

// TestPRWindowsMakeShellLegsFailTheJob is review SF-1 (2026-10-03):
// windows-make-shell is a required gate (WINDOWS_MAKE_SHELL). Before this
// fix, every leg's install/exercise step carried continue-on-error and a
// hand-written final step reconstructed the job's pass/fail from each step's
// .outcome - two mutations (an aggregator that always exits 0, and one that
// silently dropped the MSYS2 install check) both passed the suite, because
// nothing pinned the aggregator's own correctness. The fix removes the
// aggregator entirely: every step after checkout keeps `if: !cancelled()`
// (so a failing leg does not stop later legs from attempting to run), but
// only the Cygwin *setup* step (a flaky installer download, not a code
// failure) may carry continue-on-error. Every other step's failure now fails
// the job directly, with no hand-written logic in between to go stale.

// TestPRWindowsMakeShellSetupStepsHaveTimeouts is review SF-2 (2026-10-03):
// the Cygwin and MSYS2 setup steps download third-party installers from the
// network; a step timeout bounds how long a hang can block the later legs'
// exercise steps behind it, instead of consuming the whole 30-minute job
// timeout (which would also lose native/msys2 coverage, since their exercise
// steps would never get a chance to run before the job-level cancel).

type bazelWorkflowCall struct {
	Inputs map[string]struct {
		Type    string `yaml:"type"`
		Default string `yaml:"default"`
	} `yaml:"inputs"`
	Secrets map[string]struct {
		Required bool `yaml:"required"`
	} `yaml:"secrets"`
	Outputs map[string]struct {
		Value string `yaml:"value"`
	} `yaml:"outputs"`
}

func readBazelWorkflowCall(t *testing.T) bazelWorkflowCall {
	t.Helper()
	path := filepath.Join(sourceRepoRoot(t), ".github", "workflows", bazelWorkflowName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		On struct {
			WorkflowCall bazelWorkflowCall `yaml:"workflow_call"`
		} `yaml:"on"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return doc.On.WorkflowCall
}

// pr.yml calls bazel.yml once per PR and merge group, and its ci-gate
// requires the Bazel lanes, which are the only run of the Linux Go test tiers
// (ga-96smfk.22 retired the legacy jobs they mirrored in pr.yml and
// pr-risk.yml).

// A called workflow's workflow-level concurrency group is evaluated in the
// caller's context, where github.workflow is the caller's name: a group equal
// to the caller's own deadlocks and GitHub cancels the call (BAZEL cancelled,
// every PR red). So bazel.yml's group never uses github.workflow and, for
// every event its callers run on, differs from each caller's group (review
// D1 v2 N4).

// bazelLaneRunModes: the rbe modes in which a lane's `if:` runs it, for a
// call with these inputs. Only the forms TestBazelWorkflowJobsAndExecutionMode
// allows are known. GitHub's != on strings is case-insensitive.
func bazelLaneRunModes(t *testing.T, lane, ifExpr string, with map[string]string) map[string]bool {
	t.Helper()
	switch ifExpr {
	case "${{ needs.rbe.outputs.mode != 'skip' }}":
		return map[string]bool{"remote": true, "fork-ro": true, "fork-rw": true, "cache": true, "local": true}
	case "${{ needs.rbe.outputs.enabled == 'true' }}":
		return map[string]bool{"remote": true, "fork-ro": true, "fork-rw": true}
	case bazelDoltMemberIf:
		// The dolt-server tier's own job, where bazel-dolt-race does not run
		// (TestBazelDoltRaceArmsExactlyOnePerMode).
		return map[string]bool{"fork-ro": true, "fork-rw": true, "cache": true, "local": true}
	case bazelProxiedMemberIf:
		return map[string]bool{"fork-ro": true, "fork-rw": true}
	case bazelIntegIf:
		if strings.EqualFold(with["integration"], "off") {
			return map[string]bool{}
		}
		return map[string]bool{"remote": true, "fork-ro": true, "fork-rw": true, "cache": true}
	case bazelServerStorageIf:
		// Remote-only (unlike bazelIntegIf, no mode cache run), but also
		// honors `integration` like bazel-integration/bazel-cmd-dolt
		// (gastownhall/beads#7486 review). Minus mode remote: there
		// bazel-dolt-int runs this tier instead (item 5 A2).
		if strings.EqualFold(with["integration"], "off") {
			return map[string]bool{}
		}
		return map[string]bool{"fork-ro": true, "fork-rw": true}
	case bazelCmdDoltMemberIf:
		// Where bazel-integration runs, minus mode remote: there
		// bazel-dolt-int runs this tier instead (item 5 A2,
		// bazel_dolt_int_test.go).
		if strings.EqualFold(with["integration"], "off") {
			return map[string]bool{}
		}
		return map[string]bool{"fork-ro": true, "fork-rw": true, "cache": true}
	case bazelPackageGatesIf:
		// F3: unlike every other lane, these two never skip because of the
		// rbe job's decision - only because the caller turned them off
		// entirely (pr.yml always passes "on").
		if strings.EqualFold(with["package-gates"], "on") {
			return map[string]bool{"remote": true, "fork-ro": true, "fork-rw": true, "cache": true, "local": true, "skip": true}
		}
		return map[string]bool{}
	case bazelRBEPrewarmIf:
		// Mode remote only (TestBazelWorkflowJobsAndExecutionMode's comment
		// on the job: B1 removed fork-rw). Unlike the historical advisory
		// lanes, this one legitimately runs in pr.yml's own call (see
		// bazelAdvisoryLanes' name-specific exception in
		// TestBazelGateSimulation below). bazel-dolt-race's if
		// (bazelDoltRaceJobIf) is the same expression: mode remote.
		return map[string]bool{"remote": true}
	case bazelRRCSeedJobIf, bazelRRCVerifyJobIf:
		// The remote repo contents cache's writer (push to main) and
		// nightly check (schedule, dispatch): never in a pull_request,
		// merge_group or pull_request_target call, in any mode.
		return map[string]bool{}
	case bazelDoltIntJobIf:
		// Mode remote only (bazel-dolt-race's own if is the same shape), and
		// also honors `integration` like its members (#7491's review,
		// must-fix 1): a server-storage/cmd-dolt skip via integration="off"
		// must not become a silent "nothing ran the tier" gap when mode is
		// remote (bazel_dolt_int_test.go's
		// TestBazelDoltIntArmsExactlyOnePerMode checks that directly; this
		// case just keeps bazelLaneRunModes consistent with it).
		if strings.EqualFold(with["integration"], "off") {
			return map[string]bool{}
		}
		return map[string]bool{"remote": true}
	}
	t.Fatalf("%s if = %q: teach bazelLaneRunModes which modes run it", lane, ifExpr)
	return nil
}

// bazelGateScenario: what pr.yml's ci-gate sees of one bazel.yml call.
type bazelGateScenario struct {
	name          string
	event         string
	mode, enabled string            // the call's rbe-mode / rbe-enabled outputs
	call          string            // needs.bazel.result
	outputs       map[string]string // the jobs' result outputs ("" = not reported)
	// Non-Bazel needs' results that differ from success.
	results map[string]string
	// pr.yml's top-level flags the run script reads (the workflow env is
	// not part of the step's env): bazelFlagGatedLanes' flags.
	flags       map[string]string
	wantPass    bool
	wantMention string // a red gate must name this id
}

// fastChecksLegacyJobNames: pr.yml's fast-checks job (F7a) output id -> the
// standalone job name it replaced. Lets the gate simulation resolve
// needs.fast-checks.outputs.* through the same prLegacyJobResult lookup
// (and sc.results overrides) the pre-fold standalone jobs used.
var fastChecksLegacyJobNames = map[string]string{
	"build-tags":          "check-build-tags",
	"version-consistency": "check-version-consistency",
	"migration-hygiene":   "check-migration-hygiene",
	"no-beads-changes":    "check-no-beads-changes",
	"fmt":                 "fmt-check",
}

// runPRGateStep runs pr.yml's actual "Evaluate CI gate" step (its run block,
// under GitHub's bash flags) with its env evaluated for the scenario: every
// non-Bazel need succeeded; Bazel expressions read the scenario. An env
// expression of any other form fails the test, so the simulation cannot
// silently drift from the workflow.
func runPRGateStep(t *testing.T, step ciWorkflowStep, sc bazelGateScenario) (bool, string) {
	t.Helper()
	expr := regexp.MustCompile(`^\$\{\{ needs\.([A-Za-z0-9_-]+)\.(result|outputs\.([A-Za-z0-9_-]+))( \|\| 'skipped')? \}\}$`)
	env := []string{"PATH=" + os.Getenv("PATH"), "GITHUB_EVENT_NAME=" + sc.event}
	for key, value := range sc.flags {
		env = append(env, key+"="+value)
	}
	for key, value := range step.Env {
		if !strings.Contains(value, "${{") {
			env = append(env, key+"="+value)
			continue
		}
		m := expr.FindStringSubmatch(value)
		if m == nil {
			t.Fatalf("ci-gate env %s = %q: the gate simulation cannot evaluate it", key, value)
		}
		var got string
		switch {
		case m[1] == "fast-checks" && m[2] != "result":
			// F7a: fast-checks folds five formerly-standalone jobs into one
			// job's steps; ci-gate reads each step's outcome via the job's
			// outputs instead of a separate job's .result. Resolve through
			// the same prLegacyJobResult the standalone jobs used, keyed by
			// their old job name, so sc.results overrides (and the default
			// "success") work exactly as they did before the fold.
			legacy, ok := fastChecksLegacyJobNames[m[3]]
			if !ok {
				t.Fatalf("ci-gate env %s = %q: fast-checks has no such step output", key, value)
			}
			got = prLegacyJobResult(legacy, sc)
		case m[1] != "bazel" && m[2] == "result":
			got = prLegacyJobResult(m[1], sc)
		case m[1] != "bazel":
			t.Fatalf("ci-gate env %s = %q: the gate simulation cannot evaluate it", key, value)
		case m[2] == "result":
			got = sc.call
		case m[3] == "rbe-mode":
			got = sc.mode
		case m[3] == "rbe-enabled":
			got = sc.enabled
		default:
			// The call's output as GitHub evaluates it over the jobs'
			// results (a union lane's members read its result first).
			got = bazelCallOutputValue(t, m[3], sc.outputs)
		}
		if got == "" && m[4] != "" {
			got = "skipped"
		}
		env = append(env, key+"="+got)
	}
	cmd := exec.Command("bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", step.Run)
	cmd.Dir = sourceRepoRoot(t)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return err == nil, string(out)
}

// The gate over every execution mode (review D1 F3): the skip script allows
// exactly the skips the lanes' own `if:`s produce in that mode, and for every
// lane that should run, that lane alone skipped, failed or cancelled turns
// pr.yml's actual gate step red, whatever the aggregate says; a missing or
// inconsistent mode, or an aggregate failure no lane explains, turns it red;
// and a mode that did not execute the lanes on rbe-west (skip, local, cache)
// is red even when everything that ran passed (BAZEL_REMOTE).

// Artifact names are unique within a run, and a called workflow's uploads
// belong to the caller's run: a duplicate name makes the second upload fail
// (409) or a download pick the wrong file. So every run that calls bazel.yml
// (with its own inputs) must not upload any name twice (review D1 F7).

type runArtifactUpload struct {
	name, where string
	pattern     *regexp.Regexp // nil when the name resolved to a literal
	matrix      bool
}

func bazelArtifactNamesCollide(a, b runArtifactUpload) bool {
	switch {
	case a.pattern == nil && b.pattern == nil:
		return a.name == b.name
	case a.pattern == nil:
		return b.pattern.MatchString(a.name)
	case b.pattern == nil:
		return a.pattern.MatchString(b.name)
	}
	return a.name == b.name
}

// collectRunArtifactUploads lists the upload-artifact names of one run of
// file: its jobs' steps and, recursively, those of the local workflows its
// jobs call with their `with:` inputs (workflow_call defaults otherwise).
// inputs.X, `inputs.X || 'lit'` and env.X (job, then workflow env) resolve;
// any other expression (a matrix value) becomes a wildcard.
func collectRunArtifactUploads(t *testing.T, file string, with map[string]string, prefix string) []runArtifactUpload {
	t.Helper()
	path := filepath.Join(sourceRepoRoot(t), ".github", "workflows", file)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		On struct {
			WorkflowCall struct {
				Inputs map[string]struct {
					Default any `yaml:"default"`
				} `yaml:"inputs"`
			} `yaml:"workflow_call"`
		} `yaml:"on"`
		Env  map[string]string `yaml:"env"`
		Jobs map[string]struct {
			Uses     string            `yaml:"uses"`
			With     map[string]any    `yaml:"with"`
			Env      map[string]string `yaml:"env"`
			Strategy struct {
				Matrix any `yaml:"matrix"`
			} `yaml:"strategy"`
			Steps []ciWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	inputs := map[string]string{}
	for name, in := range doc.On.WorkflowCall.Inputs {
		if in.Default != nil {
			inputs[name] = fmt.Sprint(in.Default)
		}
	}
	for k, v := range with {
		inputs[k] = v
	}
	exprRe := regexp.MustCompile(`\$\{\{\s*(.*?)\s*\}\}`)
	inputOr := regexp.MustCompile(`^inputs\.([A-Za-z0-9_-]+)(?:\s*\|\|\s*'([^']*)')?$`)
	envRef := regexp.MustCompile(`^env\.([A-Za-z0-9_]+)$`)
	var out []runArtifactUpload
	var names []string
	for name := range doc.Jobs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, jobName := range names {
		job := doc.Jobs[jobName]
		if strings.HasPrefix(job.Uses, "./.github/workflows/") {
			callWith := map[string]string{}
			for k, v := range job.With {
				callWith[k] = fmt.Sprint(v)
			}
			called := strings.TrimPrefix(job.Uses, "./.github/workflows/")
			out = append(out, collectRunArtifactUploads(t, called, callWith, prefix+file+" job "+jobName+" -> ")...)
			continue
		}
		var resolve func(s string, depth int) (string, bool)
		resolve = func(s string, depth int) (string, bool) {
			literal := true
			res := exprRe.ReplaceAllStringFunc(s, func(m string) string {
				inner := exprRe.FindStringSubmatch(m)[1]
				if in := inputOr.FindStringSubmatch(inner); in != nil {
					if v := inputs[in[1]]; v != "" {
						return v
					}
					return in[2]
				}
				if e := envRef.FindStringSubmatch(inner); e != nil && depth < 4 {
					v, ok := job.Env[e[1]]
					if !ok {
						v, ok = doc.Env[e[1]]
					}
					if ok {
						r, lit := resolve(v, depth+1)
						literal = literal && lit
						return r
					}
				}
				literal = false
				return "\x00"
			})
			return res, literal
		}
		for _, step := range job.Steps {
			if actionFamily(step.Uses) != "actions/upload-artifact" {
				continue
			}
			name, literal := resolve(step.With["name"], 0)
			u := runArtifactUpload{name: name, where: prefix + file + " job " + jobName, matrix: job.Strategy.Matrix != nil}
			if !literal {
				parts := strings.Split(name, "\x00")
				for i := range parts {
					parts[i] = regexp.QuoteMeta(parts[i])
				}
				u.pattern = regexp.MustCompile("^" + strings.Join(parts, ".+") + "$")
				u.name = strings.ReplaceAll(name, "\x00", "*")
			}
			out = append(out, u)
		}
	}
	return out
}

// Every action in the Bazel lane is pinned to a full commit SHA, with the same
// released SHAs the Go lanes use, and the monolithic actions/cache is banned
// here too (restore everywhere, save only where the topology says).

// The runner cache holds only content fixed by .bazelversion and
// MODULE.bazel.lock (repository cache, Bazelisk), so it is keyed on exactly
// that and written only by a push to main that missed the key; it never holds
// the generated rc or credentials.

// The lane runs the committed ci config over //..., reports the critical path
// and the go test equivalence even when tests fail, catches BUILD drift, and
// hands secrets to nothing but setup-bazel.

// bazel.yml publishes a Bazel-built bd under pr.yml's build-artifacts contract
// (artifact name, file names, checksum file, retention), so the jobs that
// download ci-build-artifacts can switch to it without other edits.

// bazel-doltserver is the only run of what pr.yml's container-backed jobs
// ("Test (storage domain + uow)", "Contract corpus") ran: --config=doltserver
// (hermetic dolt sql-servers, remote-executable), with a dolt-server target
// in every package those jobs ran. Nothing runs those suites against
// testcontainers any more: no docker lane, no requires-docker target, no
// image pull in bazel.yml, and neither job in pr.yml.

// doltServerRuleEnv is the rule env every dolt-server target must set, and
// doltServerExtraEnv what particular targets need on top: the TestMain
// switches that turn a server which cannot start into a failure.
var (
	doltServerRuleEnv  = []string{`"BEADS_TEST_DOLT_SERVER": "local"`, `"BEADS_TEST_REQUIRE_DOLT_CONTAINER": "1"`}
	doltServerExtraEnv = map[string][]string{
		"fix_dolt_test":      {`"BEADS_FIX_REQUIRE_DOLT": "1"`},
		"protocol_dolt_test": {`"BEADS_PROTOCOL_REQUIRE_DOLT": "1"`},
		// Without it every proxied test self-skips (the jobs' step env).
		"bd_proxied_test": {`"BEADS_TEST_PROXIED_SERVER": "1"`},
		// federation_test.go fails instead of skipping without a server.
		"dolt_server_full_test": {`"BEADS_TEST_ENV_RUN_DOLT": "1"`},
	}
	// doltServerLaneTags are the tags of the lanes whose targets start
	// hermetic dolt sql-servers; checkDoltServerRules holds each of them to
	// the same rules.
	doltServerLaneTags = []string{"dolt-server", "dolt-server-proxied", "dolt-server-integration", "dolt-server-cmd"}
	bazelRuleNameRe    = regexp.MustCompile(`(?m)^\s*name\s*=\s*"([^"]+)"`)
)

// bazelTopRules splits a BUILD file into its top-level calls: each starts at
// a line matching bazelTopRuleRe and ends at the next line that is just ")".
func bazelTopRules(build string) []string {
	var rules []string
	var cur []string
	in := false
	for _, line := range strings.Split(build, "\n") {
		if !in && bazelTopRuleRe.MatchString(line) {
			in, cur = true, nil
		}
		if in {
			cur = append(cur, line)
			if line == ")" || (len(cur) == 1 && strings.HasSuffix(strings.TrimSpace(line), ")")) {
				rules = append(rules, strings.Join(cur, "\n"))
				in = false
			}
		}
	}
	return rules
}

// doltServerRuleTag returns the dolt-server lane tag a rule carries, or "".
func doltServerRuleTag(rule string) string {
	for _, tag := range doltServerLaneTags {
		if strings.Contains(rule, `"`+tag+`"`) {
			return tag
		}
	}
	return ""
}

// checkDoltServerRules checks each rule tagged for a dolt-server lane
// (doltServerLaneTags) in one package's BUILD file on its own (a file-wide
// search would accept another target's env), and that the package has at
// least one.
func checkDoltServerRules(pkg, build string) []error {
	var errs []error
	found := 0
	for _, rule := range bazelTopRules(stripStarlarkComments(build)) {
		tag := doltServerRuleTag(rule)
		if tag == "" {
			continue
		}
		found++
		name := "?"
		if m := bazelRuleNameRe.FindStringSubmatch(rule); m != nil {
			name = m[1]
		}
		where := "//" + pkg + ":" + name + " (" + tag + ")"
		for _, env := range append(append([]string{}, doltServerRuleEnv...), doltServerExtraEnv[name]...) {
			if !strings.Contains(rule, env) {
				errs = append(errs, errors.New(where+" lacks rule env "+env))
			}
		}
		if strings.Contains(rule, `"no-remote-exec"`) {
			errs = append(errs, errors.New(where+" is tagged no-remote-exec; the lane runs on remote workers"))
		}
	}
	if found == 0 {
		errs = append(errs, errors.New(pkg+"/BUILD.bazel has no dolt-server target for the dolt-server lane"))
	}
	return errs
}

// A dolt-server rule is checked on its own: another target in the same file
// (here an untagged variant) carrying the env must not cover for it.

// bazelAttrBlock returns the text of a list attribute (`    name = [` up to
// its closing `    ],`) of a rule block from bazelRuleBlock, or "".
func bazelAttrBlock(rule, name string) string {
	i := strings.Index(rule, "\n    "+name+" = [")
	if i < 0 {
		return ""
	}
	end := strings.Index(rule[i:], "\n    ],\n")
	if end < 0 {
		return rule[i:]
	}
	return rule[i : i+end+7]
}

// bazel-integration mirrors main.yml's integration jobs with the unmodified
// --config=integration (whose .bazelrc filter and flags
// TestBazelrcIntegrationLane pins): no extra filter, selector or remote
// config on the command line, a BEP for the test-count check, and no
// equivalence step. ci and integration share bazel-testlogs (review F3), so
// neither the integration lane in bazel-test nor --config=ci here.

// bazelrcLines returns .bazelrc's trimmed lines as a set.
func bazelrcLines(t *testing.T) map[string]bool {
	t.Helper()
	lines := map[string]bool{}
	for _, line := range strings.Split(readPolicyFile(t, bazelPolicyRoot(t), ".bazelrc"), "\n") {
		lines[strings.TrimSpace(line)] = true
	}
	return lines
}

// assertBazelTierStep checks the test step of a remote-only tier job in
// bazel.yml: it runs exactly `bazel test //... --config=<config>` with a BEP,
// keeps bazel's exit status, has a timeout below the job's, and is followed
// by check_testcases.py on that BEP.
func assertBazelTierStep(t *testing.T, job ciWorkflowJob, jobName, config string) {
	t.Helper()
	const id, bep, wantIf = "test", "bazel-bep.json", ""
	step := job.step(t, "bazel test //... --config="+config)
	if step.ID != id || step.If != wantIf || step.TimeoutMinutes == 0 || step.TimeoutMinutes >= job.TimeoutMinutes {
		t.Errorf("%s step --config=%s: id=%q if=%q timeout-minutes=%d; want id %q, if %q, a timeout below the job's %d",
			jobName, config, step.ID, step.If, step.TimeoutMinutes, id, wantIf, job.TimeoutMinutes)
	}
	cmd := regexp.MustCompile(`\s*\\\n\s*`).ReplaceAllString(step.Run, " ")
	wantCmd := `bazel test //... --config=` + config + ` --build_event_json_file="$RUNNER_TEMP/` + bep + `"`
	// Every `bazel test` ends its configs with nightly's fresh-results
	// switch (ci_merge_queue_test.go).
	wantCmd = `bazel test //... --config=` + config + ` ` + bazelFreshArg + ` --build_event_json_file="$RUNNER_TEMP/` + bep + `"`
	if !strings.Contains(cmd, wantCmd) || !strings.Contains(step.Run, "set -o pipefail") || strings.Count(step.Run, "bazel test //") != 1 ||
		strings.Contains(step.Run, "--config=remote-exec") || strings.Contains(step.Run, "--test_tag_filters") {
		t.Errorf("%s step --config=%s does not run exactly %q:\n%s", jobName, config, wantCmd, step.Run)
	}
	assertTestStepKeepsExitStatus(t, step)
	found := false
	for _, s := range job.Steps {
		if strings.TrimSpace(s.Run) == `python3 tools/bazel/check_testcases.py --bep "$RUNNER_TEMP/`+bep+`"` {
			found = true
			if s.If != "${{ always() && steps."+id+".outcome != 'skipped' }}" || (s.ContinueOnError != nil && s.ContinueOnError != false) {
				t.Errorf("%s: check_testcases.py for %s: if=%q continue-on-error=%v", jobName, bep, s.If, s.ContinueOnError)
			}
		}
	}
	if !found {
		t.Errorf("%s: no check_testcases.py step on %s (a shard or selector that runs no tests exits 0)", jobName, bep)
	}
}

// The proxied-server tier (pr-risk.yml "Test (Proxied Dolt Cmd N/15)") and the server-Dolt storage tier (pr-risk.yml "Test (Server Dolt
// Conformance)", "Test (Server Dolt Full Suite N/16)") as Bazel variants:
// each manifest-sharded variant runs its jobs' shard script with their shard
// total, the conformance variant the job's exact flags, with the jobs' race
// setting, subprocess binaries and switches; the lanes' configs keep the
// jobs' selection, and the storage lane shares --config=integration's build.
// bazel.yml runs both tiers remotely only.

// bazel-embedded replaces pr-risk.yml's embedded-Dolt tier: --config=embedded
// runs the embedded variants race like the jobs' binaries, with the jobs'
// parallelism; each shard variant runs its job's shard script with the job's
// shard total, and the conformance variants pass the jobs' exact selectors.
// bazel-embedded's test step, exactly (review F4).
const bazelEmbeddedTestRun = `set -o pipefail
start=$(date +%s)
rc=0
bazel test //... --config=embedded ${BAZEL_FRESH:+"$BAZEL_FRESH"} \
  --build_event_json_file="$RUNNER_TEMP/bazel-bep.json" \
  --execution_log_compact_file="$RUNNER_TEMP/bazel-exec.log.zst" \
  --profile="$RUNNER_TEMP/bazel-profile.json" \
  --experimental_build_event_upload_strategy=local \
  2>&1 | tee "$RUNNER_TEMP/bazel-test.log" || rc=$?
echo "bazel test --config=embedded: exit $rc, $(( $(date +%s) - start ))s wall" | tee -a "$GITHUB_STEP_SUMMARY"
exit "$rc"`

// .bazelrc's --config=embedded, exactly and in order (review F4): no
// --test_filter, no -test.short/-test.run/-test.skip, no retries (result
// caching as every lane's: nightly re-executes). The conformance targets' own -test.run/-test.skip args
// (the legacy jobs' partition) are checked against pr-risk.yml below.
var bazelEmbeddedRCLines = []string{
	"test:embedded --@rules_go//go/config:race",
	"test:embedded --test_tag_filters=embedded",
	"test:embedded --build_tests_only",
	"test:embedded --keep_going",
	"test:embedded --test_summary=terse",
	"test:embedded --test_timeout=-1,-1,-1,1200",
	"test:embedded --test_arg=-test.parallel=4",
	"test:embedded --test_env=GO_TEST_WRAP_TESTV=1",
	"test:embedded --remote_download_regex=.*/test\\.xml$",
	"test:embedded --experimental_remote_cache_eviction_retries=0",
	// F5 S1: the race configuration's nogo is owned by the test lane.
	"test:embedded --norun_validations",
}

// conformanceCoreSlowGroups are the conformance.RunAll groups
// embeddeddolt_conformance_core_slow_test runs and
// embeddeddolt_conformance_core_test skips. The core target skips only these
// names, so a group renamed or added in RunAll still runs there: a name here
// that RunAll no longer has matches nothing in either target and loses no
// group, only the split (core gets slower again).
var conformanceCoreSlowGroups = []string{"ReadyCountsPageChunking", "Portable"}

// bazel-pure replaces pr.yml's check-cmd-bd-puregeo-tests job: its pure
// cmd/bd test selector, the same pure build set, and the js/wasm hook test
// under the same exact-count guard.
// bazel.yml's pure-Go and js/wasm lane is the only run of the CGO_ENABLED=0
// portability boundaries pr.yml's retired check-cmd-bd-puregeo-tests job
// owned (ga-96smfk.22): the same -short cmd/bd subset, the same cmd/bd and
// test-binary builds, and the exact js/wasm refusal contract with its count
// guard. The selectors below are that job's, frozen, except that the cmd/bd
// subset also runs TestNocgoStoreFactoriesRefuseGraphWorkspace, the only run
// of the CGO_ENABLED=0 store factories' graph_mode refusal.

// swallowedExit matches shell that hides a failing command's status.
var swallowedExit = regexp.MustCompile(`\bexit\b|\|\|\s*(true|:)|;\s*(true|:)\s*$|\bset\s+\+e\b|\btrap\b`)

// shellCommands joins backslash continuations and drops blank and comment
// lines.
func shellCommands(run string) []string {
	var cmds []string
	cur := ""
	for _, line := range strings.Split(run, "\n") {
		trimmed := strings.TrimSpace(line)
		if cur == "" && (trimmed == "" || strings.HasPrefix(trimmed, "#")) {
			continue
		}
		if strings.HasSuffix(trimmed, "\\") {
			cur += strings.TrimSuffix(trimmed, "\\") + " "
			continue
		}
		cmds = append(cmds, strings.Join(strings.Fields(cur+trimmed), " "))
		cur = ""
	}
	if cur != "" {
		cmds = append(cmds, strings.Join(strings.Fields(cur), " "))
	}
	return cmds
}

// A report step's status is its script's: the script is the last command,
// nothing after it on that line, and nothing in the step can mask a failure.
func assertReportStepKeepsExitStatus(t *testing.T, step ciWorkflowStep, invocation string) {
	t.Helper()
	if step.Shell != "" {
		t.Errorf("step %q overrides shell %q; the default bash -e is part of the contract", step.Name, step.Shell)
	}
	cmds := shellCommands(step.Run)
	if len(cmds) == 0 {
		t.Errorf("step %q runs nothing", step.Name)
		return
	}
	last := cmds[len(cmds)-1]
	if !strings.HasPrefix(last, invocation+" ") || strings.ContainsAny(last, ";&|") {
		t.Errorf("step %q must end with a bare %q so its exit status is the step's; last command %q", step.Name, invocation, last)
	}
	for _, cmd := range cmds {
		if swallowedExit.MatchString(cmd) {
			t.Errorf("step %q command %q can hide the script's exit status", step.Name, cmd)
		}
	}
}

// The bazel test step captures bazel's status through tee and must exit
// with it.
func assertTestStepKeepsExitStatus(t *testing.T, step ciWorkflowStep) {
	t.Helper()
	if step.Shell != "" {
		t.Errorf("step %q overrides shell %q", step.Name, step.Shell)
	}
	cmds := shellCommands(step.Run)
	if len(cmds) == 0 || cmds[len(cmds)-1] != `exit "$rc"` {
		t.Errorf("step %q must end with exit \"$rc\"; commands %q", step.Name, cmds)
	}
	exits := 0
	for _, cmd := range cmds {
		if regexp.MustCompile(`(^|[;&|]\s*)exit\b`).MatchString(cmd) {
			exits++
		}
		if regexp.MustCompile(`\|\|\s*(true|:)|;\s*(true|:)\s*$|\bset\s+\+e\b|\btrap\b`).MatchString(cmd) {
			t.Errorf("step %q command %q can hide bazel's exit status", step.Name, cmd)
		}
	}
	if exits != 1 {
		t.Errorf("step %q has %d exit commands, want only the final exit \"$rc\"", step.Name, exits)
	}
}

func readYAMLNode(t *testing.T, rel string) *yaml.Node {
	t.Helper()
	path := filepath.Join(sourceRepoRoot(t), rel)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		t.Fatalf("%s: want a YAML mapping document", path)
	}
	return doc.Content[0]
}

// walkYAML calls fn with the dotted path of every mapping key and scalar.
func walkYAML(node *yaml.Node, path string, fn func(path string, key bool, value string)) {
	switch node.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			k := node.Content[i].Value
			fn(path+"."+k, true, k)
			walkYAML(node.Content[i+1], path+"."+k, fn)
		}
	case yaml.SequenceNode:
		for i, item := range node.Content {
			walkYAML(item, fmt.Sprintf("%s[%d]", path, i), fn)
		}
	case yaml.ScalarNode:
		fn(path, false, node.Value)
	case yaml.AliasNode:
		if node.Alias != nil {
			walkYAML(node.Alias, path, fn)
		}
	}
}

func yamlMapKeys(node *yaml.Node, key string) []string {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value != key {
			continue
		}
		v := node.Content[i+1]
		switch v.Kind {
		case yaml.MappingNode:
			var keys []string
			for j := 0; j < len(v.Content); j += 2 {
				keys = append(keys, v.Content[j].Value)
			}
			return keys
		case yaml.SequenceNode:
			var keys []string
			for _, item := range v.Content {
				keys = append(keys, item.Value)
			}
			return keys
		default:
			return []string{v.Value}
		}
	}
	return nil
}

// bazel.yml hands the RBE credentials to PR-head code on same-repo PRs (an
// accepted, documented risk). Keep that exposure from growing: only the
// listed triggers (never pull_request_target), secrets referenced from the
// setup-bazel step's env and nowhere else (not workflow/job env, run, with or
// if), and no continue-on-error anywhere, so a red lane stays red.

// The execution mode (remote, cache, local, skip) is decided once, by the rbe job,
// so a run cannot mix modes and no lane can drift from the others (review D1
// F1/F5/F6): Dependabot PRs are same-repo but get no secrets, so a condition
// on vars and fork alone sent them to the remote-only path with nothing to
// run it. The job's decide step (its first) reads nothing but booleans, runs
// no repository code, and its outputs are the job's and the workflow_call
// outputs; the steps after it are the worker-env preflight
// (TestBazelRBEWorkerEnvPreflight), which set no output.

// bazelTestMintEnv selects runBazelRBEDecision's curl stub's answer for
// rbe-fork-mint's /v1/status: ro, rw (open, that tier), closed, rw-closed
// (open false; today's mint answers ro instead while rw is off), canary (403), busy (502), garbage, evil (open with a tier
// that is neither), or anything else (connection refused, as while the
// farm's gate is closed or DNS has no rbe-mint yet).
const bazelTestMintEnv = "BAZEL_TEST_MINT"

// bazelTestCurlStub stands in for curl on the decision step's PATH: it logs
// its arguments and prints what the mint would. Its exit status is curl's
// (no -f: HTTP errors still print the body and exit 0).
const bazelTestCurlStub = `#!/usr/bin/env bash
printf 'curl %s\n' "$*" >>"$BAZEL_TEST_CURL_LOG"
open='"instance": "oss-fork", "endpoint": "grpcs://rbe-fork.ops.gascity.com:8444"'
case "${BAZEL_TEST_MINT:-}" in
ro) echo "{\"open\": true, \"tier\": \"ro\", $open, \"reason\": \"eligible\"}" ;;
rw) echo '{"open": true, "tier": "rw", "instance": "oss", "endpoint": "grpcs://rbe-fork.ops.gascity.com:8444", "reason": "eligible"}' ;;
closed) echo "{\"open\": false, \"tier\": \"ro\", $open, \"reason\": \"rbe-fork is closed\"}" ;;
rw-closed) echo '{"open": false, "tier": "rw", "instance": "oss", "endpoint": "grpcs://rbe-fork.ops.gascity.com:8444", "reason": "the rw tier is closed"}' ;;
canary) echo '{"error": "rbe-fork canary: this PR is not enabled yet"}' ;;
busy) echo '{"error": "mint busy; retry later"}' ;;
garbage) echo '<html>bad gateway</html>' ;;
evil) echo '{"open": true, "tier": "admin", "instance": "", "endpoint": "grpcs://elsewhere:1"}' ;;
*) echo "curl: (7) Failed to connect to rbe-mint.ops.gascity.com port 8444" >&2; exit 7 ;;
esac
`

// runBazelRBEDecision runs the rbe job's step script under bash -e (as
// GitHub's default shell) and returns its $GITHUB_OUTPUT. curl is
// bazelTestCurlStub (env bazelTestMintEnv picks the mint's answer), so no
// test reaches the network.
func runBazelRBEDecision(t *testing.T, script string, env map[string]string) (map[string]string, error) {
	t.Helper()
	out, _, err := runBazelRBEDecisionLog(t, script, env)
	return out, err
}

// runBazelRBEDecisionLog: runBazelRBEDecision, plus the stub's curl log.
func runBazelRBEDecisionLog(t *testing.T, script string, env map[string]string) (map[string]string, string, error) {
	t.Helper()
	dir := t.TempDir()
	output := filepath.Join(dir, "output")
	bin := filepath.Join(dir, "bin")
	curlLog := filepath.Join(dir, "curl.log")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "curl"), []byte(bazelTestCurlStub), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script)
	cmd.Env = []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "GITHUB_OUTPUT=" + output, "GITHUB_STEP_SUMMARY=" + filepath.Join(dir, "summary"),
		"GITHUB_REPOSITORY=gastownhall/beads", "GITHUB_RUN_ID=4242", "GITHUB_RUN_ATTEMPT=2", "BAZEL_TEST_CURL_LOG=" + curlLog}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	b, err := cmd.CombinedOutput()
	logData, _ := os.ReadFile(curlLog)
	if err != nil {
		return nil, string(logData), fmt.Errorf("%v: %s", err, b)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		return nil, string(logData), err
	}
	out := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		k, v, _ := strings.Cut(line, "=")
		out[k] = v
	}
	return out, string(logData), nil
}

// write-bazelrc.sh: no secrets means a local-only rc, plus --config=fork-cache
// with BAZEL_FORK_CACHE=true (fork PRs), a partial set (or the fork cache
// with the secrets) fails, and the full set enables remote-exec with the key
// material and rc outside the workspace and the runner cache.

const (
	bazelAutofixWorkflowName = "bazel-autofix.yml"
	bazelAutofixPushScript   = "scripts/bazel-autofix-push.sh"
	bazelSyncPatchScript     = "scripts/ci/bazel-sync-patch.sh"
	bazelSyncStepName        = "BUILD files in sync (gazelle, go_srcs, MODULE.bazel)"
)

// bazel.yml's sync step stays red on drift but leaves the allowlisted part of
// the fix as the bazel-sync-patch artifact, from PR-head code that sees no
// secrets; bazel-autofix.yml is the only consumer.

// bazel-autofix.yml runs with write permissions via workflow_run, so it must
// never check out or execute PR code: the base branch's checkout, one trusted
// script, the artifact as data, no expressions in run bodies, and the push
// token visible only to the push step.

// requireWorkflowProducesArtifact: the workflow a workflow_run trigger names
// runs on pull_request and uploads the artifact the autofix job consumes,
// from one of its own jobs or from a local workflow a job calls
// unconditionally (a called workflow's uploads belong to the caller's run,
// under the caller's run id).
func requireWorkflowProducesArtifact(t *testing.T, file, name, artifact string) {
	t.Helper()
	node := readYAMLNode(t, filepath.Join(".github", "workflows", file))
	if got := yamlScalar(node, "name"); got != name {
		t.Errorf("%s name = %q, want %q (the workflow_run trigger names it)", file, got, name)
	}
	if !slices.Contains(yamlMapKeys(node, "on"), "pull_request") {
		t.Errorf("%s has no pull_request trigger; the autofix job only acts on PR runs", file)
	}
	uploads := func(wf ciWorkflow) bool {
		for _, job := range wf.Jobs {
			for _, step := range job.Steps {
				if strings.HasPrefix(step.Uses, "actions/upload-artifact@") && step.With["name"] == artifact {
					return true
				}
			}
		}
		return false
	}
	found := uploads(readCIWorkflow(t, file))
	for _, job := range readCIWorkflow(t, file).Jobs {
		if called, ok := strings.CutPrefix(job.Uses, "./.github/workflows/"); ok && job.If == "" && uploads(readCIWorkflow(t, called)) {
			found = true
		}
	}
	if !found {
		t.Errorf("%s never uploads %s (itself or through a called workflow); the workflow_run trigger on it would be dead", file, artifact)
	}
}

// Both autofix workflows push to the same PR branches. Job-level concurrency
// (skipped runs take no part) in ONE group per head branch, never cancelling:
// they queue instead of racing, and an unrelated completion cannot cancel a
// fix in flight.

// The two push scripts share their security-critical functions verbatim, and
// both apply the same hardening.

// The producer (PR code) and the consumer (base-branch code) must agree on
// the allowlist, or every patch would be refused; the consumer's copy is the
// one that is enforced.

func yamlScalar(node *yaml.Node, key string) string {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1].Value
		}
	}
	return ""
}

// Release builds sign and attest what they build, so they must not restore
// any Actions cache: setup-go's default cache is keyed predictably and falls
// back to the default branch's module and build caches, which are not
// re-verified (a poisoned build cache compiles straight into the binaries).

// Review F3: the gated Bazel lanes never retry a failing test. The legacy
// jobs they mirror (and, since D2 step 1, replace for the embedded tier) ran
// each test once, so a retry would hide a flaky failure no other job sees.
// Nothing may set --flaky_test_attempts (or --runs_per_test_detects_flakes,
// which reports a failed-then-passed test as FLAKY, not FAILED): not .bazelrc,
// a workflow, setup-bazel's generated rc, or a tools/bazel wrapper; and no
// BUILD file or macro may mark a target flaky = True (Bazel retries those up
// to three times by default).

// Every artifact bazel.yml uploads has a fixed name within its run, and a
// job re-run ("Re-run failed jobs", the documented recovery for a Bazel lane
// that hit a remote cache eviction or a flake) is the same workflow run:
// upload-artifact v4+ refuses a second artifact of the same name there
// (409 Conflict) unless overwrite is set, which would turn a passing retry
// red. bazel-test uploads the bd package even after its tests fail.

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// The cmd/bd Dolt-server tier (bazel.yml's bazel-cmd-dolt, advisory until
// pr.yml's BAZEL_CMD_DOLT_REQUIRED is "true": bazelFlagGatedLanes): the
// whole integration-tagged cmd/bd suite against a hermetic dolt sql-server,
// because every other lane, Bazel or go test, runs cmd/bd with
// BEADS_TEST_SKIP=dolt or only a manifest's tests, so its Dolt-gated tests
// ran nowhere. Coverage is exact by construction: the target's only test
// selection is go_test_pinned_shard.sh's, whose round-robin shards skip
// exactly the tests its pinned shards run (TestPinnedShardWrapperSplit,
// TestPinnedShardManifests), and check_testcases.py fails a shard that ran
// none.

// Exactly-once cmd/bd integration-build tests on PRs: bazel-integration runs
// //... minus //cmd/bd:bd_test only where the caller says bazel-cmd-dolt is
// required (inputs.cmd-dolt-required true, which only pr.yml passes and
// only as its committed BAZEL_CMD_DOLT_REQUIRED flag), and there
// bazel-cmd-dolt covers it: the same if (so it runs in every mode
// bazel-integration does, Dependabot and forks included), bazel-gate.sh
// accepts the two lanes' skips in exactly the same modes, pr.yml's gate then
// requires its result, and its target wraps the integration build's bd_test
// with no test selection (TestBazelCmdDoltJob). Everywhere else (bazel-farm,
// nightly, push, dispatch: bazel-cmd-dolt advisory) bd_test stays in
// bazel-integration. Rollback is committing "false" in both pr.yml places.
