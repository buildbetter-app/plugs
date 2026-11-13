package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Merge queue readiness (engdocs/CI_REQUIRED_CHECK_TOPOLOGY.md, "Merge
// Queue"). The default-branch ruleset requires "CI Gate / Required" (pr.yml)
// and "PR Risk Gate / Required" (pr-risk.yml); with a merge queue both must
// also report on every queue entry's merge-group commit, from a merge_group
// run that has no github.event.pull_request at all. These tests simulate
// that event with the shared evalGHExpr evaluator (ci_blacksmith_runner_test.go)
// and the workflows' real step scripts.

const (
	// Test results are cached (read and written) on every run but
	// nightly's: nightly.yml passes fresh-test-results: true, and bazel.yml
	// appends BAZEL_FRESH (--config=fresh, --nocache_test_results) last to
	// every `bazel test`.
	bazelFreshEnv     = "${{ inputs.fresh-test-results && '--config=fresh' || '' }}"
	bazelFreshArg     = `${BAZEL_FRESH:+"$BAZEL_FRESH"}`
	bazelFreshRCLine  = "test:fresh --nocache_test_results"
	mergeQueueActor   = "github-merge-queue[bot]"
	mergeQueueOwnRepo = "gastownhall/beads"
)

// mergeGroupCtx: an evalGHExpr context for a merge_group run of entry n
// (no github.event.pull_request.* key: null on this event).
func mergeGroupCtx(n int, extra map[string]string) map[string]string {
	ctx := map[string]string{
		"github.event_name":                 "merge_group",
		"github.repository":                 mergeQueueOwnRepo,
		"github.actor":                      mergeQueueActor,
		"github.ref":                        fmt.Sprintf("refs/heads/gh-readonly-queue/main/pr-%d-%040d", 7000+n, n),
		"github.run_attempt":                "1",
		"github.event.merge_group.base_sha": fmt.Sprintf("%040d", 100+n),
		"github.event.merge_group.head_sha": fmt.Sprintf("%040d", 200+n),
		"github.event.merge_group.base_ref": "refs/heads/main",
		"github.event.merge_group.head_ref": fmt.Sprintf("refs/heads/gh-readonly-queue/main/pr-%d-%040d", 7000+n, n),
	}
	for k, v := range extra {
		ctx[k] = v
	}
	return ctx
}

// pullRequestCtx: an evalGHExpr context for a same-repo pull_request run.
func pullRequestCtx(n int, extra map[string]string) map[string]string {
	ctx := map[string]string{
		"github.event_name":                             "pull_request",
		"github.repository":                             mergeQueueOwnRepo,
		"github.actor":                                  "alice",
		"github.ref":                                    fmt.Sprintf("refs/pull/%d/merge", 7000+n),
		"github.run_attempt":                            "1",
		"github.event.pull_request.number":              strconv.Itoa(7000 + n),
		"github.event.pull_request.base.sha":            fmt.Sprintf("%040d", 300+n),
		"github.event.pull_request.head.sha":            fmt.Sprintf("%040d", 400+n),
		"github.event.pull_request.head.repo.full_name": mergeQueueOwnRepo,
		"github.event.pull_request.head.repo.fork":      "false",
	}
	for k, v := range extra {
		ctx[k] = v
	}
	return ctx
}

// interpolateGH replaces every ${{ ... }} in s with its evalGHExpr value.
func interpolateGH(t *testing.T, s string, ctx map[string]string) string {
	t.Helper()
	re := regexp.MustCompile(`\$\{\{(.*?)\}\}`)
	return re.ReplaceAllStringFunc(s, func(m string) string {
		v, err := evalGHExpr(m, ctx)
		if err != nil {
			t.Fatalf("evalGHExpr(%q): %v", m, err)
		}
		switch x := v.(type) {
		case nil:
			return ""
		case bool:
			return strconv.FormatBool(x)
		default:
			return fmt.Sprint(x)
		}
	})
}

// The two required gates: their workflows run on merge_group (exactly the
// checks_requested type), the gate jobs keep their ruleset names and always
// run, and no job's or step's condition in either workflow keys on the
// event, so the merge group runs (or skips) exactly what the decision jobs
// say, like a PR.

// Concurrency: a merge group's runs key on its own gh-readonly-queue/* ref,
// so two queue entries never share a group (cancel-in-progress would
// otherwise cancel the earlier entry's required checks), and a PR's own
// runs never cancel its queue entry's or the other way round. bazel.yml's
// called-workflow group never cancels a merge group either.

// Every github.event.pull_request.* read in the required topology either
// falls back to the merge_group field (evaluated below: the queue's SHAs on
// merge_group, the PR's on pull_request) or is listed here with why null is
// right on a merge group. A new read fails until it is classified.

// Merge groups: bazel.yml's rbe step, evaluated by evalGHExpr for a merge
// group, takes mode remote with the CI secrets, so pr.yml's gate (which
// requires every lane to have run remotely, BAZEL_REMOTE) can pass. Without
// the secret or with the farm switch off the mode is cache or skip, and the
// gate is red (TestPRGateRequiresRemoteBazelLanes).

// Test result caching: every `bazel test` in bazel.yml reuses cached
// results except where the caller asks for fresh ones. BAZEL_FRESH is one
// workflow-level env, --config=fresh only when inputs.fresh-test-results is
// true (nightly.yml passes it; nothing else does), appended after every
// other --config of each `bazel test` so its --nocache_test_results wins;
// no lane config turns caching off; and nothing adds retries (eviction retries stay 0 where
// set, TestBazelRetiredLanesArePinned / TestBazelPRLanesArePinned).

// detect-package-gates.sh on merge_group diffs the queue's own bounds
// (merge_group.base_sha..head_sha), not HEAD^: a rebase-method entry
// stacks every commit of the PR, and only the first touches a package.

// fast-checks' .beads guard runs on merge_group against the queue's base
// commit and on pull_request against origin/<base branch>.

// The guard fails closed when its diff base is missing (a failed git diff
// piped into grep -q would otherwise pass).

// check-doc-flags' CLI-docs drift attribution needs a diff base; a merge
// group has no GITHUB_BASE_REF, so the step passes the queue's base commit
// (and nothing on pull_request, where origin/$GITHUB_BASE_REF applies).
// Without it, drift already on main fails every queue entry.

// The shared evaluator follows GitHub's loose equality, so a boolean input
// compared with a string ('true') is false, as on GitHub, instead of
// masking that classic workflow_call bug.
func TestGHExprLooseEquality(t *testing.T) {
	for _, c := range []struct {
		expr string
		ctx  map[string]any
		want bool
	}{
		{"${{ inputs.b == 'true' }}", map[string]any{"inputs.b": true}, false},
		{"${{ inputs.b == true }}", map[string]any{"inputs.b": true}, true},
		{"${{ inputs.b == false }}", map[string]any{"inputs.b": false}, true},
		{"${{ inputs.b == '' }}", map[string]any{"inputs.b": false}, true},
		{"${{ github.event.pull_request.head.repo.fork == true }}", map[string]any{}, false},
		{"${{ github.event.pull_request.number == '' }}", map[string]any{}, true},
		{"${{ vars.X == 'true' }}", map[string]any{"vars.X": "TRUE"}, true},
		{"${{ github.run_attempt != '1' }}", map[string]any{"github.run_attempt": "2"}, true},
	} {
		got, err := evalGHExprTyped(c.expr, c.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("%s with %v = %v, want %v", c.expr, c.ctx, got, c.want)
		}
	}
}

// mergeQueueRepo: a git repo with a base commit and one commit per file
// group on top; returns its dir, the base SHA and the head SHA.
func mergeQueueRepo(t *testing.T, commits ...[]string) (dir, base, head string) {
	t.Helper()
	dir = t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	write := func(rel, content string) {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("base.txt", "base\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "base")
	base = strings.TrimSpace(gitIn(t, dir, "rev-parse", "HEAD"))
	for i, files := range commits {
		for _, f := range files {
			write(f, fmt.Sprintf("change %d\n", i))
		}
		gitIn(t, dir, "add", "-A")
		gitIn(t, dir, "commit", "-q", "-m", fmt.Sprintf("change %d", i))
	}
	head = strings.TrimSpace(gitIn(t, dir, "rev-parse", "HEAD"))
	return dir, base, head
}

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull,
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}
