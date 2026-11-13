package scripts_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// Bazel's remote repo contents cache on rbe-west (ga-vnycm2.29, ported from
// gascity's bazel.yml, gascity #7467 and #7478). bazel.yml's rbe job turns
// vars.RBE_REPO_CONTENTS_CACHE into its rrc output (off, seed, canary, on);
// each reading lane's reader step appends bazelRRCReadLines to
// .bazelrc.local; the rrc-seed job, on push to main only, runs every lane's
// loading and analysis (.github/scripts/rrc-lane-commands.txt, through
// rrc-run-lanes.sh) and uploads repository trees with a 30-minute
// rbe-rrc-writer-beads certificate (rrc-writer-credential.sh); the
// rrc-verify job, nightly, compares the cache with a cold fetch
// (tools/bazel/rrc_verify.py).

const (
	bazelRRCReadStep    = "Remote repo contents cache (read)"
	bazelRRCModeStepID  = "rrc"
	bazelRRCVar         = "vars.RBE_REPO_CONTENTS_CACHE"
	bazelRRCSeedJob     = "rrc-seed"
	bazelRRCSeedJobIf   = "${{ github.event_name == 'push' && github.ref == 'refs/heads/main' && needs.rbe.outputs.mode == 'remote' && needs.rbe.outputs.rrc != 'off' }}"
	bazelRRCVerifyJob   = "rrc-verify"
	bazelRRCVerifyJobIf = "${{ (github.event_name == 'schedule' || github.event_name == 'workflow_dispatch') && needs.rbe.outputs.mode == 'remote' && needs.rbe.outputs.rrc != 'off' }}"
	bazelRRCCanaryLane  = "bazel-test"
	bazelRRCCredential  = ".github/scripts/rrc-writer-credential.sh"
	bazelRRCRunLanes    = ".github/scripts/rrc-run-lanes.sh"
	bazelRRCLaneCmds    = ".github/scripts/rrc-lane-commands.txt"
	bazelRRCVerifyTool  = "tools/bazel/rrc_verify.py"
	bazelRRCVerifyTest  = "tools/bazel/rrc_verify_test.py"
	bazelSetupBazelUses = "./.github/actions/setup-bazel"
)

// bazelRRCReadLanes: the lanes that read the cache under `on` (bazel-test
// alone under `canary`). Not bazel-release-cross (other target platforms,
// not seeded) nor the package gates.
var bazelRRCReadLanes = []string{
	"bazel-cmd-dolt", "bazel-dolt-int", "bazel-dolt-race", "bazel-doltserver",
	"bazel-embedded", "bazel-integration", "bazel-proxied", "bazel-pure",
	"bazel-server-storage", "bazel-test",
}

// bazelRRCReadLines: the reader's .bazelrc.local lines; both are key neutral
// (a startup option, and loading parallelism).
var bazelRRCReadLines = []string{
	"startup --experimental_remote_repo_contents_cache",
	"common --loading_phase_threads=64",
}

// bazelRRCJobs: bazel.yml's jobs that are not lanes (no result output, never
// gated): the remote repo contents cache's writer and its nightly check.
var bazelRRCJobs = []string{bazelRRCSeedJob, bazelRRCVerifyJob}

// isBazelRRCJob reports whether a bazel.yml job is one of bazelRRCJobs.
func isBazelRRCJob(name string) bool { return slices.Contains(bazelRRCJobs, name) }

// evalRRCIf evaluates a job or step if: with ctx, failing the test on an
// expression the evaluator cannot read.
func evalRRCIf(t *testing.T, expr string, ctx map[string]string) bool {
	t.Helper()
	v, err := evalGHExpr(expr, ctx)
	if err != nil {
		t.Fatalf("evalGHExpr(%q): %v", expr, err)
	}
	return ghTruthy(v)
}

// runRRCScript runs a step's script (or a script file) under GitHub's bash
// flags in dir with env, returning its combined output.
func runRRCScript(t *testing.T, dir, script string, env map[string]string) (string, error) {
	t.Helper()
	cmd := exec.Command("bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// rrcOutputs reads a $GITHUB_OUTPUT file (key=value lines).
func rrcOutputs(t *testing.T, path string) map[string]string {
	t.Helper()
	out := map[string]string{}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return out
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			out[k] = v
		}
	}
	return out
}

func rrcStep(t *testing.T, job ciWorkflowJob, what string, pick func(ciWorkflowStep) bool) ciWorkflowStep {
	t.Helper()
	var found []ciWorkflowStep
	for _, s := range job.Steps {
		if pick(s) {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want one %s step, got %d", what, len(found))
	}
	return found[0]
}

// TestBazelRRCModeStep runs the rbe job's rrc step for every variable value
// and execution mode: only mode remote enables anything, unset and off are
// off, and an unknown value is off with a warning (a typo never fails the
// required gate). It reads no secret and runs before the checkout.

// TestBazelRRCReadSteps: exactly bazelRRCReadLanes read through these steps,
// in mode remote only (fork modes and local never; mode cache reads through
// setup-bazel's rc instead, behind cache-rrc-probe.sh: TestSetupBazelRCWriter),
// bazel-test under canary and every one of them under on; each
// writes exactly bazelRRCReadLines, never mentions uploads, and comes before
// the job's Set up Bazel (so no Bazel server starts without it).

// bazelRRCLaneCommands parses rrc-lane-commands.txt as rrc-run-lanes.sh does.
func bazelRRCLaneCommands(t *testing.T, root string) []string {
	t.Helper()
	var cmds []string
	for _, line := range strings.Split(readPolicyFile(t, root, bazelRRCLaneCmds), "\n") {
		line, _, _ = strings.Cut(line, "#")
		if line = strings.TrimSpace(line); line != "" {
			cmds = append(cmds, line)
		}
	}
	return cmds
}

// bazelRRCEnablingJobs: bazel.yml's jobs that enable the remote repo
// contents cache flag in any mode: a trusted reader step (mode remote) or
// setup-bazel's BAZEL_RRC_READ opt-in (mode cache, write-bazelrc.sh), with
// the steps that do it.
func bazelRRCEnablingJobs(t *testing.T) map[string][]string {
	t.Helper()
	jobs := map[string][]string{}
	for name, job := range readCIWorkflow(t, bazelWorkflowName).Jobs {
		if isBazelRRCJob(name) {
			continue // the writer and its check set the flag themselves
		}
		for _, s := range job.Steps {
			if s.Name == bazelRRCReadStep {
				jobs[name] = append(jobs[name], s.Name)
			}
			if s.Uses == bazelSetupBazelUses && s.Env["BAZEL_RRC_READ"] != "" {
				jobs[name] = append(jobs[name], "setup-bazel BAZEL_RRC_READ="+s.Env["BAZEL_RRC_READ"])
			}
		}
	}
	return jobs
}

// TestBazelRRCOptInMirrorsTheTrustedReaders: setup-bazel's BAZEL_RRC_READ
// (mode cache reads through rbe-cache) is exactly "true" on exactly the
// trusted readers' jobs, bazelRRCReadLanes: not on every mode cache job
// (bazel-release-cross builds for platforms rrc-lane-commands.txt does not
// seed, and the package gates are not lanes).

// TestBazelRRCLaneCommandsCoverEveryReader: rrc-seed seeds (and rrc-verify
// checks) what the readers fetch, so every job that enables the flag
// (bazelRRCEnablingJobs: the trusted reader step or the mode cache opt-in)
// is a reading lane, every --config it passes to `bazel test` or `bazel
// build` has a command in rrc-lane-commands.txt, and every command there is
// a reading lane's.

// TestRRCRunLanes runs rrc-run-lanes.sh with a stub bazel: one --nobuild
// invocation per lane command, in order, startup flags first and command
// flags after; `test --nobuild`'s exit 1 after a successful analysis
// passes, a failed analysis fails, and an empty command list fails.
func TestRRCRunLanes(t *testing.T) {
	// rrc-run-lanes.sh runs only in bazel.yml's rrc-seed and rrc-verify jobs,
	// on Linux runners. It reads its lane list with `mapfile`, a bash 4
	// builtin that macOS's /bin/bash 3.2 lacks (exit 127, "mapfile: command
	// not found"); its owner keeps it Linux-only rather than porting it.
	if runtime.GOOS != "linux" {
		t.Skipf("rrc-run-lanes.sh is Linux-CI-only (bazel.yml rrc-seed/rrc-verify) and uses bash 4's mapfile; GOOS=%s", runtime.GOOS)
	}
	root := sourceRepoRoot(t)
	script := filepath.Join(root, bazelRRCRunLanes)
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(dir, "calls")
	stub := "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>" + calls + "\n" +
		"if [ -n \"${BAZEL_TEST_FAIL:-}\" ]; then echo 'ERROR: no such package'; echo 'ERROR: Build did NOT complete successfully'; exit 1; fi\n" +
		"case \"$*\" in *' build '*) echo 'INFO: Build completed successfully, 0 total actions'; exit 0 ;; esac\n" +
		"echo 'INFO: Build completed successfully, 0 total actions'\necho \"ERROR: Couldn't start the build. Unable to run tests\"\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "bazel"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"PATH":                bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"GITHUB_STEP_SUMMARY": filepath.Join(dir, "summary"),
	}
	run := "bash " + script + " --experimental_remote_repo_contents_cache -- --loading_phase_threads=64 --remote_upload_local_results"
	if out, err := runRRCScript(t, dir, run, env); err != nil {
		t.Fatalf("rrc-run-lanes.sh: %v\n%s", err, out)
	}
	var want []string
	for _, cmd := range bazelRRCLaneCommands(t, root) {
		want = append(want, "--experimental_remote_repo_contents_cache "+cmd+" --nobuild --loading_phase_threads=64 --remote_upload_local_results")
	}
	if got := strings.Split(strings.TrimSpace(readPolicyFile(t, dir, "calls")), "\n"); !slices.Equal(got, want) {
		t.Errorf("bazel calls:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if err := os.Remove(calls); err != nil {
		t.Fatal(err)
	}
	if out, err := runRRCScript(t, dir, "bash "+script+" --", env); err != nil {
		t.Fatalf("without flags: %v\n%s", err, out)
	}
	if got := strings.Split(strings.TrimSpace(readPolicyFile(t, dir, "calls")), "\n"); len(got) == 0 || !strings.HasPrefix(got[0], "test ") || !strings.HasSuffix(got[0], " --nobuild") {
		t.Errorf("without flags, first call %q", got)
	}

	failEnv := map[string]string{"BAZEL_TEST_FAIL": "1"}
	for k, v := range env {
		failEnv[k] = v
	}
	if out, err := runRRCScript(t, dir, run, failEnv); err == nil {
		t.Errorf("a failed analysis passed:\n%s", out)
	}
	empty := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(empty, []byte("# nothing\n\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	emptyEnv := map[string]string{"RRC_LANE_COMMANDS": empty}
	for k, v := range env {
		emptyEnv[k] = v
	}
	if out, err := runRRCScript(t, dir, run, emptyEnv); err == nil {
		t.Errorf("no lane commands passed; it must fail rather than seed nothing:\n%s", out)
	}
	if out, err := runRRCScript(t, dir, "bash "+script+" --flag", env); err == nil {
		t.Errorf("no -- separator passed:\n%s", out)
	}
}

// TestBazelRRCSeedJob: the only writer runs on push to main in mode remote
// with rrc not off, holds the only id-token permission in bazel.yml, reads
// no secret (its credential is the OIDC-minted certificate), sets Bazel up
// in local mode, seeds only with a certificate, uploads to the cache alone
// (no executor), and always removes the key.

// bazelRRCJobText returns a job's raw text in a workflow.
func bazelRRCJobText(t *testing.T, raw, job string) string {
	t.Helper()
	start := strings.Index(raw, "\n  "+job+":\n")
	if start < 0 {
		t.Fatalf("cannot find the %s job's text", job)
	}
	text := raw[start+1:]
	if next := regexp.MustCompile(`\n  [a-z0-9-]+:\n`).FindStringIndex(text); next != nil {
		text = text[:next[0]]
	}
	return text
}

// TestBazelRRCVerifyJob: the nightly check runs from nightly.yml (schedule)
// or a dispatch while the cache is in use, fetches every lane cold without
// the repo contents cache, compares with rrc_verify.py using the lanes'
// read-only certificate and setup-bazel's endpoint (no secret outside
// setup-bazel), and alerts through an issue only when the comparison fails.

// permissionRank orders GitHub token permission levels so a grant can be
// compared with a request: write covers read, read covers none.
var permissionRank = map[string]int{"": 0, "none": 0, "read": 1, "write": 2}

type permWorkflow struct {
	Jobs map[string]struct {
		Uses        string            `yaml:"uses"`
		Permissions map[string]string `yaml:"permissions"`
	} `yaml:"jobs"`
}

// TestBazelWorkflowCallersGrantEveryRequestedPermission guards against
// startup_failure (gascity #7478): GitHub validates a reusable workflow's
// job permissions when the caller starts, including jobs whose `if:` would
// skip, so every workflow that calls bazel.yml must grant at least the
// highest level any bazel.yml job requests for each scope.

// rrcMintCurlStub stands in for curl in rrc-writer-credential.sh. The OIDC
// request (its URL carries audience=) logs the audience and authorization
// and answers a token. A mint request logs its Authorization header and
// answers as the space-separated RBE_TEST_MINT says, one word per request
// (the last repeats): ok (a certificate for the runner's key, CN
// rbe-rrc-writer-beads O=gascity), cn, endpoint, instance, otherkey, nocert
// (each wrong in that one way), 000 (connection refused) or an HTTP status
// with an error body.
const rrcMintCurlStub = `#!/usr/bin/env bash
set -euo pipefail
out= url= headers=()
while [ $# -gt 0 ]; do
	case "$1" in
	-o) out=$2; shift 2 ;;
	-H) headers+=("$2"); shift 2 ;;
	-w | --data | --connect-timeout | --max-time | --retry) shift 2 ;;
	-*) shift ;;
	*) url=$1; shift ;;
	esac
done
case "$url" in
*audience=*)
	echo "oidc audience=${url##*audience=} auth=${headers[0]}" >>"$RBE_TEST_MINT_LOG"
	echo '{"value": "oidc-jwt-value"}'
	exit 0
	;;
esac
echo "mint $url auth=${headers[0]}" >>"$RBE_TEST_MINT_LOG"
n=$(grep -c '^mint ' "$RBE_TEST_MINT_LOG")
read -r -a answers <<<"$RBE_TEST_MINT"
i=$((n - 1)); [ "$i" -lt "${#answers[@]}" ] || i=$((${#answers[@]} - 1))
answer=${answers[$i]}
key="$BAZEL_CI_SECRET_DIR/rrc-writer.key" cn=rbe-rrc-writer-beads endpoint=grpcs://rbe-west.ops.gascity.com:443 instance=oss
case "$answer" in
ok | cn | endpoint | instance | otherkey | nocert) ;;
000) echo "curl: (7) Failed to connect" >&2; exit 7 ;;
*) printf '{"error": "stub %s"}' "$answer" >"$out"; printf '%s' "$answer"; exit 0 ;;
esac
case "$answer" in
cn) cn=rbe-ci ;;
endpoint) endpoint=grpcs://attacker.example:443 ;;
instance) instance=main ;;
otherkey) openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out other.key 2>/dev/null; key=other.key ;;
esac
pem=
if [ "$answer" != nocert ]; then
	pem=$(openssl req -new -x509 -key "$key" -subj "/O=gascity/CN=$cn" -days 1 2>/dev/null)
fi
jq -cn --arg pem "$pem" --arg e "$endpoint" --arg i "$instance" --arg cn "$cn" \
	'{cert_pem: $pem, endpoint: $e, instance: $i, cn: $cn}' >"$out"
printf 200
`

// TestRRCWriterCredential runs rrc-writer-credential.sh against a stubbed
// curl (rrcMintCurlStub): it asks GitHub for an OIDC token with audience
// rbe-rrc-writer, masks it, sends it to the mint, retries only 429, 502 and
// the network, treats 503 or an unreachable mint as seeding off (no
// outputs, exit 0), and accepts only a certificate for the key it generated,
// CN rbe-rrc-writer[-x] O=gascity, for rbe-west's endpoint and instance oss.
func TestRRCWriterCredential(t *testing.T) {
	for _, tool := range []string{"bash", "jq", "openssl"} {
		requireHostTool(t, tool)
	}
	script := filepath.Join(sourceRepoRoot(t), bazelRRCCredential)
	for _, c := range []struct {
		answers string
		ok, off bool
		mints   int
	}{
		{"ok", true, false, 1},
		{"429 ok", true, false, 2},
		{"502 ok", true, false, 2},
		{"000 ok", true, false, 2},
		{"502", false, false, 4},
		{"403", false, false, 1},
		{"401", false, false, 1},
		{"409", false, false, 1},
		{"cn", false, false, 1},
		{"endpoint", false, false, 1},
		{"instance", false, false, 1},
		{"otherkey", false, false, 1},
		{"nocert", false, false, 1},
		{"503", true, true, 1},
		{"000", true, true, 4},
	} {
		dir := t.TempDir()
		bin := filepath.Join(dir, "bin")
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatal(err)
		}
		for name, body := range map[string]string{"curl": rrcMintCurlStub, "sleep": "#!/bin/sh\nexit 0\n"} {
			if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		secret := filepath.Join(t.TempDir(), "secret")
		output := filepath.Join(dir, "output")
		out, err := runRRCScript(t, dir, "bash "+script, map[string]string{
			"PATH":                           bin + string(os.PathListSeparator) + os.Getenv("PATH"),
			"BAZEL_CI_SECRET_DIR":            secret,
			"GITHUB_OUTPUT":                  output,
			"ACTIONS_ID_TOKEN_REQUEST_URL":   "https://token.invalid/oidc?api-version=2.0",
			"ACTIONS_ID_TOKEN_REQUEST_TOKEN": "request-token",
			"RBE_TEST_MINT":                  c.answers,
			"RBE_TEST_MINT_LOG":              filepath.Join(dir, "mint.log"),
		})
		if (err == nil) != c.ok {
			t.Errorf("%q: ok=%v, want %v\n%s", c.answers, err == nil, c.ok, out)
			continue
		}
		log := readPolicyFile(t, dir, "mint.log")
		if !strings.Contains(log, "oidc audience=rbe-rrc-writer auth=Authorization: bearer request-token\n") {
			t.Errorf("%q: OIDC request %q", c.answers, log)
		}
		if got := strings.Count(log, "mint https://rbe-mint.ops.gascity.com:8444/v1/rrc-writer/cert auth=Authorization: Bearer oidc-jwt-value\n"); got != c.mints {
			t.Errorf("%q: %d mint requests with the OIDC token, want %d:\n%s", c.answers, got, c.mints, log)
		}
		if !strings.Contains(out, "::add-mask::oidc-jwt-value") {
			t.Errorf("%q: the OIDC token is not masked:\n%s", c.answers, out)
		}
		if !c.ok {
			continue
		}
		if c.off {
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Errorf("%q: wrote outputs (%v); the seed must skip", c.answers, err)
			}
			if !strings.Contains(out, "seeding off (HTTP "+c.answers[len(c.answers)-3:]+")") {
				t.Errorf("%q: no seeding-off warning:\n%s", c.answers, out)
			}
			continue
		}
		got := rrcOutputs(t, output)
		for name, want := range map[string]string{
			"cert": filepath.Join(secret, "rrc-writer.crt"), "key": filepath.Join(secret, "rrc-writer.key"),
			"endpoint": "grpcs://rbe-west.ops.gascity.com:443", "instance": "oss",
		} {
			if got[name] != want {
				t.Errorf("%q: output %s=%q, want %q", c.answers, name, got[name], want)
			}
		}
		if key := readPolicyFile(t, secret, "rrc-writer.key"); !strings.Contains(key, "-----BEGIN PRIVATE KEY-----") {
			t.Errorf("%q: key is not PKCS#8 (Bazel's TLS refuses SEC1)", c.answers)
		}
		if fi, err := os.Stat(filepath.Join(secret, "rrc-writer.key")); err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("%q: key mode %v (%v), want 0600", c.answers, fi, err)
		}
		if _, err := os.Stat(filepath.Join(secret, "rrc-mint.json")); !os.IsNotExist(err) {
			t.Errorf("%q: the mint reply stays on disk (%v)", c.answers, err)
		}
	}

	dir := t.TempDir()
	if out, err := runRRCScript(t, dir, "bash "+script, map[string]string{
		"BAZEL_CI_SECRET_DIR": filepath.Join(t.TempDir(), "secret"), "GITHUB_OUTPUT": filepath.Join(dir, "output"),
	}); err == nil || !strings.Contains(out, "id-token: write") {
		t.Errorf("without an OIDC request URL: err %v, want a failure naming id-token: write\n%s", err, out)
	}
}

// The writer credential script and rrc_verify.py with its unit test are byte
// copies of gascity's (gascity 78c0e5c6f198306b96fe854c18b4d72f16395ce7,
// #7467), canonical there like tools/bazel/ci_analytics_extract.py: the
// pins catch a local edit that would drift out of lockstep.
var bazelRRCVendored = map[string]string{
	bazelRRCCredential: "021788a724876bd1df7e5c117a4b3e1e60c5b25bb18bca472d61c35a08cb1fac",
	bazelRRCVerifyTool: "00d50e569e2a50d8ef3d2740460de7337b71afd14dc76d2d2442dbd162236f94",
	bazelRRCVerifyTest: "d934f13b466660f3148cf3c8dc7273803b4417f2a52f4edc2dd2f43afb93bfaa",
}

func TestBazelRRCVendoredFromGascity(t *testing.T) {
	root := sourceRepoRoot(t)
	for path, want := range bazelRRCVendored {
		sum := sha256.Sum256([]byte(readPolicyFile(t, root, path)))
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Errorf("%s sha256 = %s, want %s: it drifted from gascity's copy, or gascity changed it and this pin (and the copy) needs updating", path, got, want)
		}
	}
}

// TestRRCVerifyUnitTests runs gascity's rrc_verify_test.py in place: the
// AC key construction pinned against Bazel 9.3.0's own writes, the
// roll-forward of intermediate entries, a poisoned entry, and a GUID for
// .bazelversion's release.
func TestRRCVerifyUnitTests(t *testing.T) {
	root := sourceRepoRoot(t)
	python := requireHostTool(t, "python3")
	cmd := exec.Command(python, filepath.Join(root, bazelRRCVerifyTest), "-v")
	cmd.Dir = filepath.Join(root, "tools", "bazel")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s failed: %v\n%s", bazelRRCVerifyTest, err, out)
	}
}
