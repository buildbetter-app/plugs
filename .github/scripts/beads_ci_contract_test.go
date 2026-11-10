package beadsciscripts

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func contractPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs("beads-ci-contract.py")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	return path
}

func write(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
}

func invoke(t *testing.T, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command("python3", append([]string{"-B", contractPath(t)}, args...)...)
	// These are dummy sentinels. The fake Bazel rejects any forwarded secret.
	cmd.Env = append(os.Environ(), "GH_TOKEN=forbidden-test-sentinel", "AWS_SECRET_ACCESS_KEY=forbidden-test-sentinel")
	return cmd.CombinedOutput()
}

func carrier(sha string) string {
	return fmt.Sprintf(`name: Plug Bazel

on:
  pull_request:
    branches: [beads]

permissions:
  contents: read

concurrency:
  group: beads-bazel-${{ github.event.pull_request.number }}
  cancel-in-progress: true

jobs:
  bazel:
    uses: buildbetter-app/plugs/.github/workflows/plug-beads-bazel.yml@%s
    with:
      harness_sha: %s
`, sha, sha)
}

func TestWorkflowSecurityContract(t *testing.T) {
	workflow, err := os.ReadFile(filepath.Join(filepath.Dir(contractPath(t)), "../workflows/plug-beads-bazel.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(workflow)
	cases := []struct{ name, old, replacement string }{
		{"valid", "", ""},
		{"privileged-event", "workflow_call:", "pull_request_target:"},
		{"extra-trigger", "  workflow_call:", "  push:\n  workflow_call:"},
		{"write-permission", "contents: read", "contents: write"},
		{"secret", "jobs:", "env:\n  TOKEN: ${{ secrets.TOKEN }}\njobs:"},
		{"mutable-action", "runs-on/action@13fbb6a39310a0dac9ab4716da2c4e74a3326dfc", "runs-on/action@v2"},
		{"persist-token", "persist-credentials: false", "persist-credentials: true"},
		{"mutable-source", "ref: ${{ github.event.pull_request.head.sha }}", "ref: beads"},
		{"mutable-harness", "ref: ${{ inputs.harness_sha }}", "ref: main"},
		{"fork-runner", "github.event.pull_request.head.repo.full_name == github.repository", "true"},
		{"dependabot-runner", "github.actor != 'dependabot[bot]'", "true"},
		{"runner-class", "runner=bb-x64-16", "runner=other"},
		{"trusted-rc", "--rc \"$RUNNER_TEMP/beads.bazelrc\"", "--rc /untrusted.rc"},
		{"missing-actual-gate", "ci-harness/.github/scripts/beads-ci-contract.py run", "echo skipped"},
		{"missing-event-check", "--event-name \"$GITHUB_EVENT_NAME\"", ""},
		{"missing-evidence", "if-no-files-found: error", "if-no-files-found: warn"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mutated := text
			if tc.old != "" {
				if !strings.Contains(text, tc.old) {
					t.Fatalf("mutation not applied: %q", tc.old)
				}
				mutated = strings.ReplaceAll(text, tc.old, tc.replacement)
			}
			path := filepath.Join(t.TempDir(), "workflow.yml")
			write(t, path, mutated)
			output, err := invoke(t, "check-workflow", path)
			if (err == nil) != (tc.name == "valid") {
				t.Fatalf("unexpected result: %v\n%s", err, output)
			}
		})
	}
}

func TestCarrierCannotExtendPrivilegesOrChangePins(t *testing.T) {
	sha := strings.Repeat("a", 40)
	original := carrier(sha)
	cases := map[string]string{
		"valid":              original,
		"comment":            "# immutable CI registration\n" + original,
		"mutable-pin":        strings.ReplaceAll(original, "@"+sha, "@main"),
		"mismatched-pin":     strings.ReplaceAll(original, "harness_sha: "+sha, "harness_sha: "+strings.Repeat("b", 40)),
		"write":              strings.ReplaceAll(original, "contents: read", "contents: write"),
		"extra-trigger":      strings.ReplaceAll(original, "on:\n", "on:\n  push:\n"),
		"privileged-trigger": strings.ReplaceAll(original, "pull_request:", "pull_request_target:"),
		"secrets":            original + "    secrets: inherit\n",
		"extra-job":          original + "  extra:\n    runs-on: self-hosted\n    steps: []\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "carrier.yml")
			write(t, path, body)
			output, err := invoke(t, "check-carrier", path, "--harness-sha", sha)
			wantPass := name == "valid" || name == "comment"
			if (err == nil) != wantPass {
				t.Fatalf("unexpected result: %v\n%s", err, output)
			}
		})
	}
}

func TestExistingPlugCIContract(t *testing.T) {
	root := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if root == "" {
		t.Fatal("the remote gate must supply PLUG_SOURCE_MODULE_ROOT")
	}
	cmd := exec.Command("bash", "scripts/ci/plug-bazel-config-test.sh")
	cmd.Dir = root
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"TMPDIR=" + t.TempDir(),
		"LANG=C.UTF-8",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"PYTHONDONTWRITEBYTECODE=1",
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("existing Plug CI contract failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "plug-ci-config: shell syntax and fail-closed argument checks passed") {
		t.Fatalf("existing config validation did not finish: %s", output)
	}
	t.Log(string(output))
}

func git(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=CI Fixture", "GIT_AUTHOR_EMAIL=ci@example.invalid", "GIT_COMMITTER_NAME=CI Fixture", "GIT_COMMITTER_EMAIL=ci@example.invalid")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func commitFixture(t *testing.T, root string) string {
	t.Helper()
	git(t, root, "init", "-q")
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "disposable CI fixture")
	return git(t, root, "rev-parse", "HEAD")
}

func TestGateExecutesAndFailsClosed(t *testing.T) {
	modes := []string{"valid", "symlink-xml", "bazel-failure", "whole-failure", "xml-failure", "missing-xml", "zero-cases", "stale-whole", "source-mutated", "harness-mutated", "shutdown-failure", "dirty-source", "dirty-harness", "wrong-event", "wrong-base", "wrong-head", "wrong-module", "wrong-bazel-pin"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			source, harness := filepath.Join(root, "source"), filepath.Join(root, "harness")
			write(t, filepath.Join(harness, "README"), "disposable public CI fixture\n")
			harnessSHA := commitFixture(t, harness)
			write(t, filepath.Join(source, "go.mod"), "module github.com/steveyegge/beads\n")
			write(t, filepath.Join(source, ".bazelversion"), "9.3.0\n")
			write(t, filepath.Join(source, ".gitignore"), "bazel-*\n")
			write(t, filepath.Join(source, ".github/workflows/plug-bazel.yml"), carrier(harnessSHA))
			if mode == "wrong-module" {
				write(t, filepath.Join(source, "go.mod"), "module github.com/buildbetter-app/plugs\n")
			}
			if mode == "wrong-bazel-pin" {
				write(t, filepath.Join(source, ".bazelversion"), "8.0.0\n")
			}
			sha := commitFixture(t, source)
			if mode == "dirty-source" {
				write(t, filepath.Join(source, "untracked"), "dirty\n")
			}
			if mode == "dirty-harness" {
				write(t, filepath.Join(harness, "untracked"), "dirty\n")
			}
			base, eventHead, eventName := "beads", sha, "pull_request"
			if mode == "wrong-event" {
				eventName = "workflow_dispatch"
			}
			if mode == "wrong-base" {
				base = "main"
			}
			if mode == "wrong-head" {
				eventHead = strings.Repeat("0", 40)
			}
			event := fmt.Sprintf(`{"pull_request":{"base":{"ref":%q,"repo":{"full_name":"buildbetter-app/plugs"}},"head":{"sha":%q}}}`, base, eventHead)
			write(t, filepath.Join(root, "event.json"), event)
			write(t, filepath.Join(root, "trusted.rc"), "common --remote_cache=\ncommon --remote_executor=\n")
			fake := filepath.Join(root, "fake-bazel")
			write(t, fake, fmt.Sprintf(`#!/usr/bin/env python3
import json, os, pathlib, sys
mode = %q
harness = pathlib.Path(%q)
args = sys.argv[1:]
assert not any(name in os.environ for name in ('GH_TOKEN', 'GITHUB_TOKEN', 'AWS_SECRET_ACCESS_KEY', 'GITHUB_ENV', 'ACTIONS_RUNTIME_TOKEN'))
assert os.environ['USE_BAZEL_VERSION'] == '9.3.0'
assert 'evidence-runtime/home' in os.environ['HOME']
with open('bazel-fake-calls', 'a') as out:
    out.write(json.dumps(args) + '\n')
if 'shutdown' in args:
    sys.exit(7 if mode == 'shutdown-failure' else 0)
assert 'test' in args
assert '--run_under=//tools/bazel:test_env' in args
assert '--nocache_test_results' in args
assert '--noworkspace_rc' in args
assert '--nosystem_rc' in args
assert '--nohome_rc' in args
assert '--test_env=BEADS_TEST_SKIP=dolt' in args
assert '--@rules_go//go/config:race' in args
whole = '//...' in args
assert whole or all(target in args for target in ('//internal/plugdump:plugdump_test', '//cmd/bd:bd_test', '//scripts:scripts_test', '//internal/storage/dolt:dolt_test', '//tools/bazel:dolt_version_test'))
logs = pathlib.Path('bazel-testlogs/fake')
logs.mkdir(parents=True, exist_ok=True)
if mode != 'stale-whole' or not whole:
    (logs / 'test.log').write_text('fresh disposable test execution\n')
    if mode != 'missing-xml':
        xml = '<testsuites><testsuite><testcase name="executed"/></testsuite></testsuites>'
        if mode == 'zero-cases':
            xml = '<testsuite><testcase name="skipped"><skipped/></testcase></testsuite>'
        if mode == 'xml-failure':
            xml = '<testsuite><testcase name="failed"><failure message="failed"/></testcase></testsuite>'
        if mode == 'symlink-xml':
            target = pathlib.Path(os.environ['TMPDIR']) / 'test.xml'
            target.write_text(xml)
            if not (logs / 'test.xml').is_symlink():
                (logs / 'test.xml').symlink_to(target)
        else:
            (logs / 'test.xml').write_text(xml)
if mode == 'source-mutated':
    pathlib.Path('go.mod').write_text('changed source\n')
if mode == 'harness-mutated':
    (harness / 'README').write_text('changed harness\n')
sys.exit(3 if mode == 'bazel-failure' or mode == 'whole-failure' and whole else 0)
`, mode, harness))
			if err := os.Chmod(fake, 0755); err != nil {
				t.Fatal(err)
			}
			evidence := filepath.Join(root, "evidence")
			output, err := invoke(t, "run", "--source", source, "--harness", harness, "--harness-sha", harnessSHA,
				"--evidence", evidence, "--expected-sha", sha, "--event", filepath.Join(root, "event.json"),
				"--event-name", eventName, "--repository", "buildbetter-app/plugs", "--bazel", fake, "--rc", filepath.Join(root, "trusted.rc"))
			pass := mode == "valid" || mode == "symlink-xml"
			if (err == nil) != pass {
				t.Fatalf("unexpected gate result: %v\n%s", err, output)
			}
			resultBytes, readErr := os.ReadFile(filepath.Join(evidence, "result.json"))
			if readErr != nil {
				t.Fatalf("failure must retain result evidence: %v\n%s", readErr, output)
			}
			var result struct {
				Success      bool   `json:"success"`
				SHA          string `json:"sha"`
				Tree         string `json:"tree"`
				SourceClean  bool   `json:"source_clean_after"`
				HarnessClean bool   `json:"harness_clean_after"`
				Phases       map[string]struct {
					Cases int `json:"executed_cases"`
				} `json:"phases"`
			}
			if err := json.Unmarshal(resultBytes, &result); err != nil {
				t.Fatal(err)
			}
			if result.Success != pass || result.SHA != sha {
				t.Fatalf("incorrect result: %s", resultBytes)
			}
			if !pass {
				exit, err := os.ReadFile(filepath.Join(evidence, "exit_code"))
				if err != nil || strings.TrimSpace(string(exit)) == "0" {
					t.Fatalf("failure must retain nonzero exit: %s, %v", exit, err)
				}
				return
			}
			if result.Tree != git(t, source, "rev-parse", "HEAD^{tree}") || !result.SourceClean || !result.HarnessClean || len(result.Phases) != 2 {
				t.Fatalf("missing provenance: %s", resultBytes)
			}
			calls, err := os.ReadFile(filepath.Join(source, "bazel-fake-calls"))
			if err != nil || len(strings.Split(strings.TrimSpace(string(calls)), "\n")) != 3 {
				t.Fatalf("must execute focused, whole and shutdown: %s, %v", calls, err)
			}
			for _, phase := range []string{"focused", "whole"} {
				if result.Phases[phase].Cases < 1 {
					t.Fatalf("%s did not execute a testcase", phase)
				}
				assertArchiveHasXML(t, filepath.Join(evidence, phase, "testlogs.tgz"))
			}
		})
	}
}

func assertArchiveHasXML(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	archive := tar.NewReader(gz)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			t.Fatal("archive lacks actual XML bytes")
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(header.Name, "test.xml") {
			body, err := io.ReadAll(archive)
			if err != nil || header.Typeflag != tar.TypeReg || !strings.Contains(string(body), "testcase") {
				t.Fatalf("archive must contain XML bytes, not a dangling symlink: %v %q", header, body)
			}
			return
		}
	}
}
