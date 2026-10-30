#!/usr/bin/env bash
# Static and executable checks for the Plug CI contract.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
workflow=.github/workflows/plug-bazel.yml
targets=scripts/ci/plug-bazel-targets.sh
target_tests=scripts/ci/plug-bazel-targets-test.sh
collector=scripts/ci/plug-bazel-delivery.py
delivery_tests=scripts/ci/plug-bazel-delivery-test.sh

[[ -f "$workflow" ]] || { echo "config: missing $workflow" >&2; exit 1; }
[[ -x "$targets" ]] || { echo "config: $targets is not executable" >&2; exit 1; }
[[ -x "$target_tests" ]] || { echo "config: $target_tests is not executable" >&2; exit 1; }
[[ -x "$collector" ]] || { echo "config: $collector is not executable" >&2; exit 1; }
[[ -x "$delivery_tests" ]] || { echo "config: $delivery_tests is not executable" >&2; exit 1; }
bash -n "$targets"
bash -n "$target_tests"
bash -n "$delivery_tests"

python3 - "$workflow" "$targets" "$collector" <<'PYTHON'
from pathlib import Path
import ast
import re
import sys

workflow = Path(sys.argv[1]).read_text(encoding="utf-8")
targets = Path(sys.argv[2]).read_text(encoding="utf-8")
collector = Path(sys.argv[3]).read_text(encoding="utf-8")
ast.parse(collector)  # Syntax without writing a cache into the source tree.
# Existing executable flag/selection assertions follow their implementation
# into the collector. Comments alone cannot satisfy these checks.
workflow_code = "\n".join(line for line in workflow.splitlines() if not line.lstrip().startswith("#"))
collector_literals = {node.value for node in ast.walk(ast.parse(collector))
                      if isinstance(node, ast.Constant) and isinstance(node.value, str)}

required_workflow = {
    "name: Plug Bazel",
    "pull_request:",
    "workflow_dispatch:",
    "runs-on: ubuntu-latest",
    "timeout-minutes: 45",
    "permissions:\n  contents: read",
    "actions/cache/restore@",
    "actions/upload-artifact@",
    "PLUG_BAZEL_EVIDENCE_DIR=$RUNNER_TEMP/plug-bazel-evidence",
    "PLUG_BAZEL_TARGETS_DIR=$RUNNER_TEMP/plug-bazel-targets",
    "PLUG_BAZEL_REPOSITORY_CACHE=$RUNNER_TEMP/plug-bazel-repository-cache",
    'export PLUG_BAZEL_EVIDENCE_DIR="$RUNNER_TEMP/plug-bazel-evidence"',
    'export PLUG_BAZEL_TARGETS_DIR="$RUNNER_TEMP/plug-bazel-targets"',
    'export PLUG_BAZEL_REPOSITORY_CACHE="$RUNNER_TEMP/plug-bazel-repository-cache"',
    '>> "$GITHUB_ENV"',
}
missing = sorted(fragment for fragment in required_workflow if fragment not in workflow_code)
if missing:
    raise SystemExit("config: workflow is missing: " + ", ".join(missing))
required_collector = {
    "//:lexicon_gate", "touched-targets.txt", "reverse-targets.txt",
    "--lockfile_mode=error", "--nocache_test_results", "--test_timeout=900",
    "--nosystem_rc", "--nohome_rc", "--noworkspace_rc", "--remote_cache=", "--remote_executor=",
    "--repo_contents_cache=", "--jobs=2", "--local_resources=memory=8192", "--test_env=GO_TEST_WRAP_TESTV=1",
}
if missing := sorted(required_collector - collector_literals):
    raise SystemExit("config: collector is missing executable literals: " + ", ".join(missing))
for phase in ("begin", "run", "finish"):
    if f"python3 scripts/ci/plug-bazel-delivery.py {phase}" not in workflow_code:
        raise SystemExit("config: workflow must invoke delivery phase " + phase)
for fragment in ("persist-credentials: false", "fetch-depth: 0", "${{ github.event.pull_request.head.sha || github.sha }}",
                 "if-no-files-found: error", "retention-days: 14", "--job-status \"$PRIOR_JOB_STATUS\"",
                 "if: github.event_name == 'push' && github.ref == 'refs/heads/main' && success()"):
    if fragment not in workflow_code:
        raise SystemExit("config: workflow lost trust/cache/upload guard: " + fragment)
for name in ("Stop Bazel and finalize delivery bundle", "Upload Plug CI evidence"):
    step = workflow_code.split("- name: " + name, 1)[1].split("- name:", 1)[0]
    if "if: ${{ always() }}" not in step:
        raise SystemExit("config: cleanup and upload must always execute")
if "|| true" in workflow_code or "|| true" in targets or "|| true" in collector:
    raise SystemExit("config: collection/query/shutdown failures must propagate")
actions = re.findall(r"uses: (\S+)", workflow_code)
if len(actions) != 4 or any(not re.fullmatch(r"actions/[a-z/-]+@[0-9a-f]{40}", action) for action in actions):
    raise SystemExit("config: every action must retain its full immutable pin")
if ".plug-bazel-evidence" in workflow or ".plug-bazel-targets" in workflow or ".plug-bazel-repository-cache" in workflow:
    raise SystemExit("config: CI runtime paths must stay outside the checked-out source tree")
for invalid in ("--no_system_rc", "--no_home_rc", "--no_workspace_rc"):
    if invalid in workflow or invalid in targets or invalid in collector:
        raise SystemExit(f"config: invalid Bazel startup option present: {invalid}")

def encoded(values):
    return "".join(chr(value) for value in values)

forbidden = (
    "pull_request_target:",
    "rbe-west",
    "RBE_",
    "RBE-",
    encoded((99, 109, 100)) + "/" + encoded((98, 100)),
    encoded((103, 97, 115)) + encoded((116, 111, 119, 110, 104, 97, 108, 108)),
    encoded((115, 116, 101, 118, 101, 121, 101, 103, 103, 101)) + "/" + encoded((98, 101, 97, 100, 115)),
)
for token in forbidden:
    if token in workflow or token in collector:
        raise SystemExit(f"config: inherited or privileged token present: {token}")

required_targets = ("rdeps(//...", "full_graph", "lockfile_mode=error", "//:lexicon_gate")
missing = sorted(fragment for fragment in required_targets if fragment not in targets)
if missing:
    raise SystemExit("config: target resolver is missing: " + ", ".join(missing))

workflow_files = sorted(Path(".github/workflows").glob("*.yml"))
if [path.name for path in workflow_files] != ["plug-bazel.yml", "plug-runs-on-smoke.yml"]:
    raise SystemExit("config: inherited workflow files remain: " + ", ".join(str(path) for path in workflow_files))
print("plug-ci-config: workflow, permissions, cache, lock, targets, and retirement checks passed")
PYTHON

if "$targets" --help >/dev/null; then :; else
  echo "config: target resolver help failed" >&2
  exit 1
fi
if "$targets" --unknown --output "${TMPDIR:-/tmp}/plug-ci-invalid" >/dev/null 2>&1; then
  echo "config: target resolver accepted an unknown option" >&2
  exit 1
fi

"$target_tests"
"$delivery_tests"

echo "plug-ci-config: shell syntax and fail-closed argument checks passed"
