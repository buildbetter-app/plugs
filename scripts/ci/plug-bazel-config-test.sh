#!/usr/bin/env bash
# Static and executable checks for the Plug CI contract.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
workflow=.github/workflows/plug-bazel.yml
targets=scripts/ci/plug-bazel-targets.sh
target_tests=scripts/ci/plug-bazel-targets-test.sh

[[ -f "$workflow" ]] || { echo "config: missing $workflow" >&2; exit 1; }
[[ -x "$targets" ]] || { echo "config: $targets is not executable" >&2; exit 1; }
[[ -x "$target_tests" ]] || { echo "config: $target_tests is not executable" >&2; exit 1; }
bash -n "$targets"
bash -n "$target_tests"

python3 - "$workflow" "$targets" <<'PYTHON'
from pathlib import Path
import sys

workflow = Path(sys.argv[1]).read_text(encoding="utf-8")
targets = Path(sys.argv[2]).read_text(encoding="utf-8")

required_workflow = {
    "name: Plug Bazel",
    "pull_request:",
    "workflow_dispatch:",
    "runs-on: ubuntu-latest",
    "timeout-minutes: 45",
    "permissions:\n  contents: read",
    "actions/cache/restore@",
    "actions/upload-artifact@",
    "//:lexicon_gate",
    "touched-targets.txt",
    "reverse-targets.txt",
    "--lockfile_mode=error",
    "--nocache_test_results",
    "--test_timeout=900",
    "--remote_cache=",
    "--remote_executor=",
    "PLUG_BAZEL_EVIDENCE_DIR=$RUNNER_TEMP/plug-bazel-evidence",
    "PLUG_BAZEL_TARGETS_DIR=$RUNNER_TEMP/plug-bazel-targets",
    "PLUG_BAZEL_REPOSITORY_CACHE=$RUNNER_TEMP/plug-bazel-repository-cache",
    'export PLUG_BAZEL_EVIDENCE_DIR="$RUNNER_TEMP/plug-bazel-evidence"',
    'export PLUG_BAZEL_TARGETS_DIR="$RUNNER_TEMP/plug-bazel-targets"',
    'export PLUG_BAZEL_REPOSITORY_CACHE="$RUNNER_TEMP/plug-bazel-repository-cache"',
    '>> "$GITHUB_ENV"',
}
missing = sorted(fragment for fragment in required_workflow if fragment not in workflow)
if missing:
    raise SystemExit("config: workflow is missing: " + ", ".join(missing))
if ".plug-bazel-evidence" in workflow or ".plug-bazel-targets" in workflow or ".plug-bazel-repository-cache" in workflow:
    raise SystemExit("config: CI runtime paths must stay outside the checked-out source tree")

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
    if token in workflow:
        raise SystemExit(f"config: inherited or privileged token present: {token}")

required_targets = ("rdeps(//...", "full_graph", "lockfile_mode=error", "//:lexicon_gate")
missing = sorted(fragment for fragment in required_targets if fragment not in targets)
if missing:
    raise SystemExit("config: target resolver is missing: " + ", ".join(missing))

workflow_files = sorted(Path(".github/workflows").glob("*.yml"))
if [path.name for path in workflow_files] != ["plug-bazel.yml"]:
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

echo "plug-ci-config: shell syntax and fail-closed argument checks passed"
