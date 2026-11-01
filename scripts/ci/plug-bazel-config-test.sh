#!/usr/bin/env bash
# Static and executable checks for the Plug CI contract.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
workflow=.github/workflows/plug-bazel.yml
nightly_workflow=.github/workflows/plug-nightly-restore-diffs.yml
public_workflow=.github/workflows/plug-public-bundle.yml
remote_workflow=.github/workflows/plug-remote-bazel.yml
targets=scripts/ci/plug-bazel-targets.sh
target_tests=scripts/ci/plug-bazel-targets-test.sh
collector=scripts/ci/plug-bazel-delivery.py
delivery_tests=scripts/ci/plug-bazel-delivery-test.sh

[[ -f "$workflow" ]] || { echo "config: missing $workflow" >&2; exit 1; }
[[ -f "$nightly_workflow" ]] || { echo "config: missing $nightly_workflow" >&2; exit 1; }
[[ -f "$remote_workflow" ]] || { echo "config: missing $remote_workflow" >&2; exit 1; }
[[ -x "$targets" ]] || { echo "config: $targets is not executable" >&2; exit 1; }
[[ -x "$target_tests" ]] || { echo "config: $target_tests is not executable" >&2; exit 1; }
[[ -x "$collector" ]] || { echo "config: $collector is not executable" >&2; exit 1; }
[[ -x "$delivery_tests" ]] || { echo "config: $delivery_tests is not executable" >&2; exit 1; }
bash -n "$targets"
bash -n "$target_tests"
bash -n "$delivery_tests"

python3 - "$workflow" "$targets" "$collector" "$remote_workflow" "$nightly_workflow" "$public_workflow" <<'PYTHON'
from pathlib import Path
import ast
import re
import sys

workflow = Path(sys.argv[1]).read_text(encoding="utf-8")
remote_workflow = Path(sys.argv[4]).read_text(encoding="utf-8")
nightly_workflow = Path(sys.argv[5]).read_text(encoding="utf-8")
public_path = Path(sys.argv[6])
public_workflow = public_path.read_text(encoding="utf-8") if public_path.exists() else None
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
    "format('runs-on={0}/runner=bb-x64-16', github.run_id)",
    "runs-on/action@",
    "RUNSON_TRUSTED",
    "actions/setup-node@",
    "node-version: 24",
    "command -v google-chrome",
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
if len(actions) != 6 or any(not re.fullmatch(r"(?:actions|runs-on)/[a-z/-]+@[0-9a-f]{40}", action) for action in actions):
    raise SystemExit("config: every Plug Bazel action must retain its full immutable pin")
if ".plug-bazel-evidence" in workflow or ".plug-bazel-targets" in workflow or ".plug-bazel-repository-cache" in workflow:
    raise SystemExit("config: CI runtime paths must stay outside the checked-out source tree")
for invalid in ("--no_system_rc", "--no_home_rc", "--no_workspace_rc"):
    if invalid in workflow or invalid in targets or invalid in collector:
        raise SystemExit(f"config: invalid Bazel startup option present: {invalid}")

def encoded(values):
    return "".join(chr(value) for value in values)

def without_comments(text):
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))

def require_fragments(label, code, fragments):
    missing = sorted(fragment for fragment in fragments if fragment not in code)
    if missing:
        raise SystemExit(f"config: {label} is missing: " + ", ".join(missing))

def yaml_blocks(code, key):
    """Return (indent, inline_value, child_lines) for each mapping named key."""
    lines = code.splitlines()
    blocks = []
    for index, line in enumerate(lines):
        match = re.match(rf"^( *)(?:{re.escape(key)}):(?:\s*(.*))?$", line)
        if not match:
            continue
        indent = len(match.group(1))
        children = []
        for child in lines[index + 1:]:
            if child.strip():
                child_indent = len(child) - len(child.lstrip(" "))
                if child_indent <= indent:
                    break
            children.append(child)
        blocks.append((indent, (match.group(2) or "").strip(), children))
    return blocks

def immediate_keys(indent, children):
    keys = []
    for line in children:
        if not line.strip():
            continue
        if len(line) - len(line.lstrip(" ")) != indent + 2:
            continue
        match = re.match(r"^\s{2,}([A-Za-z0-9_-]+):(?:\s*(.*))?$", line)
        if match:
            keys.append((match.group(1), (match.group(2) or "").strip()))
    return keys

def check_dispatch_workflow(label, code, fragments, action_count, expected_triggers):
    """Check a workflow that may execute only trusted scheduled or dispatched code."""
    require_fragments(label, code, fragments)
    trigger_blocks = [block for block in yaml_blocks(code, "on") if block[0] == 0]
    if len(trigger_blocks) != 1 or trigger_blocks[0][1]:
        raise SystemExit(f"config: {label} must declare triggers as a mapping")
    trigger_keys = immediate_keys(trigger_blocks[0][0], trigger_blocks[0][2])
    if len(trigger_keys) != len({key for key, _ in trigger_keys}) or {
        key for key, _ in trigger_keys
    } != set(expected_triggers):
        raise SystemExit(f"config: {label} must be schedule/workflow_dispatch only")
    permission_blocks = yaml_blocks(code, "permissions")
    top_level_permissions = []
    for indent, inline_value, children in permission_blocks:
        if inline_value and inline_value != "{}":
            raise SystemExit(f"config: {label} permissions must be an explicit read/none mapping")
        entries = immediate_keys(indent, children)
        if len(entries) != len({key for key, _ in entries}):
            raise SystemExit(f"config: {label} contains duplicate permission keys")
        for key, value in entries:
            value = value.split(" #", 1)[0].strip()
            if value not in {"read", "none"}:
                raise SystemExit(f"config: {label} grants non-read permission {key}: {value}")
        if indent == 0:
            top_level_permissions.extend(entries)
    if dict(top_level_permissions).get("contents") != "read":
        raise SystemExit(f"config: {label} must grant top-level contents: read")
    if re.search(r"\bsecrets\b|github\.event\.(?:pull_request|issue|discussion)", code):
        raise SystemExit(f"config: {label} exposes a secret or untrusted event context")
    actions = re.findall(r"(?m)^\s*uses:\s*(\S+)", code)
    if len(actions) != action_count or any(
        not re.fullmatch(r"(?:actions|runs-on)/[a-z/-]+@[0-9a-f]{40}", action)
        for action in actions
    ):
        raise SystemExit(f"config: every {label} action must retain its full immutable pin")
    # The expression contains spaces; compare the complete mapping line after
    # normalizing whitespace instead of trusting a substring in a run script.
    runson_lines = [re.sub(r"\s+", " ", line.strip()) for line in code.splitlines()
                    if re.match(r"^\s*runs-on:\s*", line)]
    if runson_lines != ["runs-on: runs-on=${{ github.run_id }}/runner=bb-x64-16"]:
        raise SystemExit(f"config: {label} must use the pinned RunsOn runner")

nightly_code = without_comments(nightly_workflow)
check_dispatch_workflow(
    "nightly restore-diffs workflow",
    nightly_code,
    (
        "name: Plug Nightly Restore Diffs", "schedule:", "workflow_dispatch:",
        "permissions:\n  contents: read",
        "runs-on: runs-on=${{ github.run_id }}/runner=bb-x64-16",
        "timeout-minutes: 45", "persist-credentials: false", "fetch-depth: 1",
        "actions/cache/restore@", "actions/upload-artifact@",
        "if-no-files-found: error", "retention-days: 30",
    ),
    4,
    ("schedule", "workflow_dispatch"),
)

if public_workflow is not None:
    public_code = without_comments(public_workflow)
    check_dispatch_workflow(
        "public bundle workflow",
        public_code,
        (
            "name: Plug public executable bundle", "workflow_dispatch:", "source_ref:",
            "permissions:\n  contents: read",
            "runs-on: runs-on=${{ github.run_id }}/runner=bb-x64-16",
            "timeout-minutes: 60", "persist-credentials: false", "fetch-depth: 0",
            "actions/upload-artifact@", "actions/download-artifact@",
            "if-no-files-found: error", "retention-days: 14",
            "public-bundle.py create", "public-bundle.py verify",
            "git fetch --no-tags origin refs/heads/beads",
            '[[ "$PLUG_COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]',
            '[[ "$PLUG_SOURCE_REF" =~ ^[A-Za-z0-9._/-]+$ ]]',
            '[[ "$BUNDLE_LABEL" =~ ^[A-Za-z0-9._-]{1,48}$ ]]',
            '[[ "$PLUG_TARGET" =~ ^//[A-Za-z0-9_./:-]+$ ]]',
            "BEADS_COMMIT: 9d6fff760b7a314b476539198504d77ce0959656",
        ),
        4,
        ("workflow_dispatch",),
    )

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

remote_code = without_comments(remote_workflow)
if "workflow_dispatch:" not in remote_code or "  push:" in remote_code or "  pull_request:" in remote_code:
    raise SystemExit("config: remote Bazel workflow must be dispatch-only")
for fragment in (
    "run-name: remote-bazel ${{ inputs.label }} ${{ inputs.sha }}",
    "sha:", "label:", "args:", "type: string",
    "ref: ${{ inputs.sha }}", "fetch-depth: 1", "persist-credentials: false",
    "runs-on=${{ github.run_id }}/runner=bb-x64-16", "timeout-minutes: 180",
    "actions/cache/restore@", "actions/setup-node@", "node-version: 24",
    "command -v google-chrome", "read -ra USER_ARGS", "bazel \"${ARGS[@]}\"",
    "--ignore_all_rc_files", "--output_user_root", "--bazelrc",
    "remote-bazel-${{ inputs.label }}", "bazel.log", "exit_code",
    "toolchain.txt", "bazel-testlogs.tgz", "if: always()",
):
    if fragment not in remote_code:
        raise SystemExit("config: remote workflow is missing: " + fragment)
if "eval " in remote_code or "set -x" in remote_code or "pull_request_target:" in remote_code:
    raise SystemExit("config: remote workflow contains an unsafe command or trigger")
remote_actions = re.findall(r"uses: (\S+)", remote_code)
if len(remote_actions) != 5 or any(not re.fullmatch(r"(?:actions|runs-on)/[a-z/-]+@[0-9a-f]{40}", action) for action in remote_actions):
    raise SystemExit("config: every remote Bazel action must retain its full immutable pin")
workflow_files = sorted(
    path.name
    for pattern in ("*.yml", "*.yaml")
    for path in Path(".github/workflows").glob(pattern)
)
required_workflows = {
    "plug-bazel.yml", "plug-nightly-restore-diffs.yml", "plug-remote-bazel.yml", "plug-runs-on-smoke.yml",
}
allowed_workflows = required_workflows | {"plug-public-bundle.yml"}
missing = sorted(required_workflows - set(workflow_files))
unexpected = sorted(set(workflow_files) - allowed_workflows)
if missing:
    raise SystemExit("config: required workflow files are missing: " + ", ".join(missing))
if unexpected:
    raise SystemExit("config: inherited workflow files remain: " + ", ".join(unexpected))
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
